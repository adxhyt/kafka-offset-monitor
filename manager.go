package main

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"os"
)

type Manager struct {
	Worker       *OffsetWorker
	PusherGeters []*PusherGeter
	Zookeepers   []string
	ZkCluster    string

	Logger_file   string
	Monitor_file  string
	Err_file      string
	Logger_switch int

	Distance int
	Passby   []Clusterlist
}

func NewManager(monitor string, logger string, switcher int, errfile string) *Manager {
	return &Manager{Monitor_file: monitor, Logger_file: logger, Logger_switch: switcher, Err_file: errfile}
}

func (this *Manager) Init(config *Config) error {
	this.Zookeepers = config.Zookeepers
	this.ZkCluster = config.ZkCluster

	this.Distance = config.Distance
	this.Passby = config.Passby

	this.Worker = NewOffsetWorker(this.Zookeepers[0], this.ZkCluster, this.Err_file)
	err := this.Worker.Init()
	if err != nil {
		AddLogger(this.Err_file, "[Distance Err MAN_INIT_ERR]", err)
		return err
	}

	for _, urlList := range config.Urls {
		pusher := NewPusherGeter(urlList, this.Err_file)
		this.PusherGeters = append(this.PusherGeters, pusher)
	}
	return nil
}

func (this *Manager) Work() error {
	// kafka get data from brokerList
	host, err := os.Hostname()

	pusherData, medianVector, err := this.AssemblePusherData()

	// pass_by
	passBy := map[string]map[string]string{}
	for _, value := range this.Passby {
		item := map[string]string{}
		item[value.ConsumerGroup] = value.Topic
		passBy[value.Cluster] = item
	}

	var data []LogData
	kafkaOffset, err := this.Worker.GetLastOffset()
	if nil != err {
		AddLogger(this.Err_file, "[Distance Err MAN_WORKER_ERR]", err)
		return err
	}

	topicKeyList := []string{}
	for topicKey, v := range kafkaOffset {
		topicKeyList = append(topicKeyList, topicKey)
		data = append(data, LogData{
			Host:          host,
			Zabbix_key:    ZABBIX_KEY_LASTEST_OFFSET,
			Cluster:       this.ZkCluster,
			ConsumerGroup: "na",
			Url:           "na",
			Topic:         topicKey,
			Threshold:     INT64_MAX,
			Distance:      v["total"],
		})
	}

	if pusherData != nil {
		msgLog := []string{}
		distanceData := map[string]map[string]int64{}
		for consumergroup, group := range pusherData {
			cgItem := map[string]int64{}
			for topic, topicData := range group {
				// pass_by filter
				passbytopic, ok := passBy[this.ZkCluster][getGroupNameByUrl(consumergroup)]
				if ok && passbytopic == topic {
					continue
				}
				for partition, offset := range topicData {
					value, ok := kafkaOffset[topic][partition]
					if ok && offset >= 0 {
						temp := value - offset
						cgItem[topic] += temp

						median, exist := medianVector[consumergroup][topic]
						if exist {
							s := fmt.Sprintf("[Distance Data] topic:%v cg:%v url:%v partition:%v distance:%d offset:%d median:%d", topic, getGroupNameByUrl(consumergroup), consumergroup, partition, temp, offset, median)
						} else {
							s := fmt.Sprintf("[Distance Data] topic:%v cg:%v url:%v partition:%v distance:%d offset:%d", topic, getGroupNameByUrl(consumergroup), consumergroup, partition, temp, offset)
						}
						msgLog = append(msgLog, s)
					}
				}
			}
			distanceData[consumergroup] = cgItem
		}

		if this.Logger_switch == 1 {
			logger := NewFileLogger(this.Logger_file, msgLog)
			logger.RecordLogger()
		}

		for consumergroup, cgData := range distanceData {
			for topic, distance := range cgData {
				data = append(data, LogData{
					Host:          host,
					Zabbix_key:    ZABBIX_KEY_DISTANCE,
					Cluster:       this.ZkCluster,
					ConsumerGroup: getGroupNameByUrl(consumergroup),
					Url:           consumergroup,
					Topic:         topic,
					Threshold:     this.Distance,
					Distance:      distance,
				})
			}
		}
	}
	writer := NewFileWriter(this.Monitor_file, data)
	writer.WriteToFile()

	return nil
}

func (this *Manager) AssemblePusherData() (pusherDataMap map[string]map[string]map[string]int64, medianVector map[string]map[string]int64, err error) {
	var dataList, data []*ClientResp
	for _, pusher := range this.PusherGeters {
		if data, err = pusher.RemoteGet(); err != nil {
			return nil, nil, err
		}
		dataList = append(dataList, data...)
	}

	pusherDataMap = map[string]map[string]map[string]int64{}
	for _, item := range dataList {
		g, ok := pusherDataMap[item.Url]
		if !ok {
			g = map[string]map[string]int64{}
			pusherDataMap[item.Url] = g
		}
		t, ok := g[item.Topic]
		if !ok {
			t = map[string]int64{}
			g[item.Topic] = t
		}
		value, ok := t[fmt.Sprintf("%d", item.Partition)]
		if !ok {
			t[fmt.Sprintf("%d", item.Partition)] = item.Offset
		} else {
			if item.Offset > value {
				t[fmt.Sprintf("%d", item.Partition)] = item.Offset
			}
		}
	}

	medianVector = map[string]map[string]int64{}
	for cg, cgData := range pusherDataMap {
		dataSlice := map[string]int64{}
		for topic, topicData := range cgData {
			dataMap := []int64{}
			for _, offset := range topicData {
				dataMap = append(dataMap, offset)
			}
			dataSlice[topic] = Median(dataMap)
		}
		medianVector[cg] = dataSlice
	}

	return pusherDataMap, medianVector, nil
}

func (this *Manager) Close() {
	this.Worker.Close()
}

func getGroupNameByUrl(url string) string {
	m := md5.New()
	m.Write([]byte(url))
	s := hex.EncodeToString(m.Sum(nil))
	return s
}

func AddLogger(logger_path string, err_type string, err error) {
	msgLog := []string{}
	s := fmt.Sprintf("%v, %v", err_type, err)
	msgLog = append(msgLog, s)
	logger := NewFileLogger(logger_path, msgLog)
	logger.RecordLogger()
}
