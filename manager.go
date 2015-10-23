package main

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"os"
)

type Manager struct {
	Workers     []*OffsetWorker
	PusherGeter *PusherGeter
	Zookeepers  []string
	ZkCluster   string

	Logger_file   string
	Monitor_file  string
	Logger_switch int

	Distance int
	Passby   []Clusterlist
}

func NewManager(monitor string, logger string, switcher int) *Manager {
	return &Manager{Monitor_file: monitor, Logger_file: logger, Logger_switch: switcher}
}

func (this *Manager) Init(config *Config) error {
	this.Zookeepers = config.Zookeepers
	this.ZkCluster = config.ZkCluster

	this.Distance = config.Distance
	this.Passby = config.Passby

	for _, zookeeper := range this.Zookeepers {
		worker := NewOffsetWorker(zookeeper, this.ZkCluster)
		err := worker.Init()
		if err != nil {
			return err
		}
		this.Workers = append(this.Workers, worker)
	}

	this.PusherGeter = NewPusherGeter(config.Url)
	return nil
}

func (this *Manager) Work() error {
	// kafka get data from brokerList
	host, err := os.Hostname()

	// get data from pusher
	var msg *RetMsg
	if msg, err = this.PusherGeter.RemoteGet(); err != nil {
		return err
	}

	// pass_by
	passBy := map[string]map[string]string{}
	for _, value := range this.Passby {
		item := map[string]string{}
		item[value.ConsumerGroup] = value.Topic
		passBy[value.Cluster] = item
	}

	defer this.Close()

	var data []LogData
	for _, worker := range this.Workers {
		kafkaOffset, err := worker.GetLastOffset()
		if nil != err {
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

		newResp := []ClientResp{}
		for _, v := range msg.Data {
			// pass_by filter
			temp, ok := passBy[this.ZkCluster][getGroupNameByUrl(v.Url)]
			if ok && temp == v.Topic {
				continue
			}
			value, ok := kafkaOffset[v.Topic][fmt.Sprintf("%d", v.Partition)]
			if ok {
				v.Offset = value - v.Offset
				newResp = append(newResp, ClientResp{
					Topic:     v.Topic,
					Partition: v.Partition,
					Offset:    v.Offset, // already distance
					Url:       v.Url,
				})
			}
		}

		if this.Logger_switch == 1 {
			msgLog := []string{}
			for _, v := range newResp {
				s := fmt.Sprintf("[Distance Data] topic:%s cg:%s url:%s partition:%d distance:%d", v.Topic, getGroupNameByUrl(v.Url), v.Url, v.Partition, v.Offset)
				msgLog = append(msgLog, s)
			}
			logger := NewFileLogger(this.Logger_file, msgLog)
			logger.RecordLogger()
		}
		//change []slice => map
		pusherDataMap := map[string]map[string]map[int32]int64{}
		for _, group := range newResp {
			g, ok := pusherDataMap[group.Url]
			if !ok {
				g = make(map[string]map[int32]int64)
				pusherDataMap[group.Url] = g
			}
			t, ok := g[group.Topic]
			if !ok {
				t = make(map[int32]int64)
				g[group.Topic] = t
			}
			t[group.Partition] = group.Offset
		}

		distanceData := map[string]map[string]int64{}
		for cg, cgData := range pusherDataMap {
			cgItem := map[string]int64{}
			for topic, topicData := range cgData {
				for _, offset := range topicData {
					cgItem[topic] += offset
				}
			}
			distanceData[cg] = cgItem
		}

		for cg, cgData := range distanceData {
			for topic, distance := range cgData {
				data = append(data, LogData{
					Host:          host,
					Zabbix_key:    ZABBIX_KEY_DISTANCE,
					Cluster:       this.ZkCluster,
					ConsumerGroup: getGroupNameByUrl(cg),
					Url:           cg,
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

func (this *Manager) Close() {
	for _, worker := range this.Workers {
		worker.Close()
	}
}

func getGroupNameByUrl(url string) string {
	m := md5.New()
	m.Write([]byte(url))
	s := hex.EncodeToString(m.Sum(nil))
	return s
}
