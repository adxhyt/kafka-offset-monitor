package main

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"time"
)

var (
	configFile, filename, logger_file string
	ZABBIX_KEY_LASTEST_OFFSET         = "latest_offset"
	ZABBIX_KEY_DISTANCE               = "distance"
	INT64_MAX                         = 9223372036854775807
)

type ClientResp struct {
	Topic     string
	Partition int32
	Offset    int64
	Url       string
}

type RetMsg struct {
	Data   []*ClientResp
	Errmsg string
	Errno  int
}

type LogData struct {
	Host          string
	Zabbix_key    string
	Cluster       string
	ConsumerGroup string
	Url           string
	Topic         string
	Threshold     int
	Distance      int64
}

func init() {
	flag.StringVar(&configFile, "c", "config.json", "the config file")
	flag.StringVar(&filename, "f", "/home/work/kafka-monitor/log/kafka_offset_monitor", "the log path")
	flag.StringVar(&logger_file, "l", "/home/work/kafka-monitor/log/kafka_offset_logger", "the runtime logger path")
}

func main() {
	flag.Parse()

	config, err := loadConfig(configFile)

	if err != nil {
		log.Fatalf("[OFFSET MON]Load Config err: %s", err)
	}

	t := time.NewTicker(time.Duration(config.Interval) * time.Second)

	for {
		select {
		case <-t.C:

			go func() {
				r := rand.Intn(config.Interval)
				time.Sleep(time.Duration(r))

				var logContents []LogData
				for _, zookeeper := range config.Zookeepers {
					data := run(config, zookeeper)
					logContents = append(logContents, data...)
				}

				writer := NewFileWriter(filename, logContents)
				writer.WriteToFile()
			}()

		}
	}
}

func run(config *Config, zookeeper string) (data []LogData) {
	// kafka get data from brokerList
	worker := NewOffsetWorker(zookeeper, config.ZkCluster)
	kafkaOffset, err := worker.GetLastOffset()
	if nil != err {
		return nil
	}
	host, err := os.Hostname()

	topicKeyList := []string{}
	for topicKey, v := range kafkaOffset {
		topicKeyList = append(topicKeyList, topicKey)
		data = append(data, LogData{
			Host:          host,
			Zabbix_key:    ZABBIX_KEY_LASTEST_OFFSET,
			Cluster:       config.ZkCluster,
			ConsumerGroup: "na",
			Url:           "na",
			Topic:         topicKey,
			Threshold:     INT64_MAX,
			Distance:      v["total"],
		})
	}

	// get data from pusher
	var msg *RetMsg
	if msg, err = RemoteGet(config.Url); err != nil {
		return nil
	}

	// pass_by
	passBy := map[string]map[string]string{}
	for _, value := range config.Passby {
		item := map[string]string{}
		item[value.ConsumerGroup] = value.Topic
		passBy[value.Cluster] = item
	}

	newResp := []ClientResp{}
	for _, v := range msg.Data {
		// pass_by filter
		temp, ok := passBy[config.ZkCluster][v.getGroupName()]
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

	msgLog := []string{}
	for _, v := range newResp {
		s := fmt.Sprintf("[Distance Data] topic:%s cg:%s url:%s partition:%d distance:%d", v.Topic, getGroupNameByUrl(v.Url), v.Url, v.Partition, v.Offset)
		msgLog = append(msgLog, s)
	}
	logger := NewFileLogger(logger_file, msgLog)
	logger.RecordLogger()

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
				Cluster:       config.ZkCluster,
				ConsumerGroup: getGroupNameByUrl(cg),
				Url:           cg,
				Topic:         topic,
				Threshold:     config.Distance,
				Distance:      distance,
			})
		}
	}
	return data
}

func RemoteGet(url string) (msg *RetMsg, err error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal([]byte(body), &msg)

	//msgLog := []string{}
	//s := fmt.Sprintf("[Pusher Data] %s", string(body))
	//msgLog = append(msgLog, s)
	//logger := NewFileLogger(logger_file, msgLog)
	//logger.RecordLogger()

	if err != nil {
		return nil, err
	}
	return msg, err
}

func (this *ClientResp) getGroupName() string {
	m := md5.New()
	m.Write([]byte(this.Url))
	s := hex.EncodeToString(m.Sum(nil))
	return s
}

func getGroupNameByUrl(url string) string {
	m := md5.New()
	m.Write([]byte(url))
	s := hex.EncodeToString(m.Sum(nil))
	return s
}
