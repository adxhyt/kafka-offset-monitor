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
	configFile, filename      string
	ZABBIX_KEY_LASTEST_OFFSET = "latest_offset"
	ZABBIX_KEY_DISTANCE       = "distance"
	INT64_MAX                 = 9223372036854775807
)

type ClientResp struct {
	Topic     string
	Partition int32
	Offset    int64
	Url       string
	UUID      string
	Name      string
}

type RetMsg struct {
	Data   []*ClientResp
	Errmsg string
	Errno  int
}

type Topic struct {
	TopicName     string
	ConsumerGroup string
	PartitionData map[int32]int64
}

type LogData struct {
	Host          string
	Zabbix_key    string
	Cluster       string
	ConsumerGroup string
	Topic         string
	Threshold     int
	Distance      int64
}

func init() {
	flag.StringVar(&configFile, "c", "config.json", "the config file")
	flag.StringVar(&filename, "f", "/home/work/kafka-monitor/log/kafka_offset_monitor", "the log path")
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

	for topicKey, v := range kafkaOffset {
		data = append(data, LogData{
			Host:          host,
			Zabbix_key:    ZABBIX_KEY_LASTEST_OFFSET,
			Cluster:       config.ZkCluster,
			ConsumerGroup: "na",
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

	topicList := make(map[string]Topic)
	topicItem := map[int32]int64{}
	for _, v := range msg.Data {
		topicItem[v.Partition] = v.Offset
		topic := Topic{v.Topic, v.getGroupName(), topicItem}
		topicList[v.Topic] = topic
	}

	// compute each topic total_distance
	topicDistance := map[string]int64{}
	for topicKey, value := range topicList {
		for partitionKey, offset := range value.PartitionData {
			topicDistance[topicKey] += kafkaOffset[topicKey][fmt.Sprintf("%d", partitionKey)] - offset
		}
	}

	rtn := map[string]map[string]string{}
	for _, value := range config.Passby {
		item := map[string]string{}
		item[value.ConsumerGroup] = value.Topic
		rtn[value.Cluster] = item
	}

	// record distance for each topic
	for topicKey, d := range topicDistance {
		// pass_by filter
		temp, ok := rtn[config.ZkCluster][topicList[topicKey].ConsumerGroup]
		if ok && temp == topicList[topicKey].TopicName {
			continue
		}
		data = append(data, LogData{
			Host:          host,
			Zabbix_key:    ZABBIX_KEY_DISTANCE,
			Cluster:       config.ZkCluster,
			ConsumerGroup: topicList[topicKey].ConsumerGroup,
			Topic:         topicList[topicKey].TopicName,
			Threshold:     config.Distance,
			Distance:      d,
		})
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
