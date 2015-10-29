package main

import (
	"fmt"
	"github.com/wvanbergen/kazoo-go"
	"gopkg.in/Shopify/sarama.v1"
)

type OffsetWorker struct {
	kazooClient *kazoo.Kazoo
	kafkaClient sarama.Client
	zookeeper   string
	cluster     string

	Err_file string
}

func NewOffsetWorker(zookeeper string, cluster string, errfile string) *OffsetWorker {
	return &OffsetWorker{zookeeper: zookeeper, cluster: cluster, Err_file: errfile}
}

func (this *OffsetWorker) Init() error {

	kazooConfig := kazoo.NewConfig()
	kazooClient, err := kazoo.NewKazooFromConnectionString(this.zookeeper, kazooConfig)
	if nil != err {
		AddLogger(this.Err_file, "[Distance Err KAZOO_CONN_ERR]", err)
		return err
	}

	kafkaClientConfig := sarama.NewConfig()
	brokerList, err := kazooClient.BrokerList()
	if nil != err {
		AddLogger(this.Err_file, "[Distance Err KAZOO_EMPTY_BROKER_ERR]", err)
		return err
	}

	kafkaClient, err := sarama.NewClient(brokerList, kafkaClientConfig)
	if nil != err {
		AddLogger(this.Err_file, "[Distance Err KAFKA_CLIENT_INIT_ERR]", err)
		return err
	}

	this.kafkaClient = kafkaClient
	this.kazooClient = kazooClient
	return nil
}

func (this *OffsetWorker) GetLastOffset() (map[string]map[string]int64, error) {
	rtn := map[string]map[string]int64{}

	topics, err := this.kafkaClient.Topics()
	if nil != err {
		errMsg := fmt.Sprintf("[Distance Err KAFKA_CLIENT_EMPTY_TOPICS_ERR] %v", topics)
		AddLogger(this.Err_file, errMsg, err)
		return nil, ErrKafkaClientNoTopic
	}

	for _, topic := range topics {
		item := map[string]int64{}

		partitions, err := this.kafkaClient.Partitions(topic)
		if nil != err {
			errMsg := fmt.Sprintf("[Distance Err KAFKA_CLIENT_EMPTY_PARTITION_ERR] %v %v", topic, partitions)
			AddLogger(this.Err_file, errMsg, err)
			return nil, ErrKafkaClientNoPartition
		}
		var offset_total int64
		offset_total = 0
		for _, partition := range partitions {
			offset, err := this.kafkaClient.GetOffset(topic, partition, sarama.OffsetNewest)
			if nil != err {
				errMsg := fmt.Sprintf("[Distance Err KAFKA_CLIENT_EMPTY_OFFSET_ERR] %v %v %v", topic, partition, offset)
				AddLogger(this.Err_file, errMsg, err)
				return nil, ErrKafkaClientEmptyOffset
			}
			offset_total += offset
			item[fmt.Sprintf("%d", partition)] = offset
		}
		item["total"] = offset_total
		rtn[topic] = item
	}
	return rtn, nil
}

func (this *OffsetWorker) Close() {
	this.kafkaClient.Close()
	this.kazooClient.Close()
}
