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
}

func NewOffsetWorker(zookeeper string, cluster string) *OffsetWorker {
	return &OffsetWorker{zookeeper: zookeeper, cluster: cluster}
}

func (this *OffsetWorker) Init() error {

	kazooConfig := kazoo.NewConfig()
	kazooClient, err := kazoo.NewKazooFromConnectionString(this.zookeeper, kazooConfig)
	if nil != err {
		return err
	}

	kafkaClientConfig := sarama.NewConfig()
	brokerList, err := kazooClient.BrokerList()
	if nil != err {
		return err
	}

	kafkaClient, err := sarama.NewClient(brokerList, kafkaClientConfig)
	if nil != err {
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
		return nil, err
	}

	for _, topic := range topics {
		item := map[string]int64{}

		partitions, err := this.kafkaClient.Partitions(topic)
		if nil != err {
			return nil, err
		}
		var offset_total int64
		offset_total = 0
		for _, partition := range partitions {
			offset, err := this.kafkaClient.GetOffset(topic, partition, sarama.OffsetNewest)
			if nil != err {
				return nil, err
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
