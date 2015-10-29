package main

import (
	"errors"
)

var ErrKafkaClientNoTopic = errors.New("kafka client: cannot get topics")

var ErrKafkaClientNoPartition = errors.New("kafka client: get empty partitions on given topic")

var ErrKafkaClientEmptyOffset = errors.New("kafka client: get empty offset with certain topic and given partition")

var ErrPusherTimeOut = errors.New("pusher http response: fetch kafka-pusher remote resource time out, default time out config is 3s")

var ErrPusherReadErr = errors.New("pusher http response: fetch kafka-pusher data read response err")

var ErrPusherJsonErr = errors.New("pusher http response: fetch kafka-pusher data json unmarshal err")
