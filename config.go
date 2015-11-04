package main

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

type Config struct {
	Urls       []string      `json:"url"`
	Zookeepers []string      `json:"zookeepers"`
	ZkPath     string        `json:"zk_path"`
	ZkCluster  string        `json:"zk_cluster"`
	Interval   int           `json:"interval"`
	Sleep      int           `json:"sleep"`
	Distance   int           `json:"distance_threshold"`
	Passby     []Clusterlist `json:"pass_by"`
}

type Clusterlist struct {
	Cluster       string `json:"cluster"`
	ConsumerGroup string `json:"consumer_group"`
	Topic         string `json:"topic"`
}

func loadConfig(configFile string) (*Config, error) {
	var c *Config
	path := configFile
	fi, err := os.Open(path)
	defer fi.Close()
	if nil != err {
		return nil, err
	}
	defer fi.Close()

	fd, err := ioutil.ReadAll(fi)

	err = json.Unmarshal([]byte(fd), &c)
	if nil != err {
		return nil, err
	}
	return c, nil
}
