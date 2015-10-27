package main

import (
	"flag"
	"log"
	"math/rand"
	"time"
)

var (
	configFile, monitor_file, logger_file string
	logger_switch                         int
	ZABBIX_KEY_LASTEST_OFFSET             = "latest_offset"
	ZABBIX_KEY_DISTANCE                   = "distance"
	INT64_MAX                             = 9223372036854775807
)

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
	flag.StringVar(&monitor_file, "f", "/home/work/kafka-monitor/log/kafka_offset_monitor", "the log path")
	flag.StringVar(&logger_file, "l", "/home/work/kafka-monitor/log/kafka_offset_logger", "the runtime logger path")
	flag.IntVar(&logger_switch, "sw", 0, "the logger switcher")
}

func main() {
	flag.Parse()
	config, err := loadConfig(configFile)

	if err != nil {
		log.Fatalf("[OFFSET MON]Load Config err: %s", err)
	}
	ticker := time.NewTicker(time.Duration(config.Interval) * time.Second)
	defer ticker.Stop()

	// init
	manager := NewManager(monitor_file, logger_file, logger_switch)
	manager.Init(config)
	defer manager.Close()

	for {
		select {
		case <-ticker.C:
			r := rand.Intn(config.Sleep)
			time.Sleep(time.Duration(r))
			go manager.Work()
		}
	}
}
