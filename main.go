package main

import (
	"flag"
	"log"
	"math/rand"
	"time"
)

var (
	configFile, monitor_file, logger_file, err_file string
	logger_switch, http_time_out                    int
	ZABBIX_KEY_LASTEST_OFFSET                       = "latest_offset"
	ZABBIX_KEY_DISTANCE                             = "distance"
	INT64_MAX                                       = 9223372036854775807
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
	flag.StringVar(&err_file, "err", "/home/work/kafka-monitor/log/kafka_offset_err", "the error logger path")
	flag.IntVar(&logger_switch, "sw", 0, "the logger switcher")
	flag.IntVar(&http_time_out, "timeout", 3, "http default time out")
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
	manager := NewManager(monitor_file, logger_file, logger_switch, err_file)
	err = manager.Init(config)
	if err != nil {
		log.Fatalf("[OFFSET MON]Init Manager err: %s", err)
	}
	defer manager.Close()

	for {
		select {
		case <-ticker.C:
			r := rand.Intn(config.Sleep)
			time.Sleep(time.Duration(r) * time.Second)
			go manager.Work()
		}
	}
}
