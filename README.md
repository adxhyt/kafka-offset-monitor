# kafka-offset-monitor

start: ./kafka-offset-monitor -c config.json -sw=1 

    // Kafka offset monitor params
    -f=<path>
        the monitor log path; default value:/home/work/kafka-monitor/log/kafka_offset_monitor
    -l=<path>
        the runtime logger path; record runtime data; default value:/home/work/kafka-monitor/log/kafka_offset_logger
    -sw <runtime logger switcher>
        0 means close; 1 means open; default close
    -c=<file>
        config.json; read config

Tips: should work with https://github.com/crask/kafka-pusher. forked from https://github.com/crask/kafka-pusher

