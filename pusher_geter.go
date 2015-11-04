package main

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"time"
)

type PusherGeter struct {
	WorkerUrl string
	Err_file  string
}

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

type RetTracker struct {
	Data   []*TrackerData
	Errmsg string
	Errno  int
}

type TrackerData struct {
	ConsumerGroupName string
	Topic             string
	Partition         string
	TimeStamp         int64
	LogId             string
	Offset            int64
}

func NewPusherGeter(worker_url string, errfile string) *PusherGeter {
	return &PusherGeter{WorkerUrl: worker_url, Err_file: errfile}
}

func (this *PusherGeter) RemoteGet() ([]*ClientResp, error) {
	var msg *RetMsg
	var err error
	timeout := time.Duration(http_time_out) * time.Second
	client := http.Client{
		Timeout: timeout,
	}
	resp, err := client.Get(this.WorkerUrl)
	if err != nil {
		AddLogger(this.Err_file, "[Distance Err REMOTE_GET_EMPTY_DATA]", err)
		return nil, ErrPusherTimeOut
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		AddLogger(this.Err_file, "[Distance Err READ_ERROR]", err)
		return nil, ErrPusherReadErr
	}

	err = json.Unmarshal([]byte(body), &msg)
	if err != nil {
		AddLogger(this.Err_file, "[Distance Err JSON_UNMARSHAL_ERR]", err)
		return nil, ErrPusherJsonErr
	}

	if msg.Errno != 0 {
		AddLogger(this.Err_file, "[Distance Err ERRNO_ERR]", err)
		return nil, ErrPusherDataErr
	}

	//if msg.Data == nil {
	// check tracker
	//tracker, err := client.Get(this.TrackerUrl)
	//defer tracker.Body.Close()
	//if err != nil {
	//return nil, err
	//}
	//body, err := ioutil.ReadAll(tracker.Body)
	//if err != nil {
	//return nil, err
	//}
	//tracker := TrackerData{}
	//err = json.Unmarshal([]byte(body), &tracker)
	//if err != nil {
	//	return nil, err
	//}
	//currentTimeStamp := time.Now().UnixNano() / 1000000
	// tracker exists && lastest tracker stamp in 1 hour
	//if tracker.Topic != "" && currentTimeStamp-tracker.TimeStamp < 3600000 {
	//AddLogger(this.Err_file, "[Distance Err TRACKER_SUSPEND_ERR]", ErrTrackerServiceSuspend)
	//return nil, ErrTrackerServiceSuspend
	//}
	//return data, nil
	//}
	return msg.Data, err
}
