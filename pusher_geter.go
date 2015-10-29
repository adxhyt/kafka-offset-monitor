package main

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"time"
)

type PusherGeter struct {
	Url      string
	Err_file string
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

func NewPusherGeter(url string, errfile string) *PusherGeter {
	return &PusherGeter{Url: url, Err_file: errfile}
}

func (this *PusherGeter) RemoteGet() (msg *RetMsg, err error) {
	timeout := time.Duration(5 * time.Second)
	client := http.Client{
		Timeout: timeout,
	}
	resp, err := client.Get(this.Url)
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
	return msg, err
}
