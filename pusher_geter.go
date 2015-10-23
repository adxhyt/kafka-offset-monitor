package main

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
)

type PusherGeter struct {
	Url string
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

func NewPusherGeter(url string) *PusherGeter {
	return &PusherGeter{Url: url}
}

func (this *PusherGeter) RemoteGet() (msg *RetMsg, err error) {
	resp, err := http.Get(this.Url)
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
