package main

import (
	"fmt"
	"io"
	"os"
)

type FileWriter struct {
	Path     string
	Contents []LogData
}

func NewFileWriter(filename string, contents []LogData) *FileWriter {
	return &FileWriter{Path: filename, Contents: contents}
}

func (this *FileWriter) Exist() error {
	if _, err := os.Stat(this.Path); os.IsNotExist(err) {
		return err
	}
	return nil
}

func (this *FileWriter) WriteToFile() error {
	var fileHandle *os.File
	var err error
	if this.Exist() == nil {
		fileHandle, err = os.OpenFile(this.Path, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0666)
	} else {
		fileHandle, err = os.Create(this.Path)
	}
	check(err)

	defer fileHandle.Close()

	for _, content := range this.Contents {
		s := fmt.Sprintf("%s kafka_monitor[%s,%s,%s,%s,%d] %d \n", content.Host, content.Zabbix_key, content.Cluster, content.ConsumerGroup, content.Topic, content.Threshold, content.Distance)
		if _, err := io.WriteString(fileHandle, s); err != nil {
			return err
		}
	}
	return nil
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}
