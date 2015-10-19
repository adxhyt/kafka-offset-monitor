package main

import (
	"log"
	"os"
)

type FileLogger struct {
	Path     string
	Contents []string
}

func NewFileLogger(filename string, contents []string) *FileLogger {
	return &FileLogger{Path: filename, Contents: contents}
}

func (this *FileLogger) Exist() error {
	if _, err := os.Stat(this.Path); os.IsNotExist(err) {
		return err
	}
	return nil
}

func (this *FileLogger) RecordLogger() error {
	var fileHandle *os.File
	var err error
	if this.Exist() == nil {
		fileHandle, err = os.OpenFile(this.Path, os.O_WRONLY|os.O_APPEND, 0666)
	} else {
		fileHandle, err = os.Create(this.Path)
	}
	Check(err)

	defer fileHandle.Close()

	logger := log.New(fileHandle, "\n", log.Ldate|log.Ltime)
	for _, content := range this.Contents {
		logger.Println(content)
	}
	return nil
}
