package main

import (
	"fmt"
	"os"
	"sync"
)

type WAL struct {
	mu   sync.Mutex
	file *os.File
}

func NewWAL() *WAL {
	file := initFile()
	return &WAL{
		file: file,
	}
}
func initFile() *os.File {
	f, err := os.OpenFile("systemlog.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic("file is not created")
	}
	fmt.Printf("systemlog is created")
	return f
}

func (w *WAL) Close() {
	w.file.Close()
}

func (w *WAL) saveToFile(subject string, msg []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	log := make([]byte, 0)
	_, err := w.file.Write(log)
	if err == nil {
		return nil
	}
	return err
}
