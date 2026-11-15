package main

import (
	"encoding/binary"
	"io"
	"os"
	"sync"
)

const walFileName = "systemlog.txt"

type Record struct {
	Subject string
	Msg     []byte
}


type WAL struct {
	mu   sync.Mutex
	file *os.File
}


func NewWAL() *WAL {
	f, err := os.OpenFile(walFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	return &WAL{file: f}
}

func (w *WAL) Close() {
	w.file.Close()
}

func (w *WAL) saveToFile(subject string, msg []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	buf := []byte{byte(len(subject))}
	buf = append(buf, subject...)
	msgLen := make([]byte, 4) 
	binary.BigEndian.PutUint32(msgLen, uint32(len(msg)))
	buf = append(buf, msgLen...)
	buf = append(buf, msg...)

	if _, err := w.file.Write(buf); err != nil {
		return err
	}
	return w.file.Sync() // commit file changes
}


func replay(path string) ([]Record, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var records []Record
	for {
		var subjectLen [1]byte
		if _, err := io.ReadFull(f, subjectLen[:]); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		subject := make([]byte, int(subjectLen[0]))
		if _, err := io.ReadFull(f, subject); err != nil {
			return nil, err
		}
		var msgLen [4]byte
		if _, err := io.ReadFull(f, msgLen[:]); err != nil {
			return nil, err
		}
		msg := make([]byte, binary.BigEndian.Uint32(msgLen[:]))
		if _, err := io.ReadFull(f, msg); err != nil {
			return nil, err
		}
		records = append(records, Record{Subject: string(subject), Msg: msg})
	}
	return records, nil
}
