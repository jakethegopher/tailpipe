package main

import (
	"encoding/binary"
	"io"
	"os"
	"time"
)

const (
	walFileName = "systemlog.txt"

	walBatchSize = 64 * 1024

	walFlushInterval = 5 * time.Millisecond
)

type Record struct {
	Subject string
	Msg     []byte
}

type writeReq struct {
	record []byte
	done   chan error 
}


type WAL struct {
	file   *os.File
	writes chan writeReq
	stop   chan struct{}
	done   chan struct{}
}

func NewWAL() *WAL {
	f, err := os.OpenFile(walFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	w := &WAL{
		file:   f,
		writes: make(chan writeReq, 1024),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	go w.run()
	return w
}

func (w *WAL) Close() {
	close(w.stop)
	<-w.done
	w.file.Close()
}


func (w *WAL) appendRecord(subject string, msg []byte) error {
	done := make(chan error, 1)
	w.writes <- writeReq{record: encodeRecord(subject, msg), done: done}
	return <-done
}


func encodeRecord(subject string, msg []byte) []byte {
	buf := []byte{byte(len(subject))}
	buf = append(buf, subject...)
	msgLen := make([]byte, 4)
	binary.BigEndian.PutUint32(msgLen, uint32(len(msg)))
	buf = append(buf, msgLen...)
	buf = append(buf, msg...)
	return buf
}

func (w *WAL) run() {
	defer close(w.done)
	ticker := time.NewTicker(walFlushInterval)
	defer ticker.Stop()

	var batch []byte
	var pending []chan error

	flush := func() {
		if len(pending) == 0 {
			return
		}
		err := w.writeBatch(batch)
		for _, ch := range pending {
			ch <- err
		}
		batch = batch[:0]
		pending = pending[:0]
	}

	for {
		select {
		case req := <-w.writes:
			batch = append(batch, req.record...)
			pending = append(pending, req.done)
			if len(batch) >= walBatchSize {
				flush()
			}

		case <-ticker.C:
			flush()

		case <-w.stop:
			for {
				select {
				case req := <-w.writes:
					batch = append(batch, req.record...)
					pending = append(pending, req.done)
				default:
					flush()
					return
				}
			}
		}
	}
}

func (w *WAL) writeBatch(batch []byte) error {
	if _, err := w.file.Write(batch); err != nil {
		return err
	}
	return w.file.Sync()
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
