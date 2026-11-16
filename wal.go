package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	walDir = "wal"
	batchSize = 64 * 1024
	segmentSize = 256 * 1024
	maxSegments = 4
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
	active *os.File
	seq    int
	size   int
	writes chan writeReq
	stop   chan struct{}
	done   chan struct{}
}

func segmentPath(seq int) string {
	return filepath.Join(walDir, fmt.Sprintf("wal-%06d.log", seq))
}

func openSegment(seq int) *os.File {
	f, err := os.OpenFile(segmentPath(seq), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	return f
}

func NewWAL() *WAL {
	if err := os.MkdirAll(walDir, 0755); err != nil {
		panic(err)
	}
	seq := highestSegmentSeq() + 1
	w := &WAL{
		active: openSegment(seq),
		seq:    seq,
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
	w.active.Close()
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
			if len(batch) >= batchSize {
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
	if _, err := w.active.Write(batch); err != nil {
		return err
	}
	if err := w.active.Sync(); err != nil {
		return err
	}
	w.size += len(batch)
	if w.size >= segmentSize {
		w.rotate()
	}
	return nil
}

// segments beyond maxSegments.
func (w *WAL) rotate() {
	w.active.Close()
	w.seq++
	w.active = openSegment(w.seq)
	w.size = 0
	w.applyRetention()
}

func (w *WAL) applyRetention() {
	segs := listSegments()
	for len(segs) > maxSegments {
		os.Remove(segs[0])
		segs = segs[1:]
	}
}

func listSegments() []string {
	entries, err := os.ReadDir(walDir)
	if err != nil {
		return nil
	}
	var segs []string
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, "wal-") && strings.HasSuffix(name, ".log") {
			segs = append(segs, filepath.Join(walDir, name))
		}
	}
	sort.Slice(segs, func(i, j int) bool {
		return segmentSeq(segs[i]) < segmentSeq(segs[j])
	})
	return segs
}

func segmentSeq(path string) int {
	base := filepath.Base(path)
	num := strings.TrimSuffix(strings.TrimPrefix(base, "wal-"), ".log")
	n, _ := strconv.Atoi(num)
	return n
}

func highestSegmentSeq() int {
	segs := listSegments()
	if len(segs) == 0 {
		return -1
	}
	return segmentSeq(segs[len(segs)-1])
}

func replay() ([]Record, error) {
	var records []Record
	for _, path := range listSegments() {
		recs, err := replaySegment(path)
		if err != nil {
			return nil, err
		}
		records = append(records, recs...)
	}
	return records, nil
}

func replaySegment(path string) ([]Record, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var records []Record
	for {
		var sl [1]byte
		if _, err := io.ReadFull(f, sl[:]); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		subject := make([]byte, int(sl[0]))
		if _, err := io.ReadFull(f, subject); err != nil {
			return nil, err
		}
		var ml [4]byte
		if _, err := io.ReadFull(f, ml[:]); err != nil {
			return nil, err
		}
		msg := make([]byte, binary.BigEndian.Uint32(ml[:]))
		if _, err := io.ReadFull(f, msg); err != nil {
			return nil, err
		}
		records = append(records, Record{Subject: string(subject), Msg: msg})
	}
	return records, nil
}
