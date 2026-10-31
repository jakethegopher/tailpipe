package main

import (
	"fmt"
	"sync"
)

type Topic struct {
	subscribers []chan []byte
	mu          sync.RWMutex
}

func (t *Topic) publish(msg []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()
	in := append([]byte(nil), msg...)
	for _, ch := range t.subscribers {
		ch <- in
	}
}

func (t *Topic) subscribe() chan []byte {
	t.mu.Lock()
	defer t.mu.Unlock()
	subChan := make(chan []byte, 1)
	t.subscribers = append(t.subscribers, subChan)
	return subChan
}

func (t *Topic) getMetadata() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	metadata := fmt.Sprintf("Topic { \nsubscribers size: %d\n}", len(t.subscribers))
	return metadata
}

func NewTopic() *Topic {
	return &Topic{
		subscribers: make([]chan []byte, 0),
	}
}
