package main

import (
	"fmt"
	"sync"
)

type Topic struct {
	name        string
	messages    []Message
	subscribers []chan Message
	mu          sync.RWMutex
	nextOffset  int64
}

type Message struct {
	Offset int64
	Data   []byte
}

func (t *Topic) publish(msg []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()
	in := append([]byte(nil), msg...)

	t.messages = append(t.messages, Message{
		Offset: t.nextOffset,
		Data:   in,
	})
	t.nextOffset++
}

func (t *Topic) read(targetOffset int64, chunkSize int) []Message {
	if chunkSize <= 0 {
		return nil
	}
	t.mu.RLock()
	defer t.mu.RUnlock()

	for index, elem := range t.messages {
		if elem.Offset >= targetOffset {
			n := min(len(t.messages)-index, chunkSize)
			out := make([]Message, n)
			for i := range n {
				out[i] = Message{
					Offset: t.messages[index+i].Offset,
					Data:   append([]byte(nil), t.messages[index+i].Data...),
				}
			}
			return out
		}
	}
	return nil
}

func (t *Topic) getMetadata() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	metadata := fmt.Sprintf("Topic { \nname: %s\nnextOffset: %d\nmessages size: %d\nsubscribers size: %d\n}", t.name, t.nextOffset, len(t.messages), len(t.subscribers))
	return metadata
}
