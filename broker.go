package main

import (
	"fmt"
	"strings"
	"sync"
)

const (
	levelSeparator      = "."
	singleLevelWildcard = "*"
	multiLevelWildcard  = ">"
	systemTopicPrefix   = "$SYS"
)

type Broker struct {
	subscribers []Subscription
	mu          sync.RWMutex
}

type Subscription struct {
	pattern string
	channel chan []byte
}

func match(pattern, subject string) bool {
	patternTokens := strings.Split(pattern, levelSeparator)
	subjectTokens := strings.Split(subject, levelSeparator)
	i := 0
	for i < len(patternTokens) {
		token := patternTokens[i]
		if token == multiLevelWildcard {
			return i < len(subjectTokens)
		}
		if i >= len(subjectTokens) {
			return false
		}
		if token == singleLevelWildcard || token == subjectTokens[i] {
			i++
			continue
		}
		return false
	}
	return i == len(subjectTokens)
}

func (b *Broker) publish(subject string, msg []byte) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	in := append([]byte(nil), msg...)
	for _, subscription := range b.subscribers {
		if match(subscription.pattern, subject) {
			select {
			case subscription.channel <- in:
			default:
				//buffer is full, drop the msg
			}
		}
	}
}

func (b *Broker) subscribe(pattern string) chan []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	newSub := Subscription{
		pattern: pattern,
		channel: make(chan []byte, 64),
	}
	b.subscribers = append(b.subscribers, newSub)
	return newSub.channel
}

func (b *Broker) unsubscribe(unsubChan chan []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for index, subscription := range b.subscribers {
		if subscription.channel == unsubChan {
			b.subscribers[index] = b.subscribers[len(b.subscribers)-1]
			b.subscribers[len(b.subscribers)-1] = Subscription{}
			b.subscribers = b.subscribers[:len(b.subscribers)-1]
			return
		}
	}
}

func (b *Broker) getMetadata() string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	metadata := fmt.Sprintf("Broker { \nsubscribers size: %d\n}", len(b.subscribers))
	return metadata
}

func NewBroker() *Broker {
	return &Broker{
		subscribers: make([]Subscription, 0),
	}
}
