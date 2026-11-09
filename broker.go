package main

import (
	"fmt"
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
	pLen, sLen := len(pattern), len(subject)
	pIdx, sIdx := 0, 0
	for pIdx < pLen {
		pEnd := pIdx
		for pEnd < pLen && pattern[pEnd] != '.' {
			pEnd++
		}
		pToken := pattern[pIdx:pEnd]
		if pToken == ">" {
			return sIdx <= sLen
		}
		if sIdx >= sLen {
			return false
		}
		sEnd := sIdx
		for sEnd < sLen && subject[sEnd] != '.' {
			sEnd++
		}
		sToken := subject[sIdx:sEnd]
		if pToken != "*" && pToken != sToken {
			return false
		}
		pIdx = pEnd + 1
		sIdx = sEnd + 1
	}
	return sIdx > sLen
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
