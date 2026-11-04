package main

import "testing"

// Task 3: a message published to a topic reaches a subscriber.
func TestTopicPublishDeliversToSubscriber(t *testing.T) {
	topic := NewBroker("test")
	ch := topic.subscribe() // register a subscriber, keep its channel

	topic.publish([]byte("hello"))

	got := <-ch // read what the subscriber received
	if string(got) != "hello" {
		t.Errorf("got %q, want %q", string(got), "hello")
	}
}

// Task 3 (fan-out): one message reaches ALL subscribers, not just the first.
func TestTopicPublishDeliversToAllSubscribers(t *testing.T) {
	topic := NewBroker("test")
	ch1 := topic.subscribe()
	ch2 := topic.subscribe()

	topic.publish([]byte("hi"))

	for i, ch := range []chan []byte{ch1, ch2} {
		got := <-ch
		if string(got) != "hi" {
			t.Errorf("subscriber %d: got %q, want %q", i, string(got), "hi")
		}
	}
}

// The copy-on-write guarantee: the caller can mutate its buffer after publish
// without corrupting what subscribers received.
func TestTopicPublishCopiesData(t *testing.T) {
	topic := NewBroker("test")
	ch := topic.subscribe()

	msg := []byte("hello")
	topic.publish(msg)

	msg[0] = 'X' // mutate the ORIGINAL slice after publishing

	got := <-ch
	if string(got) != "hello" {
		t.Errorf("publish did not copy: got %q, want %q", string(got), "hello")
	}
}
