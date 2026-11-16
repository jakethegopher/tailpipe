package main

import (
	"testing"
	"time"
)

// ===================benchmark===================//
func BenchmarkPublish(b *testing.B) {
	broker := NewBroker()
	wal := NewWAL()
	defer wal.Close()
	ch := broker.subscribe("orders.btc")
	go func() {
		for range ch {
		}
	}() // drain the channel to avoid blocking the benchmark

	msg := []byte("hello world")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		broker.publish("orders.btc", msg, wal)
	}
}

func BenchmarkPublishParallel(b *testing.B) {
	broker := NewBroker()
	wal := NewWAL()
	defer wal.Close()
	ch := broker.subscribe("orders.btc")
	go func() {
		for range ch {
		}
	}()

	msg := []byte("hello world")

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := broker.publish("orders.btc", msg, wal); err != nil {
				panic(err)
			}
		}
	})
}

// ===================test===================//
func TestMatch(t *testing.T) {
	cases := []struct {
		pattern string
		subject string
		want    bool
	}{
		{"orders.btc", "orders.btc", true},      // exact match
		{"orders.*", "orders.btc", true},        // * matches exactly one level
		{"orders.*", "orders.btc.limit", false}, // * does NOT span levels
		{"orders.>", "orders.btc.limit", true},  // > matches the rest
		{"orders.>", "orders", false},           // > needs at least one more level
		{"orders.*", "orders", false},           // * needs exactly one more level
		{"foo.bar", "foo.baz", false},           // exact mismatch
	}
	for _, c := range cases {
		if got := match(c.pattern, c.subject); got != c.want {
			t.Errorf("match(%q, %q) = %v, want %v", c.pattern, c.subject, got, c.want)
		}
	}
}

func TestBrokerPublishDeliversToSubscriber(t *testing.T) {
	b := NewBroker()
	wal := NewWAL()
	defer wal.Close()
	ch := b.subscribe("orders.btc")

	b.publish("orders.btc", []byte("hello"), wal)

	if got := <-ch; string(got) != "hello" {
		t.Errorf("got %q, want %q", string(got), "hello")
	}
}

func TestBrokerPublishMatchesWildcard(t *testing.T) {
	b := NewBroker()
	wal := NewWAL()
	defer wal.Close()

	ch := b.subscribe("orders.*")

	b.publish("orders.btc", []byte("hi"), wal)

	if got := <-ch; string(got) != "hi" {
		t.Errorf("got %q, want %q", string(got), "hi")
	}
}

func TestBrokerPublishDeliversToAllSubscribers(t *testing.T) {
	b := NewBroker()
	wal := NewWAL()
	defer wal.Close()
	ch1 := b.subscribe("orders.btc") // exact
	ch2 := b.subscribe("orders.>")   // wildcard

	b.publish("orders.btc", []byte("hi"), wal)

	for i, ch := range []chan []byte{ch1, ch2} {
		if got := <-ch; string(got) != "hi" {
			t.Errorf("subscriber %d: got %q, want %q", i, string(got), "hi")
		}
	}
}

func TestBrokerPublishCopiesData(t *testing.T) {
	b := NewBroker()
	wal := NewWAL()
	defer wal.Close()
	ch := b.subscribe("orders.btc")

	msg := []byte("hello")
	b.publish("orders.btc", msg, wal)

	msg[0] = 'X' // mutate the ORIGINAL slice after publishing

	if got := <-ch; string(got) != "hello" {
		t.Errorf("publish did not copy: got %q, want %q", string(got), "hello")
	}
}

func TestBrokerUnsubscribeStopsDelivery(t *testing.T) {
	b := NewBroker()
	wal := NewWAL()
	defer wal.Close()
	ch := b.subscribe("orders.btc")
	b.unsubscribe(ch)

	b.publish("orders.btc", []byte("hi"), wal)

	select {
	case msg := <-ch:
		t.Errorf("received message after unsubscribe: %q", msg)
	case <-time.After(50 * time.Millisecond):
		// expected: nothing arrives
	}
}
