package main

import (
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"go.uber.org/goleak"
)

// sendFrame writes a single length-prefixed frame to conn.
// It's the client-side mirror of the server's readFrame.
func sendFrame(conn net.Conn, s string) error {
	lengthBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBuf, uint32(len(s)))
	if _, err := conn.Write(lengthBuf); err != nil {
		return err
	}
	_, err := conn.Write([]byte(s))
	return err
}

func TestRoundTrip(t *testing.T) {
	// SERVER side: listen on a random free port, accept in a goroutine.
	topic := NewBroker("test")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		handleClient(conn, topic, context.Background())
	}()

	// CLIENT side: dial and send "ping".
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := sendFrame(conn, "ping"); err != nil {
		t.Fatal(err)
	}

	// Read the reply and assert.
	reply := make([]byte, 1024)
	n, err := conn.Read(reply)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(reply[:n]), "pong\n"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}

	conn.Close()
	goleak.VerifyNone(t)
}

// Task 3.5: a single-frame "publish hello" reaches a subscriber.
func TestPublishDeliversToSubscriber(t *testing.T) {
	topic := NewBroker("test")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	// Accept loop: each connection gets its own goroutine.
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go handleClient(conn, topic, context.Background())
		}
	}()

	// Subscriber: connect and send "subscribe".
	sub, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if err := sendFrame(sub, "subscribe"); err != nil {
		t.Fatal(err)
	}

	// The server doesn't acknowledge "subscribe", so give it a moment to
	// register the subscriber before publishing. (A sleep — not ideal, but
	// there's no other sync signal yet.)
	time.Sleep(50 * time.Millisecond)

	// Publisher: connect and send "publish hello" in ONE frame.
	pub, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	if err := sendFrame(pub, "publish hello"); err != nil {
		t.Fatal(err)
	}

	// The subscriber should receive "hello" (raw bytes, no newline).
	reply := make([]byte, 1024)
	n, err := sub.Read(reply)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(reply[:n]), "hello"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}
