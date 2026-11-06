package main

import (
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func sendFrame(conn net.Conn, opcode byte, pattern, message string) error {
	buf := []byte{opcode}
	buf = append(buf, byte(len(pattern)))
	buf = append(buf, pattern...)
	msgLen := make([]byte, 4)
	binary.BigEndian.PutUint32(msgLen, uint32(len(message)))
	buf = append(buf, msgLen...)
	buf = append(buf, message...)
	_, err := conn.Write(buf)
	return err
}

func TestRoundTrip(t *testing.T) {
	broker := NewBroker()
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
		handleClient(conn, broker, context.Background())
	}()

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if err := sendFrame(conn, OpPing, "", ""); err != nil {
		t.Fatal(err)
	}

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

func TestPublishDeliversToSubscriber(t *testing.T) {
	broker := NewBroker()
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
			go handleClient(conn, broker, context.Background())
		}
	}()

	// Subscriber: connect and send "subscribe orders.*".
	sub, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if err := sendFrame(sub, OpSubscribe, "orders.*", ""); err != nil {
		t.Fatal(err)
	}

	// The server doesn't acknowledge subscribe, so give it a moment to register.
	time.Sleep(50 * time.Millisecond)

	// Publisher: connect and publish "hello" to "orders.btc" (matches orders.*).
	pub, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	if err := sendFrame(pub, OpPublish, "orders.btc", "hello"); err != nil {
		t.Fatal(err)
	}

	// The subscriber should receive "hello" (raw bytes — response framing is a
	// separate, later concern).
	reply := make([]byte, 1024)
	n, err := sub.Read(reply)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(reply[:n]), "hello"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}
