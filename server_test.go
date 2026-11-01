package main

import (
	"context"
	"encoding/binary"
	"net"
	"testing"

	"go.uber.org/goleak"
)

func TestRoundTrip(t *testing.T) {
	// SERVER side: listen on a random free port, accept in a goroutine (Accept blocks).
	topic := NewTopic()
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

	// CLIENT side: dial the random port.
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Send a length-prefixed "ping".
	msg := "ping"
	lengthBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBuf, uint32(len(msg)))
	conn.Write(lengthBuf)
	conn.Write([]byte(msg))

	// Read the reply and assert.
	reply := make([]byte, 1024)
	n, err := conn.Read(reply)
	if err != nil {
		t.Fatal(err)
	}
	got := string(reply[:n])
	want := "pong\n"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}

	// Close the client → server sees EOF → handleClient returns.
	conn.Close()

	goleak.VerifyNone(t)
}
