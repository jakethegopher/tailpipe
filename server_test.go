package main

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"go.uber.org/goleak"
)

// sendFrame encodes ONE request frame in the wire format and writes it to conn:
// [opcode][pattern-len][pattern][msg-len][msg]. It is the client-side mirror of
// readFrame (main.go).
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

// readResponse decodes ONE response frame: [opcode][4-byte len][payload].
// It is the client-side mirror of connectionWriter.write (main.go).
func readResponse(conn net.Conn) (byte, []byte, error) {
	var op [1]byte
	if _, err := io.ReadFull(conn, op[:]); err != nil {
		return 0, nil, err
	}
	var l [4]byte
	if _, err := io.ReadFull(conn, l[:]); err != nil {
		return 0, nil, err
	}
	payload := make([]byte, binary.BigEndian.Uint32(l[:]))
	if _, err := io.ReadFull(conn, payload); err != nil {
		return 0, nil, err
	}
	return op[0], payload, nil
}

func TestRoundTrip(t *testing.T) {
	broker := NewBroker()
	wal := NewWAL()
	defer wal.Close()
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
		handleClient(conn, broker, context.Background(), wal)
	}()

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if err := sendFrame(conn, OpPing, "", ""); err != nil {
		t.Fatal(err)
	}

	op, payload, err := readResponse(conn)
	if err != nil {
		t.Fatal(err)
	}
	if op != RespPong || string(payload) != "pong" {
		t.Errorf("got op=%#x payload=%q, want RespPong + \"pong\"", op, payload)
	}

	conn.Close()
	goleak.VerifyNone(t)
}

func TestPublishDeliversToSubscriber(t *testing.T) {
	broker := NewBroker()
	wal := NewWAL()
	defer wal.Close()
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
			go handleClient(conn, broker, context.Background(), wal)
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

	// The subscriber receives the message, framed as RespMsg.
	op, payload, err := readResponse(sub)
	if err != nil {
		t.Fatal(err)
	}
	if op != RespMsg || string(payload) != "hello" {
		t.Errorf("got op=%#x payload=%q, want RespMsg + \"hello\"", op, payload)
	}
}
