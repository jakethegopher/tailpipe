package main

import (
	"encoding/binary"
	"net"
	"testing"

	"go.uber.org/goleak"
)

func TestRoundTrip(t *testing.T) {
	// ── SERVER side (must run in a goroutine, because Accept() blocks) ──
	// 127.0.0.1:0 → "listen on loopback, and OS, you pick a free port for me"
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	go func() {
		conn, err := listener.Accept() // sits here until the client dials
		if err != nil {
			return
		}
		handleClient(conn) // ← YOUR code under test
	}()

	// ── CLIENT side (the test itself dials in) ──
	// listener.Addr() answers the question "which port did the OS give me?"
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Send a length-prefixed "hi" — the mirror image of what your server reads.
	msg := "hi"
	lengthBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBuf, uint32(len(msg))) // number → 4 bytes
	conn.Write(lengthBuf)
	conn.Write([]byte(msg))

	// Read the reply and assert it's right.
	reply := make([]byte, 1024)
	n, err := conn.Read(reply)
	if err != nil {
		t.Fatal(err)
	}
	got := string(reply[:n])
	want := "Henlo good sir\n"
	if got != want {
		t.Errorf("got %q, want %q", got, want) // Go's "assertEquals"
	}

	// Close the client → server's ReadFull returns EOF → handleClient returns.
	conn.Close()

	// Assert no goroutines were left behind. (goleak retries briefly, so the
	// handler usually finishes in time after the close.)
	goleak.VerifyNone(t)
}
