package main

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"testing"
)

func TestMain(m *testing.M) {
	if os.Getenv("WAL_CRASH_CHILD") == "1" {
		crashChild()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func crashChild() {
	wal := NewWAL()
	if err := wal.appendRecord("orders.btc", []byte("kill-proof")); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("ready")
	select {} // block forever - the parent kills child
}

func TestWALSurvivesSIGKILL(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping subprocess kill test in -short mode")
	}
	os.Remove(walFileName)
	defer os.Remove(walFileName)

	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command(exe, "-test.run=^TestWALSurvivesSIGKILL$")
	cmd.Env = append(os.Environ(), "WAL_CRASH_CHILD=1")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() {
		t.Fatalf("child died before writing: %v", scanner.Err())
	}
	if scanner.Text() != "ready" {
		t.Fatalf("unexpected handshake: %q", scanner.Text())
	}

	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	cmd.Wait()

	records, err := replay(walFileName)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].Subject != "orders.btc" || string(records[0].Msg) != "kill-proof" {
		t.Fatalf("acked record lost after SIGKILL: %+v", records)
	}
}

func TestReplayRecoversRecords(t *testing.T) {
	os.Remove(walFileName)
	defer os.Remove(walFileName)

	wal := NewWAL()
	defer wal.Close()

	if err := wal.appendRecord("orders.btc", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := wal.appendRecord("orders.eth", []byte("world")); err != nil {
		t.Fatal(err)
	}

	records, err := replay(walFileName)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Fatalf("got %d records, want 2", len(records))
	}
	if records[0].Subject != "orders.btc" || string(records[0].Msg) != "hello" {
		t.Errorf("record 0 = %q / %q, want orders.btc / hello", records[0].Subject, records[0].Msg)
	}
	if records[1].Subject != "orders.eth" || string(records[1].Msg) != "world" {
		t.Errorf("record 1 = %q / %q, want orders.eth / world", records[1].Subject, records[1].Msg)
	}
}

func TestRecordsSurviveWithoutClose(t *testing.T) {
	os.Remove(walFileName)
	defer os.Remove(walFileName)

	wal := NewWAL()
	if err := wal.appendRecord("orders.btc", []byte("durable")); err != nil {
		t.Fatal(err)
	}
	// No wal.Close() - simulate a crash. fsync already flushed to disk

	records, err := replay(walFileName)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].Subject != "orders.btc" || string(records[0].Msg) != "durable" {
		t.Errorf("record not recovered after crash: %+v", records)
	}
}
