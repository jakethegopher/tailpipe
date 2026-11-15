package main

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"testing"
)

// TestMain runs first, before any test. When the WAL_CRASH_CHILD env var is
// set, this process is the subprocess spawned by TestWALSurvivesSIGKILL — it
// does the "crash work" and exits instead of running the normal test suite.
func TestMain(m *testing.M) {
	if os.Getenv("WAL_CRASH_CHILD") == "1" {
		crashChild()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// crashChild writes one durable record, prints "ready", then blocks forever so
// the parent can SIGKILL a live process. The record is already fsynced to disk,
// so the kill must not lose it.
func crashChild() {
	wal := NewWAL()
	if err := wal.saveToFile("orders.btc", []byte("kill-proof")); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("ready")
	select {} // block forever; the parent kills us
}

// TestWALSurvivesSIGKILL is the literal kill -9 test: spawn a subprocess that
// publishes one record, SIGKILL it, then replay and confirm the record survived.
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

	// Re-run this test binary as a child, flagged by the env var. -test.run
	// limits what the child would run if it ever got past TestMain (it won't).
	cmd := exec.Command(exe, "-test.run=^TestWALSurvivesSIGKILL$")
	cmd.Env = append(os.Environ(), "WAL_CRASH_CHILD=1")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	// Handshake: wait until the child has written and fsynced its record.
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() {
		t.Fatalf("child died before writing: %v", scanner.Err())
	}
	if scanner.Text() != "ready" {
		t.Fatalf("unexpected handshake: %q", scanner.Text())
	}

	// Hard-kill: SIGKILL on Unix, TerminateProcess on Windows.
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

// TestReplayRecoversRecords writes two records, replays the log, and checks
// they come back in order with the right subject and payload.
func TestReplayRecoversRecords(t *testing.T) {
	os.Remove(walFileName)
	defer os.Remove(walFileName)

	wal := NewWAL()
	defer wal.Close()

	if err := wal.saveToFile("orders.btc", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := wal.saveToFile("orders.eth", []byte("world")); err != nil {
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

// TestRecordsSurviveWithoutClose proves durability: a record written with fsync
// is recoverable even though the WAL is never closed. This is the "crash" case
// — the process dies without a graceful Close, but fsync already put the bytes
// on disk, so replay (a fresh read handle) still sees them.
func TestRecordsSurviveWithoutClose(t *testing.T) {
	os.Remove(walFileName)
	defer os.Remove(walFileName)

	wal := NewWAL()
	if err := wal.saveToFile("orders.btc", []byte("durable")); err != nil {
		t.Fatal(err)
	}
	// No wal.Close() here — simulate a crash. fsync already flushed to disk.

	records, err := replay(walFileName)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].Subject != "orders.btc" || string(records[0].Msg) != "durable" {
		t.Errorf("record not recovered after crash: %+v", records)
	}
}
