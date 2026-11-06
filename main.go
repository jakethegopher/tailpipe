package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
)

const maxCmdLength = 1024
const (
	OpPublish byte = iota
	OpSubscribe
	OpUnsubscribe
	OpPing
	OpExit
)

type Frame struct {
	opcode  byte
	pattern string
	message []byte
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	broker := NewBroker()

	listener, err := net.Listen("tcp", ":4222")
	if err != nil {
		log.Print(err)
		return
	}
	fmt.Println("listening on port :4222")
	defer listener.Close()

	go func() {
		<-ctx.Done()
		fmt.Println("closing server with Ctrl - C")
		listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Print(err)
			return
		}
		go handleClient(conn, broker, ctx)
	}
}

func handleClient(conn net.Conn, broker *Broker, sysCtx context.Context) {
	defer conn.Close()
	clientCtx, cancel := context.WithCancel(sysCtx)
	defer cancel() // fires when handleClient returns → wakes the subscriber goroutine
	var subscriberContexts []context.CancelFunc

innerLoop:
	for {
		frame, err := readFrame(conn)
		if err != nil {
			return
		}

		switch frame.opcode {
		case OpPing:
			conn.Write([]byte("pong\n"))

		case OpPublish:
			if len(frame.message) == 0 {
				return // malformed publish — drop the connection
			}
			broker.publish(frame.pattern, frame.message)

		case OpSubscribe:
			subCtx, subCancel := context.WithCancel(clientCtx)
			subChan := broker.subscribe(frame.pattern)
			subscriberContexts = append(subscriberContexts, subCancel)

			go func(subChan chan []byte, ctx context.Context) {
				defer broker.unsubscribe(subChan)
				for {
					select {
					case msg := <-subChan: // a message arrived — deliver it
						if _, err := conn.Write(msg); err != nil {
							return
						}
					case <-ctx.Done(): // canceled (unsubscribe, or disconnect via parent)
						broker.unsubscribe(subChan)
						return
					}
				}
			}(subChan, subCtx)

		case OpUnsubscribe:
			// Stop every subscription: call each cancel func, then forget them.
			for i := range subscriberContexts {
				subscriberContexts[i]()
			}
			subscriberContexts = nil

		case OpExit:
			conn.Write([]byte("exiting connection\n"))
			break innerLoop

		default:
			conn.Write([]byte("unrecognized prompt has been provided\n"))
		}
	}
}

func readFrame(conn net.Conn) (*Frame, error) {
	// opcode — 1 byte
	var opCodeBuf [1]byte
	if _, err := io.ReadFull(conn, opCodeBuf[:]); err != nil {
		if err == io.EOF {
			return nil, err // client hung up — normal
		}
		log.Print(err)
		return nil, err
	}
	opcode := opCodeBuf[0]

	// pattern length — 1 byte (so a subject is at most 255 bytes)
	var patternLenBuf [1]byte
	if _, err := io.ReadFull(conn, patternLenBuf[:]); err != nil {
		if err == io.EOF {
			return nil, err
		}
		log.Print(err)
		return nil, err
	}
	patternLen := patternLenBuf[0]

	// pattern — patternLen bytes
	patternBuf := make([]byte, patternLen)
	if _, err := io.ReadFull(conn, patternBuf); err != nil {
		if err == io.EOF {
			return nil, err
		}
		log.Print(err)
		return nil, err
	}
	pattern := string(patternBuf)

	// message length — 4 bytes, big-endian
	var msgLenBuf [4]byte
	if _, err := io.ReadFull(conn, msgLenBuf[:]); err != nil {
		if err == io.EOF {
			return nil, err
		}
		log.Print(err)
		return nil, err
	}
	msgLen := binary.BigEndian.Uint32(msgLenBuf[:])

	// Guard against a hostile/garbled length before allocating.
	if msgLen > maxCmdLength {
		log.Print("command rejected due to too many bytes.\n")
		return nil, fmt.Errorf("length exceeded: %d", msgLen)
	}

	// message — msgLen bytes
	message := make([]byte, msgLen)
	if _, err := io.ReadFull(conn, message); err != nil {
		if err == io.EOF {
			return nil, err
		}
		log.Print(err)
		return nil, err
	}

	return &Frame{
		opcode:  opcode,
		pattern: pattern,
		message: message,
	}, nil
}
