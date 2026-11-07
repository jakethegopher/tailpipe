package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
)

const (
	maxCmdLength  = 1024
	maxBufferSize = 1024
)
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

var BufPool = sync.Pool{
	New: func() any {
		return make([]byte, maxBufferSize)
	},
}

func readFrame(conn net.Conn) (*Frame, error) {
	// Get a reusable byte slice from the pool (capacity: maxCmdLength = 1024 bytes)
	buffer := BufPool.Get().([]byte)
	defer BufPool.Put(buffer)

	// 1. opcode — 1 byte (buffer index: 0)
	if _, err := io.ReadFull(conn, buffer[:1]); err != nil {
		if err == io.EOF {
			return nil, err // client hung up — normal
		}
		log.Print(err)
		return nil, err
	}
	opcode := buffer[0]

	// 2. pattern length — 1 byte (buffer index: 1)
	if _, err := io.ReadFull(conn, buffer[1:2]); err != nil {
		if err == io.EOF {
			return nil, err
		}
		log.Print(err)
		return nil, err
	}
	patternLen := int(buffer[1])

	// 3. pattern — patternLen bytes (buffer range: from index 2 to 2 + patternLen)
	patternEnd := 2 + patternLen
	if _, err := io.ReadFull(conn, buffer[2:patternEnd]); err != nil {
		if err == io.EOF {
			return nil, err
		}
		log.Print(err)
		return nil, err
	}
	pattern := string(buffer[2:patternEnd])

	// 4. message length — 4 bytes, big-endian (buffer range: from patternEnd to patternEnd + 4)
	msgLenStart := patternEnd
	msgLenEnd := msgLenStart + 4
	if msgLenEnd > maxBufferSize {
		return nil, errors.New("frame header exceeds max limit")
	}

	if _, err := io.ReadFull(conn, buffer[msgLenStart:msgLenEnd]); err != nil {
		if err == io.EOF {
			return nil, err
		}
		log.Print(err)
		return nil, err
	}
	msgLen := binary.BigEndian.Uint32(buffer[msgLenStart:msgLenEnd])

	// Guard against a hostile/garbled length before allocating.
	if msgLen > maxCmdLength {
		log.Print("command rejected due to too many bytes.\n")
		return nil, fmt.Errorf("length exceeded: %d", msgLen)
	}

	// 5. message — msgLen bytes (allocated separately so it survives pool recycling)
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
