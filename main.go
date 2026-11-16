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

// Response opcodes. Offset by 0x80 so they never collide
const (
	RespAck  byte = 0x80
	RespMsg  byte = 0x81
	RespPong byte = 0x82
	RespInfo byte = 0x83
	RespErr  byte = 0x84
)

type Frame struct {
	opcode  byte
	subject string
	message []byte
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	broker := NewBroker()
	wal := NewWAL()
	defer wal.Close()

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
		go handleClient(conn, broker, ctx, wal)
	}
}

func handleClient(conn net.Conn, broker *Broker, sysCtx context.Context, wal *WAL) {
	defer conn.Close()
	cw := newConnectionWriter(conn)
	clientCtx, cancel := context.WithCancel(sysCtx)
	defer cancel() // fires when handleClient returns -> wakes subscriber goroutine
	var subscriberContexts []context.CancelFunc

innerLoop:
	for {
		frame, err := readFrame(conn)
		if err != nil {
			return
		}

		switch frame.opcode {
		case OpPing:
			cw.write(RespPong, []byte("pong"))

		case OpPublish:
			if len(frame.message) == 0 {
				return // malformed publish - drop the connection
			}
			if err := broker.publish(frame.subject, frame.message, wal); err != nil {
				cw.write(RespErr, []byte(err.Error()))
				return
			}
			cw.write(RespAck, nil)

		case OpSubscribe:
			subCtx, subCancel := context.WithCancel(clientCtx)
			subChan := broker.subscribe(frame.subject)
			subscriberContexts = append(subscriberContexts, subCancel)

			go func(subChan chan []byte, ctx context.Context) {
				defer broker.unsubscribe(subChan)
				for {
					select {
					case msg := <-subChan: // a message arrived - deliver it
						if err := cw.write(RespMsg, msg); err != nil {
							return
						}
					case <-ctx.Done(): // canceled (unsubscribe, or disconnect via parent)
						return
					}
				}
			}(subChan, subCtx)

		case OpUnsubscribe:
			// call each cancel func, then forget them.
			for i := range subscriberContexts {
				subscriberContexts[i]()
			}
			subscriberContexts = nil

		case OpExit:
			cw.write(RespInfo, []byte("exiting connection"))
			break innerLoop

		default:
			cw.write(RespErr, []byte("unrecognized opcode"))
		}
	}
}

var BufPool = sync.Pool{
	New: func() any {
		return make([]byte, maxBufferSize)
	},
}

func readFrame(conn net.Conn) (*Frame, error) {
	// reusable byte slice from the pool with capacity: maxBufferSize
	buffer := BufPool.Get().([]byte)
	defer BufPool.Put(buffer)

	// opcode - 1 byte at buffer index: 0
	if _, err := io.ReadFull(conn, buffer[:1]); err != nil {
		if err == io.EOF {
			return nil, err // client hung up - normal
		}
		log.Print(err)
		return nil, err
	}
	opcode := buffer[0]

	// pattern length - 1 byte at buffer index: 1
	if _, err := io.ReadFull(conn, buffer[1:2]); err != nil {
		if err == io.EOF {
			return nil, err
		}
		log.Print(err)
		return nil, err
	}
	patternLen := int(buffer[1])

	// pattern - patternLen bytes buffer range: from index 2 to 2 + patternLen
	patternEnd := 2 + patternLen
	if _, err := io.ReadFull(conn, buffer[2:patternEnd]); err != nil {
		if err == io.EOF {
			return nil, err
		}
		log.Print(err)
		return nil, err
	}
	pattern := string(buffer[2:patternEnd])

	// message length - 4 bytes, bigendian buffer range: from patternEnd to patternEnd + 4
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

	// guard against a hostile length before allocating
	if msgLen > maxCmdLength {
		log.Print("command rejected due to too many bytes.\n")
		return nil, fmt.Errorf("length exceeded: %d", msgLen)
	}

	// message - msgLen bytes
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
		subject: pattern,
		message: message,
	}, nil
}

type connectionWriter struct {
	mu   sync.Mutex
	conn net.Conn
}

func newConnectionWriter(conn net.Conn) *connectionWriter {
	return &connectionWriter{conn: conn}
}

func (cw *connectionWriter) write(opcode byte, payload []byte) error {
	cw.mu.Lock()
	defer cw.mu.Unlock()

	buf := make([]byte, 0, 5+len(payload))
	buf = append(buf, opcode)
	l := make([]byte, 4)
	binary.BigEndian.PutUint32(l, uint32(len(payload)))
	buf = append(buf, l...)
	buf = append(buf, payload...)
	_, err := cw.conn.Write(buf)
	return err
}
