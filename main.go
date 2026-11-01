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

const (
	maxCmdLength = 20
	maxMsgLength = 1024
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	topic := NewTopic()

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
		go handleClient(conn, topic, ctx)
	}
}

func handleClient(conn net.Conn, topic *Topic, ctx context.Context) {
	defer conn.Close()
	headerBuf := make([]byte, 4)
	ctx, cancel := context.WithCancel(ctx)

innerLoop:
	for {
		msg, err := readFrame(conn, headerBuf, maxCmdLength)
		if err != nil {
			log.Print(err)
			return
		}

		switch string(msg) {
		case "ping":
			conn.Write([]byte("pong\n"))
		case "publish":
			conn.Write([]byte("provide msg\n"))
			cmd, err := readFrame(conn, headerBuf, maxMsgLength)
			if err != nil {
				log.Print(err)
				return
			}
			topic.publish(cmd)
		case "subscribe":
			subChan := topic.subscribe()
			for {
				select {
					case : _, err := conn.Write(msg)
					 if err != nil {
						topic.unsubscribe(subChan)
						return
					}
					case <-ctx.Done()
				}
			}

		case "exit":
			conn.Write([]byte("exiting connection\n"))
			break innerLoop
		default:
			conn.Write([]byte("unrecognized prompt has been provided"))
		}
	}
}

func readFrame(conn net.Conn, headerBuf []byte, maxLength uint32) ([]byte, error) {
	if _, err := io.ReadFull(conn, headerBuf); err != nil {
		if err == io.EOF {
			return nil, err
		}
		log.Print(err)
		return nil, err
	}

	length := binary.BigEndian.Uint32(headerBuf)
	if length > maxLength {
		log.Print("command rejected due to too many bytes.\n")
		return nil, fmt.Errorf("length exceeded: %d", length)
	}
	msgBuf := make([]byte, length)

	if _, err := io.ReadFull(conn, msgBuf); err != nil {
		if err == io.EOF {
			return nil, err
		}
		log.Print(err)
		return nil, err
	}
	return msgBuf, nil
}
