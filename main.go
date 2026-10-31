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

const maxCmdLength = 20

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	initTopics()

	listener, err := net.Listen("tcp", ":4222")
	if err != nil {
		log.Print(err)
		return
	}
	fmt.Println("Listening on port :4222")
	defer listener.Close()

	go func() {
		<-ctx.Done()
		fmt.Println("Closing server with Ctrl - C")
		listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Print(err)
			return
		}
		go handleClient(conn)
	}
}

func handleClient(conn net.Conn) {
	defer conn.Close()
	headerBuf := make([]byte, 4)
innerLoop:
	for {
		if _, err := io.ReadFull(conn, headerBuf); err != nil {
			if err == io.EOF {
				return
			}
			log.Print(err)
			return
		}

		length := binary.BigEndian.Uint32(headerBuf)
		if length > maxCmdLength {
			log.Printf("Command rejected due to too many bytes.\nAllowed length: %d bytes, cmd length: %d bytes", maxCmdLength, length)
			return
		}
		msgBuf := make([]byte, length)

		if _, err := io.ReadFull(conn, msgBuf); err != nil {
			if err == io.EOF {
				return
			}
			log.Print(err)
			return
		}
		msg := string(msgBuf)

		switch msg {
		case "ping":
			conn.Write([]byte("pong\n"))
		case "publish to":

		case "subscribe to":

		case "exit":
			conn.Write([]byte("Exiting connection\n"))
			break innerLoop
		default:
			conn.Write([]byte("Unrecognized prompt has been provided"))
		}
	}
}

func initTopics() map[string]*Topic {
	return map[string]*Topic{
		"topic1": NewTopic(),
		"topic2": NewTopic(),
		"topic3": NewTopic(),
	}
}
