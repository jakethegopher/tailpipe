package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
)

func main() {
	listener, err := net.Listen("tcp", ":4222")
	if err != nil {
		log.Print(err)
		return
	}
	fmt.Println("Listening on port :4222")
	defer listener.Close()

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
	lengthBuf := make([]byte, 4)
innerLoop:
	for {
		if _, err := io.ReadFull(conn, lengthBuf); err != nil {
			if err == io.EOF {
				return
			}
			log.Print(err)
			return
		}

		length := binary.BigEndian.Uint32(lengthBuf)
		msgBuf := make([]byte, length)

		if _, err := io.ReadFull(conn, msgBuf); err != nil {
			if err == io.EOF {
				return
			}
			log.Print(err)
			return
		}
		msg := string(msgBuf)
		fmt.Print(msg)

		switch msg {
		case "hi":
			conn.Write([]byte("Henlo good sir\n"))
		case "hello there":
			conn.Write([]byte("General Kenobi...\n"))
		case "exit":
			conn.Write([]byte("Exiting connection\n"))
			break innerLoop
		default:
			conn.Write([]byte("You've hit default case good sir\n"))
		}
	}
}
