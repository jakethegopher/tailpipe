package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

func main() {

	listener, err := net.Listen("tcp", ":4222")
	if err != nil {
		return
	}
	fmt.Println("Listening on port :4222")
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		reader := bufio.NewReader(conn)
		//writer := bufio.NewWriter(conn)
		for {
			msg, _, err := reader.ReadLine()
			if err != nil {
				os.Exit(1)
			}
			fmt.Println(string(msg))

			switch string(msg) {
			case "hi":
				conn.Write([]byte(("Hello there\n")))
			case "dupa":
				conn.Write([]byte("Zbita\n"))
			default:
				conn.Write([]byte("Default response\n"))
			}
		}
	}
}
