package main

import (
	"bufio"
	"fmt"
	"net"
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
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			scannedLine := scanner.Text()
			fmt.Println(scannedLine)
			switch scannedLine {
			case "hi":
				conn.Write([]byte("hi\n"))
			case "dupa":
				conn.Write([]byte("zbita\n"))
			default:
			}
		}
	}
}
