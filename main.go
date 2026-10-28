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

	innerLoop:
		for {
			if !scanner.Scan() {
				if err := scanner.Err(); err != nil {
					fmt.Print(err)
				}
				break
			}

			msg := scanner.Text()
			fmt.Println(msg)

			switch msg {
			case "hi":
				conn.Write([]byte("Henlo good sir\n"))
			case "hello there":
				conn.Write([]byte("General Kenobi...\n"))
			case "exit":
				conn.Write([]byte("Exiting connection\n"))
				conn.Close()
				break innerLoop
			default:
				conn.Write([]byte("You've hit default case good sir\n"))
			}
		}
	}
}
