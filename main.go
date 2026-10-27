package main

import (
	"bufio"
	"fmt"
	"net"
)

func main() {

	ln, err := net.Listen("tcp", ":4222")
	if err != nil {
		return
	}
	fmt.Println("Listening on port :4222")
	defer ln.Close()
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			scannedLine := scanner.Text()
			fmt.Println(scannedLine)
		}
	}
}
