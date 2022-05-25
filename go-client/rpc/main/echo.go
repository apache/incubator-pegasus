// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package main

import (
	"bufio"
	"encoding/hex"
	"log"
	"net"
)

// This is an echo server(localhost:8800) we used for testing
// the rpc implementation. For debugging purpose we don't use
// the inetd echo service (port 7).

func main() {
	log.Printf("echo-server listening on tcp port 8800")
	ln, err := net.Listen("tcp", ":8800")
	if err != nil {
		log.Fatalf("listen error, err=%s", err)
	}

	accepted := 0
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatalf("accept error, err=%s", err)
		}
		accepted++
		go handleConnection(conn)
		log.Printf("connection accepted %d", accepted)
	}
}

func handleConnection(conn net.Conn) {
	bufr := bufio.NewReader(conn)
	buf := make([]byte, 1024)

	for {
		readBytes, err := bufr.Read(buf)
		if err != nil {
			log.Printf("handle connection error, err=%s", err)
			conn.Close()
			return
		}
		log.Printf("<->\n%s", hex.Dump(buf[:readBytes]))
		conn.Write(buf[:readBytes])
	}
}
