/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
