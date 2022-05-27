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
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/apache/incubator-pegasus/go-client/pegalog"
	"github.com/apache/incubator-pegasus/go-client/pegasus"
)

// This test intends to verify if go-client can failover automatically on TCP breakdown.
// Usually there is a local Pegasus cluster running in background, via docker-compose.
// We can inject network faults through https://github.com/alexei-led/pumba and see when
// Pegasus cluster is going well, whether the go-client gets work in expected time.

func main() {
	client := pegasus.NewClient(pegasus.Config{MetaServers: []string{"172.21.0.11:35601"}})
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	tb, err := client.OpenTable(ctx, "test")
	if err != nil {
		panic(err)
	}

	var firstTimeoutTime time.Time
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		err := tb.Set(ctx, []byte(time.Now().Format(time.RFC3339Nano)), []byte(""), []byte("value"))
		cancel()

		if err != nil {
			pegalog.GetLogger().Print(err)

			if !strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
				continue
			}
			if firstTimeoutTime.IsZero() {
				firstTimeoutTime = time.Now()
			}
			if time.Since(firstTimeoutTime) > 3*time.Minute {
				panic("unable to recover from failure in 3min")
			}
		}
		if err == nil && !firstTimeoutTime.IsZero() && time.Since(firstTimeoutTime) > 10*time.Minute {
			fmt.Println("test passed")
			return
		}
		time.Sleep(time.Millisecond * 500)
	}
}
