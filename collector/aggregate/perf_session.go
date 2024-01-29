// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package aggregate

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/incubator-pegasus/go-client/admin"
	"github.com/apache/incubator-pegasus/go-client/session"
	"github.com/tidwall/gjson"
)

// PerfSession is a client to get perf-counters from a Pegasus ReplicaServer.
type PerfSession struct {
	session.NodeSession

	Address string
}

// PerfCounter is a Pegasus perf-counter.
type PerfCounter struct {
	Name  string
	Value float64
}

func (p *PerfCounter) String() string {
	return fmt.Sprintf("{Name: %s, Value: %f}", p.Name, p.Value)
}

// NewPerfSession returns an instance of PerfSession.
func NewPerfSession(addr string) *PerfSession {
	return &PerfSession{
		Address:     addr,
		NodeSession: session.NewNodeSession(addr, session.NodeTypeReplica),
	}
}

// WrapPerf returns an instance of PerfSession using an existed session.
func WrapPerf(addr string, session session.NodeSession) *PerfSession {
	return &PerfSession{
		Address:     addr,
		NodeSession: session,
	}
}

// GetPerfCounters retrieves all perf-counters matched with `filter` from the remote node.
func (c *PerfSession) GetPerfCounters(filter string) ([]*PerfCounter, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	rc := &admin.RemoteCommand{
		Command:   "perf-counters-by-substr",
		Arguments: []string{filter},
	}

	result, err := rc.Call(ctx, c.NodeSession)
	if err != nil {
		return nil, err
	}
	resultJSON := gjson.Parse(result)
	perfCounters := resultJSON.Get("counters").Array()
	var ret []*PerfCounter
	for _, perfCounter := range perfCounters {
		ret = append(ret, &PerfCounter{
			Name:  perfCounter.Get("name").String(),
			Value: perfCounter.Get("value").Float(),
		})
	}
	return ret, nil
}

// Close terminates the session to replica.
func (c *PerfSession) Close() {
	c.NodeSession.Close()
}
