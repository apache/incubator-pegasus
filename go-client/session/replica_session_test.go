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

package session

import (
	"testing"

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestReplicaManager_GetReplica(t *testing.T) {
	defer leaktest.Check(t)()

	rm := NewReplicaManager(NewNodeSession)
	defer rm.Close()

	r1 := rm.GetReplica("127.0.0.1:34802")
	assert.Equal(t, len(rm.replicas), 1)

	r2 := rm.GetReplica("127.0.0.1:34802")
	assert.Equal(t, r1, r2)
}

func TestReplicaManager_Close(t *testing.T) {
	defer leaktest.Check(t)()

	rm := NewReplicaManager(NewNodeSession)

	rm.GetReplica("127.0.0.1:34801")
	rm.GetReplica("127.0.0.1:34802")
	rm.GetReplica("127.0.0.1:34803")

	rm.Close()
}

func TestReplicaManager_UnresponsiveHandler(t *testing.T) {
	defer leaktest.Check(t)()

	rm := NewReplicaManager(NewNodeSession)
	defer rm.Close()

	r := rm.GetReplica("127.0.0.1:34801")
	n := r.NodeSession.(*nodeSession)
	assert.Nil(t, n.unresponsiveHandler)

	handler := func(NodeSession) {}
	rm.SetUnresponsiveHandler(handler)
	r = rm.GetReplica("127.0.0.1:34802")
	n = r.NodeSession.(*nodeSession)
	assert.NotNil(t, n.unresponsiveHandler)
}
