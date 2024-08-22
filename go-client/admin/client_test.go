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

package admin

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/apache/incubator-pegasus/go-client/idl/admin"
	"github.com/apache/incubator-pegasus/go-client/idl/replication"
	"github.com/apache/incubator-pegasus/go-client/pegasus"
	"github.com/stretchr/testify/assert"
)

const (
	replicaCount   = 3
	maxWaitSeconds = 600
	reserveSeconds = 1
)

func defaultConfig() Config {
	return Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
		Timeout:     30 * time.Second,
	}
}

func defaultReplicaServerPorts() []int {
	return []int{34801, 34802, 34803}
}

func timeoutConfig() Config {
	return Config{
		MetaServers: []string{"0.0.0.0:123456"},
		Timeout:     500 * time.Millisecond,
	}
}

func testAdmin_Timeout(t *testing.T, exec func(c Client) error) {
	c := NewClient(timeoutConfig())
	assert.Equal(t, context.DeadlineExceeded, exec(c))
}

func TestAdmin_Table(t *testing.T) {
	c := NewClient(defaultConfig())

	hasTable := func(tables []*replication.AppInfo, tableName string) bool {
		for _, tb := range tables {
			if tb.AppName == tableName {
				return true
			}
		}
		return false
	}

	err := c.DropTable("admin_table_test", reserveSeconds)
	assert.Nil(t, err)

	// no such table after deletion
	tables, err := c.ListTables()
	assert.Nil(t, err)
	assert.False(t, hasTable(tables, "admin_table_test"))

	_, err = c.CreateTable("admin_table_test", 16, replicaCount, make(map[string]string), maxWaitSeconds)
	assert.Nil(t, err)

	tables, err = c.ListTables()
	assert.Nil(t, err)
	assert.True(t, hasTable(tables, "admin_table_test"))

	err = c.DropTable("admin_table_test", reserveSeconds)
	assert.Nil(t, err)
}

func TestAdmin_ListTablesTimeout(t *testing.T) {
	testAdmin_Timeout(t, func(c Client) error {
		_, err := c.ListTables()
		return err
	})
}

// Ensures after the call `CreateTable` ends, the table must be right available to access.
func TestAdmin_CreateTableMustAvailable(t *testing.T) {
	const tableName = "admin_table_test"

	c := NewClient(defaultConfig())

	_, err := c.CreateTable(tableName, 8, replicaCount, make(map[string]string), maxWaitSeconds)
	if !assert.NoError(t, err) {
		assert.Fail(t, err.Error())
	}

	// ensures the created table must be available for read and write
	rwClient := pegasus.NewClient(pegasus.Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	})
	defer func() {
		err = rwClient.Close()
		assert.NoError(t, err)
	}()

	var tb pegasus.TableConnector
	retries := 0
	for { // retry for timeout
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		tb, err = rwClient.OpenTable(ctx, tableName)
		if err != nil && strings.Contains(err.Error(), "context deadline exceeded") && retries <= 3 {
			retries++
			continue
		} else if err != nil {
			assert.Fail(t, err.Error())
			return
		} else {
			break
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err = tb.Set(ctx, []byte("a"), []byte("a"), []byte("a"))
	if !assert.NoError(t, err) {
		assert.Fail(t, err.Error())
	}

	// cleanup
	err = c.DropTable(tableName, reserveSeconds)
	if !assert.NoError(t, err) {
		assert.Fail(t, err.Error())
	}
}

func TestAdmin_GetAppEnvs(t *testing.T) {
	c := NewClient(defaultConfig())

	tables, err := c.ListTables()
	assert.Nil(t, err)
	for _, tb := range tables {
		assert.Empty(t, tb.Envs)
	}
}

func TestAdmin_ListNodes(t *testing.T) {
	c := NewClient(defaultConfig())

	nodes, err := c.ListNodes()
	assert.Nil(t, err)

	expectedReplicaServerPorts := defaultReplicaServerPorts()

	// Compare slice length.
	assert.Equal(t, len(expectedReplicaServerPorts), len(nodes))

	actualReplicaServerPorts := make([]int, len(nodes))
	for i, node := range nodes {
		// Each node should be alive.
		assert.Equal(t, admin.NodeStatus_NS_ALIVE, node.Status)
		actualReplicaServerPorts[i] = node.GetNode().GetPort()
	}

	// Match elements without extra ordering.
	assert.ElementsMatch(t, expectedReplicaServerPorts, actualReplicaServerPorts)
}

func TestAdmin_ListNodesTimeout(t *testing.T) {
	testAdmin_Timeout(t, func(c Client) error {
		_, err := c.ListNodes()
		return err
	})
}
