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

	"github.com/apache/incubator-pegasus/go-client/pegasus"
	"github.com/stretchr/testify/assert"
)

func TestAdmin_Table(t *testing.T) {
	c := NewClient(Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	})

	hasTable := func(tables []*TableInfo, tableName string) bool {
		for _, tb := range tables {
			if tb.Name == tableName {
				return true
			}
		}
		return false
	}

	err := c.DropTable(context.Background(), "admin_table_test")
	assert.Nil(t, err)

	// no such table after deletion
	tables, err := c.ListTables(context.Background())
	assert.Nil(t, err)
	assert.False(t, hasTable(tables, "admin_table_test"))

	err = c.CreateTable(context.Background(), "admin_table_test", 16)
	assert.Nil(t, err)

	tables, err = c.ListTables(context.Background())
	assert.Nil(t, err)
	assert.True(t, hasTable(tables, "admin_table_test"))

	err = c.DropTable(context.Background(), "admin_table_test")
	assert.Nil(t, err)
}

func TestAdmin_ListTablesTimeout(t *testing.T) {
	c := NewClient(Config{
		MetaServers: []string{"0.0.0.0:123456"},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_, err := c.ListTables(ctx)
	assert.Equal(t, err, context.DeadlineExceeded)
}

// Ensures after the call `CreateTable` ends, the table must be right available to access.
func TestAdmin_CreateTableMustAvailable(t *testing.T) {
	const tableName = "admin_table_test"

	c := NewClient(Config{
		MetaServers: []string{"0.0.0.0:34601"},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := c.CreateTable(context.Background(), tableName, 1)
	if !assert.NoError(t, err) {
		assert.Fail(t, err.Error())
	}

	// ensures the created table must be available for read and write
	rwClient := pegasus.NewClient(pegasus.Config{
		MetaServers: []string{"0.0.0.0:34601"},
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

	err = tb.Set(ctx, []byte("a"), []byte("a"), []byte("a"))
	if !assert.NoError(t, err) {
		assert.Fail(t, err.Error())
	}

	// cleanup
	err = c.DropTable(context.Background(), tableName)
	if !assert.NoError(t, err) {
		assert.Fail(t, err.Error())
	}
}

func TestAdmin_GetAppEnvs(t *testing.T) {
	c := NewClient(Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	})

	tables, err := c.ListTables(context.Background())
	assert.Nil(t, err)
	for _, tb := range tables {
		assert.Empty(t, tb.Envs)
	}
}
