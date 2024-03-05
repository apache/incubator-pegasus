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
	"fmt"
	"reflect"
	"time"

	"github.com/apache/incubator-pegasus/go-client/idl/admin"
	"github.com/apache/incubator-pegasus/go-client/idl/base"
	"github.com/apache/incubator-pegasus/go-client/idl/replication"
	"github.com/apache/incubator-pegasus/go-client/session"
)

// Client provides the administration API to a specific cluster.
// Remember only the superusers configured to the cluster have the admin priviledges.
type Client interface {
	Close() error

	GetTimeout() time.Duration
	SetTimeout(timeout time.Duration)

	CreateTable(ctx context.Context, tableName string, partitionCount int, successIfExist_optional ...bool) (int32, error)

	DropTable(ctx context.Context, tableName string) error

	ListAvailTables() ([]*admin.AppInfo, error)
	ListTables(ctx context.Context) ([]*TableInfo, error)
}

// TableInfo is the table information.
type TableInfo struct {
	Name string

	// Envs is a set of attributes binding to this table.
	Envs map[string]string
}

type Config struct {
	MetaServers []string `json:"meta_servers"`
	Timeout     time.Duration
}

// NewClient returns an instance of Client.
func NewClient(cfg Config) Client {
	return &rpcBasedClient{
		meta:       session.NewMetaManager(cfg.MetaServers, session.NewNodeSession),
		rpcTimeout: cfg.Timeout,
	}
}

type rpcBasedClient struct {
	meta       *session.MetaManager
	rpcTimeout time.Duration
}

func (c *rpcBasedClient) Close() error {
	return c.meta.Close()
}

func (c *rpcBasedClient) GetTimeout() time.Duration {
	return c.rpcTimeout
}

func (c *rpcBasedClient) SetTimeout(timeout time.Duration) {
	c.rpcTimeout = timeout
}

// `callback` always accepts non-nil `resp`.
func (m *rpcBasedMeta) callMeta(methodName string, req interface{}, callback func(resp interface{})) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.rpcTimeout)
	defer cancel()

	// There are 2 kinds of structs for the retured slice:
	// * [response, error], or
	// * error.
	result := reflect.ValueOf(m.meta).MethodByName(methodName).Call([]reflect.Value{
		reflect.ValueOf(ctx),
		reflect.ValueOf(req),
	})

	// The last element must be error.
	ierr := result[len(result)-1].Interface()

	var err error
	if ierr != nil {
		err = ierr.(error)
	}

	if len(result) == 1 {
		return err
	}

	// The struct of returned slice must be [response, error].
	if !result[0].IsNil() {
		callback(result[0].Interface())
	}

	return err
}

func (c *rpcBasedClient) waitTableReady(ctx context.Context, tableName string, partitionCount int) error {
	const replicaCount int = 3

	for {
		resp, err := c.meta.QueryConfig(ctx, tableName)
		if err != nil {
			return err
		}
		if resp.GetErr().Errno != base.ERR_OK.String() {
			return fmt.Errorf("QueryConfig failed: %s", resp.GetErr().String())
		}

		readyCount := 0
		for _, part := range resp.Partitions {
			if part.Primary.GetRawAddress() != 0 && len(part.Secondaries)+1 == replicaCount {
				readyCount++
			}
		}
		if readyCount == partitionCount {
			break
		}
		time.Sleep(time.Second)
	}
	return nil
}

func (c *rpcBasedClient) CreateTable(ctx context.Context, tableName string, partitionCount int, successIfExist_optional ...bool) error {
	successIfExist := true
	if len(successIfExist_optional) > 0 {
		successIfExist = successIfExist_optional[0]
	}
	_, err := c.meta.CreateApp(ctx, &admin.ConfigurationCreateAppRequest{
		AppName: tableName,
		Options: &admin.CreateAppOptions{
			PartitionCount: int32(partitionCount),
			ReplicaCount:   3,
			SuccessIfExist: successIfExist,
			AppType:        "pegasus",
			Envs:           make(map[string]string),
			IsStateful:     true,
		},
	})
	if err != nil {
		return err
	}
	err = c.waitTableReady(ctx, tableName, partitionCount)
	return err
}

func (c *rpcBasedClient) DropTable(ctx context.Context, tableName string) error {
	req := admin.NewConfigurationDropAppRequest()
	req.AppName = tableName
	reserveSeconds := int64(1) // delete immediately. the caller is responsible for the soft deletion of table.
	req.Options = &admin.DropAppOptions{
		SuccessIfNotExist: true,
		ReserveSeconds:    &reserveSeconds,
	}
	_, err := c.meta.DropApp(ctx, req)
	return err
}

func (c *rpcBasedClient) ListTables(ctx context.Context) ([]*TableInfo, error) {
	resp, err := c.meta.ListApps(ctx, &admin.ConfigurationListAppsRequest{
		Status: replication.AppStatus_AS_AVAILABLE,
	})
	if err != nil {
		return nil, err
	}

	var results []*TableInfo
	for _, app := range resp.Infos {
		results = append(results, &TableInfo{Name: app.AppName, Envs: app.Envs})
	}
	return results, nil
}
