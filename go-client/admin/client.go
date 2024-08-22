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

	// The timeout specify the max duration that is spent on an client request. For
	// example, if the client is based on RPC, it would be the timeout for the RPC
	// request.
	GetTimeout() time.Duration
	SetTimeout(timeout time.Duration)

	// `maxWaitSeconds` specify the number of seconds that is spent on waiting for
	// the created table to be ready. This method would return error once the table
	// is still not ready after `maxWaitSeconds`. The administrator should check if
	// there is something wrong with the table.
	CreateTable(tableName string, partitionCount int32, replicaCount int32, envs map[string]string, maxWaitSeconds int32, successIfExistOptional ...bool) (int32, error)

	// `reserveSeconds` specify the retention interval for a table before it is actually dropped.
	DropTable(tableName string, reserveSeconds int64) error

	// Empty `args` means "list all available tables"; Otherwise, the only parameter would
	// specify the status of the returned tables.
	ListTables(args ...interface{}) ([]*replication.AppInfo, error)

	// Empty `args` means "list all alive nodes"; Otherwise, the only parameter would
	// specify the status of the returned nodes.
	ListNodes(args ...interface{}) ([]*admin.NodeInfo, error)
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

// Call RPC methods(go-client/session/admin_rpc_types.go) of session.MetaManager by reflection.
// `req` and `resp` are the request and response structs of RPC. `callback` always accepts
// non-nil `resp`.
func (c *rpcBasedClient) callMeta(methodName string, req interface{}, callback func(resp interface{})) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.rpcTimeout)
	defer cancel()

	// There are 2 kinds of structs for the result which could be processed:
	// * error
	// * (response, error)
	result := reflect.ValueOf(c.meta).MethodByName(methodName).Call([]reflect.Value{
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

	// The struct of result must be (response, error).
	if !result[0].IsNil() {
		callback(result[0].Interface())
	}

	return err
}

func (c *rpcBasedClient) waitTableReady(tableName string, partitionCount int32, replicaCount int32, maxWaitSeconds int32) error {
	for ; maxWaitSeconds > 0; maxWaitSeconds-- {
		var resp *replication.QueryCfgResponse
		err := c.callMeta("QueryConfig", tableName, func(iresp interface{}) {
			resp = iresp.(*replication.QueryCfgResponse)
		})
		if err != nil {
			return err
		}
		if resp.GetErr().Errno != base.ERR_OK.String() {
			return fmt.Errorf("QueryConfig failed: %s", base.GetResponseError(resp))
		}

		readyCount := int32(0)
		for _, part := range resp.Partitions {
			if part.Primary.GetRawAddress() != 0 && int32(len(part.Secondaries)+1) == replicaCount {
				readyCount++
			}
		}
		if readyCount == partitionCount {
			break
		}
		time.Sleep(time.Second)
	}

	if maxWaitSeconds <= 0 {
		return fmt.Errorf("After %d seconds, table '%s' is still not ready", maxWaitSeconds, tableName)
	}

	return nil
}

func (c *rpcBasedClient) CreateTable(tableName string, partitionCount int32, replicaCount int32, envs map[string]string, maxWaitSeconds int32, successIfExistOptional ...bool) (int32, error) {
	successIfExist := true
	if len(successIfExistOptional) > 0 {
		successIfExist = successIfExistOptional[0]
	}

	req := &admin.ConfigurationCreateAppRequest{
		AppName: tableName,
		Options: &admin.CreateAppOptions{
			PartitionCount: int32(partitionCount),
			ReplicaCount:   replicaCount,
			SuccessIfExist: successIfExist,
			AppType:        "pegasus",
			IsStateful:     true,
			Envs:           envs,
		}}

	var appID int32
	var respErr error
	err := c.callMeta("CreateApp", req, func(iresp interface{}) {
		resp := iresp.(*admin.ConfigurationCreateAppResponse)
		appID = resp.Appid
		respErr = base.GetResponseError(resp)
	})
	if err != nil {
		return appID, err
	}

	err = c.waitTableReady(tableName, partitionCount, replicaCount, maxWaitSeconds)
	if err != nil {
		return appID, err
	}

	return appID, respErr
}

func (c *rpcBasedClient) DropTable(tableName string, reserveSeconds int64) error {
	req := &admin.ConfigurationDropAppRequest{
		AppName: tableName,
		Options: &admin.DropAppOptions{
			SuccessIfNotExist: true,
			ReserveSeconds:    &reserveSeconds, // Optional for thrift
		},
	}

	var respErr error
	err := c.callMeta("DropApp", req, func(iresp interface{}) {
		respErr = base.GetResponseError(iresp.(*admin.ConfigurationDropAppResponse))
	})
	if err != nil {
		return err
	}

	return respErr
}

func (c *rpcBasedClient) listTables(status replication.AppStatus) ([]*replication.AppInfo, error) {
	req := &admin.ConfigurationListAppsRequest{
		Status: status,
	}

	var tables []*replication.AppInfo
	var respErr error
	err := c.callMeta("ListApps", req, func(iresp interface{}) {
		resp := iresp.(*admin.ConfigurationListAppsResponse)
		tables = resp.Infos
		respErr = base.GetResponseError(resp)
	})
	if err != nil {
		return tables, err
	}

	return tables, respErr
}

func (c *rpcBasedClient) ListTables(args ...interface{}) ([]*replication.AppInfo, error) {
	if len(args) == 0 {
		return c.listTables(replication.AppStatus_AS_AVAILABLE)
	}
	return c.listTables(args[0].(replication.AppStatus))
}

func (c *rpcBasedClient) listNodes(status admin.NodeStatus) ([]*admin.NodeInfo, error) {
	req := &admin.ConfigurationListNodesRequest{
		Status: status,
	}

	var nodes []*admin.NodeInfo
	var respErr error
	err := c.callMeta("ListNodes", req, func(iresp interface{}) {
		resp := iresp.(*admin.ConfigurationListNodesResponse)
		nodes = resp.Infos
		respErr = base.GetResponseError(resp)
	})
	if err != nil {
		return nodes, err
	}

	return nodes, respErr
}

func (c *rpcBasedClient) ListNodes(args ...interface{}) ([]*admin.NodeInfo, error) {
	if len(args) == 0 {
		return c.listNodes(admin.NodeStatus_NS_ALIVE)
	}
	return c.listNodes(args[0].(admin.NodeStatus))
}
