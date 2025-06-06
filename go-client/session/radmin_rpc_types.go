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

// Code generated by "generator -i=admin.csv > admin_rpc_types.go"; DO NOT EDIT.
package session

import (
	"context"
	"fmt"

	"github.com/apache/incubator-pegasus/go-client/idl/base"
	"github.com/apache/incubator-pegasus/go-client/idl/radmin"
)

// QueryDiskInfo is auto-generated
func (rs *ReplicaSession) QueryDiskInfo(ctx context.Context, req *radmin.QueryDiskInfoRequest) (*radmin.QueryDiskInfoResponse, error) {
	arg := radmin.NewReplicaClientQueryDiskInfoArgs()
	arg.Req = req
	result, err := rs.CallWithGpid(ctx, &base.Gpid{Appid: 0, PartitionIndex: 0}, 0, arg, "RPC_QUERY_DISK_INFO")
	if err == nil {
		ret, _ := result.(*radmin.ReplicaClientQueryDiskInfoResult)
		resp := ret.GetSuccess()
		if resp.GetErr().Errno != base.ERR_OK.String() {
			return resp, fmt.Errorf("QueryDiskInfo to session %s failed: %s", rs, resp.GetErr().String())
		}
		return resp, nil
	}
	return nil, err
}

// DiskMigrate is auto-generated
func (rs *ReplicaSession) DiskMigrate(ctx context.Context, req *radmin.ReplicaDiskMigrateRequest) (*radmin.ReplicaDiskMigrateResponse, error) {
	arg := radmin.NewReplicaClientDiskMigrateArgs()
	arg.Req = req
	result, err := rs.CallWithGpid(ctx, &base.Gpid{Appid: 0, PartitionIndex: 0}, 0, arg, "RPC_REPLICA_DISK_MIGRATE")
	if err == nil {
		ret, _ := result.(*radmin.ReplicaClientDiskMigrateResult)
		resp := ret.GetSuccess()
		if resp.GetErr().Errno != base.ERR_OK.String() {
			return resp, fmt.Errorf("DiskMigrate to session %s failed: %s", rs, resp.GetErr().String())
		}
		return resp, nil
	}
	return nil, err
}

// DetectHotkey is auto-generated
func (rs *ReplicaSession) DetectHotkey(ctx context.Context, req *radmin.DetectHotkeyRequest) (*radmin.DetectHotkeyResponse, error) {
	arg := radmin.NewReplicaClientDetectHotkeyArgs()
	arg.Req = req
	result, err := rs.CallWithGpid(ctx, &base.Gpid{Appid: 0, PartitionIndex: 0}, 0, arg, "RPC_DETECT_HOTKEY")
	if err == nil {
		ret, _ := result.(*radmin.ReplicaClientDetectHotkeyResult)
		resp := ret.GetSuccess()
		if resp.GetErr().Errno != base.ERR_OK.String() {
			return resp, fmt.Errorf("DetectHotkey to session %s failed: %s", rs, resp.GetErr().String())
		}
		return resp, nil
	}
	return nil, err
}

// AddDisk is auto-generated
func (rs *ReplicaSession) AddDisk(ctx context.Context, req *radmin.AddNewDiskRequest) (*radmin.AddNewDiskResponse, error) {
	arg := radmin.NewReplicaClientAddDiskArgs()
	arg.Req = req
	result, err := rs.CallWithGpid(ctx, &base.Gpid{Appid: 0, PartitionIndex: 0}, 0, arg, "RPC_ADD_NEW_DISK")
	if err == nil {
		ret, _ := result.(*radmin.ReplicaClientAddDiskResult)
		resp := ret.GetSuccess()
		if resp.GetErr().Errno != base.ERR_OK.String() {
			return resp, fmt.Errorf("AddDisk to session %s failed: %s", rs, resp.GetErr().String())
		}
		return resp, nil
	}
	return nil, err
}
