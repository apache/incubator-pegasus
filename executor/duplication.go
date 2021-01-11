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

package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
)

// QueryDuplication command
func QueryDuplication(c *Client, tableName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	resp, err := c.Meta.QueryDuplication(ctx, &admin.DuplicationQueryRequest{
		AppName: tableName,
	})
	if err != nil {
		return err
	}
	// formats into JSON
	outputBytes, _ := json.MarshalIndent(resp.EntryList, "", "  ")
	fmt.Fprintln(c, string(outputBytes))
	return nil
}

// AddDuplication command
func AddDuplication(c *Client, tableName string, remoteCluster string, freezed bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	resp, err := c.Meta.AddDuplication(ctx, &admin.DuplicationAddRequest{
		AppName:           tableName,
		RemoteClusterName: remoteCluster,
		Freezed:           freezed,
	})
	if err != nil {
		if resp != nil && resp.IsSetHint() {
			return fmt.Errorf("%s\nHint: %s", err, resp.GetHint())
		}
		return err
	}
	fmt.Fprintf(c, "successfully add duplication [dupid: %d]\n", resp.Dupid)
	return nil
}

// ModifyDuplication command
func ModifyDuplication(c *Client, tableName string, dupid int, status admin.DuplicationStatus) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	_, err := c.Meta.ModifyDuplication(ctx, &admin.DuplicationModifyRequest{
		AppName: tableName,
		Dupid:   int32(dupid),
		Status:  &status,
	})
	if err != nil {
		return err
	}
	fmt.Fprintf(c, "%s table \"%s\" done [dupid: %d]\n", status, tableName, dupid)
	return nil
}
