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
	"encoding/json"
	"fmt"

	"github.com/apache/incubator-pegasus/go-client/idl/admin"
)

// QueryDuplication command
func QueryDuplication(c *Client, tableName string) error {
	resp, err := c.Meta.QueryDuplication(tableName)
	if err != nil {
		return err
	}
	// formats into JSON
	outputBytes, _ := json.MarshalIndent(resp.EntryList, "", "  ")
	fmt.Fprintln(c, string(outputBytes))
	return nil
}

// AddDuplication command
func AddDuplication(c *Client, tableName string, remoteCluster string, duplicateCheckpoint bool) error {
	resp, err := c.Meta.AddDuplication(tableName, remoteCluster, duplicateCheckpoint)
	if err != nil {
		return err
	}
	fmt.Fprintf(c, "successfully add duplication [dupid: %d]\n", resp.Dupid)
	return nil
}

// ModifyDuplication command
func ModifyDuplication(c *Client, tableName string, dupid int, status admin.DuplicationStatus) error {
	err := c.Meta.ModifyDuplication(tableName, dupid, status)
	if err != nil {
		return err
	}
	fmt.Fprintf(c, "%s table \"%s\" done [dupid: %d]\n", status, tableName, dupid)
	return nil
}
