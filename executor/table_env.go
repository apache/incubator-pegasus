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
)

// ListAppEnvs command
func ListAppEnvs(c *Client, useTable string) error {
	tables, err := c.Meta.ListAvailableApps()
	if err != nil {
		return err
	}

	for _, app := range tables {
		if app.AppName == useTable {
			outputBytes, _ := json.MarshalIndent(app.Envs, "", "  ")
			fmt.Fprintln(c, string(outputBytes))
			return nil
		}
	}
	return nil
}

// SetAppEnv command
func SetAppEnv(c *Client, useTable string, key, value string) error {
	return wrapUpdateAppEnvError(c.Meta.UpdateAppEnvs(useTable, map[string]string{key: value}))
}

// ClearAppEnv command
func ClearAppEnv(c *Client, useTable string) error {
	return wrapUpdateAppEnvError(c.Meta.ClearAppEnvs(useTable, "" /*empty prefix means clear of all*/))
}

// DelAppEnv command
// TODO(wutao): deleting a non-existed key returns "OK" now.
func DelAppEnv(c *Client, useTable string, key string, deletePrefix bool) error {
	if deletePrefix {
		return wrapUpdateAppEnvError(c.Meta.ClearAppEnvs(useTable, key))
	}
	return wrapUpdateAppEnvError(c.Meta.DelAppEnvs(useTable, []string{key}))
}

func wrapUpdateAppEnvError(err error) error {
	if err != nil {
		return fmt.Errorf("failed to update app envs:\nErr:%s", err)
	}
	return nil
}
