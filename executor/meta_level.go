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
	"strings"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
)

// GetMetaLevel command
func GetMetaLevel(c *Client) error {
	oldLevel, err := c.Meta.MetaControl(admin.MetaFunctionLevel_fl_invalid)
	if err != nil {
		return err
	}

	var result = map[string]string{
		"meta_level": oldLevel.String(),
	}
	outputBytes, _ := json.MarshalIndent(result, "", "  ")
	fmt.Fprintln(c, string(outputBytes))
	return nil
}

// SetMetaLevel command
func SetMetaLevel(c *Client, lvlStr string) error {
	metaLvl, err := admin.MetaFunctionLevelFromString("fl_" + strings.ToLower(lvlStr))
	if err != nil {
		return err
	}

	oldLevel, err := c.Meta.MetaControl(metaLvl)
	if err != nil {
		return err
	}

	fmt.Fprintf(c, "Successfully update meta function level from \"%s\" to \"%s\"\n", oldLevel.String(), metaLvl.String())
	return nil
}
