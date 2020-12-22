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
	"fmt"
	"time"
)

func Get(rootCtx *Context, hashKeyStr, sortkeyStr string) error {
	pegasusArgs, err := readPegasusArgs(rootCtx, []string{hashKeyStr, sortkeyStr})
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	rawValue, err := rootCtx.UseTable.Get(ctx, pegasusArgs[0], pegasusArgs[1])
	if err != nil {
		return err
	}
	if rawValue == nil {
		return fmt.Errorf("record not found\nHASH_KEY=%s\nSORT_KEY=%s", hashKeyStr, sortkeyStr)
	}
	value, err := rootCtx.ValueEnc.DecodeAll(rawValue)
	if err != nil {
		return err
	}
	fmt.Fprintf(rootCtx, "\n%s : %s : %s\n", hashKeyStr, sortkeyStr, value)
	return nil
}
