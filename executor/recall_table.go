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

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
)

func RecallTable(client *Client, originTableID int, newTableName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	resp, errRecall := client.Meta.RecallApp(ctx, &admin.RecallAppRequest{AppID: int32(originTableID), NewAppName_: newTableName})
	if errRecall != nil {
		return errRecall
	}

	fmt.Printf("Recalling table \"%s\", ", resp.Info.AppName)
	errWait := waitTableReady(client, resp.Info.AppName, int(resp.Info.PartitionCount), int(resp.Info.MaxReplicaCount))

	if errWait != nil {
		return errWait
	}

	return nil
}
