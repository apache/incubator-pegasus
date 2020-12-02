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
	"github.com/pegasus-kv/admin-cli/tabular"

	"github.com/pegasus-kv/collector/aggregate"
)

// TableStat is table-stat command.
func TableStat(c *Client) error {
	ag := aggregate.NewTableStatsAggregator(c.Nodes.MetaAddresses)
	tableStats, _ := ag.Aggregate()
	// TODO(wutao): limit table count, if table count exceeds a number, the result
	// can be written to a file or somewhere instead.

	tabular.PrintTableStatsTabular(c, tableStats)
	return nil
}
