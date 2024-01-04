// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package webui

import (
	"github.com/apache/incubator-pegasus/collector/aggregate"
	"github.com/kataras/iris/v12"
)

var indexPageClusterStats = []string{
	"write_bytes",
	"read_bytes",
	"write_qps",
	"read_qps",
}

func renderIndexClusterCharts(ctx iris.Context) {
	type perfCounterHTML struct {
		PerfCounter string
		Values      []float64
	}
	var PerfCounters []*perfCounterHTML

	snapshots := aggregate.SnapshotClusterStats()
	for _, s := range indexPageClusterStats {
		PerfCounters = append(PerfCounters, &perfCounterHTML{
			PerfCounter: s,
		})
		p := PerfCounters[len(PerfCounters)-1]
		for _, snapshot := range snapshots {
			if v, found := snapshot.Stats[s]; found {
				p.Values = append(p.Values, v)
			}
		}
	}
	ctx.ViewData("PerfCounters", PerfCounters)

	var PerfIDs []string
	for _, sn := range snapshots {
		PerfIDs = append(PerfIDs, sn.Timestamp.Format("15:04:00"))
	}
	ctx.ViewData("PerfIDs", PerfIDs)
}

func indexHandler(ctx iris.Context) {
	renderIndexClusterCharts(ctx)

	// metaClient := client(viper.GetString("meta_server"))
	// tables, err := metaClient.ListTables()
	// if err != nil {
	// 	ctx.ResponseWriter().WriteString("Failed to list tables from MetaServer")
	// 	ctx.StatusCode(iris.StatusInternalServerError)
	// 	return
	// }
	// type tableHTMLRow struct {
	// 	TableName string
	// 	Link      string
	// }
	// var Tables []tableHTMLRow
	// for _, tb := range tables {
	// 	Tables = append(Tables, tableHTMLRow{TableName: tb.TableName})
	// }
	// ctx.ViewData("Tables", Tables)

	err := ctx.View("index.html")
	if err != nil {
		return
	}
}
