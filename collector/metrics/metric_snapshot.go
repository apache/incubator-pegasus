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

package metrics

type MetricQueryBaseInfo struct {
	Cluster     string `json:"cluster"`
	Role        string `json:"role"`
	Host        string `json:"host"`
	Port        int    `json:"port"`
	TimestampNS int64  `json:"timestamp_ns"`
}

// All the *BriefValue* structures are used as the target objects for the http requests
// that ask for only 2 fields of each metric: "name" and "value".

type MetricQueryBriefValueSnapshot struct {
	MetricQueryBaseInfo
	Entities []MetricEntityBriefValueSnapshot `json:"entities"`
}

type MetricEntityBriefValueSnapshot struct {
	Type       string                     `json:"type"`
	ID         string                     `json:"id"`
	Attributes map[string]string          `json:"attributes"`
	Metrics    []MetricBriefValueSnapshot `json:"metrics"`
}

type MetricBriefValueSnapshot struct {
	Name  string  `json:"name"`
	Value float64 `json:"value"`
}
