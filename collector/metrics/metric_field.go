// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package metrics

const (
	MetricQueryPath             string = "/metrics"
	MetricQueryWithMetricFields string = "with_metric_fields"
	MetricQueryTypes            string = "types"
	MetricQueryIDs              string = "ids"
	MetricQueryAttributes       string = "attributes"
	MetricQueryMetrics          string = "metrics"

	MetricRegistryFieldCluster     string = "cluster"
	MetricRegistryFieldRole        string = "role"
	MetricRegistryFieldHost        string = "host"
	MetricRegistryFieldPort        string = "port"
	MetricRegistryFieldTimestampNs string = "timestamp_ns"
	MetricRegistryFieldEntities    string = "entities"

	MetricEntityFieldType    string = "type"
	MetricEntityFieldId      string = "id"
	MetricEntityFieldAttrs   string = "attributes"
	MetricEntityFieldMetrics string = "metrics"

	MetricEntityTypeReplica string = "replica"
	MetricEntityTableID     string = "table_id"
	MetricEntityPartitionID string = "partition_id"

	MetricFieldType        string = "type"
	MetricFieldName        string = "name"
	MetricFieldUnit        string = "unit"
	MetricFieldDesc        string = "desc"
	MetricFieldSingleValue string = "value"

	MetricReplicaGetRequests      string = "get_requests"
	MetricReplicaMultiGetRequests string = "multi_get_requests"
	MetricReplicaBatchGetRequests string = "batch_get_requests"
	MetricReplicaScanRequests     string = "scan_requests"

	MetricReplicaPutRequests            string = "put_requests"
	MetricReplicaMultiPutRequests       string = "multi_put_requests"
	MetricReplicaRemoveRequests         string = "remove_requests"
	MetricReplicaMultiRemoveRequests    string = "multi_remove_requests"
	MetricReplicaIncrRequests           string = "incr_requests"
	MetricReplicaCheckAndSetRequests    string = "check_and_set_requests"
	MetricReplicaCheckAndMutateRequests string = "check_and_mutate_requests"
	MetricReplicaDupRequests            string = "dup_requests"
)
