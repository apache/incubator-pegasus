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

#pragma once

#include "partition_split_types.h"
#include "runtime/rpc/rpc_holder.h"

namespace dsn {
namespace replication {
typedef rpc_holder<start_partition_split_request, start_partition_split_response> start_split_rpc;
typedef rpc_holder<control_split_request, control_split_response> control_split_rpc;
typedef rpc_holder<query_split_request, query_split_response> query_split_rpc;
typedef rpc_holder<register_child_request, register_child_response> register_child_rpc;
typedef rpc_holder<notify_stop_split_request, notify_stop_split_response> notify_stop_split_rpc;
typedef rpc_holder<query_child_state_request, query_child_state_response> query_child_state_rpc;
} // namespace replication
} // namespace dsn
