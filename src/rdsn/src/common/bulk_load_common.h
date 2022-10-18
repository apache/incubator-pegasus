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

#include "bulk_load_types.h"
#include "runtime/rpc/rpc_holder.h"

namespace dsn {
namespace replication {

typedef rpc_holder<start_bulk_load_request, start_bulk_load_response> start_bulk_load_rpc;
typedef rpc_holder<bulk_load_request, bulk_load_response> bulk_load_rpc;
typedef rpc_holder<control_bulk_load_request, control_bulk_load_response> control_bulk_load_rpc;
typedef rpc_holder<query_bulk_load_request, query_bulk_load_response> query_bulk_load_rpc;
typedef rpc_holder<clear_bulk_load_state_request, clear_bulk_load_state_response>
    clear_bulk_load_rpc;

class bulk_load_constant
{
public:
    static const std::string BULK_LOAD_INFO;
    static const int32_t BULK_LOAD_REQUEST_INTERVAL;
    static const std::string BULK_LOAD_METADATA;
    static const std::string BULK_LOAD_LOCAL_ROOT_DIR;
    static const int32_t PROGRESS_FINISHED;
};
} // namespace replication
} // namespace dsn
