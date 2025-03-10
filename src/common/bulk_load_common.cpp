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

#include "bulk_load_common.h"

namespace dsn::replication {
const std::string bulk_load_constant::BULK_LOAD_INFO("bulk_load_info");
const int32_t bulk_load_constant::BULK_LOAD_REQUEST_INTERVAL = 10000;
const int32_t bulk_load_constant::BULK_LOAD_INGEST_REQUEST_INTERVAL = 150;
const std::string bulk_load_constant::BULK_LOAD_METADATA("bulk_load_metadata");
const std::string bulk_load_constant::BULK_LOAD_LOCAL_ROOT_DIR("bulk_load");
const int32_t bulk_load_constant::PROGRESS_FINISHED = 100;
} // namespace dsn::replication
