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

#include <gtest/gtest_prod.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <iomanip> // std::setfill, std::setw
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "backup_types.h"
#include "common/gpid.h"
#include "common/json_helper.h"
#include "common/replication_other_types.h"
#include "meta_rpc_types.h"
#include "runtime/task/task.h"
#include "runtime/task/task_tracker.h"
#include "utils/api_utilities.h"
#include "utils/autoref_ptr.h"
#include "utils/error_code.h"
#include "utils/fmt_utils.h"
#include "utils/metrics.h"
#include "utils/ports.h"
#include "utils/zlocks.h"

namespace dsn {
class message_ex;
class rpc_address;

namespace dist {
namespace block_service {
class block_filesystem;
} // namespace block_service
} // namespace dist

namespace replication {

class backup_engine;
class backup_service;
class meta_service;
class server_state;

// TODO(heyuchen): implement it
class backup_service
{
public:
    explicit backup_service(meta_service *meta_svc,
                            const std::string &policy_meta_root,
                            const std::string &backup_root);
    void start();

    void start_backup_app(start_backup_app_rpc rpc);
    void query_backup_status(query_backup_status_rpc rpc);

    meta_service *get_meta_service() const { return _meta_svc; }
    server_state *get_state() const { return _state; }

    const std::string &backup_root() const { return _backup_root; }
    const std::string &policy_root() const { return _policy_meta_root; }

private:
    friend class backup_engine;

    meta_service *_meta_svc;
    server_state *_state;

    // the root of policy metas, stored on remote_storage(zookeeper)
    std::string _policy_meta_root;
    // the root of cold backup data, stored on block service
    std::string _backup_root;
};

} // namespace replication
} // namespace dsn

//USER_DEFINED_STRUCTURE_FORMATTER(::dsn::replication::backup_start_time);
