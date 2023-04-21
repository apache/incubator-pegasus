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

#pragma once

#include <fmt/core.h>
#include <string>
#include <vector>

#include "common/gpid.h"
#include "dsn.layer2_types.h"
#include "replica/replica_base.h"
#include "runtime/rpc/rpc_address.h"
#include "runtime/task/task_tracker.h"
#include "utils/error_code.h"
#include "utils/zlocks.h"

namespace dsn {
namespace replication {
class learn_response;
class replica;

class replica_follower : replica_base
{
public:
    explicit replica_follower(replica *r);
    ~replica_follower();
    error_code duplicate_checkpoint();

    const std::string &get_master_cluster_name() const { return _master_cluster_name; };

    const std::string &get_master_app_name() const { return _master_app_name; };

    const std::vector<rpc_address> &get_master_meta_list() const { return _master_meta_list; };

    const bool is_need_duplicate() const { return need_duplicate; }

private:
    replica *_replica;
    task_tracker _tracker;
    bool _duplicating_checkpoint{false};
    mutable zlock _lock;

    std::string _master_cluster_name;
    std::string _master_app_name;
    std::vector<rpc_address> _master_meta_list;
    partition_configuration _master_replica_config;

    bool need_duplicate{false};

    void init_master_info();
    void async_duplicate_checkpoint_from_master_replica();
    error_code update_master_replica_config(error_code err, query_cfg_response &&resp);
    void copy_master_replica_checkpoint();
    error_code nfs_copy_checkpoint(error_code err, learn_response &&resp);
    void nfs_copy_remote_files(const rpc_address &remote_node,
                               const std::string &remote_disk,
                               const std::string &remote_dir,
                               std::vector<std::string> &file_list,
                               const std::string &dest_dir);

    std::string master_replica_name()
    {
        std::string app_info = fmt::format("{}.{}", _master_cluster_name, _master_app_name);
        if (_master_replica_config.primary != rpc_address::s_invalid_address) {
            return fmt::format("{}({}|{})",
                               app_info,
                               _master_replica_config.primary.to_string(),
                               _master_replica_config.pid.to_string());
        }
        return app_info;
    }

    friend class replica_follower_test;
};

} // namespace replication
} // namespace dsn
