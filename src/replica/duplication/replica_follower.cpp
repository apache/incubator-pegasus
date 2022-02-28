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

#include "replica_follower.h"
#include "replica/replica_stub.h"
#include "dsn/utility/filesystem.h"
#include "dsn/dist/replication/duplication_common.h"

#include <boost/algorithm/string.hpp>
#include <dsn/tool-api/group_address.h>
#include <dsn/dist/nfs_node.h>

namespace dsn {
namespace replication {

replica_follower::replica_follower(replica *r) : replica_base(r), _replica(r)
{
    init_master_info();
}

replica_follower::~replica_follower() { _tracker.wait_outstanding_tasks(); }

void replica_follower::init_master_info()
{
    const auto &envs = _replica->get_app_info()->envs;

    if (envs.find(duplication_constants::kDuplicationEnvMasterClusterKey) == envs.end() ||
        envs.find(duplication_constants::kDuplicationEnvMasterMetasKey) == envs.end()) {
        return;
    }

    need_duplicate = true;

    _master_cluster_name = envs.at(duplication_constants::kDuplicationEnvMasterClusterKey);
    _master_app_name = _replica->get_app_info()->app_name;

    const auto &meta_list_str = envs.at(duplication_constants::kDuplicationEnvMasterMetasKey);
    std::vector<std::string> metas;
    boost::split(metas, meta_list_str, boost::is_any_of(","));
    dassert_f(!metas.empty(), "master cluster meta list is invalid!");
    for (const auto &meta : metas) {
        dsn::rpc_address node;
        dassert_f(node.from_string_ipv4(meta.c_str()), "{} is invalid meta address", meta);
        _master_meta_list.emplace_back(std::move(node));
    }
}

// todo(jiashuo1)
error_code replica_follower::duplicate_checkpoint() { return ERR_OK; }

} // namespace replication
} // namespace dsn
