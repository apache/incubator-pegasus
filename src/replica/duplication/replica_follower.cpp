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
#include <dsn/utility/fail_point.h>

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

error_code replica_follower::duplicate_checkpoint()
{
    if (_duplicating_checkpoint) {
        dwarn_replica("duplicate master[{}] checkpoint is running", master_replica_name());
        return ERR_BUSY;
    }

    ddebug_replica("start duplicate master[{}] checkpoint", master_replica_name());
    zauto_lock l(_lock);
    _duplicating_checkpoint = true;
    async_duplicate_checkpoint_from_master_replica();
    _tracker.wait_outstanding_tasks();
    _duplicating_checkpoint = false;
    return _tracker.all_tasks_success() ? ERR_OK : ERR_CORRUPTION;
}

void replica_follower::async_duplicate_checkpoint_from_master_replica()
{
    rpc_address meta_servers;
    meta_servers.assign_group(_master_cluster_name.c_str());
    meta_servers.group_address()->add_list(_master_meta_list);

    configuration_query_by_index_request meta_config_request;
    meta_config_request.app_name = _master_app_name;
    meta_config_request.partition_indices = {get_gpid().get_partition_index()};

    ddebug_replica("query master[{}] replica configuration", master_replica_name());
    dsn::message_ex *msg = dsn::message_ex::create_request(
        RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX, 0, get_gpid().thread_hash());
    dsn::marshall(msg, meta_config_request);
    rpc::call(meta_servers,
              msg,
              &_tracker,
              [&](error_code err, configuration_query_by_index_response &&resp) mutable {
                  tasking::enqueue(LPC_DUPLICATE_CHECKPOINT, &_tracker, [=]() mutable {
                      FAIL_POINT_INJECT_F("duplicate_checkpoint_ok", [&](string_view s) -> void {
                          _tracker.set_success();
                          return;
                      });

                      FAIL_POINT_INJECT_F("duplicate_checkpoint_failed",
                                          [&](string_view s) -> void { return; });

                      update_master_replica_config_callback(err, std::move(resp));
                  });
              });
}

// todo(jiashuo1)
void replica_follower::update_master_replica_config_callback(
    error_code err, configuration_query_by_index_response &&resp)
{
    copy_master_replica_checkpoint(_master_replica_config.primary, _master_replica_config.pid);
}

// todo(jiashuo1)
void replica_follower::copy_master_replica_checkpoint(const rpc_address &node, const gpid &pid)
{
    query_last_checkpoint_info_callback(ERR_OK, learn_response() /* todo(jiashuo1): placeholder */);
}

// todo(jiashuo1)
void replica_follower::query_last_checkpoint_info_callback(error_code err, learn_response &&resp)
{
    nfs_copy_remote_files(resp.address,
                          resp.replica_disk_tag,
                          resp.base_local_dir,
                          resp.state.files,
                          "todo(jiashuo1): placeholder");
}

// todo(jiashuo1)
void replica_follower::nfs_copy_remote_files(const rpc_address &remote_node,
                                             const std::string &remote_disk,
                                             const std::string &remote_dir,
                                             std::vector<std::string> &file_list,
                                             const std::string &dest_dir)
{
    _tracker.set_success();
}

} // namespace replication
} // namespace dsn
