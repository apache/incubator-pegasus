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
#include "utils/filesystem.h"
#include "common/duplication_common.h"

#include <boost/algorithm/string.hpp>
#include "runtime/rpc/group_address.h"
#include "nfs/nfs_node.h"
#include "utils/fail_point.h"

namespace dsn {
namespace replication {

replica_follower::replica_follower(replica *r) : replica_base(r), _replica(r)
{
    init_master_info();
}

replica_follower::~replica_follower() { _tracker.wait_outstanding_tasks(); }

// ThreadPool: THREAD_POOL_REPLICATION_LONG
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
    CHECK(!metas.empty(), "master cluster meta list is invalid!");
    for (const auto &meta : metas) {
        dsn::rpc_address node;
        CHECK(node.from_string_ipv4(meta.c_str()), "{} is invalid meta address", meta);
        _master_meta_list.emplace_back(std::move(node));
    }
}

// ThreadPool: THREAD_POOL_REPLICATION_LONG
error_code replica_follower::duplicate_checkpoint()
{
    zauto_lock l(_lock);
    if (_duplicating_checkpoint) {
        LOG_WARNING_PREFIX("duplicate master[{}] checkpoint is running", master_replica_name());
        return ERR_BUSY;
    }

    LOG_INFO_PREFIX("start duplicate master[{}] checkpoint", master_replica_name());
    _duplicating_checkpoint = true;
    tasking::enqueue(LPC_DUPLICATE_CHECKPOINT, &_tracker, [=]() mutable {
        async_duplicate_checkpoint_from_master_replica();
    });
    _tracker.wait_outstanding_tasks();
    _duplicating_checkpoint = false;
    if (_tracker.all_tasks_success()) {
        _tracker.clear_tasks_state();
        return ERR_OK;
    }
    return ERR_TRY_AGAIN;
}

// ThreadPool: THREAD_POOL_DEFAULT
void replica_follower::async_duplicate_checkpoint_from_master_replica()
{
    rpc_address meta_servers;
    meta_servers.assign_group(_master_cluster_name.c_str());
    meta_servers.group_address()->add_list(_master_meta_list);

    configuration_query_by_index_request meta_config_request;
    meta_config_request.app_name = _master_app_name;
    // just fetch the same partition config
    meta_config_request.partition_indices = {get_gpid().get_partition_index()};

    LOG_INFO_PREFIX("query master[{}] replica configuration", master_replica_name());
    dsn::message_ex *msg = dsn::message_ex::create_request(
        RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX, 0, get_gpid().thread_hash());
    dsn::marshall(msg, meta_config_request);
    rpc::call(meta_servers,
              msg,
              &_tracker,
              [&](error_code err, configuration_query_by_index_response &&resp) mutable {
                  FAIL_POINT_INJECT_F("duplicate_checkpoint_ok", [&](string_view s) -> void {
                      _tracker.set_tasks_success();
                      return;
                  });

                  FAIL_POINT_INJECT_F("duplicate_checkpoint_failed",
                                      [&](string_view s) -> void { return; });
                  if (update_master_replica_config(err, std::move(resp)) == ERR_OK) {
                      copy_master_replica_checkpoint();
                  }
              });
}

// ThreadPool: THREAD_POOL_DEFAULT
error_code
replica_follower::update_master_replica_config(error_code err,
                                               configuration_query_by_index_response &&resp)
{
    error_code err_code = err != ERR_OK ? err : resp.err;
    if (dsn_unlikely(err_code != ERR_OK)) {
        LOG_ERROR_PREFIX(
            "query master[{}] config failed: {}", master_replica_name(), err_code.to_string());
        return err_code;
    }

    if (dsn_unlikely(resp.partition_count != _replica->get_app_info()->partition_count)) {
        LOG_ERROR_PREFIX("master[{}] partition count is inconsistent: local = {} vs master = {}",
                         master_replica_name(),
                         _replica->get_app_info()->partition_count,
                         resp.partition_count);
        return ERR_INCONSISTENT_STATE;
    }

    if (dsn_unlikely(resp.partitions.size() != 1)) {
        LOG_ERROR_PREFIX("master[{}] config size must be single, but actually is {}",
                         master_replica_name(),
                         resp.partitions.size());
        return ERR_INVALID_DATA;
    }

    if (dsn_unlikely(resp.partitions[0].pid.get_partition_index() !=
                     get_gpid().get_partition_index())) {
        LOG_ERROR_PREFIX("master[{}] partition index is inconsistent: local = {} vs master = {}",
                         master_replica_name(),
                         get_gpid().get_partition_index(),
                         resp.partitions[0].pid.get_partition_index());
        return ERR_INCONSISTENT_STATE;
    }

    if (dsn_unlikely(resp.partitions[0].primary == rpc_address::s_invalid_address)) {
        LOG_ERROR_PREFIX("master[{}] partition address is invalid", master_replica_name());
        return ERR_INVALID_STATE;
    }

    // since the request just specify one partition, the result size is single
    _master_replica_config = resp.partitions[0];
    LOG_INFO_PREFIX(
        "query master[{}] config successfully and update local config: remote={}, gpid={}",
        master_replica_name(),
        _master_replica_config.primary.to_string(),
        _master_replica_config.pid.to_string());
    return ERR_OK;
}

// ThreadPool: THREAD_POOL_DEFAULT
void replica_follower::copy_master_replica_checkpoint()
{
    LOG_INFO_PREFIX("query master[{}] replica checkpoint info and start use nfs copy the data",
                    master_replica_name());
    learn_request request;
    request.pid = _master_replica_config.pid;
    dsn::message_ex *msg = dsn::message_ex::create_request(
        RPC_QUERY_LAST_CHECKPOINT_INFO, 0, _master_replica_config.pid.thread_hash());
    dsn::marshall(msg, request);
    rpc::call(_master_replica_config.primary,
              msg,
              &_tracker,
              [&](error_code err, learn_response &&resp) mutable {
                  nfs_copy_checkpoint(err, std::move(resp));
              });
}

// ThreadPool: THREAD_POOL_DEFAULT
error_code replica_follower::nfs_copy_checkpoint(error_code err, learn_response &&resp)
{
    error_code err_code = err != ERR_OK ? err : resp.err;
    if (dsn_unlikely(err_code != ERR_OK)) {
        LOG_ERROR_PREFIX("query master[{}] replica checkpoint info failed, err = {}",
                         master_replica_name(),
                         err_code.to_string());
        return err_code;
    }

    std::string dest = utils::filesystem::path_combine(
        _replica->dir(), duplication_constants::kDuplicationCheckpointRootDir);
    if (!utils::filesystem::remove_path(dest)) {
        LOG_ERROR_PREFIX(
            "clear master[{}] replica checkpoint dest dir {} failed", master_replica_name(), dest);
        return ERR_FILE_OPERATION_FAILED;
    }

    nfs_copy_remote_files(
        resp.address, resp.replica_disk_tag, resp.base_local_dir, resp.state.files, dest);
    return ERR_OK;
}

// ThreadPool: THREAD_POOL_DEFAULT
void replica_follower::nfs_copy_remote_files(const rpc_address &remote_node,
                                             const std::string &remote_disk,
                                             const std::string &remote_dir,
                                             std::vector<std::string> &file_list,
                                             const std::string &dest_dir)
{
    LOG_INFO_PREFIX(
        "nfs start copy master[{}] replica checkpoint: {}", master_replica_name(), remote_dir);
    std::shared_ptr<remote_copy_request> request = std::make_shared<remote_copy_request>();
    request->source = remote_node;
    request->source_disk_tag = remote_disk;
    request->source_dir = remote_dir;
    request->files = file_list;
    request->dest_disk_tag = _replica->get_replica_disk_tag();
    request->dest_dir = dest_dir;
    request->overwrite = true;
    request->high_priority = false;

    _replica->_stub->_nfs->copy_remote_files(
        request,
        LPC_DUPLICATE_CHECKPOINT_COMPLETED,
        &_tracker,
        [&, remote_dir](error_code err, size_t size) mutable {
            FAIL_POINT_INJECT_NOT_RETURN_F("nfs_copy_ok",
                                           [&](string_view s) -> void { err = ERR_OK; });

            if (dsn_unlikely(err != ERR_OK)) {
                LOG_ERROR_PREFIX("nfs copy master[{}] checkpoint failed: checkpoint = {}, err = {}",
                                 master_replica_name(),
                                 remote_dir,
                                 err.to_string());
                return;
            }
            LOG_INFO_PREFIX("nfs copy master[{}] checkpoint completed: checkpoint = {}, size = {}",
                            master_replica_name(),
                            remote_dir,
                            size);
            _tracker.set_tasks_success();
        });
}

} // namespace replication
} // namespace dsn
