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

#include "replica_duplicator.h"

#include <rapidjson/document.h>
#include <rapidjson/encodings.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <algorithm>
#include <cstdint>
#include <map>
#include <string_view>
#include <utility>

#include "common/duplication_common.h"
#include "common/gpid.h"
#include "common/replication.codes.h"
#include "dsn.layer2_types.h"
#include "duplication_pipeline.h"
#include "load_from_private_log.h"
#include "replica/mutation_log.h"
#include "replica/replica.h"
#include "task/task_code.h"
#include "utils/autoref_ptr.h"
#include "utils/error_code.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"

METRIC_DEFINE_counter(replica,
                      dup_confirmed_mutations,
                      dsn::metric_unit::kMutations,
                      "The number of confirmed mutations for dup");

DSN_DEFINE_string(
    replication,
    dup_load_plog_task,
    "LPC_REPLICATION_LONG_LOW",
    "The task code for incremental loading from private logs while duplicating. Tasks with "
    "TASK_PRIORITY_HIGH are not recommended.");
DSN_TAG_VARIABLE(dup_load_plog_task, FT_MUTABLE);

namespace dsn::replication {

replica_duplicator::replica_duplicator(const duplication_entry &ent, replica *r)
    : replica_base(r),
      _id(ent.dupid),
      _remote_cluster_name(ent.remote),
      // remote_app_name is missing means meta server is of old version(< v2.6.0),
      // in which case source app_name would be used as remote_app_name.
      _remote_app_name(ent.__isset.remote_app_name ? ent.remote_app_name
                                                   : r->get_app_info()->app_name),
      _replica(r),
      _stub(r->get_replica_stub()),
      METRIC_VAR_INIT_replica(dup_confirmed_mutations)
{
    // Ensure that the checkpoint decree is at least 1. Otherwise, the checkpoint could not be
    // created in time for empty replica; in consequence, the remote cluster would inevitably
    // fail to pull the checkpoint files.
    //
    // The max decree in rocksdb memtable (the last applied decree) is considered as the min
    // decree that should be covered by the checkpoint, which means currently all of the data
    // in current rocksdb should be included into the created checkpoint.
    //
    // `_min_checkpoint_decree` is not persisted into zk. Once replica server was restarted,
    // it would be reset to the decree that is applied most recently.
    const auto last_applied_decree = _replica->last_applied_decree();
    _min_checkpoint_decree = std::max(last_applied_decree, static_cast<decree>(1));
    LOG_INFO_PREFIX("initialize checkpoint decree: min_checkpoint_decree={}, "
                    "last_committed_decree={}, last_applied_decree={}, "
                    "last_flushed_decree={}, last_durable_decree={}, "
                    "plog_max_decree_on_disk={}, plog_max_commit_on_disk={}",
                    _min_checkpoint_decree,
                    _replica->last_committed_decree(),
                    last_applied_decree,
                    _replica->last_flushed_decree(),
                    _replica->last_durable_decree(),
                    _replica->private_log()->max_decree_on_disk(),
                    _replica->private_log()->max_commit_on_disk());

    _status = ent.status;

    const auto it = ent.progress.find(get_gpid().get_partition_index());
    CHECK_PREFIX_MSG(it != ent.progress.end(),
                     "partition({}) not found in duplication progress: "
                     "app_name={}, dup_id={}, remote_cluster_name={}, remote_app_name={}",
                     get_gpid(),
                     r->get_app_info()->app_name,
                     id(),
                     _remote_cluster_name,
                     _remote_app_name);

    // Initial progress would be `invalid_decree` which was synced from meta server
    // immediately after the duplication was created.
    // See `init_progress()` in `meta_duplication_service::new_dup_from_init()`.
    //
    // _progress.last_decree would be used to update the state in meta server.
    // See `replica_duplicator_manager::get_duplication_confirms_to_update()`.
    if (it->second == invalid_decree) {
        _progress.last_decree = _min_checkpoint_decree;
    } else {
        _progress.last_decree = _progress.confirmed_decree = it->second;
    }

    LOG_INFO_PREFIX("initialize replica_duplicator: app_name={}, dup_id={}, "
                    "remote_cluster_name={}, remote_app_name={}, status={}, "
                    "replica_confirmed_decree={}, meta_persisted_decree={}/{}",
                    r->get_app_info()->app_name,
                    id(),
                    _remote_cluster_name,
                    _remote_app_name,
                    duplication_status_to_string(_status),
                    _progress.last_decree,
                    it->second,
                    _progress.confirmed_decree);

    thread_pool(LPC_REPLICATION_LOW).task_tracker(tracker()).thread_hash(get_gpid().thread_hash());

    if (_status == duplication_status::DS_PREPARE) {
        prepare_dup();
    } else if (_status == duplication_status::DS_LOG) {
        start_dup_log();
    }
}

void replica_duplicator::prepare_dup()
{
    LOG_INFO_PREFIX("start to trigger checkpoint: min_checkpoint_decree={}, "
                    "last_committed_decree={}, last_applied_decree={}, "
                    "last_flushed_decree={}, last_durable_decree={}, "
                    "plog_max_decree_on_disk={}, plog_max_commit_on_disk={}",
                    _min_checkpoint_decree,
                    _replica->last_committed_decree(),
                    _replica->last_applied_decree(),
                    _replica->last_flushed_decree(),
                    _replica->last_durable_decree(),
                    _replica->private_log()->max_decree_on_disk(),
                    _replica->private_log()->max_commit_on_disk());

    _replica->async_trigger_manual_emergency_checkpoint(_min_checkpoint_decree, 0);
}

void replica_duplicator::start_dup_log()
{
    LOG_INFO_PREFIX("starting duplication: {}, replica_confirmed_decree={}, "
                    "meta_persisted_decree={}",
                    to_string(),
                    _progress.last_decree,
                    _progress.confirmed_decree);

    /// ===== pipeline declaration ===== ///

    // load -> ship -> load
    _ship = std::make_unique<ship_mutation>(this);
    _load_private = std::make_unique<load_from_private_log>(_replica, this);
    _load = std::make_unique<load_mutation>(this, _replica, _load_private.get());

    from(*_load).link(*_ship).link(*_load);
    auto dup_load_plog_task = dsn::task_code::try_get(FLAGS_dup_load_plog_task, TASK_CODE_INVALID);
    if (dup_load_plog_task == TASK_CODE_INVALID) {
        dup_load_plog_task = LPC_REPLICATION_LONG_LOW;
        LOG_ERROR_PREFIX("invalid dup_load_plog_task ({}), set it to LPC_REPLICATION_LONG_LOW",
                         FLAGS_dup_load_plog_task);
    }
    fork(*_load_private, dup_load_plog_task, 0).link(*_ship);

    run_pipeline();
}

void replica_duplicator::pause_dup_log()
{
    LOG_INFO_PREFIX("pausing duplication: {}", to_string());

    pause();
    cancel_all();

    _load.reset();
    _ship.reset();
    _load_private.reset();

    LOG_INFO_PREFIX("duplication paused: {}", to_string());
}

std::string replica_duplicator::to_string() const
{
    rapidjson::Document doc;
    doc.SetObject();
    auto &alloc = doc.GetAllocator();

    doc.AddMember("dupid", id(), alloc);
    doc.AddMember("status", rapidjson::StringRef(duplication_status_to_string(_status)), alloc);
    doc.AddMember("remote", rapidjson::StringRef(_remote_cluster_name.data()), alloc);
    doc.AddMember("confirmed", _progress.confirmed_decree, alloc);
    doc.AddMember("app",
                  rapidjson::StringRef(_replica->get_app_info()->app_name.data(),
                                       _replica->get_app_info()->app_name.size()),
                  alloc);

    rapidjson::StringBuffer sb;
    rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
    doc.Accept(writer);
    return sb.GetString();
}

void replica_duplicator::update_status_if_needed(duplication_status::type next_status)
{
    if (is_duplication_status_invalid(next_status)) {
        LOG_ERROR_PREFIX("unexpected duplication status ({})",
                         duplication_status_to_string(next_status));
        return;
    }

    // DS_PREPARE means this replica is making checkpoint, which might need to be triggered
    // multiple times to catch up with _min_checkpoint_decree.
    if (_status == next_status && next_status != duplication_status::DS_PREPARE) {
        return;
    }

    LOG_INFO_PREFIX("update duplication status: {}=>{} [min_checkpoint_decree={}, "
                    "last_committed_decree={}, last_durable_decree={}]",
                    duplication_status_to_string(_status),
                    duplication_status_to_string(next_status),
                    _min_checkpoint_decree,
                    _replica->last_committed_decree(),
                    _replica->last_durable_decree());

    _status = next_status;
    if (_status == duplication_status::DS_PREPARE) {
        prepare_dup();
        return;
    }

    // DS_APP means the replica follower is duplicate checkpoint from master, just return and wait
    // next loop
    if (_status == duplication_status::DS_APP) {
        return;
    }

    if (_status == duplication_status::DS_LOG) {
        start_dup_log();
        return;
    }

    if (_status == duplication_status::DS_PAUSE) {
        pause_dup_log();
        return;
    }
}

replica_duplicator::~replica_duplicator()
{
    pause();
    cancel_all();
    LOG_INFO_PREFIX("closing duplication {}", to_string());
}

error_s replica_duplicator::update_progress(const duplication_progress &p)
{
    zauto_write_lock l(_lock);

    if (p.confirmed_decree >= 0 && p.confirmed_decree < _progress.confirmed_decree) {
        return FMT_ERR(ERR_INVALID_STATE,
                       "never decrease confirmed_decree: new({}) old({})",
                       p.confirmed_decree,
                       _progress.confirmed_decree);
    }

    decree last_confirmed_decree = _progress.confirmed_decree;
    _progress.confirmed_decree = std::max(_progress.confirmed_decree, p.confirmed_decree);
    _progress.last_decree = std::max(_progress.last_decree, p.last_decree);
    _progress.checkpoint_has_prepared = _min_checkpoint_decree <= _replica->last_durable_decree();

    if (_progress.confirmed_decree > _progress.last_decree) {
        return FMT_ERR(ERR_INVALID_STATE,
                       "last_decree({}) should always larger than confirmed_decree({})",
                       _progress.last_decree,
                       _progress.confirmed_decree);
    }
    if (_progress.confirmed_decree > last_confirmed_decree) {
        // has confirmed_decree updated.
        METRIC_VAR_INCREMENT_BY(dup_confirmed_mutations,
                                _progress.confirmed_decree - last_confirmed_decree);
    }

    return error_s::ok();
}

void replica_duplicator::verify_start_decree(decree start_decree)
{
    const auto max_gced_decree = get_max_gced_decree();
    CHECK_LT_PREFIX_MSG(
        max_gced_decree,
        start_decree,
        "the logs haven't yet duplicated were accidentally truncated [max_gced_decree: {}, "
        "start_decree: {}, replica_confirmed_decree: {}, meta_persisted_decree: {}]",
        max_gced_decree,
        start_decree,
        progress().last_decree,
        progress().confirmed_decree);
}

decree replica_duplicator::get_max_gced_decree() const
{
    return _replica->private_log()->max_gced_decree(_replica->get_gpid());
}

uint64_t replica_duplicator::get_pending_mutations_count() const
{
    // it's not atomic to read last_committed_decree in not-REPLICATION thread pool,
    // but enough for approximate statistic.
    int64_t cnt = _replica->last_committed_decree() - progress().last_decree;
    // since last_committed_decree() is not atomic, `cnt` could probably be negative.
    return cnt > 0 ? static_cast<uint64_t>(cnt) : 0;
}

void replica_duplicator::set_duplication_plog_checking(bool checking)
{
    _replica->set_duplication_plog_checking(checking);
}

} // namespace dsn::replication
