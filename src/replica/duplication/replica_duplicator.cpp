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
#include <utility>

#include "common/duplication_common.h"
#include "common/gpid.h"
#include "common/replication.codes.h"
#include "dsn.layer2_types.h"
#include "duplication_pipeline.h"
#include "load_from_private_log.h"
#include "perf_counter/perf_counter.h"
#include "perf_counter/perf_counter_wrapper.h"
#include "replica/mutation_log.h"
#include "replica/replica.h"
#include "replica/replica_stub.h"
#include "runtime/task/async_calls.h"
#include "utils/autoref_ptr.h"
#include "utils/error_code.h"
#include "utils/fmt_logging.h"

namespace dsn {
namespace replication {

replica_duplicator::replica_duplicator(const duplication_entry &ent, replica *r)
    : replica_base(r),
      _id(ent.dupid),
      _remote_cluster_name(ent.remote),
      _replica(r),
      _stub(r->get_replica_stub())
{
    _status = ent.status;

    auto it = ent.progress.find(get_gpid().get_partition_index());
    if (it->second == invalid_decree) {
        // keep current max committed_decree as start point.
        // todo(jiashuo1) _start_point_decree hasn't be ready to persist zk, so if master restart,
        // the value will be reset 0
        _start_point_decree = _progress.last_decree = _replica->private_log()->max_commit_on_disk();
    } else {
        _progress.last_decree = _progress.confirmed_decree = it->second;
    }
    LOG_INFO_PREFIX("initialize replica_duplicator[{}] [dupid:{}, meta_confirmed_decree:{}]",
                    duplication_status_to_string(_status),
                    id(),
                    it->second);
    thread_pool(LPC_REPLICATION_LOW).task_tracker(tracker()).thread_hash(get_gpid().thread_hash());

    if (_status == duplication_status::DS_PREPARE) {
        prepare_dup();
    } else if (_status == duplication_status::DS_LOG) {
        start_dup_log();
    }
}

void replica_duplicator::prepare_dup()
{
    LOG_INFO_PREFIX("start prepare checkpoint to catch up with latest durable decree: "
                    "start_point_decree({}) < last_durable_decree({}) = {}",
                    _start_point_decree,
                    _replica->last_durable_decree(),
                    _start_point_decree < _replica->last_durable_decree());

    tasking::enqueue(
        LPC_REPLICATION_COMMON,
        &_tracker,
        [this]() { _replica->trigger_manual_emergency_checkpoint(_start_point_decree); },
        get_gpid().thread_hash());
}

void replica_duplicator::start_dup_log()
{
    LOG_INFO_PREFIX("starting duplication {} [last_decree: {}, confirmed_decree: {}]",
                    to_string(),
                    _progress.last_decree,
                    _progress.confirmed_decree);

    /// ===== pipeline declaration ===== ///

    // load -> ship -> load
    _ship = std::make_unique<ship_mutation>(this);
    _load_private = std::make_unique<load_from_private_log>(_replica, this);
    _load = std::make_unique<load_mutation>(this, _replica, _load_private.get());

    from(*_load).link(*_ship).link(*_load);
    fork(*_load_private, LPC_REPLICATION_LONG_LOW, 0).link(*_ship);

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

    // DS_PREPARE means replica is checkpointing, it may need trigger multi time to catch
    // _start_point_decree of the plog
    if (_status == next_status && next_status != duplication_status::DS_PREPARE) {
        return;
    }

    LOG_INFO_PREFIX(
        "update duplication status: {}=>{}[start_point={}, last_commit={}, last_durable={}]",
        duplication_status_to_string(_status),
        duplication_status_to_string(next_status),
        _start_point_decree,
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
    _progress.checkpoint_has_prepared = _start_point_decree <= _replica->last_durable_decree();

    if (_progress.confirmed_decree > _progress.last_decree) {
        return FMT_ERR(ERR_INVALID_STATE,
                       "last_decree({}) should always larger than confirmed_decree({})",
                       _progress.last_decree,
                       _progress.confirmed_decree);
    }
    if (_progress.confirmed_decree > last_confirmed_decree) {
        // has confirmed_decree updated.
        _stub->_counter_dup_confirmed_rate->add(_progress.confirmed_decree - last_confirmed_decree);
    }

    return error_s::ok();
}

void replica_duplicator::verify_start_decree(decree start_decree)
{
    decree confirmed_decree = progress().confirmed_decree;
    decree last_decree = progress().last_decree;
    decree max_gced_decree = get_max_gced_decree();
    CHECK_LT_MSG(max_gced_decree,
                 start_decree,
                 "the logs haven't yet duplicated were accidentally truncated "
                 "[max_gced_decree: {}, start_decree: {}, confirmed_decree: {}, last_decree: {}]",
                 max_gced_decree,
                 start_decree,
                 confirmed_decree,
                 last_decree);
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

} // namespace replication
} // namespace dsn
