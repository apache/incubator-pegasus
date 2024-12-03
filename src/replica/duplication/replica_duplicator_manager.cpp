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

#include <string_view>
#include <algorithm>
#include <cstdint>
#include <memory>

#include "common//duplication_common.h"
#include "common/gpid.h"
#include "common/replication_enums.h"
#include "metadata_types.h"
#include "replica/duplication/replica_duplicator.h"
#include "replica/duplication/replica_duplicator_manager.h"
#include "replica/replica.h"
#include "replica_duplicator_manager.h"
#include "utils/autoref_ptr.h"
#include "utils/errors.h"
#include "utils/fmt_logging.h"

METRIC_DEFINE_gauge_int64(replica,
                          dup_pending_mutations,
                          dsn::metric_unit::kMutations,
                          "The number of pending mutations for dup");

namespace dsn {
namespace replication {

replica_duplicator_manager::replica_duplicator_manager(replica *r)
    : replica_base(r), _replica(r), METRIC_VAR_INIT_replica(dup_pending_mutations)
{
}

void replica_duplicator_manager::update_duplication_map(
    const std::map<int32_t, duplication_entry> &new_dup_map)
{
    if (new_dup_map.empty() || _replica->status() != partition_status::PS_PRIMARY) {
        remove_all_duplications();
        return;
    }

    remove_non_existed_duplications(new_dup_map);

    for (const auto &kv2 : new_dup_map) {
        sync_duplication(kv2.second);
    }
}

std::vector<duplication_confirm_entry>
replica_duplicator_manager::get_duplication_confirms_to_update() const
{
    zauto_lock l(_lock);

    std::vector<duplication_confirm_entry> updates;
    for (const auto &[_, dup] : _duplications) {
        // There are two conditions when we should send confirmed decrees to meta server to update
        // the progress:
        //
        // 1. the acknowledged decree from remote cluster has changed, making it different from
        // the one that is persisted in zk by meta server; otherwise,
        //
        // 2. the duplication has been in the stage of synchronizing checkpoint to the remote
        // cluster, and the synchronized checkpoint has been ready.
        const auto &progress = dup->progress();
        if (progress.last_decree == progress.confirmed_decree &&
            (dup->status() != duplication_status::DS_PREPARE ||
             !progress.checkpoint_has_prepared)) {
            continue;
        }

        if (progress.last_decree < progress.confirmed_decree) {
            LOG_ERROR_PREFIX(
                "invalid decree state: progress.last_decree({}) < progress.confirmed_decree({})",
                progress.last_decree,
                progress.confirmed_decree);
            continue;
        }

        duplication_confirm_entry entry;
        entry.dupid = dup->id();
        entry.confirmed_decree = progress.last_decree;
        entry.__set_checkpoint_prepared(progress.checkpoint_has_prepared);
        entry.__set_last_committed_decree(_replica->last_committed_decree());
        updates.emplace_back(entry);
    }
    return updates;
}

void replica_duplicator_manager::sync_duplication(const duplication_entry &ent)
{
    auto it = ent.progress.find(get_gpid().get_partition_index());
    if (it == ent.progress.end()) {
        // Inconsistent with the meta server.
        _duplications.erase(ent.dupid);
        return;
    }

    zauto_lock l(_lock);

    dupid_t dupid = ent.dupid;
    duplication_status::type next_status = ent.status;

    auto &dup = _duplications[dupid];
    if (!dup) {
        if (!is_duplication_status_invalid(next_status)) {
            dup = std::make_unique<replica_duplicator>(ent, _replica);
        } else {
            LOG_ERROR_PREFIX("illegal duplication status: {}",
                             duplication_status_to_string(next_status));
        }

        return;
    }

    // Update progress.
    CHECK_EQ_PREFIX(dup->update_progress(dup->progress().set_confirmed_decree(it->second)),
                    error_s::ok());
    dup->update_status_if_needed(next_status);
    if (ent.__isset.fail_mode) {
        dup->update_fail_mode(ent.fail_mode);
    }
}

decree replica_duplicator_manager::min_confirmed_decree() const
{
    zauto_lock l(_lock);

    decree min_decree = invalid_decree;
    if (_replica->status() == partition_status::PS_PRIMARY) {
        for (auto &kv : _duplications) {
            const duplication_progress &p = kv.second->progress();
            if (min_decree == invalid_decree) {
                min_decree = p.confirmed_decree;
            } else {
                min_decree = std::min(min_decree, p.confirmed_decree);
            }
        }
    } else if (_primary_confirmed_decree > 0) {
        // if the replica is not primary, use the latest known (from primary)
        // confirmed_decree instead.
        min_decree = _primary_confirmed_decree;
    }
    return min_decree;
}

// Remove the duplications that are not in the `new_dup_map`.
// NOTE: this function may be blocked when destroying replica_duplicator.
void replica_duplicator_manager::remove_non_existed_duplications(
    const std::map<dupid_t, duplication_entry> &new_dup_map)
{
    zauto_lock l(_lock);
    std::vector<dupid_t> removal_set;
    for (auto &pair : _duplications) {
        dupid_t cur_dupid = pair.first;
        if (new_dup_map.find(cur_dupid) == new_dup_map.end()) {
            removal_set.emplace_back(cur_dupid);
        }
    }

    for (dupid_t dupid : removal_set) {
        _duplications.erase(dupid);
    }
}

void replica_duplicator_manager::update_confirmed_decree_if_secondary(decree confirmed)
{
    // this function always runs in the same single thread with config-sync
    if (_replica->status() != partition_status::PS_SECONDARY) {
        return;
    }

    zauto_lock l(_lock);
    remove_all_duplications();
    if (confirmed >= 0) { // duplication ongoing
        // confirmed decree never decreases
        if (_primary_confirmed_decree < confirmed) {
            _primary_confirmed_decree = confirmed;
        }
    } else { // duplication add with freeze but no start or no duplication(include removed)
        _primary_confirmed_decree = confirmed;
    }
}

void replica_duplicator_manager::METRIC_FUNC_NAME_SET(dup_pending_mutations)()
{
    int64_t total = 0;
    for (const auto &dup : _duplications) {
        total += dup.second->get_pending_mutations_count();
    }
    METRIC_VAR_SET(dup_pending_mutations, total);
}

std::vector<replica_duplicator_manager::dup_state>
replica_duplicator_manager::get_dup_states() const
{
    zauto_lock l(_lock);

    std::vector<dup_state> ret;
    ret.reserve(_duplications.size());
    for (const auto &dup : _duplications) {
        dup_state state;
        state.dupid = dup.first;
        state.duplicating = !dup.second->paused();
        auto progress = dup.second->progress();
        state.last_decree = progress.last_decree;
        state.confirmed_decree = progress.confirmed_decree;
        state.fail_mode = dup.second->fail_mode();
        state.remote_app_name = dup.second->remote_app_name();
        ret.emplace_back(state);
    }
    return ret;
}

void replica_duplicator_manager::remove_all_duplications()
{
    // fast path
    if (_duplications.empty()) {
        return;
    }

    LOG_WARNING_PREFIX("remove all duplication, replica status = {}",
                       enum_to_string(_replica->status()));
    _duplications.clear();
}

} // namespace replication
} // namespace dsn
