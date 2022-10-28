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

#include "common//duplication_common.h"
#include "utils/fmt_logging.h"

#include "replica_duplicator_manager.h"

namespace dsn {
namespace replication {

std::vector<duplication_confirm_entry>
replica_duplicator_manager::get_duplication_confirms_to_update() const
{
    zauto_lock l(_lock);

    std::vector<duplication_confirm_entry> updates;
    for (const auto &kv : _duplications) {
        replica_duplicator *duplicator = kv.second.get();
        duplication_progress p = duplicator->progress();
        if (p.last_decree != p.confirmed_decree ||
            (kv.second->status() == duplication_status::DS_PREPARE && p.checkpoint_has_prepared)) {
            if (p.last_decree < p.confirmed_decree) {
                LOG_ERROR_PREFIX("invalid decree state: p.last_decree({}) < p.confirmed_decree({})",
                                 p.last_decree,
                                 p.confirmed_decree);
                continue;
            }
            duplication_confirm_entry entry;
            entry.dupid = duplicator->id();
            entry.confirmed_decree = p.last_decree;
            entry.__set_checkpoint_prepared(p.checkpoint_has_prepared);
            updates.emplace_back(entry);
        }
    }
    return updates;
}

void replica_duplicator_manager::sync_duplication(const duplication_entry &ent)
{
    // state is inconsistent with meta-server
    auto it = ent.progress.find(get_gpid().get_partition_index());
    if (it == ent.progress.end()) {
        _duplications.erase(ent.dupid);
        return;
    }

    zauto_lock l(_lock);

    dupid_t dupid = ent.dupid;
    duplication_status::type next_status = ent.status;

    replica_duplicator_u_ptr &dup = _duplications[dupid];
    if (dup == nullptr) {
        if (!is_duplication_status_invalid(next_status)) {
            dup = make_unique<replica_duplicator>(ent, _replica);
        } else {
            LOG_ERROR_PREFIX("illegal duplication status: {}",
                             duplication_status_to_string(next_status));
        }
    } else {
        // update progress
        duplication_progress newp = dup->progress().set_confirmed_decree(it->second);
        CHECK_EQ_PREFIX(dup->update_progress(newp), error_s::ok());
        dup->update_status_if_needed(next_status);
        if (ent.__isset.fail_mode) {
            dup->update_fail_mode(ent.fail_mode);
        }
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

int64_t replica_duplicator_manager::get_pending_mutations_count() const
{
    int64_t total = 0;
    for (const auto &dup : _duplications) {
        total += dup.second->get_pending_mutations_count();
    }
    return total;
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
        ret.emplace_back(state);
    }
    return ret;
}

} // namespace replication
} // namespace dsn
