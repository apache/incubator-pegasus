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

#include "replica/replica_stub.h"
#include "replica/replica.h"

#include "duplication_sync_timer.h"
#include "replica_duplicator_manager.h"

#include "utils/fmt_logging.h"
#include "utils/command_manager.h"
#include "utils/output_utils.h"
#include "utils/string_conv.h"

namespace dsn {
namespace replication {

DEFINE_TASK_CODE(LPC_DUPLICATION_SYNC_TIMER, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)

void duplication_sync_timer::run()
{
    // ensure duplication sync never be concurrent
    if (_rpc_task) {
        LOG_INFO("a duplication sync is already ongoing");
        return;
    }

    {
        zauto_lock l(_stub->_state_lock);
        if (_stub->_state == replica_stub::NS_Disconnected) {
            LOG_INFO("stop this round of duplication sync because this server is disconnected from "
                     "meta server");
            return;
        }
    }

    auto req = make_unique<duplication_sync_request>();
    req->node = _stub->primary_address();

    // collects confirm points from all primaries on this server
    uint64_t pending_muts_cnt = 0;
    for (const replica_ptr &r : get_all_primaries()) {
        auto confirmed = r->get_duplication_manager()->get_duplication_confirms_to_update();
        if (!confirmed.empty()) {
            req->confirm_list[r->get_gpid()] = std::move(confirmed);
        }
        pending_muts_cnt += r->get_duplication_manager()->get_pending_mutations_count();
    }
    _stub->_counter_dup_pending_mutations_count->set(pending_muts_cnt);

    duplication_sync_rpc rpc(std::move(req), RPC_CM_DUPLICATION_SYNC, 3_s);
    rpc_address meta_server_address(_stub->get_meta_server_address());
    LOG_INFO("duplication_sync to meta({})", meta_server_address.to_string());

    zauto_lock l(_lock);
    _rpc_task =
        rpc.call(meta_server_address, &_stub->_tracker, [this, rpc](error_code err) mutable {
            on_duplication_sync_reply(err, rpc.response());
        });
}

void duplication_sync_timer::on_duplication_sync_reply(error_code err,
                                                       const duplication_sync_response &resp)
{
    if (err == ERR_OK && resp.err != ERR_OK) {
        err = resp.err;
    }
    if (err != ERR_OK) {
        LOG_ERROR("on_duplication_sync_reply: err({})", err.to_string());
    } else {
        update_duplication_map(resp.dup_map);
    }

    zauto_lock l(_lock);
    _rpc_task = nullptr;
}

void duplication_sync_timer::update_duplication_map(
    const std::map<int32_t, std::map<int32_t, duplication_entry>> &dup_map)
{
    for (replica_ptr &r : get_all_replicas()) {
        auto it = dup_map.find(r->get_gpid().get_app_id());
        if (it == dup_map.end()) {
            // no duplication is assigned to this app
            r->get_duplication_manager()->update_duplication_map({});
        } else {
            r->get_duplication_manager()->update_duplication_map(it->second);
        }
    }
}

duplication_sync_timer::duplication_sync_timer(replica_stub *stub) : _stub(stub) {}

duplication_sync_timer::~duplication_sync_timer() {}

std::vector<replica_ptr> duplication_sync_timer::get_all_primaries()
{
    std::vector<replica_ptr> replica_vec;
    {
        zauto_read_lock l(_stub->_replicas_lock);
        for (auto &kv : _stub->_replicas) {
            replica_ptr r = kv.second;
            if (r->status() != partition_status::PS_PRIMARY) {
                continue;
            }
            replica_vec.emplace_back(std::move(r));
        }
    }
    return replica_vec;
}

std::vector<replica_ptr> duplication_sync_timer::get_all_replicas()
{
    std::vector<replica_ptr> replica_vec;
    {
        zauto_read_lock l(_stub->_replicas_lock);
        for (auto &kv : _stub->_replicas) {
            replica_ptr r = kv.second;
            replica_vec.emplace_back(std::move(r));
        }
    }
    return replica_vec;
}

void duplication_sync_timer::close()
{
    LOG_INFO("stop duplication sync");

    {
        zauto_lock l(_lock);
        if (_rpc_task) {
            _rpc_task->cancel(true);
            _rpc_task = nullptr;
        }
    }

    if (_timer_task) {
        _timer_task->cancel(true);
        _timer_task = nullptr;
    }
}

void duplication_sync_timer::start()
{
    LOG_INFO("run duplication sync periodically in {}s", DUPLICATION_SYNC_PERIOD_SECOND);

    _timer_task = tasking::enqueue_timer(LPC_DUPLICATION_SYNC_TIMER,
                                         &_stub->_tracker,
                                         [this]() { run(); },
                                         DUPLICATION_SYNC_PERIOD_SECOND * 1_s,
                                         0,
                                         DUPLICATION_SYNC_PERIOD_SECOND * 1_s);
}

std::multimap<dupid_t, duplication_sync_timer::replica_dup_state>
duplication_sync_timer::get_dup_states(int app_id, /*out*/ bool *app_found)
{
    *app_found = false;
    std::multimap<dupid_t, replica_dup_state> result;
    for (const replica_ptr &r : get_all_primaries()) {
        gpid rid = r->get_gpid();
        if (rid.get_app_id() != app_id) {
            continue;
        }
        *app_found = true;
        replica_dup_state state;
        state.id = rid;
        auto states = r->get_duplication_manager()->get_dup_states();
        decree last_committed_decree = r->last_committed_decree();
        for (const auto &s : states) {
            state.duplicating = s.duplicating;
            state.not_confirmed = std::max(decree(0), last_committed_decree - s.confirmed_decree);
            state.not_duplicated = std::max(decree(0), last_committed_decree - s.last_decree);
            state.fail_mode = s.fail_mode;
            result.emplace(std::make_pair(s.dupid, state));
        }
    }
    return result;
}

} // namespace replication
} // namespace dsn
