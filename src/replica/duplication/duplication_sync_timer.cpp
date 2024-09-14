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

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "common/duplication_common.h"
#include "common/replication.codes.h"
#include "duplication_sync_timer.h"
#include "replica/replica.h"
#include "replica/replica_stub.h"
#include "replica_duplicator_manager.h"
#include "rpc/rpc_address.h"
#include "rpc/rpc_host_port.h"
#include "task/async_calls.h"
#include "task/task_code.h"
#include "utils/autoref_ptr.h"
#include "utils/chrono_literals.h"
#include "utils/error_code.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/metrics.h"
#include "utils/threadpool_code.h"

DSN_DEFINE_uint64(
    replication,
    duplication_sync_period_second,
    10,
    "The period seconds of duplication to sync data from local cluster to remote cluster");

namespace dsn {
namespace replication {
using namespace literals::chrono_literals;

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

    auto req = std::make_unique<duplication_sync_request>();
    SET_IP_AND_HOST_PORT(*req, node, _stub->primary_address(), _stub->primary_host_port());

    // collects confirm points from all primaries on this server
    for (const replica_ptr &r : _stub->get_all_primaries()) {
        const auto dup_mgr = r->get_duplication_manager();
        if (!dup_mgr) {
            continue;
        }

        auto confirmed = dup_mgr->get_duplication_confirms_to_update();
        if (!confirmed.empty()) {
            req->confirm_list[r->get_gpid()] = std::move(confirmed);
        }
        METRIC_SET(*r, dup_pending_mutations);
    }

    duplication_sync_rpc rpc(std::move(req), RPC_CM_DUPLICATION_SYNC, 3_s);
    rpc_address meta_server_address(_stub->get_meta_server_address());
    LOG_INFO("duplication_sync to meta({})", meta_server_address);

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
        LOG_ERROR("on_duplication_sync_reply: err({})", err);
    } else {
        update_duplication_map(resp.dup_map);
    }

    zauto_lock l(_lock);
    _rpc_task = nullptr;
}

void duplication_sync_timer::update_duplication_map(
    const std::map<int32_t, std::map<int32_t, duplication_entry>> &dup_map)
{
    for (replica_ptr &r : _stub->get_all_replicas()) {
        auto dup_mgr = r->get_duplication_manager();
        if (!dup_mgr) {
            continue;
        }

        const auto &it = dup_map.find(r->get_gpid().get_app_id());

        // TODO(wangdan): at meta server side, an app is considered duplicating
        // as long as any duplication of this app has valid status(i.e.
        // duplication_info::is_invalid_status() returned false, see
        // meta_duplication_service::refresh_duplicating_no_lock()). And duplications
        // in duplication_sync_response returned by meta server could also be
        // considered duplicating according to meta_duplication_service::duplication_sync().
        // Thus we could update `duplicating` in both memory and file(.app-info).
        //
        // However, most of properties of an app(struct `app_info`) are written to .app-info
        // file in replica::on_config_sync(), such as max_replica_count; on the other hand,
        // in-memory `duplicating` is also updated in replica::on_config_proposal(). Thus we'd
        // better think about a unique entrance to update `duplicating`(in both memory and disk),
        // rather than update them at anywhere.
        const auto duplicating = it != dup_map.end();
        r->update_app_duplication_status(duplicating);

        if (!duplicating) {
            // No duplication is assigned to this app.
            dup_mgr->update_duplication_map({});
            continue;
        }

        dup_mgr->update_duplication_map(it->second);
    }
}

duplication_sync_timer::duplication_sync_timer(replica_stub *stub) : _stub(stub) {}

duplication_sync_timer::~duplication_sync_timer() {}

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
    LOG_INFO("run duplication sync periodically in {}s", FLAGS_duplication_sync_period_second);

    _timer_task = tasking::enqueue_timer(
        LPC_DUPLICATION_SYNC_TIMER,
        &_stub->_tracker,
        [this]() { run(); },
        FLAGS_duplication_sync_period_second * 1_s,
        0,
        FLAGS_duplication_sync_period_second * 1_s);
}

std::multimap<dupid_t, duplication_sync_timer::replica_dup_state>
duplication_sync_timer::get_dup_states(int app_id, /*out*/ bool *app_found)
{
    *app_found = false;
    std::multimap<dupid_t, replica_dup_state> result;
    for (const replica_ptr &r : _stub->get_all_primaries()) {
        const gpid rid = r->get_gpid();
        if (rid.get_app_id() != app_id) {
            continue;
        }

        const auto dup_mgr = r->get_duplication_manager();
        if (!dup_mgr) {
            continue;
        }

        *app_found = true;
        replica_dup_state state;
        state.id = rid;
        const auto &states = dup_mgr->get_dup_states();
        decree last_committed_decree = r->last_committed_decree();
        for (const auto &s : states) {
            state.duplicating = s.duplicating;
            state.not_confirmed = std::max(decree(0), last_committed_decree - s.confirmed_decree);
            state.not_duplicated = std::max(decree(0), last_committed_decree - s.last_decree);
            state.fail_mode = s.fail_mode;
            state.remote_app_name = s.remote_app_name;
            result.emplace(std::make_pair(s.dupid, state));
        }
    }
    return result;
}
} // namespace replication
} // namespace dsn
