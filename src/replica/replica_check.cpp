/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#include <atomic>
#include <chrono>
#include <memory>
#include <unordered_map>
#include <utility>

#include "common/fs_manager.h"
#include "common/gpid.h"
#include "common/replication.codes.h"
#include "common/replication_common.h"
#include "common/replication_enums.h"
#include "common/replication_other_types.h"
#include "consensus_types.h"
#include "dsn.layer2_types.h"
#include "duplication/replica_duplicator_manager.h"
#include "metadata_types.h"
#include "mutation.h"
#include "perf_counter/perf_counter.h"
#include "perf_counter/perf_counter_wrapper.h"
#include "replica.h"
#include "replica/prepare_list.h"
#include "replica/replica_context.h"
#include "replica/replication_app_base.h"
#include "replica_stub.h"
#include "runtime/api_layer1.h"
#include "runtime/rpc/rpc_address.h"
#include "runtime/task/async_calls.h"
#include "runtime/task/task.h"
#include "split/replica_split_manager.h"
#include "utils/autoref_ptr.h"
#include "utils/error_code.h"
#include "utils/fail_point.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "absl/strings/string_view.h"
#include "utils/thread_access_checker.h"

namespace dsn {
namespace replication {

// The replica membership state periodical checking part of replica.

DSN_DEFINE_bool(replication, group_check_disabled, false, "whether group check is disabled");
DSN_DEFINE_int32(replication,
                 group_check_interval_ms,
                 10000,
                 "every what period (ms) we check the replica healthness");

DSN_DECLARE_bool(empty_write_disabled);

void replica::init_group_check()
{
    FAIL_POINT_INJECT_F("replica_init_group_check", [](absl::string_view) {});

    _checker.only_one_thread_access();

    LOG_INFO_PREFIX("init group check");

    if (partition_status::PS_PRIMARY != status() || FLAGS_group_check_disabled)
        return;

    CHECK(nullptr == _primary_states.group_check_task, "");
    _primary_states.group_check_task =
        tasking::enqueue_timer(LPC_GROUP_CHECK,
                               &_tracker,
                               [this] { broadcast_group_check(); },
                               std::chrono::milliseconds(FLAGS_group_check_interval_ms),
                               get_gpid().thread_hash());
}

void replica::broadcast_group_check()
{
    FAIL_POINT_INJECT_F("replica_broadcast_group_check", [](absl::string_view) {});

    CHECK_NOTNULL(_primary_states.group_check_task, "");

    LOG_INFO_PREFIX("start to broadcast group check");

    if (_primary_states.group_check_pending_replies.size() > 0) {
        LOG_WARNING_PREFIX(
            "{} group check replies are still pending when doing next round check, cancel first",
            _primary_states.group_check_pending_replies.size());

        for (auto it = _primary_states.group_check_pending_replies.begin();
             it != _primary_states.group_check_pending_replies.end();
             ++it) {
            it->second->cancel(true);
        }
        _primary_states.group_check_pending_replies.clear();
    }

    for (auto it = _primary_states.statuses.begin(); it != _primary_states.statuses.end(); ++it) {
        if (it->first == _stub->_primary_address)
            continue;

        ::dsn::rpc_address addr = it->first;
        std::shared_ptr<group_check_request> request(new group_check_request);

        request->app = _app_info;
        request->node = addr;
        _primary_states.get_replica_config(it->second, request->config);
        request->last_committed_decree = last_committed_decree();
        request->__set_confirmed_decree(_duplication_mgr->min_confirmed_decree());
        // set split context in group_check_request
        if (request->config.status == partition_status::PS_SECONDARY &&
            _split_mgr->get_meta_split_status() != split_status::NOT_SPLIT) {
            request->__set_meta_split_status(_split_mgr->get_meta_split_status());
            if (_split_mgr->is_splitting()) {
                request->__set_child_gpid(_split_mgr->get_child_gpid());
            }
        }

        if (request->config.status == partition_status::PS_POTENTIAL_SECONDARY) {
            auto it = _primary_states.learners.find(addr);
            CHECK(it != _primary_states.learners.end(), "learner {} is missing", addr);
            request->config.learner_signature = it->second.signature;
        }

        LOG_INFO_PREFIX("send group check to {} with state {}", addr, enum_to_string(it->second));

        dsn::task_ptr callback_task =
            rpc::call(addr,
                      RPC_GROUP_CHECK,
                      *request,
                      &_tracker,
                      [=](error_code err, group_check_response &&resp) {
                          auto alloc = std::make_shared<group_check_response>(std::move(resp));
                          on_group_check_reply(err, request, alloc);
                      },
                      std::chrono::milliseconds(0),
                      get_gpid().thread_hash());

        _primary_states.group_check_pending_replies[addr] = callback_task;
    }

    // send empty prepare when necessary
    if (!FLAGS_empty_write_disabled &&
        dsn_now_ms() >= _primary_states.last_prepare_ts_ms + FLAGS_group_check_interval_ms) {
        mutation_ptr mu = new_mutation(invalid_decree);
        mu->add_client_request(RPC_REPLICATION_WRITE_EMPTY, nullptr);
        init_prepare(mu, false);
    }
}

void replica::on_group_check(const group_check_request &request,
                             /*out*/ group_check_response &response)
{
    _checker.only_one_thread_access();

    LOG_INFO_PREFIX("process group check, primary = {}, ballot = {}, status = {}, "
                    "last_committed_decree = {}, confirmed_decree = {}",
                    request.config.primary.to_string(),
                    request.config.ballot,
                    enum_to_string(request.config.status),
                    request.last_committed_decree,
                    request.__isset.confirmed_decree ? request.confirmed_decree : invalid_decree);

    if (request.config.ballot < get_ballot()) {
        response.err = ERR_VERSION_OUTDATED;
        LOG_WARNING_PREFIX("on_group_check reply {}", response.err);
        return;
    } else if (request.config.ballot > get_ballot()) {
        if (!update_local_configuration(request.config)) {
            response.err = ERR_INVALID_STATE;
            LOG_WARNING_PREFIX("on_group_check reply {}", response.err);
            return;
        }
    } else if (is_same_ballot_status_change_allowed(status(), request.config.status)) {
        update_local_configuration(request.config, true);
    }

    _duplication_mgr->update_confirmed_decree_if_secondary(request.confirmed_decree);

    switch (status()) {
    case partition_status::PS_INACTIVE:
        break;
    case partition_status::PS_SECONDARY:
        if (request.last_committed_decree > last_committed_decree()) {
            _prepare_list->commit(request.last_committed_decree, COMMIT_TO_DECREE_HARD);
        }
        // the group check may trigger start/finish/cancel/pause a split on the secondary.
        _split_mgr->trigger_secondary_parent_split(request, response);
        response.__set_disk_status(_dir_node->status);
        break;
    case partition_status::PS_POTENTIAL_SECONDARY:
        init_learn(request.config.learner_signature);
        break;
    case partition_status::PS_ERROR:
        break;
    default:
        CHECK(false, "invalid partition_status, status = {}", enum_to_string(status()));
    }

    response.pid = get_gpid();
    response.node = _stub->_primary_address;
    response.err = ERR_OK;
    if (status() == partition_status::PS_ERROR) {
        response.err = ERR_INVALID_STATE;
        LOG_WARNING_PREFIX("on_group_check reply {}", response.err);
    }

    response.last_committed_decree_in_app = _app->last_committed_decree();
    response.last_committed_decree_in_prepare_list = last_committed_decree();
    response.learner_status_ = _potential_secondary_states.learning_status;
    response.learner_signature = _potential_secondary_states.learning_version;
}

void replica::on_group_check_reply(error_code err,
                                   const std::shared_ptr<group_check_request> &req,
                                   const std::shared_ptr<group_check_response> &resp)
{
    _checker.only_one_thread_access();

    if (partition_status::PS_PRIMARY != status() || req->config.ballot < get_ballot()) {
        return;
    }

    auto r = _primary_states.group_check_pending_replies.erase(req->node);
    CHECK_EQ_MSG(r, 1, "invalid node address, address = {}", req->node);

    if (err != ERR_OK || resp->err != ERR_OK) {
        if (ERR_OK == err) {
            err = resp->err;
        }
        handle_remote_failure(req->config.status, req->node, err, "group check");
        _stub->_counter_replicas_recent_group_check_fail_count->increment();
    } else {
        if (resp->learner_status_ == learner_status::LearningSucceeded &&
            req->config.status == partition_status::PS_POTENTIAL_SECONDARY) {
            handle_learning_succeeded_on_primary(req->node, resp->learner_signature);
        }
        _split_mgr->primary_parent_handle_stop_split(req, resp);
        if (req->config.status == partition_status::PS_SECONDARY) {
            _primary_states.secondary_disk_status[req->node] = resp->disk_status;
        }
    }
}

void replica::inject_error(error_code err)
{
    tasking::enqueue(LPC_REPLICATION_ERROR,
                     &_tracker,
                     [this, err]() { handle_local_failure(err); },
                     get_gpid().thread_hash());
}
} // namespace replication
} // namespace dsn
