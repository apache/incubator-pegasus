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

#include <inttypes.h>
#include <stdio.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include <fmt/std.h> // IWYU pragma: keep

#include "common/fs_manager.h"
#include "common/gpid.h"
#include "common/replication.codes.h"
#include "common/replication_enums.h"
#include "common/replication_other_types.h"
#include "consensus_types.h"
#include "dsn.layer2_types.h"
#include "metadata_types.h"
#include "mutation.h"
#include "mutation_log.h"
#include "nfs/nfs_node.h"
#include "perf_counter/perf_counter.h"
#include "perf_counter/perf_counter_wrapper.h"
#include "replica.h"
#include "replica/duplication/replica_duplicator_manager.h"
#include "replica/prepare_list.h"
#include "replica/replica_context.h"
#include "replica/replication_app_base.h"
#include "replica_stub.h"
#include "runtime/api_layer1.h"
#include "runtime/rpc/rpc_address.h"
#include "runtime/rpc/rpc_message.h"
#include "runtime/rpc/serialization.h"
#include "runtime/task/async_calls.h"
#include "runtime/task/task.h"
#include "utils/autoref_ptr.h"
#include "utils/binary_reader.h"
#include "utils/binary_writer.h"
#include "utils/blob.h"
#include "utils/error_code.h"
#include "utils/filesystem.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/thread_access_checker.h"

namespace dsn {
namespace replication {

// The replication learning process part of replica.

DSN_DEFINE_int32(replication,
                 learn_app_max_concurrent_count,
                 5,
                 "max count of learning app concurrently");

DSN_DECLARE_int32(max_mutation_count_in_prepare_list);

void replica::init_learn(uint64_t signature)
{
    _checker.only_one_thread_access();

    if (status() != partition_status::PS_POTENTIAL_SECONDARY) {
        LOG_WARNING_PREFIX(
            "state is not potential secondary but {}, skip learning with signature[{:#018x}]",
            enum_to_string(status()),
            signature);
        return;
    }

    if (signature == invalid_signature) {
        LOG_WARNING_PREFIX("invalid learning signature, skip");
        return;
    }

    // at most one learning task running
    if (_potential_secondary_states.learning_round_is_running) {
        LOG_WARNING_PREFIX(
            "previous learning is still running, skip learning with signature [{:#018x}]",
            signature);
        return;
    }

    if (signature < _potential_secondary_states.learning_version) {
        LOG_WARNING_PREFIX(
            "learning request is out-dated, therefore skipped: [{:#018x}] vs [{:#018x}]",
            signature,
            _potential_secondary_states.learning_version);
        return;
    }

    // learn timeout or primary change, the (new) primary starts another round of learning process
    // be cautious: primary should not issue signatures frequently to avoid learning abort
    if (signature != _potential_secondary_states.learning_version) {
        if (!_potential_secondary_states.cleanup(false)) {
            LOG_WARNING_PREFIX(
                "previous learning with signature[{:#018x}] is still in-process, skip "
                "init new learning with signature [{:#018x}]",
                _potential_secondary_states.learning_version,
                signature);
            return;
        }

        _stub->_counter_replicas_learning_recent_start_count->increment();

        _potential_secondary_states.learning_version = signature;
        _potential_secondary_states.learning_start_ts_ns = dsn_now_ns();
        _potential_secondary_states.learning_status = learner_status::LearningWithoutPrepare;
        _prepare_list->truncate(_app->last_committed_decree());
    } else {
        switch (_potential_secondary_states.learning_status) {
        // any failues in the process
        case learner_status::LearningFailed:
            break;

        // learned state (app state) completed
        case learner_status::LearningWithPrepare:
            CHECK_GE_MSG(_app->last_durable_decree() + 1,
                         _potential_secondary_states.learning_start_prepare_decree,
                         "learned state is incomplete");
            {
                // check missing state due to _app->flush to checkpoint the learned state
                auto ac = _app->last_committed_decree();
                auto pc = _prepare_list->last_committed_decree();

                // TODO(qinzuoyan): to test the following lines
                // missing commits
                if (pc > ac) {
                    // missed ones are covered by prepare list
                    if (_prepare_list->count() > 0 && ac + 1 >= _prepare_list->min_decree()) {
                        for (auto d = ac + 1; d <= pc; d++) {
                            auto mu = _prepare_list->get_mutation_by_decree(d);
                            CHECK_NOTNULL(mu, "mutation must not be nullptr, decree = {}", d);
                            auto err = _app->apply_mutation(mu);
                            if (ERR_OK != err) {
                                handle_learning_error(err, true);
                                return;
                            }
                        }
                    }

                    // missed ones need to be loaded via private logs
                    else {
                        _stub->_counter_replicas_learning_recent_round_start_count->increment();
                        _potential_secondary_states.learning_round_is_running = true;
                        _potential_secondary_states.catchup_with_private_log_task =
                            tasking::create_task(LPC_CATCHUP_WITH_PRIVATE_LOGS,
                                                 &_tracker,
                                                 [this]() {
                                                     this->catch_up_with_private_logs(
                                                         partition_status::PS_POTENTIAL_SECONDARY);
                                                 },
                                                 get_gpid().thread_hash());
                        _potential_secondary_states.catchup_with_private_log_task->enqueue();

                        return; // incomplete
                    }
                }

                // no missing commits
                else {
                }

                // convert to success if app state and prepare list is connected
                _potential_secondary_states.learning_status = learner_status::LearningSucceeded;
                // fall through to success
            }

        // app state and prepare list is connected
        case learner_status::LearningSucceeded: {
            check_state_completeness();
            notify_learn_completion();
            return;
        } break;
        case learner_status::LearningWithoutPrepare:
            break;
        default:
            CHECK(false,
                  "invalid learner_status, status = {}",
                  enum_to_string(_potential_secondary_states.learning_status));
        }
    }

    if (_app->last_committed_decree() == 0 &&
        _stub->_learn_app_concurrent_count.load() >= FLAGS_learn_app_max_concurrent_count) {
        LOG_WARNING_PREFIX(
            "init_learn[{:#018x}]: learnee = {}, learn_duration = {} ms, need to learn app "
            "because app_committed_decree = 0, but learn_app_concurrent_count({}) >= "
            "FLAGS_learn_app_max_concurrent_count({}), skip",
            _potential_secondary_states.learning_version,
            _config.primary,
            _potential_secondary_states.duration_ms(),
            _stub->_learn_app_concurrent_count,
            FLAGS_learn_app_max_concurrent_count);
        return;
    }

    _stub->_counter_replicas_learning_recent_round_start_count->increment();
    _potential_secondary_states.learning_round_is_running = true;

    learn_request request;
    request.pid = get_gpid();
    request.__set_max_gced_decree(get_max_gced_decree_for_learn());
    request.last_committed_decree_in_app = _app->last_committed_decree();
    request.last_committed_decree_in_prepare_list = _prepare_list->last_committed_decree();
    request.learner = _stub->_primary_address;
    request.signature = _potential_secondary_states.learning_version;
    _app->prepare_get_checkpoint(request.app_specific_learn_request);

    LOG_INFO_PREFIX("init_learn[{:#018x}]: learnee = {}, learn_duration = {} ms, max_gced_decree = "
                    "{}, local_committed_decree = {}, app_committed_decree = {}, "
                    "app_durable_decree = {}, current_learning_status = {}, total_copy_file_count "
                    "= {}, total_copy_file_size = {}, total_copy_buffer_size = {}",
                    request.signature,
                    _config.primary,
                    _potential_secondary_states.duration_ms(),
                    request.max_gced_decree,
                    last_committed_decree(),
                    _app->last_committed_decree(),
                    _app->last_durable_decree(),
                    enum_to_string(_potential_secondary_states.learning_status),
                    _potential_secondary_states.learning_copy_file_count,
                    _potential_secondary_states.learning_copy_file_size,
                    _potential_secondary_states.learning_copy_buffer_size);

    dsn::message_ex *msg = dsn::message_ex::create_request(RPC_LEARN, 0, get_gpid().thread_hash());
    dsn::marshall(msg, request);
    _potential_secondary_states.learning_task = rpc::call(
        _config.primary,
        msg,
        &_tracker,
        [ this, req_cap = std::move(request) ](error_code err, learn_response && resp) mutable {
            on_learn_reply(err, std::move(req_cap), std::move(resp));
        });
}

// ThreadPool: THREAD_POOL_REPLICATION
decree replica::get_max_gced_decree_for_learn() const // on learner
{
    decree max_gced_decree_for_learn;

    decree plog_max_gced_decree = max_gced_decree_no_lock();
    decree first_learn_start = _potential_secondary_states.first_learn_start_decree;
    if (first_learn_start == invalid_decree) {
        // this is the first round of learn
        max_gced_decree_for_learn = plog_max_gced_decree;
    } else {
        if (plog_max_gced_decree < 0) {
            // The previously learned logs may still reside in learn_dir, and
            // the actual plog dir is empty. In this condition the logs in learn_dir
            // are taken as not-GCed.
            max_gced_decree_for_learn = first_learn_start - 1;
        } else {
            // The actual plog dir is not empty. Use the minimum.
            max_gced_decree_for_learn = std::min(plog_max_gced_decree, first_learn_start - 1);
        }
    }

    return max_gced_decree_for_learn;
}

/*virtual*/ decree replica::max_gced_decree_no_lock() const
{
    return _private_log->max_gced_decree_no_lock(get_gpid());
}

// ThreadPool: THREAD_POOL_REPLICATION
decree replica::get_learn_start_decree(const learn_request &request) // on primary
{
    decree local_committed_decree = last_committed_decree();
    CHECK_LE_PREFIX(request.last_committed_decree_in_app, local_committed_decree);

    decree learn_start_decree_no_dup = request.last_committed_decree_in_app + 1;
    if (!is_duplication_master()) {
        // fast path for no duplication case: only learn those that the learner is not having.
        return learn_start_decree_no_dup;
    }

    decree min_confirmed_decree = _duplication_mgr->min_confirmed_decree();

    // Learner should include the mutations not confirmed by meta server
    // so as to prevent data loss during duplication. For example, when
    // the confirmed+1 decree has been missing from plog, the learner
    // needs to learn back from it.
    //
    //                confirmed but missing
    //                    |
    // learner's plog: ======[--------------]
    //                       |              |
    //                      gced           committed
    //
    // In the above case, primary should return logs started from confirmed+1.

    decree learn_start_decree_for_dup = learn_start_decree_no_dup;
    if (min_confirmed_decree >= 0) {
        learn_start_decree_for_dup = min_confirmed_decree + 1;
    } else {
        // if the confirmed_decree is unsure, copy all the logs
        // TODO(wutao1): can we reduce the copy size?
        decree local_gced = max_gced_decree_no_lock();
        if (local_gced == invalid_decree) {
            // abnormal case
            LOG_WARNING_PREFIX("no plog to be learned for duplication, continue as normal");
        } else {
            learn_start_decree_for_dup = local_gced + 1;
        }
    }

    decree learn_start_decree = learn_start_decree_no_dup;
    if (learn_start_decree_for_dup <= request.max_gced_decree ||
        request.max_gced_decree == invalid_decree) {
        // `request.max_gced_decree == invalid_decree` indicates the learner has no log,
        // see replica::get_max_gced_decree_for_learn for details.
        if (learn_start_decree_for_dup < learn_start_decree_no_dup) {
            learn_start_decree = learn_start_decree_for_dup;
            LOG_INFO_PREFIX("learn_start_decree steps back to {} to ensure learner having enough "
                            "logs for duplication [confirmed_decree={}, learner_gced_decree={}]",
                            learn_start_decree,
                            min_confirmed_decree,
                            request.max_gced_decree);
        }
    }
    CHECK_LE_PREFIX(learn_start_decree, local_committed_decree + 1);
    CHECK_GT_PREFIX(learn_start_decree, 0); // learn_start_decree can never be invalid_decree
    return learn_start_decree;
}

void replica::on_learn(dsn::message_ex *msg, const learn_request &request)
{
    _checker.only_one_thread_access();

    learn_response response;
    if (partition_status::PS_PRIMARY != status()) {
        response.err = (partition_status::PS_INACTIVE == status() && _inactive_is_transient)
                           ? ERR_INACTIVE_STATE
                           : ERR_INVALID_STATE;
        reply(msg, response);
        return;
    }

    // but just set state to partition_status::PS_POTENTIAL_SECONDARY
    _primary_states.get_replica_config(partition_status::PS_POTENTIAL_SECONDARY, response.config);

    auto it = _primary_states.learners.find(request.learner);
    if (it == _primary_states.learners.end()) {
        response.config.status = partition_status::PS_INACTIVE;
        response.err = ERR_OBJECT_NOT_FOUND;
        reply(msg, response);
        return;
    }

    remote_learner_state &learner_state = it->second;
    if (learner_state.signature != request.signature) {
        response.config.learner_signature = learner_state.signature;
        response.err = ERR_WRONG_CHECKSUM; // means invalid signature
        reply(msg, response);
        return;
    }

    // prepare learn_start_decree
    decree local_committed_decree = last_committed_decree();

    // TODO: learner machine has been down for a long time, and DDD MUST happened before
    // which leads to state lost. Now the lost state is back, what shall we do?
    if (request.last_committed_decree_in_app > last_prepared_decree()) {
        LOG_ERROR_PREFIX("on_learn[{:#018x}]: learner = {}, learner state is newer than learnee, "
                         "learner_app_committed_decree = {}, local_committed_decree = {}, learn "
                         "from scratch",
                         request.signature,
                         request.learner,
                         request.last_committed_decree_in_app,
                         local_committed_decree);

        *(decree *)&request.last_committed_decree_in_app = 0;
    }

    // mutations are previously committed already on learner (old primary)
    // this happens when the new primary does not commit the previously prepared mutations
    // yet, which it should do, so let's help it now.
    else if (request.last_committed_decree_in_app > local_committed_decree) {
        LOG_ERROR_PREFIX("on_learn[{:#018x}]: learner = {}, learner's last_committed_decree_in_app "
                         "is newer than learnee, learner_app_committed_decree = {}, "
                         "local_committed_decree = {}, commit local soft",
                         request.signature,
                         request.learner,
                         request.last_committed_decree_in_app,
                         local_committed_decree);

        // we shouldn't commit mutations hard coz these mutations may preparing on another learner
        _prepare_list->commit(request.last_committed_decree_in_app, COMMIT_TO_DECREE_SOFT);
        local_committed_decree = last_committed_decree();

        if (request.last_committed_decree_in_app > local_committed_decree) {
            LOG_ERROR_PREFIX("on_learn[{:#018x}]: try to commit primary to {}, still less than "
                             "learner({})'s committed decree({}), wait mutations to be commitable",
                             request.signature,
                             local_committed_decree,
                             request.learner,
                             request.last_committed_decree_in_app);
            response.err = ERR_INCONSISTENT_STATE;
            reply(msg, response);
            return;
        }
    }

    CHECK_LE(request.last_committed_decree_in_app, local_committed_decree);

    const decree learn_start_decree = get_learn_start_decree(request);
    response.state.__set_learn_start_decree(learn_start_decree);
    bool delayed_replay_prepare_list = false;

    LOG_INFO_PREFIX("on_learn[{:#018x}]: learner = {}, remote_committed_decree = {}, "
                    "remote_app_committed_decree = {}, local_committed_decree = {}, "
                    "app_committed_decree = {}, app_durable_decree = {}, "
                    "prepare_min_decree = {}, prepare_list_count = {}, learn_start_decree = {}",
                    request.signature,
                    request.learner,
                    request.last_committed_decree_in_prepare_list,
                    request.last_committed_decree_in_app,
                    local_committed_decree,
                    _app->last_committed_decree(),
                    _app->last_durable_decree(),
                    _prepare_list->min_decree(),
                    _prepare_list->count(),
                    learn_start_decree);

    response.address = _stub->_primary_address;
    response.prepare_start_decree = invalid_decree;
    response.last_committed_decree = local_committed_decree;
    response.err = ERR_OK;

    // learn delta state or checkpoint
    bool should_learn_cache = prepare_cached_learn_state(request,
                                                         learn_start_decree,
                                                         local_committed_decree,
                                                         learner_state,
                                                         response,
                                                         delayed_replay_prepare_list);
    if (!should_learn_cache) {
        if (learn_start_decree > _app->last_durable_decree()) {
            LOG_INFO_PREFIX("on_learn[{:#018x}]: learner = {}, choose to learn private logs, "
                            "because learn_start_decree({}) > _app->last_durable_decree({})",
                            request.signature,
                            request.learner,
                            learn_start_decree,
                            _app->last_durable_decree());
            _private_log->get_learn_state(get_gpid(), learn_start_decree, response.state);
            response.type = learn_type::LT_LOG;
        } else if (_private_log->get_learn_state(get_gpid(), learn_start_decree, response.state)) {
            LOG_INFO_PREFIX("on_learn[{:#018x}]: learner = {}, choose to learn private logs, "
                            "because mutation_log::get_learn_state() returns true",
                            request.signature,
                            request.learner);
            response.type = learn_type::LT_LOG;
        } else if (learn_start_decree < request.last_committed_decree_in_app + 1) {
            LOG_INFO_PREFIX("on_learn[{:#018x}]: learner = {}, choose to learn private logs, "
                            "because learn_start_decree steps back for duplication",
                            request.signature,
                            request.learner);
            response.type = learn_type::LT_LOG;
        } else {
            LOG_INFO_PREFIX("on_learn[{:#018x}]: learner = {}, choose to learn app, beacuse "
                            "learn_start_decree({}) <= _app->last_durable_decree({}), and "
                            "mutation_log::get_learn_state() returns false",
                            request.signature,
                            request.learner,
                            learn_start_decree,
                            _app->last_durable_decree());
            response.type = learn_type::LT_APP;
            response.state = learn_state();
        }

        if (response.type == learn_type::LT_LOG) {
            response.base_local_dir = _private_log->dir();
            if (response.state.files.size() > 0) {
                auto &last_file = response.state.files.back();
                if (last_file == learner_state.last_learn_log_file) {
                    LOG_INFO_PREFIX("on_learn[{:#018x}]: learner = {}, learn the same file {} "
                                    "repeatedly, hint to switch file",
                                    request.signature,
                                    request.learner,
                                    last_file);
                    _private_log->hint_switch_file();
                } else {
                    learner_state.last_learn_log_file = last_file;
                }
            }
            // it is safe to commit to last_committed_decree() now
            response.state.to_decree_included = last_committed_decree();
            LOG_INFO_PREFIX("on_learn[{:#018x}]: learner = {}, learn private logs succeed, "
                            "learned_meta_size = {}, learned_file_count = {}, to_decree_included = "
                            "{}",
                            request.signature,
                            request.learner,
                            response.state.meta.length(),
                            response.state.files.size(),
                            response.state.to_decree_included);
        } else {
            ::dsn::error_code err = _app->get_checkpoint(
                learn_start_decree, request.app_specific_learn_request, response.state);

            if (err != ERR_OK) {
                response.err = ERR_GET_LEARN_STATE_FAILED;
                LOG_ERROR_PREFIX(
                    "on_learn[{:#018x}]: learner = {}, get app checkpoint failed, error = {}",
                    request.signature,
                    request.learner,
                    err);
            } else {
                response.base_local_dir = _app->data_dir();
                response.__set_replica_disk_tag(_dir_node->tag);
                LOG_INFO_PREFIX(
                    "on_learn[{:#018x}]: learner = {}, get app learn state succeed, "
                    "learned_meta_size = {}, learned_file_count = {}, learned_to_decree = {}",
                    request.signature,
                    request.learner,
                    response.state.meta.length(),
                    response.state.files.size(),
                    response.state.to_decree_included);
            }
        }
    }

    for (auto &file : response.state.files) {
        file = file.substr(response.base_local_dir.length() + 1);
    }

    reply(msg, response);

    // the replayed prepare msg needs to be AFTER the learning response msg
    if (delayed_replay_prepare_list) {
        replay_prepare_list();
    }
}

void replica::on_learn_reply(error_code err, learn_request &&req, learn_response &&resp)
{
    _checker.only_one_thread_access();

    CHECK_EQ(partition_status::PS_POTENTIAL_SECONDARY, status());
    CHECK_EQ(req.signature, _potential_secondary_states.learning_version);

    if (err != ERR_OK) {
        handle_learning_error(err, false);
        return;
    }

    LOG_INFO_PREFIX(
        "on_learn_reply_start[{}]: learnee = {}, learn_duration ={} ms, response_err = "
        "{}, remote_committed_decree = {}, prepare_start_decree = {}, learn_type = {} "
        "learned_buffer_size = {}, learned_file_count = {},to_decree_included = "
        "{}, learn_start_decree = {}, last_commit_decree = {}, current_learning_status = "
        "{} ",
        req.signature,
        resp.config.primary.to_string(),
        _potential_secondary_states.duration_ms(),
        resp.err.to_string(),
        resp.last_committed_decree,
        resp.prepare_start_decree,
        enum_to_string(resp.type),
        resp.state.meta.length(),
        static_cast<uint32_t>(resp.state.files.size()),
        resp.state.to_decree_included,
        resp.state.learn_start_decree,
        _app->last_committed_decree(),
        enum_to_string(_potential_secondary_states.learning_status));

    _potential_secondary_states.learning_copy_buffer_size += resp.state.meta.length();
    _stub->_counter_replicas_learning_recent_copy_buffer_size->add(resp.state.meta.length());

    if (resp.err != ERR_OK) {
        if (resp.err == ERR_INACTIVE_STATE || resp.err == ERR_INCONSISTENT_STATE) {
            LOG_WARNING_PREFIX("on_learn_reply[{:#018x}]: learnee = {}, learnee is updating "
                               "ballot(inactive state) or reconciliation(inconsistent state), "
                               "delay to start another round of learning",
                               req.signature,
                               resp.config.primary);
            _potential_secondary_states.learning_round_is_running = false;
            _potential_secondary_states.delay_learning_task =
                tasking::create_task(LPC_DELAY_LEARN,
                                     &_tracker,
                                     std::bind(&replica::init_learn, this, req.signature),
                                     get_gpid().thread_hash());
            _potential_secondary_states.delay_learning_task->enqueue(std::chrono::seconds(1));
        } else {
            handle_learning_error(resp.err, false);
        }
        return;
    }

    if (resp.config.ballot > get_ballot()) {
        LOG_INFO_PREFIX("on_learn_reply[{:#018x}]: learnee = {}, update configuration because "
                        "ballot have changed",
                        req.signature,
                        resp.config.primary);
        CHECK(update_local_configuration(resp.config), "");
    }

    if (status() != partition_status::PS_POTENTIAL_SECONDARY) {
        LOG_ERROR_PREFIX(
            "on_learn_reply[{:#018x}]: learnee = {}, current_status = {}, stop learning",
            req.signature,
            resp.config.primary,
            enum_to_string(status()));
        return;
    }

    // local state is newer than learnee
    if (resp.last_committed_decree < _app->last_committed_decree()) {
        LOG_WARNING_PREFIX("on_learn_reply[{:#018x}]: learnee = {}, learner state is newer than "
                           "learnee (primary): {} vs {}, create new app",
                           req.signature,
                           resp.config.primary,
                           _app->last_committed_decree(),
                           resp.last_committed_decree);

        _stub->_counter_replicas_learning_recent_learn_reset_count->increment();

        // close app
        auto err = _app->close(true);
        if (err != ERR_OK) {
            LOG_ERROR_PREFIX(
                "on_learn_reply[{:#018x}]: learnee = {}, close app (with clear_state=true) "
                "failed, err = {}",
                req.signature,
                resp.config.primary,
                err);
        }

        // backup old data dir
        if (err == ERR_OK) {
            std::string old_dir = _app->data_dir();
            if (dsn::utils::filesystem::directory_exists(old_dir)) {
                char rename_dir[1024];
                sprintf(rename_dir, "%s.%" PRIu64 ".discarded", old_dir.c_str(), dsn_now_us());
                CHECK(dsn::utils::filesystem::rename_path(old_dir, rename_dir),
                      "{}: failed to move directory from '{}' to '{}'",
                      name(),
                      old_dir,
                      rename_dir);
                LOG_WARNING_PREFIX("replica_dir_op succeed to move directory from '{}' to '{}'",
                                   old_dir,
                                   rename_dir);
            }
        }

        if (err == ERR_OK) {
            err = _app->open_new_internal(this,
                                          _stub->_log->on_partition_reset(get_gpid(), 0),
                                          _private_log->on_partition_reset(get_gpid(), 0));

            if (err != ERR_OK) {
                LOG_ERROR_PREFIX("on_learn_reply[{:#018x}]: learnee = {}, open app (with "
                                 "create_new=true) failed, err = {}",
                                 req.signature,
                                 resp.config.primary,
                                 err);
            }
        }

        if (err == ERR_OK) {
            CHECK_EQ_MSG(_app->last_committed_decree(), 0, "must be zero after app::open(true)");
            CHECK_EQ_MSG(_app->last_durable_decree(), 0, "must be zero after app::open(true)");

            // reset prepare list
            _prepare_list->reset(0);
        }

        if (err != ERR_OK) {
            _potential_secondary_states.learn_remote_files_task =
                tasking::create_task(LPC_LEARN_REMOTE_DELTA_FILES, &_tracker, [
                    this,
                    err,
                    copy_start = _potential_secondary_states.duration_ms(),
                    req_cap = std::move(req),
                    resp_cap = std::move(resp)
                ]() mutable {
                    on_copy_remote_state_completed(
                        err, 0, copy_start, std::move(req_cap), std::move(resp_cap));
                });
            _potential_secondary_states.learn_remote_files_task->enqueue();
            return;
        }
    }

    if (resp.type == learn_type::LT_APP) {
        if (++_stub->_learn_app_concurrent_count > FLAGS_learn_app_max_concurrent_count) {
            --_stub->_learn_app_concurrent_count;
            LOG_WARNING_PREFIX(
                "on_learn_reply[{:#018x}]: learnee = {}, learn_app_concurrent_count({}) >= "
                "FLAGS_learn_app_max_concurrent_count({}), skip this round",
                _potential_secondary_states.learning_version,
                _config.primary,
                _stub->_learn_app_concurrent_count,
                FLAGS_learn_app_max_concurrent_count);
            _potential_secondary_states.learning_round_is_running = false;
            return;
        } else {
            _potential_secondary_states.learn_app_concurrent_count_increased = true;
            LOG_INFO_PREFIX(
                "on_learn_reply[{:#018x}]: learnee = {}, ++learn_app_concurrent_count = {}",
                _potential_secondary_states.learning_version,
                _config.primary,
                _stub->_learn_app_concurrent_count.load());
        }
    }

    switch (resp.type) {
    case learn_type::LT_CACHE:
        _stub->_counter_replicas_learning_recent_learn_cache_count->increment();
        break;
    case learn_type::LT_APP:
        _stub->_counter_replicas_learning_recent_learn_app_count->increment();
        break;
    case learn_type::LT_LOG:
        _stub->_counter_replicas_learning_recent_learn_log_count->increment();
        break;
    default:
        // do nothing
        break;
    }

    if (resp.prepare_start_decree != invalid_decree) {
        CHECK_EQ(resp.type, learn_type::LT_CACHE);
        CHECK(resp.state.files.empty(), "");
        CHECK_EQ(_potential_secondary_states.learning_status,
                 learner_status::LearningWithoutPrepare);
        _potential_secondary_states.learning_status = learner_status::LearningWithPrepareTransient;

        // reset log positions for later mutations
        // WARNING: it still requires checkpoint operation in later
        // on_copy_remote_state_completed to ensure the state is completed
        // if there is a failure in between, our checking
        // during app::open_internal will invalidate the logs
        // appended by the mutations AFTER current position
        err = _app->update_init_info(
            this,
            _stub->_log->on_partition_reset(get_gpid(), _app->last_committed_decree()),
            _private_log->on_partition_reset(get_gpid(), _app->last_committed_decree()),
            _app->last_committed_decree());

        // switch private log to make learning easier
        _private_log->demand_switch_file();

        // reset preparelist
        _potential_secondary_states.learning_start_prepare_decree = resp.prepare_start_decree;
        _prepare_list->truncate(_app->last_committed_decree());
        LOG_INFO_PREFIX("on_learn_reply[{:#018x}]: learnee = {}, truncate prepare list, "
                        "local_committed_decree = {}, current_learning_status = {}",
                        req.signature,
                        resp.config.primary,
                        _app->last_committed_decree(),
                        enum_to_string(_potential_secondary_states.learning_status));

        // persist incoming mutations into private log and apply them to prepare-list
        std::pair<decree, decree> cache_range;
        binary_reader reader(resp.state.meta);
        while (!reader.is_eof()) {
            auto mu = mutation::read_from(reader, nullptr);
            if (mu->data.header.decree > last_committed_decree()) {
                LOG_DEBUG_PREFIX("on_learn_reply[{:#018x}]: apply learned mutation {}",
                                 req.signature,
                                 mu->name());

                // write to private log with no callback, the later 2pc ensures that logs
                // are written to the disk
                _private_log->append(mu, LPC_WRITE_REPLICATION_LOG_COMMON, &_tracker, nullptr);

                // because private log are written without callback, need to manully set flag
                mu->set_logged();

                // then we prepare, it is possible that a committed mutation exists in learner's
                // prepare log,
                // but with DIFFERENT ballot. Reference https://github.com/imzhenyu/rDSN/issues/496
                mutation_ptr existing_mutation =
                    _prepare_list->get_mutation_by_decree(mu->data.header.decree);
                if (existing_mutation != nullptr &&
                    existing_mutation->data.header.ballot > mu->data.header.ballot) {
                    LOG_INFO_PREFIX("on_learn_reply[{:#018x}]: learnee = {}, mutation({}) exist on "
                                    "the learner with larger ballot {}",
                                    req.signature,
                                    resp.config.primary,
                                    mu->name(),
                                    existing_mutation->data.header.ballot);
                } else {
                    _prepare_list->prepare(mu, partition_status::PS_POTENTIAL_SECONDARY);
                }

                if (cache_range.first == 0 || mu->data.header.decree < cache_range.first)
                    cache_range.first = mu->data.header.decree;
                if (cache_range.second == 0 || mu->data.header.decree > cache_range.second)
                    cache_range.second = mu->data.header.decree;
            }
        }

        LOG_INFO_PREFIX("on_learn_reply[{:#018x}]: learnee = {}, learn_duration = {} ms, apply "
                        "cache done, prepare_cache_range = <{}, {}>, local_committed_decree = {}, "
                        "app_committed_decree = {}, current_learning_status = {}",
                        req.signature,
                        resp.config.primary,
                        _potential_secondary_states.duration_ms(),
                        cache_range.first,
                        cache_range.second,
                        last_committed_decree(),
                        _app->last_committed_decree(),
                        enum_to_string(_potential_secondary_states.learning_status));

        // further states are synced using 2pc, and we must commit now as those later 2pc messages
        // thinks they should
        _prepare_list->commit(resp.prepare_start_decree - 1, COMMIT_TO_DECREE_HARD);
        CHECK_EQ(_prepare_list->last_committed_decree(), _app->last_committed_decree());
        CHECK(resp.state.files.empty(), "");

        // all state is complete
        CHECK_GE_MSG(_app->last_committed_decree() + 1,
                     _potential_secondary_states.learning_start_prepare_decree,
                     "state is incomplete");

        // go to next stage
        _potential_secondary_states.learning_status = learner_status::LearningWithPrepare;
        _potential_secondary_states.learn_remote_files_task =
            tasking::create_task(LPC_LEARN_REMOTE_DELTA_FILES, &_tracker, [
                this,
                err,
                copy_start = _potential_secondary_states.duration_ms(),
                req_cap = std::move(req),
                resp_cap = std::move(resp)
            ]() mutable {
                on_copy_remote_state_completed(
                    err, 0, copy_start, std::move(req_cap), std::move(resp_cap));
            });
        _potential_secondary_states.learn_remote_files_task->enqueue();
    }

    else if (resp.state.files.size() > 0) {
        auto learn_dir = _app->learn_dir();
        utils::filesystem::remove_path(learn_dir);
        utils::filesystem::create_directory(learn_dir);

        if (!dsn::utils::filesystem::directory_exists(learn_dir)) {
            LOG_ERROR_PREFIX(
                "on_learn_reply[{:#018x}]: learnee = {}, create replica learn dir {} failed",
                req.signature,
                resp.config.primary,
                learn_dir);

            _potential_secondary_states.learn_remote_files_task =
                tasking::create_task(LPC_LEARN_REMOTE_DELTA_FILES, &_tracker, [
                    this,
                    copy_start = _potential_secondary_states.duration_ms(),
                    req_cap = std::move(req),
                    resp_cap = std::move(resp)
                ]() mutable {
                    on_copy_remote_state_completed(ERR_FILE_OPERATION_FAILED,
                                                   0,
                                                   copy_start,
                                                   std::move(req_cap),
                                                   std::move(resp_cap));
                });
            _potential_secondary_states.learn_remote_files_task->enqueue();
            return;
        }

        bool high_priority = (resp.type == learn_type::LT_APP ? false : true);
        LOG_INFO_PREFIX("on_learn_reply[{:#018x}]: learnee = {}, learn_duration = {} ms, start to "
                        "copy remote files, copy_file_count = {}, priority = {}",
                        req.signature,
                        resp.config.primary,
                        _potential_secondary_states.duration_ms(),
                        resp.state.files.size(),
                        high_priority ? "high" : "low");

        _potential_secondary_states.learn_remote_files_task = _stub->_nfs->copy_remote_files(
            resp.config.primary,
            resp.replica_disk_tag,
            resp.base_local_dir,
            resp.state.files,
            _dir_node->tag,
            learn_dir,
            get_gpid(),
            true, // overwrite
            high_priority,
            LPC_REPLICATION_COPY_REMOTE_FILES,
            &_tracker,
            [
              this,
              copy_start = _potential_secondary_states.duration_ms(),
              req_cap = std::move(req),
              resp_copy = resp
            ](error_code err, size_t sz) mutable {
                on_copy_remote_state_completed(
                    err, sz, copy_start, std::move(req_cap), std::move(resp_copy));
            });
    } else {
        _potential_secondary_states.learn_remote_files_task =
            tasking::create_task(LPC_LEARN_REMOTE_DELTA_FILES, &_tracker, [
                this,
                copy_start = _potential_secondary_states.duration_ms(),
                req_cap = std::move(req),
                resp_cap = std::move(resp)
            ]() mutable {
                on_copy_remote_state_completed(
                    ERR_OK, 0, copy_start, std::move(req_cap), std::move(resp_cap));
            });
        _potential_secondary_states.learn_remote_files_task->enqueue();
    }
}

bool replica::prepare_cached_learn_state(const learn_request &request,
                                         decree learn_start_decree,
                                         decree local_committed_decree,
                                         /*out*/ remote_learner_state &learner_state,
                                         /*out*/ learn_response &response,
                                         /*out*/ bool &delayed_replay_prepare_list)
{
    // set prepare_start_decree when to-be-learn state is covered by prepare list,
    // note min_decree can be NOT present in prepare list when list.count == 0
    if (learn_start_decree > _prepare_list->min_decree() ||
        (learn_start_decree == _prepare_list->min_decree() && _prepare_list->count() > 0)) {
        if (learner_state.prepare_start_decree == invalid_decree) {
            // start from (last_committed_decree + 1)
            learner_state.prepare_start_decree = local_committed_decree + 1;

            cleanup_preparing_mutations(false);

            // the replayed prepare msg needs to be AFTER the learning response msg
            // to reduce probability that preparing messages arrive remote early than
            // learning response msg.
            delayed_replay_prepare_list = true;

            LOG_INFO_PREFIX("on_learn[{:#018x}]: learner = {}, set prepare_start_decree = {}",
                            request.signature,
                            request.learner,
                            local_committed_decree + 1);
        }

        response.prepare_start_decree = learner_state.prepare_start_decree;
    } else {
        learner_state.prepare_start_decree = invalid_decree;
    }

    // only learn mutation cache in range of [learn_start_decree, prepare_start_decree),
    // in this case, the state on the PS should be contiguous (+ to-be-sent prepare list)
    if (response.prepare_start_decree != invalid_decree) {
        binary_writer writer;
        int count = 0;
        for (decree d = learn_start_decree; d < response.prepare_start_decree; d++) {
            auto mu = _prepare_list->get_mutation_by_decree(d);
            CHECK_NOTNULL(mu, "mutation must not be nullptr, decree = {}", d);
            mu->write_to(writer, nullptr);
            count++;
        }
        response.type = learn_type::LT_CACHE;
        response.state.meta = writer.get_buffer();
        LOG_INFO_PREFIX("on_learn[{:#018x}]: learner = {}, learn mutation cache succeed, "
                        "learn_start_decree = {}, prepare_start_decree = {}, learn_mutation_count "
                        "= {}, learn_data_size = {}",
                        request.signature,
                        request.learner,
                        learn_start_decree,
                        response.prepare_start_decree,
                        count,
                        response.state.meta.length());
        return true;
    }
    return false;
}

void replica::on_copy_remote_state_completed(error_code err,
                                             size_t size,
                                             uint64_t copy_start_time,
                                             learn_request &&req,
                                             learn_response &&resp)
{
    decree old_prepared = last_prepared_decree();
    decree old_committed = last_committed_decree();
    decree old_app_committed = _app->last_committed_decree();
    decree old_app_durable = _app->last_durable_decree();

    LOG_INFO_PREFIX("on_copy_remote_state_completed[{:#018x}]: learnee = {}, learn_duration = {} "
                    "ms, copy remote state done, err = {}, copy_file_count = {}, copy_file_size = "
                    "{}, copy_time_used = {} ms, local_committed_decree = {}, app_committed_decree "
                    "= {}, app_durable_decree = {}, prepare_start_decree = {}, "
                    "current_learning_status = {}",
                    req.signature,
                    resp.config.primary,
                    _potential_secondary_states.duration_ms(),
                    err,
                    resp.state.files.size(),
                    size,
                    _potential_secondary_states.duration_ms() - copy_start_time,
                    last_committed_decree(),
                    _app->last_committed_decree(),
                    _app->last_durable_decree(),
                    resp.prepare_start_decree,
                    enum_to_string(_potential_secondary_states.learning_status));

    if (resp.type == learn_type::LT_APP) {
        --_stub->_learn_app_concurrent_count;
        _potential_secondary_states.learn_app_concurrent_count_increased = false;
        LOG_INFO_PREFIX("on_copy_remote_state_completed[{:#018x}]: learnee = {}, "
                        "--learn_app_concurrent_count = {}",
                        _potential_secondary_states.learning_version,
                        _config.primary,
                        _stub->_learn_app_concurrent_count.load());
    }

    if (err == ERR_OK) {
        _potential_secondary_states.learning_copy_file_count += resp.state.files.size();
        _potential_secondary_states.learning_copy_file_size += size;
        _stub->_counter_replicas_learning_recent_copy_file_count->add(resp.state.files.size());
        _stub->_counter_replicas_learning_recent_copy_file_size->add(size);
    }

    if (err != ERR_OK) {
        // do nothing
    } else if (_potential_secondary_states.learning_status == learner_status::LearningWithPrepare) {
        CHECK_EQ(resp.type, learn_type::LT_CACHE);
    } else {
        CHECK(resp.type == learn_type::LT_APP || resp.type == learn_type::LT_LOG,
              "invalid learn_type, type = {}",
              enum_to_string(resp.type));

        learn_state lstate;
        lstate.from_decree_excluded = resp.state.from_decree_excluded;
        lstate.to_decree_included = resp.state.to_decree_included;
        lstate.meta = resp.state.meta;
        if (resp.state.__isset.learn_start_decree) {
            lstate.__set_learn_start_decree(resp.state.learn_start_decree);
        }

        for (auto &f : resp.state.files) {
            std::string file = utils::filesystem::path_combine(_app->learn_dir(), f);
            lstate.files.push_back(file);
        }

        // apply app learning
        if (resp.type == learn_type::LT_APP) {
            auto start_ts = dsn_now_ns();
            err = _app->apply_checkpoint(replication_app_base::chkpt_apply_mode::learn, lstate);
            if (err == ERR_OK) {

                CHECK_GE(_app->last_committed_decree(), _app->last_durable_decree());
                // because if the original _app->last_committed_decree > resp.last_committed_decree,
                // the learn_start_decree will be set to 0, which makes learner to learn from
                // scratch
                CHECK_LE(_app->last_committed_decree(), resp.last_committed_decree);
                LOG_INFO_PREFIX("on_copy_remote_state_completed[{:#018x}]: learnee = {}, "
                                "learn_duration = {} ms, checkpoint duration = {} ns, apply "
                                "checkpoint succeed, app_committed_decree = {}",
                                req.signature,
                                resp.config.primary,
                                _potential_secondary_states.duration_ms(),
                                dsn_now_ns() - start_ts,
                                _app->last_committed_decree());
            } else {
                LOG_ERROR_PREFIX("on_copy_remote_state_completed[{:#018x}]: learnee = {}, "
                                 "learn_duration = {} ms, checkpoint duration = {} ns, apply "
                                 "checkpoint failed, err = {}",
                                 req.signature,
                                 resp.config.primary,
                                 _potential_secondary_states.duration_ms(),
                                 dsn_now_ns() - start_ts,
                                 err);
            }
        }

        // apply log learning
        else {
            auto start_ts = dsn_now_ns();
            err = apply_learned_state_from_private_log(lstate);
            if (err == ERR_OK) {
                LOG_INFO_PREFIX("on_copy_remote_state_completed[{:#018x}]: learnee = {}, "
                                "learn_duration = {} ms, apply_log_duration = {} ns, apply learned "
                                "state from private log succeed, app_committed_decree = {}",
                                req.signature,
                                resp.config.primary,
                                _potential_secondary_states.duration_ms(),
                                dsn_now_ns() - start_ts,
                                _app->last_committed_decree());
            } else {
                LOG_ERROR_PREFIX("on_copy_remote_state_completed[{:#018x}]: learnee = {}, "
                                 "learn_duration = {} ms, apply_log_duration = {} ns, apply "
                                 "learned state from private log failed, err = {}",
                                 req.signature,
                                 resp.config.primary,
                                 _potential_secondary_states.duration_ms(),
                                 dsn_now_ns() - start_ts,
                                 err);
            }
        }

        // reset prepare list to make it catch with app
        _prepare_list->reset(_app->last_committed_decree());

        LOG_INFO_PREFIX("on_copy_remote_state_completed[{:#018x}]: learnee = {}, learn_duration = "
                        "{} ms, apply checkpoint/log done, err = {}, last_prepared_decree = ({} => "
                        "{}), last_committed_decree = ({} => {}), app_committed_decree = ({} => "
                        "{}), app_durable_decree = ({} => {}), remote_committed_decree = {}, "
                        "prepare_start_decree = {}, current_learning_status = {}",
                        req.signature,
                        resp.config.primary,
                        _potential_secondary_states.duration_ms(),
                        err,
                        old_prepared,
                        last_prepared_decree(),
                        old_committed,
                        last_committed_decree(),
                        old_app_committed,
                        _app->last_committed_decree(),
                        old_app_durable,
                        _app->last_durable_decree(),
                        resp.last_committed_decree,
                        resp.prepare_start_decree,
                        enum_to_string(_potential_secondary_states.learning_status));
    }

    // if catch-up done, do flush to enable all learned state is durable
    if (err == ERR_OK && resp.prepare_start_decree != invalid_decree &&
        _app->last_committed_decree() + 1 >=
            _potential_secondary_states.learning_start_prepare_decree &&
        _app->last_committed_decree() > _app->last_durable_decree()) {
        err = background_sync_checkpoint();

        LOG_INFO_PREFIX("on_copy_remote_state_completed[{:#018x}]: learnee = {}, learn_duration = "
                        "{} ms, flush done, err = {}, app_committed_decree = {}, "
                        "app_durable_decree = {}",
                        req.signature,
                        resp.config.primary,
                        _potential_secondary_states.duration_ms(),
                        err,
                        _app->last_committed_decree(),
                        _app->last_durable_decree());

        if (err == ERR_OK) {
            CHECK_EQ(_app->last_committed_decree(), _app->last_durable_decree());
        }
    }

    // it is possible that the _potential_secondary_states.learn_remote_files_task is still running
    // while its body is definitely done already as being here, so we manually set its value to
    // nullptr
    // so that we don't have unnecessary failed reconfiguration later due to this non-nullptr in
    // cleanup
    _potential_secondary_states.learn_remote_files_task = nullptr;

    _potential_secondary_states.learn_remote_files_completed_task =
        tasking::create_task(LPC_LEARN_REMOTE_DELTA_FILES_COMPLETED,
                             &_tracker,
                             [this, err]() { on_learn_remote_state_completed(err); },
                             get_gpid().thread_hash());
    _potential_secondary_states.learn_remote_files_completed_task->enqueue();
}

void replica::on_learn_remote_state_completed(error_code err)
{
    _checker.only_one_thread_access();

    if (partition_status::PS_POTENTIAL_SECONDARY != status()) {
        LOG_WARNING_PREFIX("on_learn_remote_state_completed[{:#018x}]: learnee = {}, "
                           "learn_duration = {} ms, err = {}, the learner status is not "
                           "PS_POTENTIAL_SECONDARY, but {}, ignore",
                           _potential_secondary_states.learning_version,
                           _config.primary,
                           _potential_secondary_states.duration_ms(),
                           err,
                           enum_to_string(status()));
        return;
    }

    LOG_INFO_PREFIX("on_learn_remote_state_completed[{:#018x}]: learnee = {}, learn_duration = {} "
                    "ms, err = {}, local_committed_decree = {}, app_committed_decree = {}, "
                    "app_durable_decree = {}, current_learning_status = {}",
                    _potential_secondary_states.learning_version,
                    _config.primary,
                    _potential_secondary_states.duration_ms(),
                    err,
                    last_committed_decree(),
                    _app->last_committed_decree(),
                    _app->last_durable_decree(),
                    enum_to_string(_potential_secondary_states.learning_status));

    _potential_secondary_states.learning_round_is_running = false;

    if (err != ERR_OK) {
        handle_learning_error(err, true);
    } else {
        // continue
        init_learn(_potential_secondary_states.learning_version);
    }
}

void replica::handle_learning_error(error_code err, bool is_local_error)
{
    _checker.only_one_thread_access();

    LOG_ERROR_PREFIX(
        "handle_learning_error[{:#018x}]: learnee = {}, learn_duration = {} ms, err = {}, {}",
        _potential_secondary_states.learning_version,
        _config.primary,
        _potential_secondary_states.duration_ms(),
        err,
        is_local_error ? "local_error" : "remote error");

    if (is_local_error) {
        if (err == ERR_DISK_IO_ERROR) {
            _dir_node->status = disk_status::IO_ERROR;
        } else if (err == ERR_RDB_CORRUPTION) {
            _data_corrupted = true;
        }
    }

    _stub->_counter_replicas_learning_recent_learn_fail_count->increment();

    update_local_configuration_with_no_ballot_change(
        is_local_error ? partition_status::PS_ERROR : partition_status::PS_INACTIVE);
}

error_code replica::handle_learning_succeeded_on_primary(::dsn::rpc_address node,
                                                         uint64_t learn_signature)
{
    auto it = _primary_states.learners.find(node);
    if (it == _primary_states.learners.end()) {
        LOG_ERROR_PREFIX("handle_learning_succeeded_on_primary[{:#018x}]: learner = {}, learner "
                         "not found on primary, return ERR_LEARNER_NOT_FOUND",
                         learn_signature,
                         node);
        return ERR_LEARNER_NOT_FOUND;
    }

    if (it->second.signature != (int64_t)learn_signature) {
        LOG_ERROR_PREFIX("handle_learning_succeeded_on_primary[{:#018x}]: learner = {}, signature "
                         "not matched, current signature on primary is [{:#018x}], return "
                         "ERR_INVALID_STATE",
                         learn_signature,
                         node,
                         it->second.signature);
        return ERR_INVALID_STATE;
    }

    upgrade_to_secondary_on_primary(node);
    return ERR_OK;
}

void replica::notify_learn_completion()
{
    group_check_response report;
    report.pid = get_gpid();
    report.err = ERR_OK;
    report.last_committed_decree_in_app = _app->last_committed_decree();
    report.last_committed_decree_in_prepare_list = last_committed_decree();
    report.learner_signature = _potential_secondary_states.learning_version;
    report.learner_status_ = _potential_secondary_states.learning_status;
    report.node = _stub->_primary_address;

    LOG_INFO_PREFIX("notify_learn_completion[{:#018x}]: learnee = {}, learn_duration = {} ms, "
                    "local_committed_decree = {}, app_committed_decree = {}, app_durable_decree = "
                    "{}, current_learning_status = {}",
                    _potential_secondary_states.learning_version,
                    _config.primary,
                    _potential_secondary_states.duration_ms(),
                    last_committed_decree(),
                    _app->last_committed_decree(),
                    _app->last_durable_decree(),
                    enum_to_string(_potential_secondary_states.learning_status));

    if (_potential_secondary_states.completion_notify_task != nullptr) {
        _potential_secondary_states.completion_notify_task->cancel(false);
    }

    dsn::message_ex *msg =
        dsn::message_ex::create_request(RPC_LEARN_COMPLETION_NOTIFY, 0, get_gpid().thread_hash());
    dsn::marshall(msg, report);

    _potential_secondary_states.completion_notify_task =
        rpc::call(_config.primary, msg, &_tracker, [
            this,
            report = std::move(report)
        ](error_code err, learn_notify_response && resp) mutable {
            on_learn_completion_notification_reply(err, std::move(report), std::move(resp));
        });
}

void replica::on_learn_completion_notification(const group_check_response &report,
                                               /*out*/ learn_notify_response &response)
{
    _checker.only_one_thread_access();

    LOG_INFO_PREFIX(
        "on_learn_completion_notification[{:#018x}]: learner = {}, learning_status = {}",
        report.learner_signature,
        report.node,
        enum_to_string(report.learner_status_));

    if (status() != partition_status::PS_PRIMARY) {
        response.err = (partition_status::PS_INACTIVE == status() && _inactive_is_transient)
                           ? ERR_INACTIVE_STATE
                           : ERR_INVALID_STATE;
        LOG_ERROR_PREFIX("on_learn_completion_notification[{:#018x}]: learner = {}, this replica "
                         "is not primary, but {}, reply {}",
                         report.learner_signature,
                         report.node,
                         enum_to_string(status()),
                         response.err);
    } else if (report.learner_status_ != learner_status::LearningSucceeded) {
        response.err = ERR_INVALID_STATE;
        LOG_ERROR_PREFIX("on_learn_completion_notification[{:#018x}]: learner = {}, learner_status "
                         "is not LearningSucceeded, but {}, reply ERR_INVALID_STATE",
                         report.learner_signature,
                         report.node,
                         enum_to_string(report.learner_status_));
    } else {
        response.err = handle_learning_succeeded_on_primary(report.node, report.learner_signature);
        if (response.err != ERR_OK) {
            LOG_ERROR_PREFIX("on_learn_completion_notification[{:#018x}]: learner = {}, handle "
                             "learning succeeded on primary failed, reply {}",
                             report.learner_signature,
                             report.node,
                             response.err);
        }
    }
}

void replica::on_learn_completion_notification_reply(error_code err,
                                                     group_check_response &&report,
                                                     learn_notify_response &&resp)
{
    _checker.only_one_thread_access();

    CHECK_EQ(partition_status::PS_POTENTIAL_SECONDARY, status());
    CHECK_EQ(_potential_secondary_states.learning_status, learner_status::LearningSucceeded);
    CHECK_EQ(report.learner_signature, _potential_secondary_states.learning_version);

    if (err != ERR_OK) {
        handle_learning_error(err, false);
        return;
    }

    if (resp.signature != (int64_t)_potential_secondary_states.learning_version) {
        LOG_ERROR_PREFIX("on_learn_completion_notification_reply[{:#018x}]: learnee = {}, "
                         "learn_duration = {} ms, signature not matched, current signature on "
                         "primary is [{:#018x}]",
                         report.learner_signature,
                         _config.primary,
                         _potential_secondary_states.duration_ms(),
                         resp.signature);
        handle_learning_error(ERR_INVALID_STATE, false);
        return;
    }

    LOG_INFO_PREFIX("on_learn_completion_notification_reply[{:#018x}]: learnee = {}, "
                    "learn_duration = {} ms, response_err = {}",
                    report.learner_signature,
                    _config.primary,
                    _potential_secondary_states.duration_ms(),
                    resp.err);

    if (resp.err != ERR_OK) {
        if (resp.err == ERR_INACTIVE_STATE) {
            LOG_WARNING_PREFIX("on_learn_completion_notification_reply[{:#018x}]: learnee = {}, "
                               "learn_duration = {} ms, learnee is updating ballot, delay to start "
                               "another round of learning",
                               report.learner_signature,
                               _config.primary,
                               _potential_secondary_states.duration_ms());
            _potential_secondary_states.learning_round_is_running = false;
            _potential_secondary_states.delay_learning_task = tasking::create_task(
                LPC_DELAY_LEARN,
                &_tracker,
                std::bind(&replica::init_learn, this, report.learner_signature),
                get_gpid().thread_hash());
            _potential_secondary_states.delay_learning_task->enqueue(std::chrono::seconds(1));
        } else {
            handle_learning_error(resp.err, false);
        }
    } else {
        _stub->_counter_replicas_learning_recent_learn_succ_count->increment();
    }
}

void replica::on_add_learner(const group_check_request &request)
{
    LOG_INFO_PREFIX("process add learner, primary = {}, ballot ={}, status ={}, "
                    "last_committed_decree = {}, duplicating = {}",
                    request.config.primary.to_string(),
                    request.config.ballot,
                    enum_to_string(request.config.status),
                    request.last_committed_decree,
                    request.app.duplicating);

    if (request.config.ballot < get_ballot()) {
        LOG_WARNING_PREFIX("on_add_learner ballot is old, skipped");
        return;
    }

    if (request.config.ballot > get_ballot() ||
        is_same_ballot_status_change_allowed(status(), request.config.status)) {
        if (!update_local_configuration(request.config, true))
            return;

        CHECK_EQ_PREFIX(partition_status::PS_POTENTIAL_SECONDARY, status());

        _is_duplication_master = request.app.duplicating;
        init_learn(request.config.learner_signature);
    }
}

// in non-replication thread
error_code replica::apply_learned_state_from_private_log(learn_state &state)
{
    bool duplicating = is_duplication_master();
    // if no duplicate, learn_start_decree=last_commit decree, step_back means whether
    // `learn_start_decree`should be stepped back to include all the
    // unconfirmed when duplicating in this round of learn. default is false
    bool step_back = false;

    // in this case, this means `learn_start_decree` must have been stepped back to include all the
    // unconfirmed(learn_start_decree=last_confirmed_decree) when duplicating in this round of
    // learn.
    //              confirmed    gced          committed
    //                  |          |              |
    // learner's plog: ============[-----log------]
    //                   |
    //                   |                            <cache>
    // learn_state:      [----------log-files--------]------]
    //                   |                                  |
    // ==>       learn_start_decree                         |
    // learner's plog    |                              committed
    // after applied:    [---------------log----------------]
    if (duplicating && state.__isset.learn_start_decree &&
        state.learn_start_decree < _app->last_committed_decree() + 1) {
        LOG_INFO_PREFIX("learn_start_decree({}) < _app->last_committed_decree() + 1({}),   learn "
                        "must stepped back to include all the unconfirmed ",
                        state.learn_start_decree,
                        _app->last_committed_decree() + 1);

        // move the `learn/` dir to working dir (`plog/`) to replace current log files to replay
        error_code err = _private_log->reset_from(
            _app->learn_dir(),
            [](int log_length, mutation_ptr &mu) { return true; },
            [this](error_code err) {
                tasking::enqueue(LPC_REPLICATION_ERROR,
                                 &_tracker,
                                 [this, err]() { handle_local_failure(err); },
                                 get_gpid().thread_hash());
            });
        if (err != ERR_OK) {
            LOG_ERROR_PREFIX("failed to reset this private log with logs in learn/ dir: {}", err);
            return err;
        }

        // only select uncommitted logs to be replayed and applied into storage.
        learn_state tmp_state;
        _private_log->get_learn_state(get_gpid(), _app->last_committed_decree() + 1, tmp_state);
        state.files = tmp_state.files;
        step_back = true;
    }

    int64_t offset;
    error_code err;

    // temp prepare list for learning purpose
    prepare_list plist(this,
                       _app->last_committed_decree(),
                       FLAGS_max_mutation_count_in_prepare_list,
                       [this, duplicating, step_back](mutation_ptr &mu) {
                           if (mu->data.header.decree != _app->last_committed_decree() + 1) {
                               return;
                           }

                           // TODO: assign the returned error_code to err and check it
                           auto ec = _app->apply_mutation(mu);
                           if (ec != ERR_OK) {
                               handle_local_failure(ec);
                               return;
                           }

                           // appends logs-in-cache into plog to ensure them can be duplicated.
                           // if current case is step back, it means the logs has been reserved
                           // through `reset_form` above
                           if (duplicating && !step_back) {
                               _private_log->append(
                                   mu, LPC_WRITE_REPLICATION_LOG_COMMON, &_tracker, nullptr);
                           }
                       });

    err = mutation_log::replay(state.files,
                               [&plist](int log_length, mutation_ptr &mu) {
                                   auto d = mu->data.header.decree;
                                   if (d <= plist.last_committed_decree())
                                       return false;

                                   auto old = plist.get_mutation_by_decree(d);
                                   if (old != nullptr &&
                                       old->data.header.ballot >= mu->data.header.ballot)
                                       return false;

                                   plist.prepare(mu, partition_status::PS_SECONDARY);
                                   return true;
                               },
                               offset);

    // update first_learn_start_decree, the position where the first round of LT_LOG starts from.
    // we use this value to determine whether to learn back from min_confirmed_decree
    // for duplication:
    //
    //                confirmed
    //                    |
    // learner's plog: ==[=========[--------------]
    //                   |         |              |
    //                   |       gced           committed
    //     first_learn_start_decree
    //
    // because the learned logs (under `learn/` dir) have covered all the unconfirmed,
    // the next round of learn will start from committed+1.
    //
    if (state.__isset.learn_start_decree &&
        (_potential_secondary_states.first_learn_start_decree < 0 ||
         _potential_secondary_states.first_learn_start_decree > state.learn_start_decree)) {
        _potential_secondary_states.first_learn_start_decree = state.learn_start_decree;
    }

    LOG_INFO_PREFIX(
        "apply_learned_state_from_private_log[{}]: duplicating={}, step_back={}, "
        "learnee = {}, learn_duration = {} ms, apply private log files done, file_count "
        "={}, first_learn_start_decree ={}, learn_start_decree = {}, "
        "app_committed_decree = {}",
        _potential_secondary_states.learning_version,
        duplicating,
        step_back,
        _config.primary.to_string(),
        _potential_secondary_states.duration_ms(),
        state.files.size(),
        _potential_secondary_states.first_learn_start_decree,
        state.learn_start_decree,
        _app->last_committed_decree());

    // apply in-buffer private logs
    if (err == ERR_OK) {
        int replay_count = 0;
        binary_reader reader(state.meta);
        while (!reader.is_eof()) {
            auto mu = mutation::read_from(reader, nullptr);
            auto d = mu->data.header.decree;
            if (d <= plist.last_committed_decree())
                continue;

            auto old = plist.get_mutation_by_decree(d);
            if (old != nullptr && old->data.header.ballot >= mu->data.header.ballot)
                continue;

            mu->set_logged();
            plist.prepare(mu, partition_status::PS_SECONDARY);
            ++replay_count;
        }

        if (state.to_decree_included > last_committed_decree()) {
            LOG_INFO_PREFIX("apply_learned_state_from_private_log[{}]: learnee ={}, "
                            "learned_to_decree_included({}) > last_committed_decree({}), commit to "
                            "to_decree_included",
                            _potential_secondary_states.learning_version,
                            _config.primary.to_string(),
                            state.to_decree_included,
                            last_committed_decree());
            plist.commit(state.to_decree_included, COMMIT_TO_DECREE_SOFT);
        }

        LOG_INFO_PREFIX(" apply_learned_state_from_private_log[{}]: learnee ={}, "
                        "learn_duration ={} ms, apply in-buffer private logs done, "
                        "replay_count ={}, app_committed_decree = {}",
                        _potential_secondary_states.learning_version,
                        _config.primary.to_string(),
                        _potential_secondary_states.duration_ms(),
                        replay_count,
                        _app->last_committed_decree());
    }

    // awaits for unfinished mutation writes.
    if (duplicating) {
        _private_log->flush();
    }
    return err;
}
} // namespace replication
} // namespace dsn
