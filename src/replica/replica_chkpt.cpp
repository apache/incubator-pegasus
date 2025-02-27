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

#include <fmt/core.h>
#include <stdint.h>
#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "common/gpid.h"
#include "common/replication.codes.h"
#include "common/replication_enums.h"
#include "common/replication_other_types.h"
#include "consensus_types.h"
#include "duplication/replica_duplicator_manager.h"
#include "metadata_types.h"
#include "mutation_log.h"
#include "replica.h"
#include "replica/mutation.h"
#include "replica/prepare_list.h"
#include "replica/replica_context.h"
#include "replica/replication_app_base.h"
#include "replica_stub.h"
#include "rpc/rpc_address.h"
#include "rpc/rpc_holder.h"
#include "rpc/rpc_host_port.h"
#include "runtime/api_layer1.h"
#include "split/replica_split_manager.h"
#include "task/async_calls.h"
#include "task/task.h"
#include "utils/autoref_ptr.h"
#include "utils/blob.h"
#include "utils/chrono_literals.h"
#include "utils/error_code.h"
#include "utils/filesystem.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/metrics.h"
#include "utils/thread_access_checker.h"

/// The checkpoint of the replicated app part of replica.

DSN_DEFINE_int32(replication,
                 checkpoint_max_interval_hours,
                 2,
                 "The maximum time interval in hours of replica checkpoints must be generated");

DSN_DEFINE_int32(replication,
                 log_private_reserve_max_size_mb,
                 1000,
                 "The maximum size of useless private log to be reserved. NOTE: only when "
                 "'log_private_reserve_max_size_mb' and 'log_private_reserve_max_time_seconds' are "
                 "both satisfied, the useless logs can be reserved");

DSN_DEFINE_int32(
    replication,
    log_private_reserve_max_time_seconds,
    36000,
    "The maximum time in seconds of useless private log to be reserved. NOTE: only "
    "when 'log_private_reserve_max_size_mb' and 'log_private_reserve_max_time_seconds' "
    "are both satisfied, the useless logs can be reserved");

DSN_DEFINE_uint32(replication,
                  trigger_checkpoint_retry_interval_ms,
                  100,
                  "The wait interval before next attempt for empty write.");

namespace dsn {
namespace replication {

const std::string kCheckpointFolderPrefix /*NOLINT*/ = "checkpoint";

static std::string checkpoint_folder(int64_t decree)
{
    return fmt::format("{}.{}", kCheckpointFolderPrefix, decree);
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica::on_checkpoint_timer()
{
    _checker.only_one_thread_access();

    if (dsn_now_ms() > _next_checkpoint_interval_trigger_time_ms) {
        // we trigger emergency checkpoint if no checkpoint generated for a long time
        LOG_INFO_PREFIX("trigger emergency checkpoint by FLAGS_checkpoint_max_interval_hours, "
                        "config_interval = {}h ({}ms), random_interval = {}ms",
                        FLAGS_checkpoint_max_interval_hours,
                        FLAGS_checkpoint_max_interval_hours * 3600000UL,
                        _next_checkpoint_interval_trigger_time_ms -
                            _last_checkpoint_generate_time_ms);
        init_checkpoint(true);
    } else {
        LOG_INFO_PREFIX("trigger non-emergency checkpoint");
        init_checkpoint(false);
    }

    if (_private_log == nullptr) {
        return;
    }

    if (!is_plog_gc_enabled()) {
        LOG_WARNING_PREFIX("gc_private {}: skip gc because plog gc is disabled",
                           enum_to_string(status()));
        return;
    }

    if (is_duplication_plog_checking()) {
        LOG_INFO_PREFIX("gc_private {}: skip gc because duplication is checking plog files",
                        enum_to_string(status()));
        return;
    }

    mutation_log_ptr plog = _private_log;

    decree last_durable_decree = _app->last_durable_decree();
    decree min_confirmed_decree = _duplication_mgr->min_confirmed_decree();
    decree cleanable_decree = last_durable_decree;
    int64_t valid_start_offset = _app->init_info().init_offset_in_private_log;

    if (min_confirmed_decree < 0 && is_duplication_master()) {
        // Not sure whether the plog files could be dropped, because min_confirmed_decree
        // is currently unavailable.
        LOG_INFO_PREFIX("gc_private {}: skip gc because confirmed duplication progress "
                        "is unknown",
                        enum_to_string(status()));
        return;
    }

    if (min_confirmed_decree >= 0) {
        // Do not rely on valid_start_offset for GC during duplication.
        // cleanable_decree is the only GC trigger.
        valid_start_offset = 0;
        if (min_confirmed_decree < last_durable_decree) {
            LOG_INFO_PREFIX("gc_private {}: delay gc for duplication: min_confirmed_decree({}) "
                            "last_durable_decree({})",
                            enum_to_string(status()),
                            min_confirmed_decree,
                            last_durable_decree);
            cleanable_decree = min_confirmed_decree;
        } else {
            LOG_INFO_PREFIX("gc_private {}: min_confirmed_decree({}) last_durable_decree({})",
                            enum_to_string(status()),
                            min_confirmed_decree,
                            last_durable_decree);
        }
    }

    tasking::enqueue(LPC_GARBAGE_COLLECT_LOGS_AND_REPLICAS,
                     &_tracker,
                     [this, plog, cleanable_decree, valid_start_offset] {
                         // run in background thread to avoid file deletion operation blocking
                         // replication thread.
                         if (status() == partition_status::PS_ERROR ||
                             status() == partition_status::PS_INACTIVE) {

                             return;
                         }

                         plog->garbage_collection(
                             get_gpid(),
                             cleanable_decree,
                             valid_start_offset,
                             (int64_t)FLAGS_log_private_reserve_max_size_mb * 1024 * 1024,
                             (int64_t)FLAGS_log_private_reserve_max_time_seconds);
                         if (status() == partition_status::PS_PRIMARY) {
                             METRIC_VAR_SET(private_log_size_mb, _private_log->total_size() >> 20);
                         }
                     });
}

void replica::async_trigger_manual_emergency_checkpoint(decree min_checkpoint_decree,
                                                        uint32_t delay_ms,
                                                        trigger_checkpoint_callback callback)
{
    CHECK_GT_PREFIX_MSG(min_checkpoint_decree,
                        0,
                        "min_checkpoint_decree should be a number greater than 0 "
                        "which means a new checkpoint must be created");

    tasking::enqueue(
        LPC_REPLICATION_COMMON,
        &_tracker,
        [min_checkpoint_decree, callback, this]() {
            _checker.only_one_thread_access();

            if (_app == nullptr) {
                LOG_ERROR_PREFIX("app hasn't been initialized or has been released");
                return;
            }

            const auto last_applied_decree = this->last_applied_decree();
            if (last_applied_decree == 0) {
                LOG_INFO_PREFIX("ready to commit an empty write to trigger checkpoint: "
                                "min_checkpoint_decree={}, last_applied_decree={}, "
                                "last_durable_decree={}",
                                min_checkpoint_decree,
                                last_applied_decree,
                                last_durable_decree());

                // For the empty replica, here we commit an empty write would be to increase
                // the decree to at least 1, to ensure that the checkpoint would inevitably
                // be created even if the replica is empty.
                mutation_ptr mu = new_mutation(invalid_decree);
                mu->add_client_request(nullptr);
                init_prepare(mu, false);

                async_trigger_manual_emergency_checkpoint(
                    min_checkpoint_decree, FLAGS_trigger_checkpoint_retry_interval_ms, callback);

                return;
            }

            const auto err = trigger_manual_emergency_checkpoint(min_checkpoint_decree);
            if (callback) {
                callback(err);
            }
        },
        get_gpid().thread_hash(),
        std::chrono::milliseconds(delay_ms));
}

// ThreadPool: THREAD_POOL_REPLICATION
error_code replica::trigger_manual_emergency_checkpoint(decree min_checkpoint_decree)
{
    _checker.only_one_thread_access();

    if (_app == nullptr) {
        LOG_ERROR_PREFIX("app hasn't been init or has been released");
        return ERR_LOCAL_APP_FAILURE;
    }

    const auto last_durable_decree = this->last_durable_decree();
    if (min_checkpoint_decree <= last_durable_decree) {
        LOG_INFO_PREFIX(
            "checkpoint has been completed: min_checkpoint_decree={}, last_durable_decree={}",
            min_checkpoint_decree,
            last_durable_decree);
        _is_manual_emergency_checkpointing = false;
        return ERR_OK;
    }

    if (_is_manual_emergency_checkpointing) {
        LOG_WARNING_PREFIX("replica is checkpointing, last_durable_decree={}", last_durable_decree);
        return ERR_BUSY;
    }

    if (++_stub->_manual_emergency_checkpointing_count >
        FLAGS_max_concurrent_manual_emergency_checkpointing_count) {
        LOG_WARNING_PREFIX(
            "please try again later because checkpointing exceed max running count[{}]",
            FLAGS_max_concurrent_manual_emergency_checkpointing_count);
        --_stub->_manual_emergency_checkpointing_count;
        return ERR_TRY_AGAIN;
    }

    init_checkpoint(true);
    _is_manual_emergency_checkpointing = true;
    return ERR_OK;
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica::init_checkpoint(bool is_emergency)
{
    // only applicable to primary and secondary replicas
    if (status() != partition_status::PS_PRIMARY && status() != partition_status::PS_SECONDARY) {
        LOG_INFO_PREFIX("ignore doing checkpoint for status = {}, is_emergency = {}",
                        enum_to_string(status()),
                        is_emergency ? "true" : "false");
        return;
    }

    // here we demand that async_checkpoint() is implemented.
    // we delay some time to run background_async_checkpoint() to pass unit test dsn.rep_tests.
    //
    // we may issue a new task to do backgroup_async_checkpoint
    // even if the old one hasn't finished yet
    tasking::enqueue(
        LPC_CHECKPOINT_REPLICA,
        &_tracker,
        [this, is_emergency] { background_async_checkpoint(is_emergency); },
        0,
        10_ms);

    if (is_emergency) {
        METRIC_VAR_INCREMENT(emergency_checkpoints);
    }
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica::on_query_last_checkpoint(/*out*/ learn_response &response)
{
    _checker.only_one_thread_access();

    if (_app->last_durable_decree() == 0) {
        response.err = ERR_PATH_NOT_FOUND;
        return;
    }

    blob placeholder;
    int err = _app->get_checkpoint(0, placeholder, response.state);
    if (err != 0) {
        response.err = ERR_GET_LEARN_STATE_FAILED;
    } else {
        response.err = ERR_OK;
        response.last_committed_decree = last_committed_decree();
        // for example: base_local_dir = "./data" + "checkpoint.1024" = "./data/checkpoint.1024"
        response.base_local_dir = utils::filesystem::path_combine(
            _app->data_dir(), checkpoint_folder(response.state.to_decree_included));
        SET_IP_AND_HOST_PORT(
            response, learnee, _stub->primary_address(), _stub->primary_host_port());
        for (auto &file : response.state.files) {
            // response.state.files contain file absolute path， for example:
            // "./data/checkpoint.1024/1.sst" use `substr` to get the file name: 1.sst
            file = file.substr(response.base_local_dir.length() + 1);
        }
    }
}

// run in background thread
error_code replica::background_async_checkpoint(bool is_emergency)
{
    uint64_t start_time = dsn_now_ns();
    decree old_durable = _app->last_durable_decree();
    auto err = _app->async_checkpoint(is_emergency);
    uint64_t used_time = dsn_now_ns() - start_time;
    CHECK_NE(err, ERR_NOT_IMPLEMENTED);
    if (err == ERR_OK) {
        if (old_durable != _app->last_durable_decree()) {
            // if no need to generate new checkpoint, async_checkpoint() also returns ERR_OK,
            // so we should check if a new checkpoint has been generated.
            LOG_INFO_PREFIX("call app.async_checkpoint() succeed, time_used_ns = {}, "
                            "app_last_committed_decree = {}, app_last_durable_decree = ({} => {})",
                            used_time,
                            _app->last_committed_decree(),
                            old_durable,
                            _app->last_durable_decree());
            update_last_checkpoint_generate_time();
        }

        if (_is_manual_emergency_checkpointing) {
            _is_manual_emergency_checkpointing = false;
            if (_stub->_manual_emergency_checkpointing_count > 0) {
                --_stub->_manual_emergency_checkpointing_count;
            }
        }

        return err;
    }

    if (err == ERR_TRY_AGAIN) {
        // already triggered memory flushing on async_checkpoint(), then try again later.
        LOG_INFO_PREFIX("call app.async_checkpoint() returns ERR_TRY_AGAIN, time_used_ns = {}"
                        ", schedule later checkpoint after 10 seconds",
                        used_time);
        tasking::enqueue(
            LPC_PER_REPLICA_CHECKPOINT_TIMER,
            &_tracker,
            [this] { init_checkpoint(false); },
            get_gpid().thread_hash(),
            std::chrono::seconds(10));
        return err;
    }

    if (_is_manual_emergency_checkpointing) {
        _is_manual_emergency_checkpointing = false;
        if (_stub->_manual_emergency_checkpointing_count > 0) {
            --_stub->_manual_emergency_checkpointing_count;
        }
    }
    if (err == ERR_WRONG_TIMING) {
        // do nothing
        LOG_INFO_PREFIX(
            "call app.async_checkpoint() returns ERR_WRONG_TIMING, time_used_ns = {}, just ignore",
            used_time);
    } else {
        LOG_ERROR_PREFIX(
            "call app.async_checkpoint() failed, time_used_ns = {}, err = {}", used_time, err);
    }
    return err;
}

// run in init thread
error_code replica::background_sync_checkpoint()
{
    uint64_t start_time = dsn_now_ns();
    decree old_durable = _app->last_durable_decree();
    auto err = _app->sync_checkpoint();
    uint64_t used_time = dsn_now_ns() - start_time;
    CHECK_NE(err, ERR_NOT_IMPLEMENTED);
    if (err == ERR_OK) {
        if (old_durable != _app->last_durable_decree()) {
            // if no need to generate new checkpoint, sync_checkpoint() also returns ERR_OK,
            // so we should check if a new checkpoint has been generated.
            LOG_INFO_PREFIX("call app.sync_checkpoint() succeed, time_used_ns = {}, "
                            "app_last_committed_decree = {}, "
                            "app_last_durable_decree = ({} => {})",
                            used_time,
                            _app->last_committed_decree(),
                            old_durable,
                            _app->last_durable_decree());
            update_last_checkpoint_generate_time();
        }
    } else if (err == ERR_WRONG_TIMING) {
        // do nothing
        LOG_INFO_PREFIX(
            "call app.sync_checkpoint() returns ERR_WRONG_TIMING, time_used_ns = {}, just ignore",
            used_time);
    } else {
        LOG_ERROR_PREFIX(
            "call app.sync_checkpoint() failed, time_used_ns = {}, err = {}", used_time, err);
    }
    return err;
}

// in non-replication thread
void replica::catch_up_with_private_logs(partition_status::type s)
{
    learn_state state;
    _private_log->get_learn_state(get_gpid(), _app->last_committed_decree() + 1, state);

    auto err = apply_learned_state_from_private_log(state);

    if (s == partition_status::PS_POTENTIAL_SECONDARY) {
        _potential_secondary_states.learn_remote_files_completed_task = tasking::create_task(
            LPC_CHECKPOINT_REPLICA_COMPLETED,
            &_tracker,
            [this, err]() { this->on_learn_remote_state_completed(err); },
            get_gpid().thread_hash());
        _potential_secondary_states.learn_remote_files_completed_task->enqueue();
    } else if (s == partition_status::PS_PARTITION_SPLIT) {
        _split_states.async_learn_task = tasking::enqueue(
            LPC_PARTITION_SPLIT,
            tracker(),
            std::bind(&replica_split_manager::child_catch_up_states, get_split_manager()),
            get_gpid().thread_hash());
    } else {
        _secondary_states.checkpoint_completed_task = tasking::create_task(
            LPC_CHECKPOINT_REPLICA_COMPLETED,
            &_tracker,
            [this, err]() { this->on_checkpoint_completed(err); },
            get_gpid().thread_hash());
        _secondary_states.checkpoint_completed_task->enqueue();
    }
}

void replica::on_checkpoint_completed(error_code err)
{
    _checker.only_one_thread_access();

    // closing or wrong timing
    if (partition_status::PS_SECONDARY != status() || err == ERR_WRONG_TIMING) {
        _secondary_states.checkpoint_is_running = false;
        return;
    }

    // handle failure
    if (err != ERR_OK) {
        // done checkpointing
        _secondary_states.checkpoint_is_running = false;
        handle_local_failure(err);
        return;
    }

    auto c = _prepare_list->last_committed_decree();

    // missing commits
    if (c > _app->last_committed_decree()) {
        // missed ones are covered by prepare list
        if (_app->last_committed_decree() > _prepare_list->min_decree()) {
            for (auto d = _app->last_committed_decree() + 1; d <= c; d++) {
                auto mu = _prepare_list->get_mutation_by_decree(d);
                CHECK_NOTNULL(mu, "invalid mutation, decree = {}", d);
                err = _app->apply_mutation(mu);
                if (ERR_OK != err) {
                    _secondary_states.checkpoint_is_running = false;
                    handle_local_failure(err);
                    return;
                }
            }

            // everything is ok now, done checkpointing
            _secondary_states.checkpoint_is_running = false;
            update_last_checkpoint_generate_time();
        }

        // missed ones need to be loaded via private logs
        else {
            _secondary_states.catchup_with_private_log_task = tasking::create_task(
                LPC_CATCHUP_WITH_PRIVATE_LOGS,
                &_tracker,
                [this]() { this->catch_up_with_private_logs(partition_status::PS_SECONDARY); },
                get_gpid().thread_hash());
            _secondary_states.catchup_with_private_log_task->enqueue();
        }
    }

    // no missing commits
    else {
        // everything is ok now, done checkpointing
        _secondary_states.checkpoint_is_running = false;
        update_last_checkpoint_generate_time();
    }
}
} // namespace replication
} // namespace dsn
