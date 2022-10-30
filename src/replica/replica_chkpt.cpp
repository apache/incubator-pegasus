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

/*
 * Description:
 *     checkpoint the replicated app
 *
 * Revision history:
 *     Nov., 2015, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include "replica.h"
#include "mutation.h"
#include "mutation_log.h"
#include "replica_stub.h"
#include "duplication/replica_duplicator_manager.h"
#include "split/replica_split_manager.h"
#include "utils/fail_point.h"
#include "utils/filesystem.h"
#include "utils/chrono_literals.h"
#include "replica/replication_app_base.h"
#include "utils/fmt_logging.h"

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
        LOG_INFO("%s: trigger emergency checkpoint by checkpoint_max_interval_hours, "
                 "config_interval = %dh (%" PRIu64 "ms), random_interval = %" PRIu64 "ms",
                 name(),
                 _options->checkpoint_max_interval_hours,
                 _options->checkpoint_max_interval_hours * 3600000UL,
                 _next_checkpoint_interval_trigger_time_ms - _last_checkpoint_generate_time_ms);
        init_checkpoint(true);
    } else {
        LOG_INFO("%s: trigger non-emergency checkpoint",
                 name(),
                 _options->checkpoint_max_interval_hours);
        init_checkpoint(false);
    }

    if (_private_log) {
        mutation_log_ptr plog = _private_log;

        decree last_durable_decree = _app->last_durable_decree();
        decree min_confirmed_decree = _duplication_mgr->min_confirmed_decree();
        decree cleanable_decree = last_durable_decree;
        int64_t valid_start_offset = _app->init_info().init_offset_in_private_log;

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
        } else if (is_duplication_master()) {
            // unsure if the logs can be dropped, because min_confirmed_decree
            // is currently unavailable
            LOG_INFO_PREFIX(
                "gc_private {}: skip gc because confirmed duplication progress is unknown",
                enum_to_string(status()));
            return;
        }

        tasking::enqueue(LPC_GARBAGE_COLLECT_LOGS_AND_REPLICAS,
                         &_tracker,
                         [this, plog, cleanable_decree, valid_start_offset] {
                             // run in background thread to avoid file deletion operation blocking
                             // replication thread.
                             if (status() == partition_status::PS_ERROR ||
                                 status() == partition_status::PS_INACTIVE)
                                 return;
                             plog->garbage_collection(
                                 get_gpid(),
                                 cleanable_decree,
                                 valid_start_offset,
                                 (int64_t)_options->log_private_reserve_max_size_mb * 1024 * 1024,
                                 (int64_t)_options->log_private_reserve_max_time_seconds);
                             if (status() == partition_status::PS_PRIMARY)
                                 _counter_private_log_size->set(_private_log->total_size() /
                                                                1000000);
                         });
    }
}

// ThreadPool: THREAD_POOL_REPLICATION
error_code replica::trigger_manual_emergency_checkpoint(decree old_decree)
{
    _checker.only_one_thread_access();

    if (_app == nullptr) {
        LOG_ERROR_PREFIX("app hasn't been init or has been released");
        return ERR_LOCAL_APP_FAILURE;
    }

    if (old_decree <= _app->last_durable_decree()) {
        LOG_INFO_PREFIX("checkpoint has been completed: old = {} vs latest = {}",
                        old_decree,
                        _app->last_durable_decree());
        _is_manual_emergency_checkpointing = false;
        _stub->_manual_emergency_checkpointing_count == 0
            ? 0
            : (--_stub->_manual_emergency_checkpointing_count);
        return ERR_OK;
    }

    if (_is_manual_emergency_checkpointing) {
        LOG_WARNING_PREFIX("replica is checkpointing, last_durable_decree = {}",
                           _app->last_durable_decree());
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
        LOG_INFO("%s: ignore doing checkpoint for status = %s, is_emergency = %s",
                 name(),
                 enum_to_string(status()),
                 (is_emergency ? "true" : "false"));
        return;
    }

    // here we demand that async_checkpoint() is implemented.
    // we delay some time to run background_async_checkpoint() to pass unit test dsn.rep_tests.
    //
    // we may issue a new task to do backgroup_async_checkpoint
    // even if the old one hasn't finished yet
    tasking::enqueue(LPC_CHECKPOINT_REPLICA,
                     &_tracker,
                     [this, is_emergency] { background_async_checkpoint(is_emergency); },
                     0,
                     10_ms);

    if (is_emergency)
        _stub->_counter_recent_trigger_emergency_checkpoint_count->increment();
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
        response.address = _stub->_primary_address;
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
            LOG_INFO("%s: call app.async_checkpoint() succeed, time_used_ns = %" PRIu64 ", "
                     "app_last_committed_decree = %" PRId64 ", app_last_durable_decree = (%" PRId64
                     " => %" PRId64 ")",
                     name(),
                     used_time,
                     _app->last_committed_decree(),
                     old_durable,
                     _app->last_durable_decree());
            update_last_checkpoint_generate_time();
        }

        if (_is_manual_emergency_checkpointing) {
            _is_manual_emergency_checkpointing = false;
            _stub->_manual_emergency_checkpointing_count == 0
                ? 0
                : (--_stub->_manual_emergency_checkpointing_count);
        }

        return err;
    }

    if (err == ERR_TRY_AGAIN) {
        // already triggered memory flushing on async_checkpoint(), then try again later.
        LOG_INFO("%s: call app.async_checkpoint() returns ERR_TRY_AGAIN, time_used_ns = %" PRIu64
                 ", schedule later checkpoint after 10 seconds",
                 name(),
                 used_time);
        tasking::enqueue(LPC_PER_REPLICA_CHECKPOINT_TIMER,
                         &_tracker,
                         [this] { init_checkpoint(false); },
                         get_gpid().thread_hash(),
                         std::chrono::seconds(10));
        return err;
    }

    if (_is_manual_emergency_checkpointing) {
        _is_manual_emergency_checkpointing = false;
        _stub->_manual_emergency_checkpointing_count == 0
            ? 0
            : (--_stub->_manual_emergency_checkpointing_count);
    }
    if (err == ERR_WRONG_TIMING) {
        // do nothing
        LOG_INFO("%s: call app.async_checkpoint() returns ERR_WRONG_TIMING, time_used_ns = %" PRIu64
                 ", just ignore",
                 name(),
                 used_time);
    } else {
        LOG_ERROR("%s: call app.async_checkpoint() failed, time_used_ns = %" PRIu64 ", err = %s",
                  name(),
                  used_time,
                  err.to_string());
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
            LOG_INFO("%s: call app.sync_checkpoint() succeed, time_used_ns = %" PRIu64 ", "
                     "app_last_committed_decree = %" PRId64 ", app_last_durable_decree = (%" PRId64
                     " => %" PRId64 ")",
                     name(),
                     used_time,
                     _app->last_committed_decree(),
                     old_durable,
                     _app->last_durable_decree());
            update_last_checkpoint_generate_time();
        }
    } else if (err == ERR_WRONG_TIMING) {
        // do nothing
        LOG_INFO("%s: call app.sync_checkpoint() returns ERR_WRONG_TIMING, time_used_ns = %" PRIu64
                 ", just ignore",
                 name(),
                 used_time);
    } else {
        LOG_ERROR("%s: call app.sync_checkpoint() failed, time_used_ns = %" PRIu64 ", err = %s",
                  name(),
                  used_time,
                  err.to_string());
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
        _potential_secondary_states.learn_remote_files_completed_task =
            tasking::create_task(LPC_CHECKPOINT_REPLICA_COMPLETED,
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
        _secondary_states.checkpoint_completed_task =
            tasking::create_task(LPC_CHECKPOINT_REPLICA_COMPLETED,
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
