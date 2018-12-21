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
#include <dsn/utility/filesystem.h>
#include <dsn/utility/chrono_literals.h>
#include <dsn/dist/replication/replication_app_base.h>

namespace dsn {
namespace replication {

// run in replica thread
void replica::on_checkpoint_timer()
{
    _checker.only_one_thread_access();

    if (dsn_now_ms() > _next_checkpoint_interval_trigger_time_ms) {
        // we trigger emergency checkpoint if no checkpoint generated for a long time
        ddebug("%s: trigger emergency checkpoint by checkpoint_max_interval_hours, "
               "config_interval = %dh (%" PRIu64 "ms), random_interval = %" PRIu64 "ms",
               name(),
               _options->checkpoint_max_interval_hours,
               _options->checkpoint_max_interval_hours * 3600000UL,
               _next_checkpoint_interval_trigger_time_ms - _last_checkpoint_generate_time_ms);
        init_checkpoint(true);
    } else {
        ddebug("%s: trigger non-emergency checkpoint",
               name(),
               _options->checkpoint_max_interval_hours);
        init_checkpoint(false);
    }

    if (_private_log) {
        mutation_log_ptr plog = _private_log;
        decree durable_decree = _app->last_durable_decree();
        int64_t valid_start_offset = _app->init_info().init_offset_in_private_log;
        tasking::enqueue(LPC_GARBAGE_COLLECT_LOGS_AND_REPLICAS,
                         &_tracker,
                         [this, plog, durable_decree, valid_start_offset] {
                             // run in background thread to avoid file deletion operation blocking
                             // replication thread.
                             if (status() == partition_status::PS_ERROR ||
                                 status() == partition_status::PS_INACTIVE)
                                 return;
                             plog->garbage_collection(
                                 get_gpid(),
                                 durable_decree,
                                 valid_start_offset,
                                 (int64_t)_options->log_private_reserve_max_size_mb * 1024 * 1024,
                                 (int64_t)_options->log_private_reserve_max_time_seconds);
                             if (status() == partition_status::PS_PRIMARY)
                                 _counter_private_log_size->set(_private_log->total_size() /
                                                                1000000);
                         });
    }
}

// run in replica thread
void replica::init_checkpoint(bool is_emergency)
{
    // only applicable to primary and secondary replicas
    if (status() != partition_status::PS_PRIMARY && status() != partition_status::PS_SECONDARY) {
        ddebug("%s: ignore doing checkpoint for status = %s, is_emergency = %s",
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

// @ secondary
void replica::on_copy_checkpoint(const replica_configuration &request,
                                 /*out*/ learn_response &response)
{
    _checker.only_one_thread_access();

    if (request.ballot > get_ballot()) {
        if (!update_local_configuration(request)) {
            response.err = ERR_INVALID_STATE;
            return;
        }
    }

    if (status() != partition_status::PS_SECONDARY) {
        response.err = ERR_INVALID_STATE;
        return;
    }

    if (_app->last_durable_decree() == 0) {
        response.err = ERR_OBJECT_NOT_FOUND;
        return;
    }

    blob placeholder;
    int err = _app->get_checkpoint(0, placeholder, response.state);
    if (err != 0) {
        response.err = ERR_LEARN_FILE_FAILED;
    } else {
        response.err = ERR_OK;
        response.last_committed_decree = last_committed_decree();
        response.base_local_dir = _app->data_dir();

        // the state.files is returned whether with full_path or only-filename depends
        // on the app impl, we'd better handle with it
        for (std::string &file_name : response.state.files) {
            std::size_t last_splitter = file_name.find_last_of("/\\");
            if (last_splitter != std::string::npos)
                file_name = file_name.substr(last_splitter + 1);
        }
        response.address = _stub->_primary_address;
    }
}

void replica::on_copy_checkpoint_ack(error_code err,
                                     const std::shared_ptr<replica_configuration> &req,
                                     const std::shared_ptr<learn_response> &resp)
{
    _checker.only_one_thread_access();

    if (partition_status::PS_PRIMARY != status()) {
        _primary_states.checkpoint_task = nullptr;
        return;
    }

    if (err != ERR_OK || resp == nullptr) {
        dwarn("%s: copy checkpoint from secondary failed, err = %s", name(), err.to_string());
        _primary_states.checkpoint_task = nullptr;
        return;
    }

    if (resp->err != ERR_OK) {
        dinfo("%s: copy checkpoint from secondary failed, err = %s", name(), resp->err.to_string());
        _primary_states.checkpoint_task = nullptr;
        return;
    }

    if (resp->state.to_decree_included <= _app->last_durable_decree()) {
        dinfo("%s: copy checkpoint from secondary skipped, as its decree is not bigger than "
              "current durable_decree: %" PRIu64 " vs %" PRIu64 "",
              name(),
              resp->state.to_decree_included,
              _app->last_durable_decree());
        _primary_states.checkpoint_task = nullptr;
        return;
    }

    std::string ldir = utils::filesystem::path_combine(_app->learn_dir(), "checkpoint.copy");

    if (utils::filesystem::path_exists(ldir))
        utils::filesystem::remove_path(ldir);

    _primary_states.checkpoint_task = _stub->_nfs->copy_remote_files(
        resp->address,
        resp->base_local_dir,
        resp->state.files,
        ldir,
        false,
        false,
        LPC_REPLICA_COPY_LAST_CHECKPOINT_DONE,
        &_tracker,
        [this, resp, ldir](error_code err, size_t sz) {
            this->on_copy_checkpoint_file_completed(err, sz, resp, ldir);
        },
        get_gpid().thread_hash());
}

void replica::on_copy_checkpoint_file_completed(error_code err,
                                                size_t sz,
                                                std::shared_ptr<learn_response> resp,
                                                const std::string &chk_dir)
{
    _checker.only_one_thread_access();

    if (ERR_OK != err) {
        dwarn("copy checkpoint failed, err(%s), remote_addr(%s)",
              err.to_string(),
              resp->address.to_string());
        _primary_states.checkpoint_task = nullptr;
        return;
    }
    if (partition_status::PS_PRIMARY == status() &&
        resp->state.to_decree_included > _app->last_durable_decree()) {
        // we must give the app the full path of the check point
        for (std::string &filename : resp->state.files) {
            dassert(filename.find_last_of("/\\") == std::string::npos, "invalid file name");
            filename = utils::filesystem::path_combine(chk_dir, filename);
        }
        _app->apply_checkpoint(replication_app_base::chkpt_apply_mode::copy, resp->state);
        _app->reset_counters_after_learning();
    }

    _primary_states.checkpoint_task = nullptr;
}

// run in background thread
error_code replica::background_async_checkpoint(bool is_emergency)
{
    uint64_t start_time = dsn_now_ns();
    decree old_durable = _app->last_durable_decree();
    auto err = _app->async_checkpoint(is_emergency);
    uint64_t used_time = dsn_now_ns() - start_time;
    dassert(err != ERR_NOT_IMPLEMENTED, "err == ERR_NOT_IMPLEMENTED");
    if (err == ERR_OK) {
        if (old_durable != _app->last_durable_decree()) {
            // if no need to generate new checkpoint, async_checkpoint() also returns ERR_OK,
            // so we should check if a new checkpoint has been generated.
            ddebug("%s: call app.async_checkpoint() succeed, time_used_ns = %" PRIu64 ", "
                   "app_last_committed_decree = %" PRId64 ", app_last_durable_decree = (%" PRId64
                   " => %" PRId64 ")",
                   name(),
                   used_time,
                   _app->last_committed_decree(),
                   old_durable,
                   _app->last_durable_decree());
            update_last_checkpoint_generate_time();
        }
    } else if (err == ERR_TRY_AGAIN) {
        // already triggered memory flushing on async_checkpoint(), then try again later.
        ddebug("%s: call app.async_checkpoint() returns ERR_TRY_AGAIN, time_used_ns = %" PRIu64
               ", schedule later checkpoint after 10 seconds",
               name(),
               used_time);
        tasking::enqueue(LPC_PER_REPLICA_CHECKPOINT_TIMER,
                         &_tracker,
                         [this] { init_checkpoint(false); },
                         get_gpid().thread_hash(),
                         std::chrono::seconds(10));
    } else if (err == ERR_WRONG_TIMING) {
        // do nothing
        ddebug("%s: call app.async_checkpoint() returns ERR_WRONG_TIMING, time_used_ns = %" PRIu64
               ", just ignore",
               name(),
               used_time);
    } else {
        derror("%s: call app.async_checkpoint() failed, time_used_ns = %" PRIu64 ", err = %s",
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
    dassert(err != ERR_NOT_IMPLEMENTED, "err == ERR_NOT_IMPLEMENTED");
    if (err == ERR_OK) {
        if (old_durable != _app->last_durable_decree()) {
            // if no need to generate new checkpoint, sync_checkpoint() also returns ERR_OK,
            // so we should check if a new checkpoint has been generated.
            ddebug("%s: call app.sync_checkpoint() succeed, time_used_ns = %" PRIu64 ", "
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
        ddebug("%s: call app.sync_checkpoint() returns ERR_WRONG_TIMING, time_used_ns = %" PRIu64
               ", just ignore",
               name(),
               used_time);
    } else {
        derror("%s: call app.sync_checkpoint() failed, time_used_ns = %" PRIu64 ", err = %s",
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
                dassert(nullptr != mu, "invalid mutation, decree = %" PRId64, d);
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
}
} // namespace
