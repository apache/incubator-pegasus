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
 *     replication learning process
 *
 * Revision history:
 *     Mar., 2015, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include "replica.h"
#include "mutation.h"
#include "mutation_log.h"
#include "replica_stub.h"
#include "duplication/replica_duplicator_manager.h"

#include <dsn/utility/filesystem.h>
#include <dsn/dist/replication/replication_app_base.h>
#include <dsn/dist/fmt_logging.h>

namespace dsn {
namespace replication {

void replica::init_learn(uint64_t signature)
{
    _checker.only_one_thread_access();

    if (status() != partition_status::PS_POTENTIAL_SECONDARY) {
        dwarn(
            "%s: state is not potential secondary but %s, skip learning with signature[%016" PRIx64
            "]",
            name(),
            enum_to_string(status()),
            signature);
        return;
    }

    if (signature == invalid_signature) {
        dwarn("%s: invalid learning signature, skip", name());
        return;
    }

    // at most one learning task running
    if (_potential_secondary_states.learning_round_is_running) {
        dwarn("%s: previous learning is still running, skip learning with signature [%016" PRIx64
              "]",
              name(),
              signature);
        return;
    }

    if (signature < _potential_secondary_states.learning_version) {
        dwarn("%s: learning request is out-dated, therefore skipped: [%016" PRIx64
              "] vs [%016" PRIx64 "]",
              name(),
              signature,
              _potential_secondary_states.learning_version);
        return;
    }

    // learn timeout or primary change, the (new) primary starts another round of learning process
    // be cautious: primary should not issue signatures frequently to avoid learning abort
    if (signature != _potential_secondary_states.learning_version) {
        if (!_potential_secondary_states.cleanup(false)) {
            dwarn("%s: previous learning with signature[%016" PRIx64
                  "] is still in-process, skip init new learning with signature [%016" PRIx64 "]",
                  name(),
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
            dassert(_app->last_durable_decree() + 1 >=
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
                            dassert(nullptr != mu,
                                    "mutation must not be nullptr, decree = %" PRId64 "",
                                    d);
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
            dassert(false,
                    "invalid learner_status, status = %s",
                    enum_to_string(_potential_secondary_states.learning_status));
        }
    }

    if (_app->last_committed_decree() == 0 &&
        _stub->_learn_app_concurrent_count.load() >= _options->learn_app_max_concurrent_count) {
        dwarn("%s: init_learn[%016" PRIx64 "]: learnee = %s, learn_duration = %" PRIu64
              "ms, need to learn app because app_committed_decree = 0, but "
              "learn_app_concurrent_count(%d) >= learn_app_max_concurrent_count(%d), skip",
              name(),
              _potential_secondary_states.learning_version,
              _config.primary.to_string(),
              _potential_secondary_states.duration_ms(),
              _stub->_learn_app_concurrent_count.load(),
              _options->learn_app_max_concurrent_count);
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

    ddebug("%s: init_learn[%016" PRIx64 "]: learnee = %s, learn_duration = %" PRIu64
           " ms, max_gced_decree = %" PRId64 ", local_committed_decree = %" PRId64 ", "
           "app_committed_decree = %" PRId64 ", app_durable_decree = %" PRId64
           ", current_learning_status = %s, total_copy_file_count = %" PRIu64
           ", total_copy_file_size = %" PRIu64 ", total_copy_buffer_size = %" PRIu64,
           name(),
           request.signature,
           _config.primary.to_string(),
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
    dcheck_le_replica(request.last_committed_decree_in_app, local_committed_decree);

    decree learn_start_decree_no_dup = request.last_committed_decree_in_app + 1;
    if (!is_duplicating()) {
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
            dwarn_replica("no plog to be learned for duplication, continue as normal");
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
            ddebug_replica("learn_start_decree steps back to {} to ensure learner having enough "
                           "logs for duplication [confirmed_decree={}, learner_gced_decree={}]",
                           learn_start_decree,
                           min_confirmed_decree,
                           request.max_gced_decree);
        }
    }
    dcheck_le_replica(learn_start_decree, local_committed_decree + 1);
    dcheck_gt_replica(learn_start_decree, 0); // learn_start_decree can never be invalid_decree
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
        derror("%s: on_learn[%016" PRIx64 "]: learner = %s, learner state is newer than learnee, "
               "learner_app_committed_decree = %" PRId64 ", local_committed_decree = %" PRId64
               ", learn from scratch",
               name(),
               request.signature,
               request.learner.to_string(),
               request.last_committed_decree_in_app,
               local_committed_decree);

        *(decree *)&request.last_committed_decree_in_app = 0;
    }

    // mutations are previously committed already on learner (old primary)
    // this happens when the new primary does not commit the previously prepared mutations
    // yet, which it should do, so let's help it now.
    else if (request.last_committed_decree_in_app > local_committed_decree) {
        derror("%s: on_learn[%016" PRIx64
               "]: learner = %s, learner's last_committed_decree_in_app is newer than learnee, "
               "learner_app_committed_decree = %" PRId64 ", local_committed_decree = %" PRId64
               ", commit local soft",
               name(),
               request.signature,
               request.learner.to_string(),
               request.last_committed_decree_in_app,
               local_committed_decree);

        // we shouldn't commit mutations hard coz these mutations may preparing on another learner
        _prepare_list->commit(request.last_committed_decree_in_app, COMMIT_TO_DECREE_SOFT);
        local_committed_decree = last_committed_decree();

        if (request.last_committed_decree_in_app > local_committed_decree) {
            derror("%s: on_learn[%016" PRIx64 "]: try to commit primary to %" PRId64
                   ", still less than learner(%s)'s committed decree(%" PRId64
                   "), wait mutations to be commitable",
                   name(),
                   request.signature,
                   local_committed_decree,
                   request.learner.to_string(),
                   request.last_committed_decree_in_app);
            response.err = ERR_INCONSISTENT_STATE;
            reply(msg, response);
            return;
        }
    }

    dassert(request.last_committed_decree_in_app <= local_committed_decree,
            "%" PRId64 " VS %" PRId64 "",
            request.last_committed_decree_in_app,
            local_committed_decree);

    const decree learn_start_decree = get_learn_start_decree(request);
    response.state.__set_learn_start_decree(learn_start_decree);
    bool delayed_replay_prepare_list = false;

    ddebug("%s: on_learn[%016" PRIx64 "]: learner = %s, remote_committed_decree = %" PRId64 ", "
           "remote_app_committed_decree = %" PRId64 ", local_committed_decree = %" PRId64 ", "
           "app_committed_decree = %" PRId64 ", app_durable_decree = %" PRId64 ", "
           "prepare_min_decree = %" PRId64
           ", prepare_list_count = %d, learn_start_decree = %" PRId64,
           name(),
           request.signature,
           request.learner.to_string(),
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

            ddebug("%s: on_learn[%016" PRIx64
                   "]: learner = %s, set prepare_start_decree = %" PRId64,
                   name(),
                   request.signature,
                   request.learner.to_string(),
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
            dassert(mu != nullptr, "mutation must not be nullptr, decree = %" PRId64 "", d);
            mu->write_to(writer, nullptr);
            count++;
        }
        response.type = learn_type::LT_CACHE;
        response.state.meta = writer.get_buffer();
        ddebug("%s: on_learn[%016" PRIx64 "]: learner = %s, learn mutation cache succeed, "
               "learn_start_decree = %" PRId64 ", prepare_start_decree = %" PRId64 ", "
               "learn_mutation_count = %d, learn_data_size = %d",
               name(),
               request.signature,
               request.learner.to_string(),
               learn_start_decree,
               response.prepare_start_decree,
               count,
               response.state.meta.length());
    }

    // learn delta state or checkpoint
    // in this case, the state on the PS is still incomplete
    else {
        if (learn_start_decree > _app->last_durable_decree()) {
            ddebug("%s: on_learn[%016" PRIx64 "]: learner = %s, choose to learn private logs, "
                   "because learn_start_decree(%" PRId64 ") > _app->last_durable_decree(%" PRId64
                   ")",
                   name(),
                   request.signature,
                   request.learner.to_string(),
                   learn_start_decree,
                   _app->last_durable_decree());
            _private_log->get_learn_state(get_gpid(), learn_start_decree, response.state);
            response.type = learn_type::LT_LOG;
        } else if (_private_log->get_learn_state(get_gpid(), learn_start_decree, response.state)) {
            ddebug("%s: on_learn[%016" PRIx64 "]: learner = %s, choose to learn private logs, "
                   "because mutation_log::get_learn_state() returns true",
                   name(),
                   request.signature,
                   request.learner.to_string());
            response.type = learn_type::LT_LOG;
        } else if (learn_start_decree < request.last_committed_decree_in_app + 1) {
            ddebug("%s: on_learn[%016" PRIx64 "]: learner = %s, choose to learn private logs, "
                   "because learn_start_decree steps back for duplication",
                   name(),
                   request.signature,
                   request.learner.to_string());
            response.type = learn_type::LT_LOG;
        } else {
            ddebug("%s: on_learn[%016" PRIx64 "]: learner = %s, choose to learn app, "
                   "beacuse learn_start_decree(%" PRId64 ") <= _app->last_durable_decree(%" PRId64
                   "), "
                   "and mutation_log::get_learn_state() returns false",
                   name(),
                   request.signature,
                   request.learner.to_string(),
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
                    ddebug(
                        "%s: on_learn[%016" PRIx64
                        "]: learner = %s, learn the same file %s repeatedly, hint to switch file",
                        name(),
                        request.signature,
                        request.learner.to_string(),
                        last_file.c_str());
                    _private_log->hint_switch_file();
                } else {
                    learner_state.last_learn_log_file = last_file;
                }
            }
            // it is safe to commit to last_committed_decree() now
            response.state.to_decree_included = last_committed_decree();
            ddebug("%s: on_learn[%016" PRIx64 "]: learner = %s, learn private logs succeed, "
                   "learned_meta_size = %u, learned_file_count = %u, "
                   "to_decree_included = %" PRId64,
                   name(),
                   request.signature,
                   request.learner.to_string(),
                   response.state.meta.length(),
                   static_cast<uint32_t>(response.state.files.size()),
                   response.state.to_decree_included);
        } else {
            ::dsn::error_code err = _app->get_checkpoint(
                learn_start_decree, request.app_specific_learn_request, response.state);

            if (err != ERR_OK) {
                response.err = ERR_GET_LEARN_STATE_FAILED;
                derror("%s: on_learn[%016" PRIx64
                       "]: learner = %s, get app checkpoint failed, error = %s",
                       name(),
                       request.signature,
                       request.learner.to_string(),
                       err.to_string());
            } else {
                response.base_local_dir = _app->data_dir();
                ddebug(
                    "%s: on_learn[%016" PRIx64 "]: learner = %s, get app learn state succeed, "
                    "learned_meta_size = %u, learned_file_count = %u, learned_to_decree = %" PRId64,
                    name(),
                    request.signature,
                    request.learner.to_string(),
                    response.state.meta.length(),
                    static_cast<uint32_t>(response.state.files.size()),
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

    dassert(partition_status::PS_POTENTIAL_SECONDARY == status(),
            "invalid partition status, status = %s",
            enum_to_string(status()));
    dassert(req.signature == (int64_t)_potential_secondary_states.learning_version,
            "invalid learn signature, %" PRId64 " VS %" PRId64 "",
            req.signature,
            (int64_t)_potential_secondary_states.learning_version);

    if (err != ERR_OK) {
        handle_learning_error(err, false);
        return;
    }

    ddebug("%s: on_learn_reply[%016" PRIx64 "]: learnee = %s, learn_duration = %" PRIu64
           " ms, response_err = %s, remote_committed_decree = %" PRId64 ", "
           "prepare_start_decree = %" PRId64 ", learn_type = %s, learned_buffer_size = %u, "
           "learned_file_count = %u, to_decree_included = %" PRId64
           ", learn_start_decree = %" PRId64 ", current_learning_status = %s",
           name(),
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
           enum_to_string(_potential_secondary_states.learning_status));

    _potential_secondary_states.learning_copy_buffer_size += resp.state.meta.length();
    _stub->_counter_replicas_learning_recent_copy_buffer_size->add(resp.state.meta.length());

    if (resp.err != ERR_OK) {
        if (resp.err == ERR_INACTIVE_STATE || resp.err == ERR_INCONSISTENT_STATE) {
            dwarn("%s: on_learn_reply[%016" PRIx64
                  "]: learnee = %s, learnee is updating ballot(inactive state) or "
                  "reconciliation(inconsistent state), delay to start another round of learning",
                  name(),
                  req.signature,
                  resp.config.primary.to_string());
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
        ddebug("%s: on_learn_reply[%016" PRIx64
               "]: learnee = %s, update configuration because ballot have changed",
               name(),
               req.signature,
               resp.config.primary.to_string());
        bool ret = update_local_configuration(resp.config);
        dassert(ret, "");
    }

    if (status() != partition_status::PS_POTENTIAL_SECONDARY) {
        derror("%s: on_learn_reply[%016" PRIx64
               "]: learnee = %s, current_status = %s, stop learning",
               name(),
               req.signature,
               resp.config.primary.to_string(),
               enum_to_string(status()));
        return;
    }

    // local state is newer than learnee
    if (resp.last_committed_decree < _app->last_committed_decree()) {
        dwarn("%s: on_learn_reply[%016" PRIx64
              "]: learnee = %s, learner state is newer than learnee (primary): %" PRId64
              " vs %" PRId64 ", create new app",
              name(),
              req.signature,
              resp.config.primary.to_string(),
              _app->last_committed_decree(),
              resp.last_committed_decree);

        _stub->_counter_replicas_learning_recent_learn_reset_count->increment();

        // close app
        auto err = _app->close(true);
        if (err != ERR_OK) {
            derror("%s: on_learn_reply[%016" PRIx64
                   "]: learnee = %s, close app (with clear_state=true) failed, err = %s",
                   name(),
                   req.signature,
                   resp.config.primary.to_string(),
                   err.to_string());
        }

        // backup old data dir
        if (err == ERR_OK) {
            std::string old_dir = _app->data_dir();
            if (dsn::utils::filesystem::directory_exists(old_dir)) {
                char rename_dir[1024];
                sprintf(rename_dir, "%s.%" PRIu64 ".discarded", old_dir.c_str(), dsn_now_us());
                if (dsn::utils::filesystem::rename_path(old_dir, rename_dir)) {
                    dwarn("%s: {replica_dir_op} succeed to move directory from '%s' to '%s'",
                          name(),
                          old_dir.c_str(),
                          rename_dir);
                } else {
                    dassert(false,
                            "%s: failed to move directory from '%s' to '%s'",
                            name(),
                            old_dir.c_str(),
                            rename_dir);
                }
            }
        }

        if (err == ERR_OK) {
            err = _app->open_new_internal(this,
                                          _stub->_log->on_partition_reset(get_gpid(), 0),
                                          _private_log->on_partition_reset(get_gpid(), 0));

            if (err != ERR_OK) {
                derror("%s: on_learn_reply[%016" PRIx64
                       "]: learnee = %s, open app (with create_new=true) failed, err = %s",
                       name(),
                       req.signature,
                       resp.config.primary.to_string(),
                       err.to_string());
            }
        }

        if (err == ERR_OK) {
            dassert(_app->last_committed_decree() == 0, "must be zero after app::open(true)");
            dassert(_app->last_durable_decree() == 0, "must be zero after app::open(true)");

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
        if (++_stub->_learn_app_concurrent_count > _options->learn_app_max_concurrent_count) {
            --_stub->_learn_app_concurrent_count;
            dwarn("%s: on_learn_reply[%016" PRIx64
                  "]: learnee = %s, learn_app_concurrent_count(%d) >= "
                  "learn_app_max_concurrent_count(%d), skip this round",
                  name(),
                  _potential_secondary_states.learning_version,
                  _config.primary.to_string(),
                  _stub->_learn_app_concurrent_count.load(),
                  _options->learn_app_max_concurrent_count);
            _potential_secondary_states.learning_round_is_running = false;
            return;
        } else {
            _potential_secondary_states.learn_app_concurrent_count_increased = true;
            ddebug("%s: on_learn_reply[%016" PRIx64
                   "]: learnee = %s, ++learn_app_concurrent_count = %d",
                   name(),
                   _potential_secondary_states.learning_version,
                   _config.primary.to_string(),
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
        dassert(resp.type == learn_type::LT_CACHE,
                "invalid learn_type, type = %s",
                enum_to_string(resp.type));
        dassert(resp.state.files.size() == 0, "");
        dassert(_potential_secondary_states.learning_status ==
                    learner_status::LearningWithoutPrepare,
                "invalid learning_status, status = %s",
                enum_to_string(_potential_secondary_states.learning_status));
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
        ddebug("%s: on_learn_reply[%016" PRIx64
               "]: learnee = %s, truncate prepare list, local_committed_decree = %" PRId64
               ", current_learning_status = %s",
               name(),
               req.signature,
               resp.config.primary.to_string(),
               _app->last_committed_decree(),
               enum_to_string(_potential_secondary_states.learning_status));

        // persist incoming mutations into private log and apply them to prepare-list
        std::pair<decree, decree> cache_range;
        binary_reader reader(resp.state.meta);
        while (!reader.is_eof()) {
            auto mu = mutation::read_from(reader, nullptr);
            if (mu->data.header.decree > last_committed_decree()) {
                dinfo("%s: on_learn_reply[%016" PRIx64 "]: apply learned mutation %s",
                      name(),
                      req.signature,
                      mu->name());

                // write to shared log with no callback, the later 2pc ensures that logs
                // are written to the disk
                _stub->_log->append(mu, LPC_WRITE_REPLICATION_LOG_COMMON, &_tracker, nullptr);

                // because shared log are written without callback, need to manully
                // set flag and write mutations to private log
                mu->set_logged();
                _private_log->append(mu, LPC_WRITE_REPLICATION_LOG_COMMON, &_tracker, nullptr);

                // then we prepare, it is possible that a committed mutation exists in learner's
                // prepare log,
                // but with DIFFERENT ballot. Reference https://github.com/imzhenyu/rDSN/issues/496
                mutation_ptr existing_mutation =
                    _prepare_list->get_mutation_by_decree(mu->data.header.decree);
                if (existing_mutation != nullptr &&
                    existing_mutation->data.header.ballot > mu->data.header.ballot) {
                    ddebug("%s: on_learn_reply[%016" PRIx64 "]: learnee = %s, "
                           "mutation(%s) exist on the learner with larger ballot %" PRId64 "",
                           name(),
                           req.signature,
                           resp.config.primary.to_string(),
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

        ddebug("%s: on_learn_reply[%016" PRIx64 "]: learnee = %s, learn_duration = %" PRIu64 " ms, "
               "apply cache done, prepare_cache_range = <%" PRId64 ", %" PRId64 ">, "
               "local_committed_decree = %" PRId64 ", app_committed_decree = %" PRId64
               ", current_learning_status = %s",
               name(),
               req.signature,
               resp.config.primary.to_string(),
               _potential_secondary_states.duration_ms(),
               cache_range.first,
               cache_range.second,
               last_committed_decree(),
               _app->last_committed_decree(),
               enum_to_string(_potential_secondary_states.learning_status));

        // further states are synced using 2pc, and we must commit now as those later 2pc messages
        // thinks they should
        _prepare_list->commit(resp.prepare_start_decree - 1, COMMIT_TO_DECREE_HARD);
        dassert(_prepare_list->last_committed_decree() == _app->last_committed_decree(),
                "last_committed_decree of prepare_list and app isn't equal, %" PRId64 " VS %" PRId64
                "",
                _prepare_list->last_committed_decree(),
                _app->last_committed_decree());
        dassert(resp.state.files.size() == 0, "");

        // all state is complete
        dassert(_app->last_committed_decree() + 1 >=
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
            derror("%s: on_learn_reply[%016" PRIx64
                   "]: learnee = %s, create replica learn dir %s failed",
                   name(),
                   req.signature,
                   resp.config.primary.to_string(),
                   learn_dir.c_str());

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
        ddebug("%s: on_learn_reply[%016" PRIx64 "]: learnee = %s, learn_duration = %" PRIu64
               " ms, start to copy remote files, copy_file_count = %d, priority = %s",
               name(),
               req.signature,
               resp.config.primary.to_string(),
               _potential_secondary_states.duration_ms(),
               static_cast<int>(resp.state.files.size()),
               high_priority ? "high" : "low");

        _potential_secondary_states.learn_remote_files_task = _stub->_nfs->copy_remote_files(
            resp.config.primary,
            resp.base_local_dir,
            resp.state.files,
            learn_dir,
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

    ddebug("%s: on_copy_remote_state_completed[%016" PRIx64
           "]: learnee = %s, learn_duration = %" PRIu64 " ms, "
           "copy remote state done, err = %s, copy_file_count = %d, "
           "copy_file_size = %" PRIu64 ", copy_time_used = %" PRIu64 " ms, "
           "local_committed_decree = %" PRId64 ", app_committed_decree = %" PRId64
           ", app_durable_decree = %" PRId64 ", "
           "prepare_start_decree = %" PRId64 ", current_learning_status = %s",
           name(),
           req.signature,
           resp.config.primary.to_string(),
           _potential_secondary_states.duration_ms(),
           err.to_string(),
           static_cast<int>(resp.state.files.size()),
           static_cast<uint64_t>(size),
           _potential_secondary_states.duration_ms() - copy_start_time,
           last_committed_decree(),
           _app->last_committed_decree(),
           _app->last_durable_decree(),
           resp.prepare_start_decree,
           enum_to_string(_potential_secondary_states.learning_status));

    if (resp.type == learn_type::LT_APP) {
        --_stub->_learn_app_concurrent_count;
        _potential_secondary_states.learn_app_concurrent_count_increased = false;
        ddebug("%s: on_copy_remote_state_completed[%016" PRIx64
               "]: learnee = %s, --learn_app_concurrent_count = %d",
               name(),
               _potential_secondary_states.learning_version,
               _config.primary.to_string(),
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
        dassert(resp.type == learn_type::LT_CACHE,
                "invalid learn_type, type = %s",
                enum_to_string(resp.type));
    } else {
        dassert(resp.type == learn_type::LT_APP || resp.type == learn_type::LT_LOG,
                "invalid learn_type, type = %s",
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

                dassert(_app->last_committed_decree() >= _app->last_durable_decree(),
                        "invalid app state, %" PRId64 " VS %" PRId64 "",
                        _app->last_committed_decree(),
                        _app->last_durable_decree());
                // because if the original _app->last_committed_decree > resp.last_committed_decree,
                // the learn_start_decree will be set to 0, which makes learner to learn from
                // scratch
                dassert(_app->last_committed_decree() <= resp.last_committed_decree,
                        "invalid app state, %" PRId64 " VS %" PRId64 "",
                        _app->last_committed_decree(),
                        resp.last_committed_decree);
                ddebug("%s: on_copy_remote_state_completed[%016" PRIx64
                       "]: learnee = %s, learn_duration = %" PRIu64 " ms, "
                       "checkpoint duration = %" PRIu64
                       " ns, apply checkpoint succeed, app_committed_decree = %" PRId64,
                       name(),
                       req.signature,
                       resp.config.primary.to_string(),
                       _potential_secondary_states.duration_ms(),
                       dsn_now_ns() - start_ts,
                       _app->last_committed_decree());
            } else {
                derror("%s: on_copy_remote_state_completed[%016" PRIx64
                       "]: learnee = %s, learn_duration = %" PRIu64 " ms, "
                       "checkpoint duration = %" PRIu64 " ns, apply checkpoint failed, err = %s",
                       name(),
                       req.signature,
                       resp.config.primary.to_string(),
                       _potential_secondary_states.duration_ms(),
                       dsn_now_ns() - start_ts,
                       err.to_string());
            }
        }

        // apply log learning
        else {
            auto start_ts = dsn_now_ns();
            err = apply_learned_state_from_private_log(lstate);
            if (err == ERR_OK) {
                ddebug("%s: on_copy_remote_state_completed[%016" PRIx64
                       "]: learnee = %s, learn_duration = %" PRIu64 " ms, "
                       "apply_log_duration = %" PRIu64 " ns, apply learned state from private log "
                       "succeed, app_committed_decree = %" PRId64,
                       name(),
                       req.signature,
                       resp.config.primary.to_string(),
                       _potential_secondary_states.duration_ms(),
                       dsn_now_ns() - start_ts,
                       _app->last_committed_decree());
            } else {
                derror("%s: on_copy_remote_state_completed[%016" PRIx64
                       "]: learnee = %s, learn_duration = %" PRIu64 " ms, "
                       "apply_log_duration = %" PRIu64
                       " ns, apply learned state from private log failed, err = %s",
                       name(),
                       req.signature,
                       resp.config.primary.to_string(),
                       _potential_secondary_states.duration_ms(),
                       dsn_now_ns() - start_ts,
                       err.to_string());
            }
        }

        // reset prepare list to make it catch with app
        _prepare_list->reset(_app->last_committed_decree());

        ddebug("%s: on_copy_remote_state_completed[%016" PRIx64
               "]: learnee = %s, learn_duration = %" PRIu64
               " ms, apply checkpoint/log done, err = %s, "
               "last_prepared_decree = (%" PRId64 " => %" PRId64 "), "
               "last_committed_decree = (%" PRId64 " => %" PRId64 "), "
               "app_committed_decree = (%" PRId64 " => %" PRId64 "), "
               "app_durable_decree = (%" PRId64 " => %" PRId64 "), "
               "remote_committed_decree = %" PRId64 ", "
               "prepare_start_decree = %" PRId64 ", "
               "current_learning_status = %s",
               name(),
               req.signature,
               resp.config.primary.to_string(),
               _potential_secondary_states.duration_ms(),
               err.to_string(),
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

        ddebug("%s: on_copy_remote_state_completed[%016" PRIx64
               "]: learnee = %s, learn_duration = %" PRIu64 " ms, flush done, err = %s, "
               "app_committed_decree = %" PRId64 ", app_durable_decree = %" PRId64 "",
               name(),
               req.signature,
               resp.config.primary.to_string(),
               _potential_secondary_states.duration_ms(),
               err.to_string(),
               _app->last_committed_decree(),
               _app->last_durable_decree());

        if (err == ERR_OK) {
            dassert(_app->last_committed_decree() == _app->last_durable_decree(),
                    "%" PRId64 " VS %" PRId64 "",
                    _app->last_committed_decree(),
                    _app->last_durable_decree());
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
        dwarn("%s: on_learn_remote_state_completed[%016" PRIx64
              "]: learnee = %s, learn_duration = %" PRIu64 " ms, err = %s, "
              "the learner status is not PS_POTENTIAL_SECONDARY, but %s, ignore",
              name(),
              _potential_secondary_states.learning_version,
              _config.primary.to_string(),
              _potential_secondary_states.duration_ms(),
              err.to_string(),
              enum_to_string(status()));
        return;
    }

    ddebug("%s: on_learn_remote_state_completed[%016" PRIx64
           "]: learnee = %s, learn_duration = %" PRIu64 " ms, err = %s, "
           "local_committed_decree = %" PRId64 ", app_committed_decree = %" PRId64
           ", app_durable_decree = %" PRId64 ", current_learning_status = %s",
           name(),
           _potential_secondary_states.learning_version,
           _config.primary.to_string(),
           _potential_secondary_states.duration_ms(),
           err.to_string(),
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

    derror("%s: handle_learning_error[%016" PRIx64 "]: learnee = %s, learn_duration = %" PRIu64
           " ms, err = %s, %s",
           name(),
           _potential_secondary_states.learning_version,
           _config.primary.to_string(),
           _potential_secondary_states.duration_ms(),
           err.to_string(),
           is_local_error ? "local_error" : "remote error");

    _stub->_counter_replicas_learning_recent_learn_fail_count->increment();

    update_local_configuration_with_no_ballot_change(
        is_local_error ? partition_status::PS_ERROR : partition_status::PS_INACTIVE);
}

error_code replica::handle_learning_succeeded_on_primary(::dsn::rpc_address node,
                                                         uint64_t learn_signature)
{
    auto it = _primary_states.learners.find(node);
    if (it == _primary_states.learners.end()) {
        derror("%s: handle_learning_succeeded_on_primary[%016" PRIx64 "]: learner = %s, "
               "learner not found on primary, return ERR_LEARNER_NOT_FOUND",
               name(),
               learn_signature,
               node.to_string());
        return ERR_LEARNER_NOT_FOUND;
    }

    if (it->second.signature != (int64_t)learn_signature) {
        derror("%s: handle_learning_succeeded_on_primary[%016" PRIx64 "]: learner = %s, "
               "signature not matched, current signature on primary is [%016" PRIx64
               "], return ERR_INVALID_STATE",
               name(),
               learn_signature,
               node.to_string(),
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

    ddebug("%s: notify_learn_completion[%016" PRIx64 "]: learnee = %s, "
           "learn_duration = %" PRIu64 " ms, local_committed_decree = %" PRId64 ", "
           "app_committed_decree = %" PRId64 ", app_durable_decree = %" PRId64
           ", current_learning_status = %s",
           name(),
           _potential_secondary_states.learning_version,
           _config.primary.to_string(),
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

    ddebug("%s: on_learn_completion_notification[%016" PRIx64
           "]: learner = %s, learning_status = %s",
           name(),
           report.learner_signature,
           report.node.to_string(),
           enum_to_string(report.learner_status_));

    if (status() != partition_status::PS_PRIMARY) {
        response.err = (partition_status::PS_INACTIVE == status() && _inactive_is_transient)
                           ? ERR_INACTIVE_STATE
                           : ERR_INVALID_STATE;
        derror("%s: on_learn_completion_notification[%016" PRIx64
               "]: learner = %s, this replica is not primary, but %s, reply %s",
               name(),
               report.learner_signature,
               report.node.to_string(),
               enum_to_string(status()),
               response.err.to_string());
    } else if (report.learner_status_ != learner_status::LearningSucceeded) {
        response.err = ERR_INVALID_STATE;
        derror("%s: on_learn_completion_notification[%016" PRIx64 "]: learner = %s, "
               "learner_status is not LearningSucceeded, but %s, reply ERR_INVALID_STATE",
               name(),
               report.learner_signature,
               report.node.to_string(),
               enum_to_string(report.learner_status_));
    } else {
        response.err = handle_learning_succeeded_on_primary(report.node, report.learner_signature);
        if (response.err != ERR_OK) {
            derror("%s: on_learn_completion_notification[%016" PRIx64 "]: learner = %s, "
                   "handle learning succeeded on primary failed, reply %s",
                   name(),
                   report.learner_signature,
                   report.node.to_string(),
                   response.err.to_string());
        }
    }
}

void replica::on_learn_completion_notification_reply(error_code err,
                                                     group_check_response &&report,
                                                     learn_notify_response &&resp)
{
    _checker.only_one_thread_access();

    dassert(partition_status::PS_POTENTIAL_SECONDARY == status(),
            "invalid partition_status, status = %s",
            enum_to_string(status()));
    dassert(_potential_secondary_states.learning_status == learner_status::LearningSucceeded,
            "invalid learner_status, status = %s",
            enum_to_string(_potential_secondary_states.learning_status));
    dassert(report.learner_signature == (int64_t)_potential_secondary_states.learning_version,
            "%" PRId64 " VS %" PRId64 "",
            report.learner_signature,
            (int64_t)_potential_secondary_states.learning_version);

    if (err != ERR_OK) {
        handle_learning_error(err, false);
        return;
    }

    if (resp.signature != (int64_t)_potential_secondary_states.learning_version) {
        derror("%s: on_learn_completion_notification_reply[%016" PRIx64
               "]: learnee = %s, learn_duration = %" PRIu64 " ms, "
               "signature not matched, current signature on primary is [%016" PRIx64 "]",
               name(),
               report.learner_signature,
               _config.primary.to_string(),
               _potential_secondary_states.duration_ms(),
               resp.signature);
        handle_learning_error(ERR_INVALID_STATE, false);
        return;
    }

    ddebug("%s: on_learn_completion_notification_reply[%016" PRIx64
           "]: learnee = %s, learn_duration = %" PRIu64 " ms, response_err = %s",
           name(),
           report.learner_signature,
           _config.primary.to_string(),
           _potential_secondary_states.duration_ms(),
           resp.err.to_string());

    if (resp.err != ERR_OK) {
        if (resp.err == ERR_INACTIVE_STATE) {
            dwarn("%s: on_learn_completion_notification_reply[%016" PRIx64
                  "]: learnee = %s, learn_duration = %" PRIu64 " ms, "
                  "learnee is updating ballot, delay to start another round of learning",
                  name(),
                  report.learner_signature,
                  _config.primary.to_string(),
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
    ddebug("%s: process add learner, primary = %s, ballot = %" PRId64
           ", status = %s, last_committed_decree = %" PRId64,
           name(),
           request.config.primary.to_string(),
           request.config.ballot,
           enum_to_string(request.config.status),
           request.last_committed_decree);

    if (request.config.ballot < get_ballot()) {
        dwarn("%s: on_add_learner ballot is old, skipped", name());
        return;
    }

    if (request.config.ballot > get_ballot() ||
        is_same_ballot_status_change_allowed(status(), request.config.status)) {
        if (!update_local_configuration(request.config, true))
            return;

        dassert(partition_status::PS_POTENTIAL_SECONDARY == status(),
                "invalid partition_status, status = %s",
                enum_to_string(status()));
        init_learn(request.config.learner_signature);
    }
}

// in non-replication thread
error_code replica::apply_learned_state_from_private_log(learn_state &state)
{
    bool duplicating = is_duplicating();

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
        // it means this round of learn must have been stepped back
        // to include all the unconfirmed.

        // move the `learn/` dir to working dir (`plog/`).
        error_code err = _private_log->reset_from(_app->learn_dir(), [this](error_code err) {
            tasking::enqueue(LPC_REPLICATION_ERROR,
                             &_tracker,
                             [this, err]() { handle_local_failure(err); },
                             get_gpid().thread_hash());
        });
        if (err != ERR_OK) {
            derror_replica("failed to reset this private log with logs in learn/ dir: {}", err);
            return err;
        }

        // only the uncommitted logs will be replayed and applied into storage.
        learn_state tmp_state;
        _private_log->get_learn_state(get_gpid(), _app->last_committed_decree() + 1, tmp_state);
        state.files = tmp_state.files;
    }

    int64_t offset;
    error_code err;

    // temp prepare list for learning purpose
    prepare_list plist(this,
                       _app->last_committed_decree(),
                       _options->max_mutation_count_in_prepare_list,
                       [this, duplicating](mutation_ptr &mu) {
                           if (mu->data.header.decree == _app->last_committed_decree() + 1) {
                               // TODO: assign the returned error_code to err and check it
                               _app->apply_mutation(mu);

                               // appends logs-in-cache into plog to ensure them can be duplicated.
                               if (duplicating) {
                                   _private_log->append(
                                       mu, LPC_WRITE_REPLICATION_LOG_COMMON, &_tracker, nullptr);
                               }
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

    ddebug("%s: apply_learned_state_from_private_log[%016" PRIx64 "]: learnee = %s, "
           "learn_duration = %" PRIu64 " ms, apply private log files done, "
           "file_count = %d, first_learn_start_decree = %" PRId64 ", learn_start_decree = %" PRId64
           ", app_committed_decree = %" PRId64,
           name(),
           _potential_secondary_states.learning_version,
           _config.primary.to_string(),
           _potential_secondary_states.duration_ms(),
           static_cast<int>(state.files.size()),
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
            ddebug("%s: apply_learned_state_from_private_log[%016" PRIx64 "]: learnee = %s, "
                   "learned_to_decree_included(%" PRId64 ") > last_committed_decree(%" PRId64 "), "
                   "commit to to_decree_included",
                   name(),
                   _potential_secondary_states.learning_version,
                   _config.primary.to_string(),
                   state.to_decree_included,
                   last_committed_decree());
            plist.commit(state.to_decree_included, COMMIT_TO_DECREE_SOFT);
        }

        ddebug("%s: apply_learned_state_from_private_log[%016" PRIx64 "]: learnee = %s, "
               "learn_duration = %" PRIu64 " ms, apply in-buffer private logs done, "
               "replay_count = %d, app_committed_decree = %" PRId64,
               name(),
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
