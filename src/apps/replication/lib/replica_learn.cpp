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
#include "replica.h"
#include "mutation.h"
#include "mutation_log.h"
#include "replica_stub.h"

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "replica.learn"

namespace dsn { namespace replication {

void replica::init_learn(uint64_t signature)
{
    check_hashed_access();

    dassert (status() == PS_POTENTIAL_SECONDARY, "");
        
    // at most one learning task running
    if (_potential_secondary_states.learning_round_is_running || !signature)
        return;

    // learn timeout or primary change, the (new) primary starts another round of learning process
    if (signature != _potential_secondary_states.learning_signature)
    {
        _potential_secondary_states.cleanup(true);
        _potential_secondary_states.learning_signature = signature;
        _potential_secondary_states.learning_status = LearningWithoutPrepare;
        _prepare_list->reset(_app->last_committed_decree());
    }
    else
    {
        switch (_potential_secondary_states.learning_status)
        {
        // any failues in the process
        case LearningFailed:
            break;

        // state completed
        case LearningSucceeded:
        case LearningWithPrepare:
            dassert(_app->last_durable_decree() + 1 >= _potential_secondary_states.learning_start_prepare_decree, 
                "state is incomplete");
            {
                _potential_secondary_states.learning_status = LearningSucceeded;
                check_state_completeness();
                notify_learn_completion();
                return;
            }
            break;
        case LearningWithoutPrepare:
            break;
        default:
            dassert (false, "");
        }
    }
        
    _potential_secondary_states.learning_round_is_running = true;

    std::shared_ptr<learn_request> request(new learn_request);
    request->gpid = get_gpid();
    request->last_committed_decree_in_app = _app->last_committed_decree();
    request->last_committed_decree_in_prepare_list = _prepare_list->last_committed_decree();
    request->learner = primary_address();
    request->signature = _potential_secondary_states.learning_signature;
    _app->prepare_learning_request(request->app_specific_learn_request);

    _potential_secondary_states.learning_task = rpc::call_typed(
        _config.primary,
        RPC_LEARN,
        request,        
        this,
        &replica::on_learn_reply,
        gpid_to_hash(get_gpid())
        );

    ddebug(
        "%s: init_learn with lastAppC/DDecree = <%llu,%llu>, lastCDecree = %llu, learnState = %s",
        name(),
        _app->last_committed_decree(),
        _app->last_durable_decree(),
        last_committed_decree(),
        enum_to_string(_potential_secondary_states.learning_status)
        );
}

void replica::on_learn(dsn_message_t msg, const learn_request& request)
{
    check_hashed_access();
    
    learn_response response;
    if (PS_PRIMARY  != status())
    {
        response.err = ERR_INVALID_STATE;
        reply(msg, response);
        return;
    }
        
    _primary_states.get_replica_config(request.learner, response.config);

    auto it = _primary_states.learners.find(request.learner);
    if (it == _primary_states.learners.end())
    {
        response.err = (response.config.status == PS_SECONDARY ? ERR_OK : ERR_OBJECT_NOT_FOUND);
        reply(msg, response);
        return;
    }
    else if (it->second.signature != request.signature)
    {
        response.err = ERR_OBJECT_NOT_FOUND;
        reply(msg, response);
        return;
    }

    // prepare learn_start_decree
    decree local_committed_decree = last_committed_decree();
    decree learn_start_decree = request.last_committed_decree_in_app + 1;
    bool delayed_replay_prepare_list = false;

    // TODO: learner machine has been down for a long time, and DDD MUST happened before
    // which leads to state lost. Now the lost state is back, what shall we do?
    if (request.last_committed_decree_in_app > last_prepared_decree())
    {
        dassert (
            false,
            "%s: on_learn %s, learner state is newer than learnee, "
            "with its appCommittedDecree = %llu vs local_committed_decree = %llu, ",
            name(), request.learner.to_string(),
            request.last_committed_decree_in_app, local_committed_decree
            );
    }

    // mutations are previously committed already on learner (old primary)
    else if (request.last_committed_decree_in_app > local_committed_decree)
    {
        _prepare_list->commit(request.last_committed_decree_in_app, COMMIT_TO_DECREE_HARD);
        local_committed_decree = last_committed_decree();
    }

    ddebug(
        "%s: on_learn %s, with local_committed_decree = %llu, "
        "localAppC/DDecree = <%llu, %llu>, learn_start_decree = %llu",
        name(), 
        request.learner.to_string(),
        local_committed_decree, 
        _app->last_committed_decree(), 
        _app->last_durable_decree(),
        learn_start_decree
        );
    
    response.prepare_start_decree = invalid_decree;
    response.commit_decree = local_committed_decree;
    response.err = ERR_OK; 

    // set prepare_start_decree when to-be-learn state is covered by prepare list
    // note min_decree can be NOT present in prepare list when list.count == 0
    if (learn_start_decree > _prepare_list->min_decree() 
       || (learn_start_decree == _prepare_list->min_decree() 
           && _prepare_list->count() > 0)
       )
    {
        if (it->second.prepare_start_decree == invalid_decree)
        {
            // start from (last_committed_decree + 1)
            it->second.prepare_start_decree = local_committed_decree + 1;

            cleanup_preparing_mutations(true);
            
            // the replayed prepare msg needs to be AFTER the learning response msg
            delayed_replay_prepare_list = true;
            
            ddebug(
                "%s: on_learn %s, set prepareStartDecree = %llu",
                name(), request.learner.to_string(),
                local_committed_decree + 1
            );
        }

        response.prepare_start_decree = it->second.prepare_start_decree;
    }
    else
    {
        it->second.prepare_start_decree = invalid_decree;
    }

    // learn mutation cache only
    // in this case, the state on the PS should be contiguous (+ to-be-sent prepare list)
    if (response.prepare_start_decree != invalid_decree)
    {
        binary_writer writer;
        for (decree d = learn_start_decree; d < response.prepare_start_decree; d++)
        {
            auto mu = _prepare_list->get_mutation_by_decree(d);
            if (mu != nullptr)
            {
                mu->write_to(writer);
            }
        }
        response.state.meta.push_back(writer.get_buffer());
    }

    // learn delta state or checkpoint
    // in this case, the state on the PS is still incomplete
    else if (_app->is_delta_state_learning_supported() 
        || learn_start_decree <= _app->last_durable_decree())
    {
        int lerr = _app->get_learn_state(
            learn_start_decree, 
            request.app_specific_learn_request, 
            response.state
            );

        if (lerr != 0)
        {
            response.err = ERR_GET_LEARN_STATE_FALED;
            derror("%s get learn state failed, error = %d", 
                dir().c_str(), lerr);
        }
        else
        {
            response.base_local_dir = _app->data_dir();
        }
    }

    // learn replication commit logs
    // in this case, the state on the PS is still incomplete
    else
    {
        dassert(_commit_log != nullptr, 
            "log_enable_private_commit must be enabled for %s when the replicated app "
            "does not support delta state learning",
            name()
            );

        _commit_log->get_learn_state_when_as_commit_logs(get_gpid(), learn_start_decree, response.state);
        response.base_local_dir = _commit_log->dir();
    }

    
    for (auto& file : response.state.files)
    {
        file = file.substr(response.base_local_dir.length() + 1);
    }

    reply(msg, response);

    // the replayed prepare msg needs to be AFTER the learning response msg
    if (delayed_replay_prepare_list)
    {
        replay_prepare_list();
    }   
}

void replica::on_learn_reply(
    error_code err, 
    std::shared_ptr<learn_request>& req, 
    std::shared_ptr<learn_response>& resp
    )
{
    check_hashed_access();

    dassert(PS_POTENTIAL_SECONDARY == status(), "");
    dassert(req->signature == _potential_secondary_states.learning_signature, "");

    if (err != ERR_OK)
    {
        handle_learning_error(err);
        return;
    }

    ddebug(
        "%s: on_learn_reply with err = %s, remoteCommit = %llu, "
        "prepareStart = %llu, currentState = %s",
        name(), 
        resp->err.to_string(), 
        resp->commit_decree, 
        resp->prepare_start_decree,
        enum_to_string(_potential_secondary_states.learning_status)
        );

    if (resp->err != ERR_OK)
    {
        handle_learning_error(resp->err);
        return;
    }

    if (resp->config.ballot > get_ballot())
    {
        update_local_configuration(resp->config);
    }

    if (status() != PS_POTENTIAL_SECONDARY)
    {
        return;
    }

    if (resp->prepare_start_decree != invalid_decree)
    {
        dassert(_potential_secondary_states.learning_status == LearningWithoutPrepare, "");
        _potential_secondary_states.learning_status = LearningWithPrepare;

        // reset preparelist
        _potential_secondary_states.learning_start_prepare_decree = resp->prepare_start_decree;
        _prepare_list->reset(_app->last_committed_decree());
        ddebug(
            "%s: resetPrepareList = %llu, currentState = %s",
            name(), _app->last_committed_decree(),
            enum_to_string(_potential_secondary_states.learning_status)
            );

        // reset commit log if necessary
        if (_commit_log)
            _commit_log->reset_as_commit_log(get_gpid(), _app->last_committed_decree());

        dassert(resp->state.meta.size() > 0, "learn mutation cache failed");

        // apply incoming prepare-list
        binary_reader reader(resp->state.meta[0]);
        while (!reader.is_eof())
        {
            auto mu = mutation::read_from(reader, nullptr);
            mu->set_logged();
            dinfo("%s: apply learned mutation %s", name(), mu->name());
            if (mu->data.header.decree > last_committed_decree())
                _prepare_list->prepare(mu, PS_POTENTIAL_SECONDARY);
        }

        // further states are synced using 2pc
        _prepare_list->commit(resp->prepare_start_decree - 1, COMMIT_TO_DECREE_HARD);
        dassert(_prepare_list->last_committed_decree() == _app->last_committed_decree(), "");
        dassert(resp->state.files.size() == 0, "");                

        // in-memory state is complete
        // still need on-disk state completion next with flush(true)
        dassert(_app->last_committed_decree() + 1 >= _potential_secondary_states.learning_start_prepare_decree,
            "state is incomplete");       

        _potential_secondary_states.learn_remote_files_task = tasking::enqueue(
            LPC_LEARN_REMOTE_DELTA_FILES,
            this,
            std::bind(&replica::on_copy_remote_state_completed, this, ERR_OK, 0, resp)
            );
    }
   
    else if (resp->state.files.size() > 0)
    {
        utils::filesystem::remove_path(_app->learn_dir());
        utils::filesystem::create_directory(_app->learn_dir());

        _potential_secondary_states.learn_remote_files_task = 
            file::copy_remote_files(resp->config.primary,
                resp->base_local_dir,
                resp->state.files,
                _app->learn_dir(),
                true,
                LPC_COPY_REMOTE_DELTA_FILES,
                this,
                std::bind(&replica::on_copy_remote_state_completed, this,
                std::placeholders::_1,
                std::placeholders::_2,
                resp)
                );
    }
    else
    {
        _potential_secondary_states.learn_remote_files_task = tasking::enqueue(
            LPC_LEARN_REMOTE_DELTA_FILES,
            this,
            std::bind(&replica::on_copy_remote_state_completed, this, ERR_OK, 0, resp)
            );
    }
}

void replica::on_copy_remote_state_completed(
    error_code err2, 
    size_t size, 
    std::shared_ptr<learn_response> resp
    )
{   
    int err = 0;
    decree old_committed = _app->last_committed_decree();
    decree old_durable = _app->last_durable_decree();

    if (err2 != ERR_OK)
    {
        derror(
            "%s: learn failed, err = %s, transfer %d files to %s",
            name(),
            err2.to_string(),
            static_cast<int>(resp->state.files.size()),
            _dir.c_str()            
            );
    }
    else if (_potential_secondary_states.learning_status == LearningWithPrepare)
    {
        dassert(resp->state.files.size() == 0, "");
    }
    else if (resp->state.meta.size() > 0 || resp->state.files.size() > 0)
    {
        learn_state lstate;
        lstate.meta = resp->state.meta;
        
        for (auto& f : resp->state.files)
        {
            std::string file = utils::filesystem::path_combine(_app->learn_dir(), f);
            lstate.files.push_back(file);
        }

        int err = 0;

        // apply delta learning
        if (_app->is_delta_state_learning_supported())
        {
            // the only place where there is non-in-partition-thread update
            err = _app->apply_learn_state(resp->state);
            if (err == 0)
            {
                dassert(_app->last_committed_decree() >= _app->last_durable_decree(), "");
                // because if the original _app->last_committed_decree > resp->commit_decree,
                // the learn_start_decree will be set to 0, which makes learner to learn from scratch
                dassert(_app->last_committed_decree() <= resp->commit_decree, "");
            }
        }

        // apply checkpoint or replication log
        else
        {
            bool is_log = false;
            if (lstate.files.size() > 0)
            {
                auto fname = utils::filesystem::get_file_name(lstate.files[0]);
                if (fname.substr(0, 4) == std::string("log."))
                    is_log = true;
            }

            // apply checkpoint
            if (!is_log)
            {
                err = _app->apply_learn_state(lstate);
            }

            // apply logs
            else
            {
                int64_t offset;
                decree last_cd = 0;

                // apply on-disk logs
                err = mutation_log::replay(
                    lstate.files,
                    [this, &last_cd](mutation_ptr& mu)
                    {
                        if (last_cd != 0)
                        {
                            dassert(mu->data.header.decree == last_cd + 1,
                                "decrees in commit logs must be contiguous: %lld vs %lld",
                                last_cd,
                                mu->data.header.decree
                                );
                        }
                        last_cd = mu->data.header.decree;

                        if (last_cd == _app->last_committed_decree() + 1)
                        {
                            _app->write_internal(mu);
                        }
                        else
                        {
                            ddebug("%s: mutation %s skipped coz unmached decree %llu vs %llu (last_committed)",
                                name(), mu->name(),
                                last_cd,
                                _app->last_committed_decree()
                                );
                        }
                    },
                    offset
                    );

                // apply in-buffer commit logs
                if (err == ERR_OK && lstate.meta.size() > 0)
                {
                    dassert(lstate.meta.size() == 1, "only 1 buffered commit log is allowed");
                    binary_reader reader(lstate.meta[0]);
                    last_cd = 0;
                    while (!reader.is_eof())
                    {
                        auto mu = mutation::read_from(reader, nullptr);
                        if (last_cd != 0)
                        {
                            dassert(mu->data.header.decree == last_cd + 1,
                                "decrees in commit logs must be contiguous: %lld vs %lld",
                                last_cd,
                                mu->data.header.decree
                                );
                        }
                        last_cd = mu->data.header.decree;

                        if (last_cd == _app->last_committed_decree() + 1)
                        {
                            _app->write_internal(mu);
                        }
                        else
                        {
                            ddebug("%s: mutation %s skipped coz unmached decree %llu vs %llu (last_committed)",
                                name(), mu->name(),
                                last_cd,
                                _app->last_committed_decree()
                                );
                        }
                    }
                }
            }
        }
    }

    // translate to general error code
    if (err != 0)
    {
        err2 = ERR_LOCAL_APP_FAILURE;
    }

    ddebug(
        "%s: learning %d files to %s, err = 0x%x, err2 = %s, "
        "appCommit(%llu => %llu), appDurable(%llu => %llu), "
        "remoteCommit(%llu), prepareStart(%llu), currentState(%s)",
        name(), resp->state.files.size(), _dir.c_str(), err, err2.to_string(),
        old_committed, _app->last_committed_decree(),
        old_durable, _app->last_durable_decree(),
        resp->commit_decree, resp->prepare_start_decree,
        enum_to_string(_potential_secondary_states.learning_status)
        );
    
    // if catch-up done, do flush to enable all learned state is durable
    if (err2 == ERR_OK 
        && resp->prepare_start_decree != invalid_decree
        && _app->last_committed_decree() + 1 >= _potential_secondary_states.learning_start_prepare_decree
        && _app->last_committed_decree() > _app->last_durable_decree())
    {
        err = _app->flush(true);
        ddebug(
            "%s: flush done, err = %d, lastC/DDecree = <%llu, %llu>",
            name(), err, _app->last_committed_decree(), _app->last_durable_decree()
            );
        if (err == 0)
        {
            dassert(_app->last_committed_decree() == _app->last_durable_decree(), "");
        }
        else
        {
            err2 = ERR_CHECKPOINT_FAILED;
        }
    }

    _potential_secondary_states.learn_remote_files_completed_task = tasking::enqueue(
        LPC_LEARN_REMOTE_DELTA_FILES_COMPLETED,
        this,
        std::bind(&replica::on_learn_remote_state_completed, this, err2),
        gpid_to_hash(get_gpid())
        );
}

void replica::on_learn_remote_state_completed(error_code err)
{
    check_hashed_access();
    
    if (PS_POTENTIAL_SECONDARY != status())
        return;

    _potential_secondary_states.learning_round_is_running = false;

    if (err != ERR_OK)
    {
        handle_learning_error(err);
    }
    else
    {
        // continue
        init_learn(_potential_secondary_states.learning_signature);
    }
}

void replica::handle_learning_error(error_code err)
{
    check_hashed_access();

    dwarn(
        "%s: learning failed with err = %s, LastCommitted = %lld",
        name(),
        err.to_string(),
        _app->last_committed_decree()
        );

    _potential_secondary_states.cleanup(true);
    _potential_secondary_states.learning_status = LearningFailed;

    update_local_configuration_with_no_ballot_change(PS_ERROR);
}

void replica::handle_learning_succeeded_on_primary(
    ::dsn::rpc_address node, 
    uint64_t learn_signature
    )
{
    auto it = _primary_states.learners.find(node);
    if (it != _primary_states.learners.end() 
        && it->second.signature == learn_signature
        )
        upgrade_to_secondary_on_primary(node);
}

void replica::notify_learn_completion()
{
    group_check_response report;
    report.gpid = get_gpid();
    report.err = ERR_OK;
    report.last_committed_decree_in_app = _app->last_committed_decree();
    report.last_committed_decree_in_prepare_list = last_committed_decree();
    report.learner_signature = _potential_secondary_states.learning_signature;
    report.learner_status_ = _potential_secondary_states.learning_status;
    report.node = primary_address();

    ddebug(
        "%s: notify_learn_completion with lastAppC/DDecree = <%llu,%llu>, "
        "lastCDecree = %llu, learnState = %s",
        name(),
        _app->last_committed_decree(),
        _app->last_durable_decree(),
        last_committed_decree(),
        enum_to_string(_potential_secondary_states.learning_status)
        );

    rpc::call_one_way_typed(_config.primary, RPC_LEARN_COMPLETION_NOTIFY, 
        report, gpid_to_hash(get_gpid()));
}

void replica::on_learn_completion_notification(const group_check_response& report)
{
    check_hashed_access();
    report.err.end_tracking();
    if (status() != PS_PRIMARY)
        return;

    if (report.learner_status_ == LearningSucceeded)
    {
        handle_learning_succeeded_on_primary(report.node, report.learner_signature);
    }
}

void replica::on_add_learner(const group_check_request& request)
{
    if (request.config.ballot < get_ballot())
        return;

    if (request.config.ballot > get_ballot()
        || is_same_ballot_status_change_allowed(status(), request.config.status))
    {
        if (!update_local_configuration(request.config, true))
            return;

        dassert(PS_POTENTIAL_SECONDARY == status(), "");
        init_learn(request.learner_signature);
    }
}

}} // namespace
