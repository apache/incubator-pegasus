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

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "replica.chkpoint"

namespace dsn { 
    namespace replication {

        void replica::on_checkpoint_timer()
        {
            check_hashed_access();
            init_checkpoint();
            garbage_collection();
        }

        // run in replica thread
        void replica::garbage_collection()
        {
            if (_private_log)
            {
                _private_log->garbage_collection(
                    get_gpid(),
                    _app->last_durable_decree(),
                    _app->init_info().init_offset_in_private_log
                    );
            }
        }

        // run in replica thread
        void replica::init_checkpoint()
        {
            // only applicable to primary and secondary replicas
            if (status() != PS_PRIMARY && status() != PS_SECONDARY)
                return;

            // no need to checkpoint
            if (_app->is_delta_state_learning_supported())
                return;

            auto err = _app->checkpoint_async();
            if (err != ERR_NOT_IMPLEMENTED)
            {
                if (err == ERR_OK)
                {
                    ddebug("%s: checkpoint_async succeed, app_last_committed_decree=%" PRId64 ", app_last_durable_decree=%" PRId64,
                           name(), _app->last_committed_decree(), _app->last_durable_decree());
                }
                if (err != ERR_OK && err != ERR_WRONG_TIMING && err != ERR_NO_NEED_OPERATE && err != ERR_TRY_AGAIN)
                {
                    derror("%s: checkpoint_async failed, err = %s", name(), err.to_string());
                }
                return;
            }

            // private log must be enabled to make sure commits
            // are not lost during checkpinting
            dassert(nullptr != _private_log, "log_enable_private_prepare must be true for checkpointing");

            if (last_committed_decree() - last_durable_decree() < _options->checkpoint_min_decree_gap)
                return;

            // primary cannot checkpoint (TODO: test if async checkpoint is supported)
            // therefore we have to copy checkpoints from secondaries
            if (PS_PRIMARY == status())
            {
                // only one running instance
                if (nullptr == _primary_states.checkpoint_task)
                {
                    if (_primary_states.membership.secondaries.size() == 0)
                        return;

                    std::shared_ptr<replica_configuration> rc(new replica_configuration);
                    _primary_states.get_replica_config(PS_SECONDARY, *rc);

                    rpc_address sd = _primary_states.membership.secondaries
                        [dsn_random32(0, (int)_primary_states.membership.secondaries.size() - 1)];

                    _primary_states.checkpoint_task = rpc::call(
                        sd,
                        RPC_REPLICA_COPY_LAST_CHECKPOINT,
                        *rc,
                        this,
                        [=](error_code err_local, learn_response&& response)
                        {
                            auto response_alloc = std::make_shared<learn_response>(std::move(response));
                            on_copy_checkpoint_ack(err_local, rc, response_alloc);
                        },
                        gpid_to_hash(get_gpid())
                        );
                }
            }

            // secondary can start checkpint in the long running thread pool
            else
            {
                dassert(PS_SECONDARY == status(), "");

                // only one running instance
                if (!_secondary_states.checkpoint_is_running)
                {
                    _secondary_states.checkpoint_is_running = true;
                    _secondary_states.checkpoint_task = tasking::create_task(
                        LPC_CHECKPOINT_REPLICA,
                        this,
                        [this] {background_checkpoint();},
                        gpid_to_hash(get_gpid()));
                    _secondary_states.checkpoint_task->enqueue();
                }
            }
        }

        // @ secondary
        void replica::on_copy_checkpoint(const replica_configuration& request, /*out*/ learn_response& response)
        {
            check_hashed_access();

            if (request.ballot > get_ballot())
            {
                if (!update_local_configuration(request))
                {
                    response.err = ERR_INVALID_STATE;
                    return;
                }
            }

            if (status() != PS_SECONDARY)
            {
                response.err = ERR_INVALID_STATE;
                return;
            }

            if (_app->last_durable_decree() == 0)
            {
                response.err = ERR_OBJECT_NOT_FOUND;
                return;
            }

            blob placeholder;
            int err = _app->get_checkpoint(0, placeholder, response.state);
            if (err != 0)
            {
                response.err = ERR_LEARN_FILE_FAILED;
            }
            else
            {
                response.err = ERR_OK;
                response.last_committed_decree = last_committed_decree();
                response.base_local_dir = _app->data_dir();

                // the state.files is returned whether with full_path or only-filename depends
                // on the app impl, we'd better handle with it
                for (std::string& file_name: response.state.files)
                {
                    std::size_t last_splitter = file_name.find_last_of("/\\");
                    if (last_splitter != std::string::npos)
                        file_name = file_name.substr(last_splitter+1);
                }
                response.address = _stub->_primary_address;
            }
        }

        void replica::on_copy_checkpoint_ack(error_code err, const std::shared_ptr<replica_configuration>& req, const std::shared_ptr<learn_response>& resp)
        {
            check_hashed_access();

            if (PS_PRIMARY != status())
            {
                _primary_states.checkpoint_task = nullptr;
                return;
            }

            if (err != ERR_OK || resp == nullptr)
            {
                dwarn("%s: copy checkpoint from secondary failed, err = %s", name(), err.to_string());
                _primary_states.checkpoint_task = nullptr;
                return;
            }

            if (resp->err != ERR_OK)
            {
                dinfo("%s: copy checkpoint from secondary failed, err = %s", name(), resp->err.to_string());
                _primary_states.checkpoint_task = nullptr;
                return;
            }

            if (resp->state.to_decree_included <= _app->last_durable_decree())
            {
                dinfo("%s: copy checkpoint from secondary skipped, as its decree is not bigger than current durable_decree: %" PRIu64 " vs %" PRIu64 "",
                    name(), resp->state.to_decree_included, _app->last_durable_decree()
                    );
                _primary_states.checkpoint_task = nullptr;
                return;
            }
                
            std::string ldir = utils::filesystem::path_combine(
                _app->learn_dir(),
                "checkpoint.copy"
                );

            if (utils::filesystem::path_exists(ldir))
                utils::filesystem::remove_path(ldir);

            _primary_states.checkpoint_task = file::copy_remote_files(
                resp->address,
                resp->base_local_dir,
                resp->state.files,
                ldir,
                false,
                LPC_REPLICA_COPY_LAST_CHECKPOINT_DONE,
                this,
                [this, resp, ldir](error_code err, size_t sz)
                {
                    this->on_copy_checkpoint_file_completed(err, sz, resp, ldir);
                },
                gpid_to_hash(get_gpid())
                );
        }

        void replica::on_copy_checkpoint_file_completed(error_code err, size_t sz, std::shared_ptr<learn_response> resp, const std::string& chk_dir)
        {
            check_hashed_access();

            if (ERR_OK != err)
            {
                if (ERR_TIMEOUT == err)
                    dwarn("copy checkpoint failed, err(%s), remote_addr(%s)", err.to_string(), resp->address.to_string());
                _primary_states.checkpoint_task = nullptr;
                return;
            }
            if (PS_PRIMARY == status() && resp->state.to_decree_included > _app->last_durable_decree())
            {
                // we must give the app the full path of the check point
                for (std::string& filename: resp->state.files)
                {
                    dassert(filename.find_last_of("/\\")==std::string::npos, "invalid file name");
                    filename = utils::filesystem::path_combine(chk_dir, filename);
                }
                _app->apply_checkpoint(resp->state, CHKPT_COPY);
            }

            _primary_states.checkpoint_task = nullptr;
        }

        // run in background thread
        void replica::background_checkpoint()
        {
            auto err = _app->checkpoint();
            _secondary_states.checkpoint_completed_task = tasking::enqueue(
                LPC_CHECKPOINT_REPLICA_COMPLETED,
                this,
                [this, err]() { this->on_checkpoint_completed(err); },
                gpid_to_hash(get_gpid())
                );
        }

        void replica::catch_up_with_private_logs(partition_status s)
        {
            learn_state state;
            _private_log->get_learn_state(
                get_gpid(),
                _app->last_committed_decree() + 1,
                state
                );

            auto err = apply_learned_state_from_private_log(state);

            if (s == PS_POTENTIAL_SECONDARY)
            {
                _potential_secondary_states.learn_remote_files_completed_task = tasking::create_task(
                    LPC_CHECKPOINT_REPLICA_COMPLETED,
                    this,
                    [this, err]() 
                    {
                        this->on_learn_remote_state_completed(err);
                    },
                    gpid_to_hash(get_gpid())
                    );
                _potential_secondary_states.learn_remote_files_completed_task->enqueue();
            }
            else
            {
                _secondary_states.checkpoint_completed_task = tasking::create_task(
                    LPC_CHECKPOINT_REPLICA_COMPLETED,
                    this,
                    [this, err]() 
                    {
                        this->on_checkpoint_completed(err);
                    },
                    gpid_to_hash(get_gpid())
                    );
                _secondary_states.checkpoint_completed_task->enqueue();
            }
        }

        void replica::on_checkpoint_completed(error_code err)
        {
            check_hashed_access();

            // closing or wrong timing or no need operate
            if (PS_SECONDARY != status() || err == ERR_WRONG_TIMING || err == ERR_NO_NEED_OPERATE)
            {
                _secondary_states.checkpoint_is_running = false;
                return;
            } 

            // handle failure
            if (err != ERR_OK)
            {
                // done checkpointing
                _secondary_states.checkpoint_is_running = false;
                handle_local_failure(err);
                return;
            }

            auto c = _prepare_list->last_committed_decree();

            // missing commits
            if (c > _app->last_committed_decree())
            {
                // missed ones are covered by prepare list
                if (_app->last_committed_decree() > _prepare_list->min_decree())
                {
                    for (auto d = _app->last_committed_decree() + 1; d <= c; d++)
                    {
                        auto mu = _prepare_list->get_mutation_by_decree(d);
                        dassert(nullptr != mu, "");
                        err = _app->write_internal(mu);
                        if (ERR_OK != err)
                        {
                            _secondary_states.checkpoint_is_running = false;
                            handle_local_failure(err);
                            return;
                        }
                    }

                    // everything is ok now, done checkpointing
                    _secondary_states.checkpoint_is_running = false;
                }

                // missed ones need to be loaded via private logs
                else
                {
                    _secondary_states.catchup_with_private_log_task = tasking::create_task(
                        LPC_CATCHUP_WITH_PRIVATE_LOGS,
                        this,
                        [this]() { this->catch_up_with_private_logs(PS_SECONDARY); },
                        gpid_to_hash(get_gpid())
                        );
                    _secondary_states.catchup_with_private_log_task->enqueue();
                }
            }

            // no missing commits
            else
            {
                // everything is ok now, done checkpointing
                _secondary_states.checkpoint_is_running = false;
            }
        }
    }
} // namespace
