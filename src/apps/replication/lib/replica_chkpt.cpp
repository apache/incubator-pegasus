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
 *     What is this file about?
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
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
            init_checkpoint();
            gc();
        }

        void replica::gc()
        {
            if (_private_log)
                _private_log->garbage_collection(
                    get_gpid(),
                    _app->last_durable_decree(),
                    _app->log_info().init_offset_in_private_log
                    );
        }

        void replica::init_checkpoint()
        {
            check_hashed_access();
            
            // only applicable to primary and secondary replicas
            if (status() != PS_PRIMARY && status() != PS_SECONDARY)
                return;

            // no need to checkpoint
            if (_app->is_delta_state_learning_supported())
                return;

            // private log must be enabled to make sure commits
            // are not lost during checkpinting
            dassert(nullptr != _private_log, "log_enable_private_prepare must be true for checkpointing");

            // TODO: when NOT to checkpoint, but use private log replay to build the state
            if (last_committed_decree() - last_durable_decree() < 10000)
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

                    _primary_states.checkpoint_task = rpc::call_typed(
                        sd,
                        RPC_REPLICA_COPY_LAST_CHECKPOINT,
                        rc,
                        this,
                        &replica::on_copy_checkpoint_ack,
                        gpid_to_hash(get_gpid())
                        );
                }
            }

            // secondary can start checkpint in the long running thread pool
            else
            {
                dassert(PS_SECONDARY == status(), "");

                // only one running instance
                if (_secondary_states.checkpoint_task != nullptr)
                {
                    _secondary_states.checkpoint_task = tasking::enqueue(
                        LPC_CHECKPOINT_REPLICA,
                        this,
                        &replica::checkpoint,
                        gpid_to_hash(get_gpid())
                        );
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
                response.err = ERR_LEARN_FILE_FALED;
            }
            else
            {
                response.err = ERR_OK;
                response.last_committed_decree = last_committed_decree();
                response.base_local_dir = _app->data_dir();
                response.address = _stub->_primary_address;
            }
        }

        void replica::on_copy_checkpoint_ack(error_code err, std::shared_ptr<replica_configuration>& req, std::shared_ptr<learn_response>& resp)
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
                LPC_REPLICATION_COPY_REMOTE_FILES,
                this,
                [this, resp](error_code err, size_t sz)
                {
                    this->on_copy_checkpoint_file_completed(err, sz, resp);
                },
                gpid_to_hash(get_gpid())
                );
        }

        void replica::on_copy_checkpoint_file_completed(error_code err, size_t sz, std::shared_ptr<learn_response> resp)
        {
            if (PS_PRIMARY == status() && resp->state.to_decree_included > _app->last_durable_decree())
            {
                _app->apply_checkpoint(resp->state, CHKPT_COPY);
            }

            _primary_states.checkpoint_task = nullptr;
        }

        void replica::checkpoint()
        {
            auto lerr = _app->checkpoint();
            auto err = lerr == 0 ? ERR_OK :
                (lerr == ERR_WRONG_TIMING ? ERR_WRONG_TIMING : ERR_LOCAL_APP_FAILURE);
            
            tasking::enqueue(
                LPC_CHECKPOINT_REPLICA_COMPLETED,
                this,
                [this, err]() { this->on_checkpoint_completed(err); },
                gpid_to_hash(get_gpid())
                );
        }

        void replica::catch_up_with_private_logs(partition_status s)
        {
            learn_state state;
            _private_log->get_checkpoint(
                get_gpid(),
                _app->last_committed_decree() + 1,
                state
                );

            auto err = apply_learned_state_from_private_log(state);

            tasking::enqueue(
                LPC_CHECKPOINT_REPLICA_COMPLETED,
                this,
                [this, err, s]() 
                {
                    if (PS_SECONDARY == s)
                        this->on_checkpoint_completed(err);
                    else if (PS_POTENTIAL_SECONDARY == s)
                        this->on_learn_remote_state_completed(err);
                    else
                    {
                        dassert(false, "invalid state %s", enum_to_string(s));
                    }
                },
                gpid_to_hash(get_gpid())
                );
        }

        void replica::on_checkpoint_completed(error_code err)
        {
            check_hashed_access();

            // closing or wrong timing
            if (PS_SECONDARY != status() || ERR_WRONG_TIMING == err)
            {
                _secondary_states.checkpoint_task = nullptr;
                return;
            } 

            // handle failure
            if (err != ERR_OK)
            {
                // done checkpointing
                _secondary_states.checkpoint_task = nullptr;
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
                        _app->write_internal(mu);
                    }

                    // everything is ok now, done checkpointing
                    _secondary_states.checkpoint_task = nullptr;
                }

                // missed ones need to be loaded via private logs
                else
                {
                    _secondary_states.checkpoint_task = tasking::enqueue(
                        LPC_CHECKPOINT_REPLICA,
                        this,
                        [this]() { this->catch_up_with_private_logs(PS_SECONDARY); },
                        gpid_to_hash(get_gpid())
                        );
                }
            }

            // no missing commits
            else
            {
                // everything is ok now, done checkpointing
                _secondary_states.checkpoint_task = nullptr;
            }
        }
    }
} // namespace
