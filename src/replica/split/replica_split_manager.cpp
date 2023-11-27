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

#include "replica_split_manager.h"

#include <chrono>
#include <functional>
#include <utility>

#include "common/partition_split_common.h"
#include "common/replication.codes.h"
#include "common/replication_common.h"
#include "common/replication_enums.h"
#include "consensus_types.h"
#include "dsn.layer2_types.h"
#include "failure_detector/failure_detector_multimaster.h"
#include "partition_split_types.h"
#include "perf_counter/perf_counter.h"
#include "perf_counter/perf_counter_wrapper.h"
#include "replica/mutation_log.h"
#include "replica/prepare_list.h"
#include "replica/replica_context.h"
#include "replica/replica_stub.h"
#include "replica/replication_app_base.h"
#include "runtime/api_layer1.h"
#include "runtime/rpc/rpc_address.h"
#include "runtime/rpc/rpc_holder.h"
#include "runtime/task/async_calls.h"
#include "runtime/task/task.h"
#include "utils/autoref_ptr.h"
#include "utils/chrono_literals.h"
#include "utils/defer.h"
#include "utils/fail_point.h"
#include "utils/filesystem.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "absl/strings/string_view.h"
#include "utils/thread_access_checker.h"

namespace dsn {
namespace replication {

DSN_DECLARE_bool(empty_write_disabled);
DSN_DECLARE_int32(max_mutation_count_in_prepare_list);

replica_split_manager::replica_split_manager(replica *r)
    : replica_base(r), _replica(r), _stub(r->get_replica_stub())
{
    _partition_version.store(_replica->_app_info.partition_count - 1);
}

replica_split_manager::~replica_split_manager() {}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_split_manager::parent_start_split(
    const group_check_request &request) // on parent partition
{
    if (status() != partition_status::PS_PRIMARY && status() != partition_status::PS_SECONDARY &&
        (status() != partition_status::PS_INACTIVE || !_replica->_inactive_is_transient)) {
        LOG_WARNING_PREFIX("receive add child request with wrong status({}), ignore this request",
                           enum_to_string(status()));
        return;
    }

    if (request.config.ballot != get_ballot()) {
        LOG_WARNING_PREFIX(
            "receive add child request with different ballot, local ballot({}) VS request "
            "ballot({}), ignore this request",
            get_ballot(),
            request.config.ballot);
        return;
    }

    if (_split_status == split_status::SPLITTING) {
        LOG_WARNING_PREFIX("partition is already splitting, ignore this request");
        return;
    }

    gpid child_gpid = request.child_gpid;
    if (child_gpid.get_partition_index() < _replica->_app_info.partition_count) {
        LOG_WARNING_PREFIX(
            "receive old add child request, child_gpid={}, partition_count={}, ignore this request",
            child_gpid,
            _replica->_app_info.partition_count);
        return;
    }

    if (status() == partition_status::PS_PRIMARY) {
        _replica->_primary_states.cleanup_split_states();
    }
    _partition_version.store(_replica->_app_info.partition_count - 1);

    _split_status = split_status::SPLITTING;
    _child_gpid = child_gpid;
    _child_init_ballot = get_ballot();

    LOG_INFO_PREFIX("start to add child({}), init_ballot={}, status={}, primary_address={}",
                    _child_gpid,
                    _child_init_ballot,
                    enum_to_string(status()),
                    request.config.primary.to_string());

    tasking::enqueue(LPC_CREATE_CHILD,
                     tracker(),
                     std::bind(&replica_stub::create_child_replica,
                               _stub,
                               _replica->_config.primary,
                               _replica->_app_info,
                               _child_init_ballot,
                               _child_gpid,
                               get_gpid(),
                               _replica->_dir),
                     get_gpid().thread_hash());
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_split_manager::child_init_replica(gpid parent_gpid,
                                               rpc_address primary_address,
                                               ballot init_ballot) // on child partition
{
    FAIL_POINT_INJECT_F("replica_child_init_replica", [](absl::string_view) {});

    if (status() != partition_status::PS_INACTIVE) {
        LOG_WARNING_PREFIX("wrong status({})", enum_to_string(status()));
        _stub->split_replica_error_handler(
            parent_gpid,
            std::bind(&replica_split_manager::parent_cleanup_split_context, std::placeholders::_1));
        child_handle_split_error("invalid child status during initialize");
        return;
    }

    // update replica config
    _replica->_config.ballot = init_ballot;
    _replica->_config.primary = primary_address;
    _replica->_config.status = partition_status::PS_PARTITION_SPLIT;

    // initialize split context
    _replica->_split_states.parent_gpid = parent_gpid;
    _replica->_split_states.is_prepare_list_copied = false;
    _replica->_split_states.is_caught_up = false;
    _replica->_split_states.check_state_task =
        tasking::enqueue(LPC_PARTITION_SPLIT,
                         tracker(),
                         std::bind(&replica_split_manager::child_check_split_context, this),
                         get_gpid().thread_hash(),
                         std::chrono::seconds(3));
    _replica->_split_states.splitting_start_ts_ns = dsn_now_ns();
    _stub->_counter_replicas_splitting_recent_start_count->increment();

    LOG_INFO_PREFIX(
        "child initialize succeed, init_ballot={}, parent_gpid={}", init_ballot, parent_gpid);

    error_code ec =
        _stub->split_replica_exec(LPC_PARTITION_SPLIT,
                                  _replica->_split_states.parent_gpid,
                                  std::bind(&replica_split_manager::parent_prepare_states,
                                            std::placeholders::_1,
                                            _replica->_app->learn_dir()));
    if (ec != ERR_OK) {
        child_handle_split_error("parent not exist when execute parent_prepare_states");
    }
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_split_manager::child_check_split_context() // on child partition
{
    FAIL_POINT_INJECT_F("replica_child_check_split_context", [](absl::string_view) {});

    if (status() != partition_status::PS_PARTITION_SPLIT) {
        LOG_ERROR_PREFIX("wrong status({})", enum_to_string(status()));
        _replica->_split_states.check_state_task = nullptr;
        return;
    }
    // let parent partition check its status
    error_code ec = _stub->split_replica_exec(
        LPC_PARTITION_SPLIT,
        _replica->_split_states.parent_gpid,
        std::bind(&replica_split_manager::parent_check_states, std::placeholders::_1));
    if (ec != ERR_OK) {
        child_handle_split_error("check_child_state failed because parent gpid is invalid");
        return;
    }

    _replica->_split_states.check_state_task =
        tasking::enqueue(LPC_PARTITION_SPLIT,
                         tracker(),
                         std::bind(&replica_split_manager::child_check_split_context, this),
                         get_gpid().thread_hash(),
                         std::chrono::seconds(3));
}

// ThreadPool: THREAD_POOL_REPLICATION
bool replica_split_manager::parent_check_states() // on parent partition
{
    FAIL_POINT_INJECT_F("replica_parent_check_states", [](absl::string_view) { return true; });

    if (_split_status != split_status::SPLITTING || _child_init_ballot != get_ballot() ||
        _child_gpid.get_app_id() == 0 ||
        (status() != partition_status::PS_PRIMARY && status() != partition_status::PS_SECONDARY &&
         (status() != partition_status::PS_INACTIVE || !_replica->_inactive_is_transient))) {
        LOG_WARNING_PREFIX("parent wrong states: status({}), split_status({}), init_ballot({}) VS "
                           "current_ballot({}), "
                           "child_gpid({})",
                           enum_to_string(status()),
                           enum_to_string(_split_status),
                           _child_init_ballot,
                           get_ballot(),
                           _child_gpid);
        parent_handle_split_error("wrong parent states when execute parent_check_states", false);
        return false;
    }
    return true;
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_split_manager::parent_prepare_states(const std::string &dir) // on parent partition
{
    if (!parent_check_states()) {
        return;
    }

    learn_state parent_states;
    int64_t checkpoint_decree;
    // generate checkpoint
    error_code ec = _replica->_app->copy_checkpoint_to_dir(dir.c_str(), &checkpoint_decree, true);
    if (ec == ERR_OK) {
        LOG_INFO_PREFIX("prepare checkpoint succeed: checkpoint dir = {}, checkpoint decree = {}",
                        dir,
                        checkpoint_decree);
        parent_states.to_decree_included = checkpoint_decree;
        // learn_state.files[0] will be used to get learn dir in function 'storage_apply_checkpoint'
        // so we add a fake file name here, this file won't appear on disk
        parent_states.files.push_back(dsn::utils::filesystem::path_combine(dir, "file_name"));
    } else {
        LOG_WARNING_PREFIX("prepare checkpoint failed, error={}, please wait and retry", ec);
        tasking::enqueue(LPC_PARTITION_SPLIT,
                         tracker(),
                         std::bind(&replica_split_manager::parent_prepare_states, this, dir),
                         get_gpid().thread_hash(),
                         std::chrono::seconds(1));
        return;
    }

    std::vector<mutation_ptr> mutation_list;
    std::vector<std::string> files;
    uint64_t total_file_size = 0;
    // get mutation and private log
    _replica->_private_log->get_parent_mutations_and_logs(
        get_gpid(), checkpoint_decree + 1, invalid_ballot, mutation_list, files, total_file_size);

    // get prepare list
    std::shared_ptr<prepare_list> plist =
        std::make_shared<prepare_list>(_replica, *_replica->_prepare_list);
    plist->truncate(last_committed_decree());

    CHECK_EQ(last_committed_decree(), checkpoint_decree);
    CHECK_GE(mutation_list.size(), 0);
    CHECK_GE(files.size(), 0);
    LOG_INFO_PREFIX("prepare state succeed: {} mutations, {} private log files, total file size = "
                    "{}, last_committed_decree = {}",
                    mutation_list.size(),
                    files.size(),
                    total_file_size,
                    last_committed_decree());

    ec = _stub->split_replica_exec(LPC_PARTITION_SPLIT,
                                   _child_gpid,
                                   std::bind(&replica_split_manager::child_copy_prepare_list,
                                             std::placeholders::_1,
                                             parent_states,
                                             mutation_list,
                                             files,
                                             total_file_size,
                                             std::move(plist)));
    if (ec != ERR_OK) {
        parent_cleanup_split_context();
    }
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_split_manager::child_copy_prepare_list(
    learn_state lstate,
    std::vector<mutation_ptr> mutation_list,
    std::vector<std::string> plog_files,
    uint64_t total_file_size,
    std::shared_ptr<prepare_list> plist) // on child partition
{
    if (status() != partition_status::PS_PARTITION_SPLIT) {
        LOG_ERROR_PREFIX("wrong status({})", enum_to_string(status()));
        return;
    }

    // learning parent states is time-consuming, should execute in THREAD_POOL_REPLICATION_LONG
    decree last_committed_decree = plist->last_committed_decree();
    _replica->_split_states.splitting_start_async_learn_ts_ns = dsn_now_ns();
    _replica->_split_states.async_learn_task =
        tasking::enqueue(LPC_PARTITION_SPLIT_ASYNC_LEARN,
                         tracker(),
                         std::bind(&replica_split_manager::child_learn_states,
                                   this,
                                   lstate,
                                   mutation_list,
                                   plog_files,
                                   total_file_size,
                                   last_committed_decree));

    LOG_INFO_PREFIX("start to copy parent prepare list, last_committed_decree={}, prepare list min "
                    "decree={}, max decree={}",
                    last_committed_decree,
                    plist->min_decree(),
                    plist->max_decree());

    // copy parent prepare list
    plist->set_committer(std::bind(&replica::execute_mutation, _replica, std::placeholders::_1));
    _replica->_prepare_list.reset(new prepare_list(this, *plist));
    for (decree d = last_committed_decree + 1; d <= _replica->_prepare_list->max_decree(); ++d) {
        mutation_ptr mu = _replica->_prepare_list->get_mutation_by_decree(d);
        CHECK_NOTNULL_PREFIX_MSG(mu, "can not find mutation, dercee={}", d);
        mu->data.header.pid = get_gpid();
        _replica->_private_log->append(mu, LPC_WRITE_REPLICATION_LOG_COMMON, tracker(), nullptr);
        // set mutation has been logged in private log
        if (!mu->is_logged()) {
            mu->set_logged();
        }
    }
    _replica->_split_states.is_prepare_list_copied = true;
}

// ThreadPool: THREAD_POOL_REPLICATION_LONG
void replica_split_manager::child_learn_states(learn_state lstate,
                                               std::vector<mutation_ptr> mutation_list,
                                               std::vector<std::string> plog_files,
                                               uint64_t total_file_size,
                                               decree last_committed_decree) // on child partition
{
    FAIL_POINT_INJECT_F("replica_child_learn_states", [](absl::string_view) {});

    if (status() != partition_status::PS_PARTITION_SPLIT) {
        LOG_ERROR_PREFIX("wrong status({})", enum_to_string(status()));
        child_handle_async_learn_error();
        return;
    }

    LOG_INFO_PREFIX("start to learn states asynchronously, prepare_list last_committed_decree={}, "
                    "checkpoint decree range=({},{}], private log files count={}, in-memory "
                    "mutation count={}",
                    last_committed_decree,
                    lstate.from_decree_excluded,
                    lstate.to_decree_included,
                    plog_files.size(),
                    mutation_list.size());

    error_code err;
    auto cleanup = defer([this, &err]() {
        if (err != ERR_OK) {
            child_handle_async_learn_error();
        }
    });

    // apply parent checkpoint
    err = _replica->_app->apply_checkpoint(replication_app_base::chkpt_apply_mode::learn, lstate);
    if (err != ERR_OK) {
        LOG_ERROR_PREFIX("failed to apply checkpoint, error={}", err);
        return;
    }

    // replay parent private log and learn in-memory mutations
    err =
        child_apply_private_logs(plog_files, mutation_list, total_file_size, last_committed_decree);
    if (err != ERR_OK) {
        LOG_ERROR_PREFIX("failed to replay private log, error={}", err);
        return;
    }

    // generate a checkpoint synchronously
    err = _replica->_app->sync_checkpoint();
    if (err != ERR_OK) {
        LOG_ERROR_PREFIX("failed to generate checkpoint synchrounously, error={}", err);
        return;
    }

    err = _replica->update_init_info_ballot_and_decree();
    if (err != ERR_OK) {
        LOG_ERROR_PREFIX("update_init_info_ballot_and_decree failed, error={}", err);
        return;
    }

    LOG_INFO_PREFIX("learn parent states asynchronously succeed");

    tasking::enqueue(LPC_PARTITION_SPLIT,
                     tracker(),
                     std::bind(&replica_split_manager::child_catch_up_states, this),
                     get_gpid().thread_hash());
    _replica->_split_states.async_learn_task = nullptr;
}

// ThreadPool: THREAD_POOL_REPLICATION_LONG
error_code
replica_split_manager::child_apply_private_logs(std::vector<std::string> plog_files,
                                                std::vector<mutation_ptr> mutation_list,
                                                uint64_t total_file_size,
                                                decree last_committed_decree) // on child partition
{
    FAIL_POINT_INJECT_F("replica_child_apply_private_logs", [](absl::string_view arg) {
        return error_code::try_get(arg.data(), ERR_OK);
    });

    if (status() != partition_status::PS_PARTITION_SPLIT) {
        LOG_ERROR_PREFIX("wrong status({})", enum_to_string(status()));
        return ERR_INVALID_STATE;
    }

    error_code ec;
    int64_t offset;
    // temp prepare_list used for apply states
    prepare_list plist(
        _replica,
        _replica->_app->last_committed_decree(),
        FLAGS_max_mutation_count_in_prepare_list,
        [this](mutation_ptr &mu) {
            if (mu->data.header.decree != _replica->_app->last_committed_decree() + 1) {
                return;
            }

            auto e = _replica->_app->apply_mutation(mu);
            if (e != ERR_OK) {
                LOG_ERROR_PREFIX("got an error({}) in commit stage of prepare_list", e);
                return;
            }
        });

    // replay private log
    ec = mutation_log::replay(plog_files,
                              [&plist](int log_length, mutation_ptr &mu) {
                                  decree d = mu->data.header.decree;
                                  if (d <= plist.last_committed_decree()) {
                                      return false;
                                  }
                                  mutation_ptr origin_mu = plist.get_mutation_by_decree(d);
                                  if (origin_mu != nullptr &&
                                      origin_mu->data.header.ballot >= mu->data.header.ballot) {
                                      return false;
                                  }
                                  plist.prepare(mu, partition_status::PS_SECONDARY);
                                  return true;
                              },
                              offset);
    if (ec != ERR_OK) {
        LOG_ERROR_PREFIX(
            "replay private_log files failed, file count={}, app last_committed_decree={}",
            plog_files.size(),
            _replica->_app->last_committed_decree());
        return ec;
    }

    _replica->_split_states.splitting_copy_file_count += plog_files.size();
    _replica->_split_states.splitting_copy_file_size += total_file_size;
    _stub->_counter_replicas_splitting_recent_copy_file_count->add(plog_files.size());
    _stub->_counter_replicas_splitting_recent_copy_file_size->add(total_file_size);

    LOG_INFO_PREFIX("replay private_log files succeed, file count={}, app last_committed_decree={}",
                    plog_files.size(),
                    _replica->_app->last_committed_decree());

    // apply in-memory mutations if replay private logs succeed
    int count = 0;
    for (mutation_ptr &mu : mutation_list) {
        decree d = mu->data.header.decree;
        if (d <= plist.last_committed_decree()) {
            continue;
        }
        mutation_ptr origin_mu = plist.get_mutation_by_decree(d);
        if (origin_mu != nullptr && origin_mu->data.header.ballot >= mu->data.header.ballot) {
            continue;
        }
        if (!mu->is_logged()) {
            mu->set_logged();
        }
        plist.prepare(mu, partition_status::PS_SECONDARY);
        ++count;
    }
    _replica->_split_states.splitting_copy_mutation_count += count;
    _stub->_counter_replicas_splitting_recent_copy_mutation_count->add(count);
    plist.commit(last_committed_decree, COMMIT_TO_DECREE_HARD);
    LOG_INFO_PREFIX(
        "apply in-memory mutations succeed, mutation count={}, app last_committed_decree={}",
        count,
        _replica->_app->last_committed_decree());

    return ec;
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_split_manager::child_catch_up_states() // on child partition
{
    FAIL_POINT_INJECT_F("replica_child_catch_up_states", [](absl::string_view) {});

    if (status() != partition_status::PS_PARTITION_SPLIT) {
        LOG_ERROR_PREFIX("wrong status, status is {}", enum_to_string(status()));
        return;
    }

    // parent will copy mutations to child during async-learn, as a result:
    // - child prepare_list last_committed_decree = parent prepare_list last_committed_decree, also
    // is catch_up goal_decree
    // - local_decree is child local last_committed_decree which is the last decree in async-learn.
    decree goal_decree = _replica->_prepare_list->last_committed_decree();
    decree local_decree = _replica->_app->last_committed_decree();

    // there are mutations written to parent during async-learn
    // child does not catch up parent, there are still some mutations child not learn
    if (local_decree < goal_decree) {
        if (local_decree >= _replica->_prepare_list->min_decree()) {
            // all missing mutations are all in prepare list
            LOG_WARNING_PREFIX("there are some in-memory mutations should be learned, app "
                               "last_committed_decree={}, "
                               "goal decree={}, prepare_list min_decree={}",
                               local_decree,
                               goal_decree,
                               _replica->_prepare_list->min_decree());
            for (decree d = local_decree + 1; d <= goal_decree; ++d) {
                auto mu = _replica->_prepare_list->get_mutation_by_decree(d);
                CHECK_NOTNULL(mu, "");
                error_code ec = _replica->_app->apply_mutation(mu);
                if (ec != ERR_OK) {
                    child_handle_split_error("child_catchup failed because apply mutation failed");
                    return;
                }
            }
        } else {
            // some missing mutations have already in private log
            // should call `catch_up_with_private_logs` to catch up all missing mutations
            LOG_WARNING_PREFIX(
                "there are some private logs should be learned, app last_committed_decree="
                "{}, prepare_list min_decree={}, please wait",
                local_decree,
                _replica->_prepare_list->min_decree());
            _replica->_split_states.async_learn_task = tasking::enqueue(
                LPC_CATCHUP_WITH_PRIVATE_LOGS,
                tracker(),
                [this]() {
                    _replica->catch_up_with_private_logs(partition_status::PS_PARTITION_SPLIT);
                    _replica->_split_states.async_learn_task = nullptr;
                },
                get_gpid().thread_hash());
            return;
        }
    }

    LOG_INFO_PREFIX("child catch up parent states, goal decree={}, local decree={}",
                    _replica->_prepare_list->last_committed_decree(),
                    _replica->_app->last_committed_decree());
    _replica->_split_states.is_caught_up = true;

    child_notify_catch_up();
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_split_manager::child_notify_catch_up() // on child partition
{
    FAIL_POINT_INJECT_F("replica_child_notify_catch_up", [](absl::string_view) {});

    std::unique_ptr<notify_catch_up_request> request = std::make_unique<notify_catch_up_request>();
    request->parent_gpid = _replica->_split_states.parent_gpid;
    request->child_gpid = get_gpid();
    request->child_ballot = get_ballot();
    request->child_address = _stub->_primary_address;

    LOG_INFO_PREFIX("send notification to primary parent[{}@{}], ballot={}",
                    _replica->_split_states.parent_gpid,
                    _replica->_config.primary.to_string(),
                    get_ballot());

    notify_catch_up_rpc rpc(std::move(request),
                            RPC_SPLIT_NOTIFY_CATCH_UP,
                            /*never timeout*/ 0_ms,
                            /*partition_hash*/ 0,
                            _replica->_split_states.parent_gpid.thread_hash());
    rpc.call(_replica->_config.primary, tracker(), [this, rpc](error_code ec) mutable {
        auto response = rpc.response();
        if (ec == ERR_TIMEOUT) {
            LOG_WARNING_PREFIX("notify primary catch up timeout, please wait and retry");
            tasking::enqueue(LPC_PARTITION_SPLIT,
                             tracker(),
                             std::bind(&replica_split_manager::child_notify_catch_up, this),
                             get_gpid().thread_hash(),
                             std::chrono::seconds(1));
            return;
        }
        if (ec != ERR_OK || response.err != ERR_OK) {
            error_code err = (ec == ERR_OK) ? response.err : ec;
            LOG_ERROR_PREFIX("failed to notify primary catch up, error={}", err);
            _stub->split_replica_error_handler(
                _replica->_split_states.parent_gpid,
                std::bind(&replica_split_manager::parent_cleanup_split_context,
                          std::placeholders::_1));
            child_handle_split_error("notify_primary_split_catch_up failed");
            return;
        }
        LOG_INFO_PREFIX("notify primary parent[{}@{}] catch up succeed",
                        _replica->_split_states.parent_gpid,
                        _replica->_config.primary.to_string());
    });
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_split_manager::parent_handle_child_catch_up(
    const notify_catch_up_request &request,
    notify_cacth_up_response &response) // on primary parent
{
    if (status() != partition_status::PS_PRIMARY || _split_status != split_status::SPLITTING) {
        LOG_ERROR_PREFIX(
            "wrong partition status or wrong split status, partition_status={}, split_status={}",
            enum_to_string(status()),
            enum_to_string(_split_status));

        response.err = ERR_INVALID_STATE;
        return;
    }

    if (request.child_ballot != get_ballot() || request.child_gpid != _child_gpid) {
        LOG_ERROR_PREFIX(
            "receive out-date request, request ballot ({}) VS local ballot({}), request "
            "child_gpid({}) VS local child_gpid({})",
            request.child_ballot,
            get_ballot(),
            request.child_gpid,
            _child_gpid);
        response.err = ERR_INVALID_STATE;
        return;
    }

    response.err = ERR_OK;
    LOG_INFO_PREFIX("receive catch_up request from {}@{}, current ballot={}",
                    request.child_gpid,
                    request.child_address.to_string(),
                    request.child_ballot);

    _replica->_primary_states.caught_up_children.insert(request.child_address);
    // _primary_states.statuses is a map structure: rpc address -> partition_status
    // it stores replica's rpc address and partition_status of this replica group
    for (auto &iter : _replica->_primary_states.statuses) {
        if (_replica->_primary_states.caught_up_children.find(iter.first) ==
            _replica->_primary_states.caught_up_children.end()) {
            // there are child partitions not caught up its parent
            return;
        }
    }

    LOG_INFO_PREFIX("all child partitions catch up");
    _replica->_primary_states.caught_up_children.clear();
    _replica->_primary_states.sync_send_write_request = true;

    // sync_point is the first decree after parent send write request to child synchronously
    // when sync_point commit, parent consider child has all data it should have during async-learn
    decree sync_point = _replica->_prepare_list->max_decree() + 1;
    if (!FLAGS_empty_write_disabled) {
        // empty wirte here to commit sync_point
        mutation_ptr mu = _replica->new_mutation(invalid_decree);
        mu->add_client_request(RPC_REPLICATION_WRITE_EMPTY, nullptr);
        _replica->init_prepare(mu, false);
        CHECK_EQ_PREFIX_MSG(
            sync_point, mu->data.header.decree, "sync_point should be equal to mutation's decree");
    };

    // check if sync_point has been committed
    tasking::enqueue(
        LPC_PARTITION_SPLIT,
        tracker(),
        std::bind(&replica_split_manager::parent_check_sync_point_commit, this, sync_point),
        get_gpid().thread_hash(),
        std::chrono::seconds(1));
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_split_manager::parent_check_sync_point_commit(decree sync_point) // on primary parent
{
    FAIL_POINT_INJECT_F("replica_parent_check_sync_point_commit", [](absl::string_view) {});
    if (status() != partition_status::PS_PRIMARY) {
        LOG_ERROR_PREFIX("wrong status({})", enum_to_string(status()));
        parent_handle_split_error("check_sync_point_commit failed, primary changed", false);
        return;
    }

    LOG_INFO_PREFIX("sync_point = {}, app last_committed_decree = {}",
                    sync_point,
                    _replica->_app->last_committed_decree());
    if (_replica->_app->last_committed_decree() >= sync_point) {
        update_child_group_partition_count(_replica->_app_info.partition_count * 2);
    } else {
        LOG_WARNING_PREFIX("sync_point has not been committed, please wait and retry");
        tasking::enqueue(
            LPC_PARTITION_SPLIT,
            tracker(),
            std::bind(&replica_split_manager::parent_check_sync_point_commit, this, sync_point),
            get_gpid().thread_hash(),
            std::chrono::seconds(1));
    }
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_split_manager::update_child_group_partition_count(
    int new_partition_count) // on primary parent
{
    if (status() != partition_status::PS_PRIMARY || _split_status != split_status::SPLITTING) {
        LOG_ERROR_PREFIX(
            "wrong partition status or wrong split status, partition_status={}, split_status={}",
            enum_to_string(status()),
            enum_to_string(_split_status));
        parent_handle_split_error(
            "update_child_group_partition_count failed, wrong partition status or split status",
            true);
        return;
    }

    if (!_replica->_primary_states.learners.empty() ||
        _replica->_primary_states.membership.secondaries.size() + 1 <
            _replica->_primary_states.membership.max_replica_count) {
        LOG_ERROR_PREFIX("there are {} learners or not have enough secondaries(count is {})",
                         _replica->_primary_states.learners.size(),
                         _replica->_primary_states.membership.secondaries.size());
        parent_handle_split_error(
            "update_child_group_partition_count failed, have learner or lack of secondary", true);
        return;
    }

    auto not_replied_addresses = std::make_shared<std::unordered_set<rpc_address>>();
    // _primary_states.statuses is a map structure: rpc address -> partition_status
    for (const auto &kv : _replica->_primary_states.statuses) {
        not_replied_addresses->insert(kv.first);
    }
    for (const auto &iter : _replica->_primary_states.statuses) {
        parent_send_update_partition_count_request(
            iter.first, new_partition_count, not_replied_addresses);
    }
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_split_manager::parent_send_update_partition_count_request(
    const rpc_address &address,
    int32_t new_partition_count,
    std::shared_ptr<std::unordered_set<rpc_address>> &not_replied_addresses) // on primary parent
{
    FAIL_POINT_INJECT_F("replica_parent_update_partition_count_request", [](absl::string_view) {});

    CHECK_EQ_PREFIX(status(), partition_status::PS_PRIMARY);

    auto request = std::make_unique<update_child_group_partition_count_request>();
    request->new_partition_count = new_partition_count;
    request->target_address = address;
    request->child_pid = _child_gpid;
    request->ballot = get_ballot();

    LOG_INFO_PREFIX(
        "send update child group partition count request to node({}), new partition_count = {}",
        address.to_string(),
        new_partition_count);
    update_child_group_partition_count_rpc rpc(std::move(request),
                                               RPC_SPLIT_UPDATE_CHILD_PARTITION_COUNT,
                                               0_ms,
                                               0,
                                               get_gpid().thread_hash());
    rpc.call(address, tracker(), [this, rpc, not_replied_addresses](error_code ec) mutable {
        on_update_child_group_partition_count_reply(
            ec, rpc.request(), rpc.response(), not_replied_addresses);
    });
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_split_manager::on_update_child_group_partition_count(
    const update_child_group_partition_count_request &request,
    update_child_group_partition_count_response &response) // on child partition
{
    if (request.ballot != get_ballot() || !_replica->_split_states.is_caught_up) {
        LOG_ERROR_PREFIX(
            "receive outdated update child group_partition_count_request, request ballot={}, "
            "local ballot={}, is_caught_up={}",
            request.ballot,
            get_ballot(),
            _replica->_split_states.is_caught_up);
        response.err = ERR_VERSION_OUTDATED;
        return;
    }

    if (_replica->_app_info.partition_count == request.new_partition_count &&
        _partition_version.load() == request.new_partition_count - 1) {
        LOG_WARNING_PREFIX("receive repeated update child group_partition_count_request, "
                           "partition_count = {}, ignore it",
                           request.new_partition_count);
        response.err = ERR_OK;
        return;
    }

    CHECK_EQ_PREFIX(_replica->_app_info.partition_count * 2, request.new_partition_count);
    update_local_partition_count(request.new_partition_count);
    response.err = ERR_OK;
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_split_manager::update_local_partition_count(
    int32_t new_partition_count) // on all partitions
{
    // update _app_info and partition_version
    auto info = _replica->_app_info;
    // if app has not been split before, init_partition_count = -1
    // we should set init_partition_count to old_partition_count
    if (info.init_partition_count < 0) {
        info.init_partition_count = info.partition_count;
    }
    auto old_partition_count = info.partition_count;
    info.partition_count = new_partition_count;

    CHECK_EQ_PREFIX_MSG(_replica->store_app_info(info), ERR_OK, "failed to save app_info");

    _replica->_app_info = info;
    LOG_INFO_PREFIX("update partition_count from {} to {}",
                    old_partition_count,
                    _replica->_app_info.partition_count);

    _replica->_app->set_partition_version(_replica->_app_info.partition_count - 1);
    _partition_version.store(_replica->_app_info.partition_count - 1);
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_split_manager::on_update_child_group_partition_count_reply(
    error_code ec,
    const update_child_group_partition_count_request &request,
    const update_child_group_partition_count_response &response,
    std::shared_ptr<std::unordered_set<rpc_address>> &not_replied_addresses) // on primary parent
{
    _replica->_checker.only_one_thread_access();

    if (status() != partition_status::PS_PRIMARY || _split_status != split_status::SPLITTING) {
        LOG_ERROR_PREFIX(
            "wrong partition status or wrong split status, partition_status={}, split_status={}",
            enum_to_string(status()),
            enum_to_string(_split_status));
        parent_handle_split_error("on_update_child_group_partition_count_reply failed, wrong "
                                  "partition status or split status",
                                  true);
        return;
    }

    if (request.ballot != get_ballot()) {
        LOG_ERROR_PREFIX(
            "ballot changed, request ballot = {}, local ballot = {}", request.ballot, get_ballot());
        parent_handle_split_error(
            "on_update_child_group_partition_count_reply failed, ballot changed", true);
        return;
    }

    error_code error = (ec == ERR_OK) ? response.err : ec;
    if (error == ERR_TIMEOUT) {
        LOG_WARNING_PREFIX(
            "failed to update child node({}) partition_count, error = {}, wait and retry",
            request.target_address.to_string(),
            error);
        tasking::enqueue(
            LPC_PARTITION_SPLIT,
            tracker(),
            std::bind(&replica_split_manager::parent_send_update_partition_count_request,
                      this,
                      request.target_address,
                      request.new_partition_count,
                      not_replied_addresses),
            get_gpid().thread_hash(),
            std::chrono::seconds(1));
        return;
    }

    if (error != ERR_OK) {
        LOG_ERROR_PREFIX("failed to update child node({}) partition_count({}), error = {}",
                         request.target_address.to_string(),
                         request.new_partition_count,
                         error);
        parent_handle_split_error("on_update_child_group_partition_count_reply error", true);
        return;
    }

    LOG_INFO_PREFIX("update node({}) child({}) partition_count({}) succeed",
                    request.target_address.to_string(),
                    request.child_pid,
                    request.new_partition_count);

    // update group partition_count succeed
    not_replied_addresses->erase(request.target_address);
    if (not_replied_addresses->empty()) {
        LOG_INFO_PREFIX("update child({}) group partition_count, new_partition_count = {}",
                        request.child_pid,
                        request.new_partition_count);
        register_child_on_meta(get_ballot());
    } else {
        LOG_INFO_PREFIX("there are still {} replica not update partition count in child group",
                        not_replied_addresses->size());
    }
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_split_manager::register_child_on_meta(ballot b) // on primary parent
{
    FAIL_POINT_INJECT_F("replica_register_child_on_meta", [](absl::string_view) {});

    if (status() != partition_status::PS_PRIMARY || _split_status != split_status::SPLITTING) {
        LOG_ERROR_PREFIX(
            "wrong partition status or wrong split status, partition_status={}, split_status={}",
            enum_to_string(status()),
            enum_to_string(_split_status));
        parent_handle_split_error("register child failed, wrong partition status or split status",
                                  true);
        return;
    }

    if (_replica->_primary_states.reconfiguration_task != nullptr) {
        LOG_WARNING_PREFIX("under reconfiguration, delay and retry to register child");
        _replica->_primary_states.register_child_task =
            tasking::enqueue(LPC_PARTITION_SPLIT,
                             tracker(),
                             std::bind(&replica_split_manager::register_child_on_meta, this, b),
                             get_gpid().thread_hash(),
                             std::chrono::seconds(1));
        return;
    }

    partition_configuration child_config = _replica->_primary_states.membership;
    child_config.ballot++;
    child_config.last_committed_decree = 0;
    child_config.last_drops.clear();
    child_config.pid.set_partition_index(_replica->_app_info.partition_count +
                                         get_gpid().get_partition_index());

    register_child_request request;
    request.app = _replica->_app_info;
    request.child_config = child_config;
    request.parent_config = _replica->_primary_states.membership;
    request.primary_address = _stub->_primary_address;

    // reject client request
    _replica->update_local_configuration_with_no_ballot_change(partition_status::PS_INACTIVE);
    _replica->set_inactive_state_transient(true);
    int32_t old_partition_version = _partition_version.exchange(-1);
    LOG_INFO_PREFIX("update partition version from {} to {}", old_partition_version, -1);

    parent_send_register_request(request);
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_split_manager::parent_send_register_request(
    const register_child_request &request) // on primary parent
{
    FAIL_POINT_INJECT_F("replica_parent_send_register_request", [](absl::string_view) {});

    CHECK_EQ_PREFIX(status(), partition_status::PS_INACTIVE);
    LOG_INFO_PREFIX(
        "send register child({}) request to meta_server, current ballot = {}, child ballot = {}",
        request.child_config.pid,
        request.parent_config.ballot,
        request.child_config.ballot);

    rpc_address meta_address(_stub->_failure_detector->get_servers());
    std::unique_ptr<register_child_request> req = std::make_unique<register_child_request>(request);
    register_child_rpc rpc(std::move(req),
                           RPC_CM_REGISTER_CHILD_REPLICA,
                           /*never timeout*/ 0_ms,
                           /*partition hash*/ 0,
                           get_gpid().thread_hash());

    _replica->_primary_states.register_child_task =
        rpc.call(meta_address, tracker(), [this, rpc](error_code ec) mutable {
            on_register_child_on_meta_reply(ec, rpc.request(), rpc.response());
        });
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_split_manager::on_register_child_on_meta_reply(
    error_code ec,
    const register_child_request &request,
    const register_child_response &response) // on primary parent
{
    FAIL_POINT_INJECT_F("replica_on_register_child_on_meta_reply", [](absl::string_view) {});

    _replica->_checker.only_one_thread_access();

    // primary parent is under reconfiguration, whose status should be PS_INACTIVE
    if (partition_status::PS_INACTIVE != status() || !_stub->is_connected()) {
        LOG_ERROR_PREFIX("status wrong or stub is not connected, status = {}",
                         enum_to_string(status()));
        _replica->_primary_states.register_child_task = nullptr;
        return;
    }

    error_code err = ec == ERR_OK ? response.err : ec;
    if (err == ERR_INVALID_STATE || err == ERR_INVALID_VERSION || err == ERR_CHILD_REGISTERED) {
        if (err == ERR_CHILD_REGISTERED) {
            LOG_ERROR_PREFIX(
                "register child({}) failed, error = {}, child has already been registered",
                request.child_config.pid,
                err);
        } else {
            LOG_ERROR_PREFIX("register child({}) failed, error = {}, request is out-of-dated",
                             request.child_config.pid,
                             err);
            _stub->split_replica_error_handler(
                request.child_config.pid,
                std::bind(&replica_split_manager::child_handle_split_error,
                          std::placeholders::_1,
                          "register child failed, request is out-of-dated"));
        }
        parent_cleanup_split_context();
        _replica->_primary_states.register_child_task = nullptr;
        _replica->_primary_states.sync_send_write_request = false;
        if (response.parent_config.ballot >= get_ballot()) {
            LOG_INFO_PREFIX("response ballot = {}, local ballot = {}, should update configuration",
                            response.parent_config.ballot,
                            get_ballot());
            _replica->update_configuration(response.parent_config);
        }
        return;
    }

    if (err != ERR_OK) {
        LOG_WARNING_PREFIX(
            "register child({}) failed, error = {}, wait and retry", request.child_config.pid, err);
        _replica->_primary_states.register_child_task = tasking::enqueue(
            LPC_PARTITION_SPLIT,
            tracker(),
            std::bind(&replica_split_manager::parent_send_register_request, this, request),
            get_gpid().thread_hash(),
            std::chrono::seconds(1));
        return;
    }

    if (response.parent_config.ballot < get_ballot()) {
        LOG_WARNING_PREFIX(
            "register child({}) failed, parent ballot from response is {}, local ballot is {}",
            request.child_config.pid,
            response.parent_config.ballot,
            get_ballot());
        _replica->_primary_states.register_child_task = tasking::enqueue(
            LPC_PARTITION_SPLIT,
            tracker(),
            std::bind(&replica_split_manager::parent_send_register_request, this, request),
            get_gpid().thread_hash(),
            std::chrono::seconds(1));
        return;
    }

    LOG_INFO_PREFIX("register child({}) succeed, response parent ballot = {}, local ballot = "
                    "{}, local status = {}",
                    request.child_config.pid,
                    response.parent_config.ballot,
                    get_ballot(),
                    enum_to_string(status()));

    CHECK_GE_PREFIX(response.parent_config.ballot, get_ballot());
    CHECK_EQ_PREFIX(_replica->_app_info.partition_count * 2, response.app.partition_count);

    _stub->split_replica_exec(LPC_PARTITION_SPLIT,
                              response.child_config.pid,
                              std::bind(&replica_split_manager::child_partition_active,
                                        std::placeholders::_1,
                                        response.child_config));

    // update parent config
    _replica->update_configuration(response.parent_config);
    _replica->_primary_states.register_child_task = nullptr;
    _replica->_primary_states.sync_send_write_request = false;

    // update primary parent group partition_count
    update_local_partition_count(_replica->_app_info.partition_count * 2);
    _meta_split_status = split_status::NOT_SPLIT;
    _replica->broadcast_group_check();

    parent_cleanup_split_context();
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_split_manager::child_partition_active(
    const partition_configuration &config) // on child
{
    if (status() != partition_status::PS_PARTITION_SPLIT) {
        LOG_WARNING_PREFIX("child partition has been active, status={}", enum_to_string(status()));
        return;
    }

    _stub->_counter_replicas_splitting_recent_split_succ_count->increment();
    _replica->_primary_states.last_prepare_decree_on_new_primary =
        _replica->_prepare_list->max_decree();
    _replica->update_configuration(config);
    _stub->_counter_replicas_splitting_recent_split_succ_count->increment();
    LOG_INFO_PREFIX("child partition is active, status={}", enum_to_string(status()));
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_split_manager::parent_cleanup_split_context() // on parent partition
{
    _child_gpid.set_app_id(0);
    _child_init_ballot = 0;
    _split_status = split_status::NOT_SPLIT;
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_split_manager::child_handle_split_error(
    const std::string &error_msg) // on child partition
{
    if (status() != partition_status::PS_ERROR) {
        LOG_ERROR_PREFIX("child partition split failed because {}, parent = {}, split_duration = "
                         "{}ms, async_learn_duration = {}ms",
                         error_msg,
                         _replica->_split_states.parent_gpid,
                         _replica->_split_states.total_ms(),
                         _replica->_split_states.async_learn_ms());
        _stub->_counter_replicas_splitting_recent_split_fail_count->increment();
        _replica->update_local_configuration_with_no_ballot_change(partition_status::PS_ERROR);
    }
}

// ThreadPool: THREAD_POOL_REPLICATION_LONG
void replica_split_manager::child_handle_async_learn_error() // on child partition
{
    _stub->split_replica_error_handler(
        _replica->_split_states.parent_gpid,
        std::bind(&replica_split_manager::parent_cleanup_split_context, std::placeholders::_1));
    child_handle_split_error("meet error when execute child_learn_states");
    _replica->_split_states.async_learn_task = nullptr;
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_split_manager::parent_handle_split_error(const std::string &child_err_msg,
                                                      bool parent_clear_sync)
{
    _stub->split_replica_error_handler(_child_gpid,
                                       std::bind(&replica_split_manager::child_handle_split_error,
                                                 std::placeholders::_1,
                                                 child_err_msg));
    if (parent_clear_sync) {
        _replica->_primary_states.sync_send_write_request = false;
    }
    parent_cleanup_split_context();
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_split_manager::trigger_primary_parent_split(
    const int32_t meta_partition_count,
    const split_status::type meta_split_status) // on primary parent partition
{
    CHECK_EQ_PREFIX(status(), partition_status::PS_PRIMARY);
    CHECK_EQ_PREFIX(_replica->_app_info.partition_count * 2, meta_partition_count);
    LOG_INFO_PREFIX(
        "app({}) partition count changed, local({}) VS meta({}), split_status local({}) "
        "VS meta({})",
        _replica->_app_info.app_name,
        _replica->_app_info.partition_count,
        meta_partition_count,
        enum_to_string(_split_status),
        enum_to_string(meta_split_status));

    _meta_split_status = meta_split_status;
    if (meta_split_status == split_status::SPLITTING) {
        if (!_replica->_primary_states.learners.empty() ||
            _replica->_primary_states.membership.secondaries.size() + 1 <
                _replica->_primary_states.membership.max_replica_count) {
            LOG_WARNING_PREFIX(
                "there are {} learners or not have enough secondaries(count is {}), wait for "
                "next round",
                _replica->_primary_states.learners.size(),
                _replica->_primary_states.membership.secondaries.size());
            return;
        }

        group_check_request add_child_request;
        add_child_request.app = _replica->_app_info;
        _replica->_primary_states.get_replica_config(status(), add_child_request.config);
        auto child_gpid =
            gpid(get_gpid().get_app_id(),
                 get_gpid().get_partition_index() + _replica->_app_info.partition_count);
        add_child_request.__set_child_gpid(child_gpid);
        parent_start_split(add_child_request);
        // broadcast group check request to secondaries to start split
        _replica->broadcast_group_check();
        return;
    }

    if (meta_split_status == split_status::PAUSING ||
        meta_split_status == split_status::CANCELING) {
        parent_stop_split(meta_split_status);
        return;
    }

    if (meta_split_status == split_status::PAUSED) {
        LOG_WARNING_PREFIX("split has been paused, ignore it");
        return;
    }

    // meta_split_status == split_status::NOT_SPLIT
    // meta partition_count = replica paritition_count * 2
    // There will be two cases:
    // - case1. when primary replica register child succeed, but replica server crashed.
    //   meta server will consider this parent partition not splitting, but parent group
    //   partition_count is not updated
    //   in this case, child has been registered on meta server
    // - case2. when this parent partition is canceled, but other partitions is still canceling.
    //   in this case, child partition ballot is invalid_ballot
    // As a result, primary should send query_child_state rpc to meta server
    query_child_state();
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_split_manager::trigger_secondary_parent_split(
    const group_check_request &request,
    /*out*/ group_check_response &response) // on secondary parent partition
{
    if (request.app.partition_count ==
        _replica->_app_info.partition_count * 2) { // secondary update partition count
        update_local_partition_count(request.app.partition_count);
        parent_cleanup_split_context();
        return;
    }

    if (!request.__isset.meta_split_status) {
        return;
    }

    if (request.meta_split_status == split_status::SPLITTING &&
        request.__isset.child_gpid) { // secondary create child replica
        parent_start_split(request);
        return;
    }

    if (request.meta_split_status == split_status::PAUSING ||
        request.meta_split_status == split_status::CANCELING) { // secondary pause or cancel split
        parent_stop_split(request.meta_split_status);
        response.__set_is_split_stopped(true);
    }
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_split_manager::copy_mutation(mutation_ptr &mu) // on parent partition
{
    CHECK_GT_PREFIX_MSG(_child_gpid.get_app_id(), 0, "child_gpid({}) is invalid", _child_gpid);

    if (mu->is_sync_to_child()) {
        mu->wait_child();
    }

    mutation_ptr new_mu = mutation::copy_no_reply(mu);
    error_code ec = _stub->split_replica_exec(
        LPC_PARTITION_SPLIT,
        _child_gpid,
        std::bind(&replica_split_manager::on_copy_mutation, std::placeholders::_1, new_mu));
    if (ec != ERR_OK) {
        parent_cleanup_split_context();
    }
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_split_manager::on_copy_mutation(mutation_ptr &mu) // on child partition
{
    if (status() != partition_status::PS_PARTITION_SPLIT) {
        LOG_ERROR_PREFIX(
            "wrong status({}), ignore this mutation({})", enum_to_string(status()), mu->name());
        _stub->split_replica_error_handler(
            _replica->_split_states.parent_gpid, [mu](replica_split_manager *split_mgr) {
                split_mgr->parent_cleanup_split_context();
                split_mgr->on_copy_mutation_reply(
                    ERR_OK, mu->data.header.ballot, mu->data.header.decree);
            });
        return;
    }

    // It is possible for child has not copied parent prepare list, because parent and child may
    // execute in different thread. In this case, child should ignore this mutation.
    if (!_replica->_split_states.is_prepare_list_copied) {
        return;
    }

    if (mu->data.header.ballot > get_ballot()) {
        LOG_ERROR_PREFIX(
            "ballot changed, mutation ballot({}) vs local ballot({}), ignore copy this "
            "mutation({})",
            mu->data.header.ballot,
            get_ballot(),
            mu->name());
        _stub->split_replica_error_handler(
            _replica->_split_states.parent_gpid, [mu](replica_split_manager *split_mgr) {
                split_mgr->parent_cleanup_split_context();
                split_mgr->on_copy_mutation_reply(
                    ERR_OK, mu->data.header.ballot, mu->data.header.decree);
            });
        child_handle_split_error("on_copy_mutation failed because ballot changed");
        return;
    }

    mu->data.header.pid = get_gpid();
    _replica->_prepare_list->prepare(mu, partition_status::PS_SECONDARY);
    if (!mu->is_sync_to_child()) { // child copy mutation asynchronously
        if (!mu->is_logged()) {
            mu->set_logged();
        }
        mu->log_task() = _replica->_private_log->append(
            mu, LPC_WRITE_REPLICATION_LOG, tracker(), nullptr, get_gpid().thread_hash());
    } else { // child sync copy mutation
        mu->log_task() = _replica->_private_log->append(mu,
                                                        LPC_WRITE_REPLICATION_LOG,
                                                        tracker(),
                                                        std::bind(&replica::on_append_log_completed,
                                                                  _replica,
                                                                  mu,
                                                                  std::placeholders::_1,
                                                                  std::placeholders::_2),
                                                        get_gpid().thread_hash());
    }
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_split_manager::ack_parent(error_code ec, mutation_ptr &mu) // on child partition
{
    CHECK_PREFIX_MSG(
        mu->is_sync_to_child(), "mutation({}) should be copied synchronously", mu->name());
    _stub->split_replica_exec(LPC_PARTITION_SPLIT,
                              _replica->_split_states.parent_gpid,
                              std::bind(&replica_split_manager::on_copy_mutation_reply,
                                        std::placeholders::_1,
                                        ec,
                                        mu->data.header.ballot,
                                        mu->data.header.decree));
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_split_manager::on_copy_mutation_reply(error_code ec,
                                                   ballot b,
                                                   decree d) // on parent partition
{
    _replica->_checker.only_one_thread_access();

    auto mu = _replica->_prepare_list->get_mutation_by_decree(d);
    if (mu == nullptr) {
        LOG_ERROR_PREFIX("failed to get mutation in prepare list, decree = {}", d);
        return;
    }

    if (mu->data.header.ballot != b) {
        LOG_ERROR_PREFIX("ballot not match, mutation ballot({}) vs child mutation ballot({})",
                         mu->data.header.ballot,
                         b);
        return;
    }

    // set child prepare mutation flag
    if (ec == ERR_OK) {
        mu->child_acked();
    } else {
        LOG_ERROR_PREFIX("child({}) copy mutation({}) failed, ballot={}, decree={}, error={}",
                         _child_gpid,
                         mu->name(),
                         b,
                         d,
                         ec);
    }

    // handle child ack
    if (mu->data.header.ballot >= get_ballot() && status() != partition_status::PS_INACTIVE) {
        switch (status()) {
        case partition_status::PS_PRIMARY:
            if (ec != ERR_OK) {
                _replica->handle_local_failure(ec);
            } else {
                _replica->do_possible_commit_on_primary(mu);
            }
            break;
        case partition_status::PS_SECONDARY:
        case partition_status::PS_POTENTIAL_SECONDARY:
            if (ec != ERR_OK) {
                _replica->handle_local_failure(ec);
            }
            _replica->ack_prepare_message(ec, mu);
            break;
        case partition_status::PS_ERROR:
            break;
        default:
            CHECK_PREFIX_MSG(false, "wrong status({})", enum_to_string(status()));
            break;
        }
    }
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_split_manager::parent_stop_split(
    split_status::type meta_split_status) // on parent partition
{
    CHECK_PREFIX_MSG(status() == partition_status::PS_PRIMARY ||
                         status() == partition_status::PS_SECONDARY,
                     "wrong partition_status({})",
                     enum_to_string(status()));
    CHECK_PREFIX_MSG(_split_status == split_status::SPLITTING ||
                         _split_status == split_status::NOT_SPLIT,
                     "wrong split_status({})",
                     enum_to_string(_split_status));

    auto old_status = _split_status;
    if (_split_status == split_status::SPLITTING) {
        parent_handle_split_error("stop partition split", false);
    }
    _partition_version.store(_replica->_app_info.partition_count - 1);

    if (status() == partition_status::PS_PRIMARY) {
        _replica->_primary_states.sync_send_write_request = false;
        _replica->broadcast_group_check();
    }
    LOG_INFO_PREFIX(
        "{} split succeed, status = {}, old split_status = {}, child partition_index = {}",
        meta_split_status == split_status::PAUSING ? "pause" : "cancel",
        enum_to_string(status()),
        enum_to_string(old_status),
        get_gpid().get_partition_index() + _replica->_app_info.partition_count);
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_split_manager::primary_parent_handle_stop_split(
    const std::shared_ptr<group_check_request> &req,
    const std::shared_ptr<group_check_response> &resp) // on primary parent partition
{
    if (!req->__isset.meta_split_status || (req->meta_split_status != split_status::PAUSING &&
                                            req->meta_split_status != split_status::CANCELING)) {
        // partition is not executing split or not stopping split
        return;
    }

    if (!resp->__isset.is_split_stopped || !resp->is_split_stopped) {
        // secondary has not stopped split
        return;
    }

    _replica->_primary_states.split_stopped_secondary.insert(req->node);
    auto count = 0;
    for (auto &iter : _replica->_primary_states.statuses) {
        if (iter.second == partition_status::PS_SECONDARY &&
            _replica->_primary_states.split_stopped_secondary.find(iter.first) !=
                _replica->_primary_states.split_stopped_secondary.end()) {
            ++count;
        }
    }
    // all secondaries have already stop split succeed
    if (count == _replica->_primary_states.membership.max_replica_count - 1) {
        _replica->_primary_states.cleanup_split_states();
        parent_send_notify_stop_request(req->meta_split_status);
    }
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_split_manager::parent_send_notify_stop_request(
    split_status::type meta_split_status) // on primary parent
{
    FAIL_POINT_INJECT_F("replica_parent_send_notify_stop_request", [](absl::string_view) {});
    rpc_address meta_address(_stub->_failure_detector->get_servers());
    std::unique_ptr<notify_stop_split_request> req = std::make_unique<notify_stop_split_request>();
    req->app_name = _replica->_app_info.app_name;
    req->parent_gpid = get_gpid();
    req->meta_split_status = meta_split_status;
    req->partition_count = _replica->_app_info.partition_count;

    LOG_INFO_PREFIX("group {} split succeed, send notify_stop_request to meta server({})",
                    meta_split_status == split_status::PAUSING ? "pause" : "cancel",
                    meta_address.to_string());
    notify_stop_split_rpc rpc(
        std::move(req), RPC_CM_NOTIFY_STOP_SPLIT, 0_ms, 0, get_gpid().thread_hash());
    rpc.call(meta_address, tracker(), [this, rpc](error_code ec) mutable {
        error_code err = ec == ERR_OK ? rpc.response().err : ec;
        const std::string type =
            rpc.request().meta_split_status == split_status::PAUSING ? "pause" : "cancel";
        if (err != ERR_OK) {
            LOG_WARNING_PREFIX(
                "notify {} split failed, error = {}, wait for next round", type, err);
        }
    });
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_split_manager::query_child_state() // on primary parent
{
    auto request = std::make_unique<query_child_state_request>();
    request->app_name = _replica->_app_info.app_name;
    request->pid = get_gpid();
    request->partition_count = _replica->_app_info.partition_count;

    rpc_address meta_address(_stub->_failure_detector->get_servers());
    LOG_INFO_PREFIX("send query child partition state request to meta server({})",
                    meta_address.to_string());
    query_child_state_rpc rpc(
        std::move(request), RPC_CM_QUERY_CHILD_STATE, 0_ms, 0, get_gpid().thread_hash());
    _replica->_primary_states.query_child_task =
        rpc.call(meta_address, tracker(), [this, rpc](error_code ec) mutable {
            on_query_child_state_reply(ec, rpc.request(), rpc.response());
        });
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_split_manager::on_query_child_state_reply(
    error_code ec,
    const query_child_state_request &request,
    const query_child_state_response &response) // on primary parent
{
    _replica->_checker.only_one_thread_access();

    if (ec != ERR_OK) {
        LOG_WARNING_PREFIX("query child partition state failed, error = {}, retry it later", ec);
        _replica->_primary_states.query_child_task =
            tasking::enqueue(LPC_PARTITION_SPLIT,
                             tracker(),
                             std::bind(&replica_split_manager::query_child_state, this),
                             get_gpid().thread_hash(),
                             std::chrono::seconds(1));
        return;
    }

    if (response.err != ERR_OK) {
        LOG_WARNING_PREFIX("app({}) partition({}) split has been canceled, ignore it",
                           request.app_name,
                           request.pid);
        return;
    }

    LOG_INFO_PREFIX("query child partition succeed, child partition[{}] has already been ready",
                    response.child_config.pid);
    // make child partition active
    _stub->split_replica_exec(LPC_PARTITION_SPLIT,
                              response.child_config.pid,
                              std::bind(&replica_split_manager::child_partition_active,
                                        std::placeholders::_1,
                                        response.child_config));
    update_local_partition_count(response.partition_count);
    _replica->_primary_states.cleanup_split_states();
    parent_cleanup_split_context();
    // update parent group partition_count
    _replica->broadcast_group_check();
}

} // namespace replication
} // namespace dsn
