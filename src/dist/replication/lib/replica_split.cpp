// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <dsn/dist/fmt_logging.h>
#include <dsn/dist/replication/replication_app_base.h>
#include <dsn/utility/defer.h>
#include <dsn/utility/filesystem.h>
#include <dsn/utility/fail_point.h>

#include "replica.h"
#include "replica_stub.h"

namespace dsn {
namespace replication {

// ThreadPool: THREAD_POOL_REPLICATION
void replica::on_add_child(const group_check_request &request) // on parent partition
{
    if (status() != partition_status::PS_PRIMARY && status() != partition_status::PS_SECONDARY &&
        (status() != partition_status::PS_INACTIVE || !_inactive_is_transient)) {
        dwarn_replica("receive add child request with wrong status {}, ignore this request",
                      enum_to_string(status()));
        return;
    }

    if (request.config.ballot != get_ballot()) {
        dwarn_replica(
            "receive add child request with different ballot, local ballot({}) VS request "
            "ballot({}), ignore this request",
            get_ballot(),
            request.config.ballot);
        return;
    }

    gpid child_gpid = request.child_gpid;
    if (_child_gpid == child_gpid) {
        dwarn_replica("child replica({}) is already existed, might be partition splitting, ignore "
                      "this request",
                      child_gpid);
        return;
    }

    if (child_gpid.get_partition_index() < _app_info.partition_count) {
        dwarn_replica("receive old add child request, child gpid is ({}), "
                      "local partition count is {}, ignore this request",
                      child_gpid,
                      _app_info.partition_count);
        return;
    }

    _child_gpid = child_gpid;
    _child_init_ballot = get_ballot();

    ddebug_replica("process add child({}), primary is {}, ballot is {}, "
                   "status is {}, last_committed_decree is {}",
                   child_gpid,
                   request.config.primary.to_string(),
                   request.config.ballot,
                   enum_to_string(request.config.status),
                   request.last_committed_decree);

    tasking::enqueue(LPC_CREATE_CHILD,
                     tracker(),
                     std::bind(&replica_stub::create_child_replica,
                               _stub,
                               _config.primary,
                               _app_info,
                               _child_init_ballot,
                               _child_gpid,
                               get_gpid(),
                               _dir),
                     get_gpid().thread_hash());
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica::child_init_replica(gpid parent_gpid,
                                 rpc_address primary_address,
                                 ballot init_ballot) // on child partition
{
    FAIL_POINT_INJECT_F("replica_child_init_replica", [](dsn::string_view) {});

    if (status() != partition_status::PS_INACTIVE) {
        dwarn_replica("wrong status {}", enum_to_string(status()));
        _stub->split_replica_error_handler(
            parent_gpid, std::bind(&replica::parent_cleanup_split_context, std::placeholders::_1));
        return;
    }

    // update replica config
    _config.ballot = init_ballot;
    _config.primary = primary_address;
    _config.status = partition_status::PS_PARTITION_SPLIT;

    // init split states
    _split_states.parent_gpid = parent_gpid;
    _split_states.is_prepare_list_copied = false;
    _split_states.is_caught_up = false;

    ddebug_replica("init ballot is {}, parent gpid is ({})", init_ballot, parent_gpid);

    dsn::error_code ec = _stub->split_replica_exec(
        LPC_PARTITION_SPLIT,
        _split_states.parent_gpid,
        std::bind(&replica::parent_prepare_states, std::placeholders::_1, _app->learn_dir()));
    if (ec != ERR_OK) {
        child_handle_split_error("parent not exist when execute parent_prepare_states");
    }
}

// ThreadPool: THREAD_POOL_REPLICATION
bool replica::parent_check_states() // on parent partition
{
    FAIL_POINT_INJECT_F("replica_parent_check_states", [](dsn::string_view) { return true; });

    if (_child_init_ballot != get_ballot() || _child_gpid.get_app_id() == 0 ||
        (status() != partition_status::PS_PRIMARY && status() != partition_status::PS_SECONDARY &&
         (status() != partition_status::PS_INACTIVE || !_inactive_is_transient))) {
        dwarn_replica("parent wrong states: status({}), init_ballot({}) VS current_ballot({}), "
                      "child_gpid({})",
                      enum_to_string(status()),
                      _child_init_ballot,
                      get_ballot(),
                      _child_gpid);
        _stub->split_replica_error_handler(
            _child_gpid,
            std::bind(&replica::child_handle_split_error,
                      std::placeholders::_1,
                      "wrong parent states when execute parent_check_states"));
        parent_cleanup_split_context();
        return false;
    }
    return true;
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica::parent_prepare_states(const std::string &dir) // on parent partition
{
    FAIL_POINT_INJECT_F("replica_parent_prepare_states", [](dsn::string_view) {});

    if (!parent_check_states()) {
        return;
    }

    learn_state parent_states;
    int64_t checkpoint_decree;
    // generate checkpoint
    dsn::error_code ec = _app->copy_checkpoint_to_dir(dir.c_str(), &checkpoint_decree);
    if (ec == ERR_OK) {
        ddebug_replica("prepare checkpoint succeed: checkpoint dir = {}, checkpoint decree = {}",
                       dir,
                       checkpoint_decree);
        parent_states.to_decree_included = checkpoint_decree;
        // learn_state.files[0] will be used to get learn dir in function 'storage_apply_checkpoint'
        // so we add a fake file name here, this file won't appear on disk
        parent_states.files.push_back(dsn::utils::filesystem::path_combine(dir, "file_name"));
    } else {
        derror_replica("prepare checkpoint failed, error = {}", ec.to_string());
        tasking::enqueue(LPC_PARTITION_SPLIT,
                         tracker(),
                         std::bind(&replica::parent_prepare_states, this, dir),
                         get_gpid().thread_hash(),
                         std::chrono::seconds(1));
        return;
    }

    std::vector<mutation_ptr> mutation_list;
    std::vector<std::string> files;
    uint64_t total_file_size = 0;
    // get mutation and private log
    _private_log->get_parent_mutations_and_logs(
        get_gpid(), checkpoint_decree + 1, invalid_ballot, mutation_list, files, total_file_size);

    // get prepare list
    std::shared_ptr<prepare_list> plist = std::make_shared<prepare_list>(this, *_prepare_list);
    plist->truncate(last_committed_decree());

    dassert_replica(
        last_committed_decree() == checkpoint_decree || !mutation_list.empty() || !files.empty(),
        "last_committed_decree({}) VS checkpoint_decree({}), mutation_list size={}, files size={}",
        last_committed_decree(),
        checkpoint_decree,
        mutation_list.size(),
        files.size());

    ddebug_replica("prepare state succeed: {} mutations, {} private log files, total file size = "
                   "{}, last_committed_decree = {}",
                   mutation_list.size(),
                   files.size(),
                   total_file_size,
                   last_committed_decree());

    ec = _stub->split_replica_exec(LPC_PARTITION_SPLIT,
                                   _child_gpid,
                                   std::bind(&replica::child_copy_prepare_list,
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
void replica::child_copy_prepare_list(learn_state lstate,
                                      std::vector<mutation_ptr> mutation_list,
                                      std::vector<std::string> plog_files,
                                      uint64_t total_file_size,
                                      std::shared_ptr<prepare_list> plist) // on child partition
{
    if (status() != partition_status::PS_PARTITION_SPLIT) {
        dwarn_replica("wrong status, status is {}", enum_to_string(status()));
        _stub->split_replica_error_handler(
            _split_states.parent_gpid,
            std::bind(&replica::parent_cleanup_split_context, std::placeholders::_1));
        child_handle_split_error("wrong child status when execute child_copy_prepare_list");
        return;
    }

    // learning parent states is time-consuming, should execute in THREAD_POOL_REPLICATION_LONG
    decree last_committed_decree = plist->last_committed_decree();
    _split_states.async_learn_task = tasking::enqueue(LPC_PARTITION_SPLIT_ASYNC_LEARN,
                                                      tracker(),
                                                      std::bind(&replica::child_learn_states,
                                                                this,
                                                                lstate,
                                                                mutation_list,
                                                                plog_files,
                                                                total_file_size,
                                                                last_committed_decree));

    ddebug_replica("start to copy parent prepare list, last_committed_decree={}, prepare list min "
                   "decree={}, max decree={}",
                   last_committed_decree,
                   plist->min_decree(),
                   plist->max_decree());

    // copy parent prepare list
    plist->set_committer(std::bind(&replica::execute_mutation, this, std::placeholders::_1));
    delete _prepare_list;
    _prepare_list = new prepare_list(this, *plist);
    for (decree d = last_committed_decree + 1; d <= _prepare_list->max_decree(); ++d) {
        mutation_ptr mu = _prepare_list->get_mutation_by_decree(d);
        dassert_replica(mu != nullptr, "can not find mutation, dercee={}", d);
        mu->data.header.pid = get_gpid();
        _stub->_log->append(mu, LPC_WRITE_REPLICATION_LOG_COMMON, tracker(), nullptr);
        _private_log->append(mu, LPC_WRITE_REPLICATION_LOG_COMMON, tracker(), nullptr);
        // set mutation has been logged in private log
        if (!mu->is_logged()) {
            mu->set_logged();
        }
    }
    _split_states.is_prepare_list_copied = true;
}

// ThreadPool: THREAD_POOL_REPLICATION_LONG
void replica::child_learn_states(learn_state lstate,
                                 std::vector<mutation_ptr> mutation_list,
                                 std::vector<std::string> plog_files,
                                 uint64_t total_file_size,
                                 decree last_committed_decree) // on child partition
{
    FAIL_POINT_INJECT_F("replica_child_learn_states", [](dsn::string_view) {});

    if (status() != partition_status::PS_PARTITION_SPLIT) {
        dwarn_replica("wrong status, status is {}", enum_to_string(status()));
        child_handle_async_learn_error();
        return;
    }

    ddebug_replica("start to learn states asynchronously, prepare_list last_committed_decree={}, "
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
    err = _app->apply_checkpoint(replication_app_base::chkpt_apply_mode::learn, lstate);
    if (err != ERR_OK) {
        derror_replica("failed to apply checkpoint, error={}", err);
        return;
    }

    // replay parent private log and learn in-memory mutations
    err =
        child_apply_private_logs(plog_files, mutation_list, total_file_size, last_committed_decree);
    if (err != ERR_OK) {
        derror_replica("failed to replay private log, error={}", err);
        return;
    }

    // generate a checkpoint synchronously
    err = _app->sync_checkpoint();
    if (err != ERR_OK) {
        derror_replica("failed to generate checkpoint synchrounously, error={}", err);
        return;
    }

    err = _app->update_init_info_ballot_and_decree(this);
    if (err != ERR_OK) {
        derror_replica("update_init_info_ballot_and_decree failed, error={}", err);
        return;
    }

    ddebug_replica("learn parent states asynchronously succeed");

    tasking::enqueue(LPC_PARTITION_SPLIT,
                     tracker(),
                     std::bind(&replica::child_catch_up_states, this),
                     get_gpid().thread_hash());
    _split_states.async_learn_task = nullptr;
}

// ThreadPool: THREAD_POOL_REPLICATION_LONG
error_code replica::child_apply_private_logs(std::vector<std::string> plog_files,
                                             std::vector<mutation_ptr> mutation_list,
                                             uint64_t total_file_size,
                                             decree last_committed_decree) // on child partition
{
    FAIL_POINT_INJECT_F("replica_child_apply_private_logs", [](dsn::string_view arg) {
        return error_code::try_get(arg.data(), ERR_OK);
    });

    if (status() != partition_status::PS_PARTITION_SPLIT) {
        dwarn_replica("wrong status={}", enum_to_string(status()));
        return ERR_INVALID_STATE;
    }

    error_code ec;
    int64_t offset;
    // temp prepare_list used for apply states
    prepare_list plist(this,
                       _app->last_committed_decree(),
                       _options->max_mutation_count_in_prepare_list,
                       [this](mutation_ptr &mu) {
                           if (mu->data.header.decree == _app->last_committed_decree() + 1) {
                               _app->apply_mutation(mu);
                           }
                       });

    // replay private log
    ec = mutation_log::replay(plog_files,
                              [this, &plist](int log_length, mutation_ptr &mu) {
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
        dwarn_replica(
            "replay private_log files failed, file count={}, app last_committed_decree={}",
            plog_files.size(),
            _app->last_committed_decree());
        return ec;
    }

    ddebug_replica("replay private_log files succeed, file count={}, app last_committed_decree={}",
                   plog_files.size(),
                   _app->last_committed_decree());

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
    plist.commit(last_committed_decree, COMMIT_TO_DECREE_HARD);
    ddebug_replica(
        "apply in-memory mutations succeed, mutation count={}, app last_committed_decree={}",
        count,
        _app->last_committed_decree());

    return ec;
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica::child_catch_up_states() // on child partition
{
    FAIL_POINT_INJECT_F("replica_child_catch_up_states", [](dsn::string_view) {});

    if (status() != partition_status::PS_PARTITION_SPLIT) {
        dwarn_replica("wrong status, status is {}", enum_to_string(status()));
        return;
    }

    // parent will copy mutations to child during async-learn, as a result:
    // - child prepare_list last_committed_decree = parent prepare_list last_committed_decree, also
    // is catch_up goal_decree
    // - local_decree is child local last_committed_decree which is the last decree in async-learn.
    decree goal_decree = _prepare_list->last_committed_decree();
    decree local_decree = _app->last_committed_decree();

    // there are mutations written to parent during async-learn
    // child does not catch up parent, there are still some mutations child not learn
    if (local_decree < goal_decree) {
        if (local_decree >= _prepare_list->min_decree()) {
            // all missing mutations are all in prepare list
            dwarn_replica("there are some in-memory mutations should be learned, app "
                          "last_committed_decree={}, "
                          "goal decree={}, prepare_list min_decree={}",
                          local_decree,
                          goal_decree,
                          _prepare_list->min_decree());
            for (decree d = local_decree + 1; d <= goal_decree; ++d) {
                auto mu = _prepare_list->get_mutation_by_decree(d);
                dassert(mu != nullptr, "");
                error_code ec = _app->apply_mutation(mu);
                if (ec != ERR_OK) {
                    child_handle_split_error("child_catchup failed because apply mutation failed");
                    return;
                }
            }
        } else {
            // some missing mutations have already in private log
            // should call `catch_up_with_private_logs` to catch up all missing mutations
            dwarn_replica(
                "there are some private logs should be learned, app last_committed_decree="
                "{}, prepare_list min_decree={}, please wait",
                local_decree,
                _prepare_list->min_decree());
            _split_states.async_learn_task = tasking::enqueue(
                LPC_CATCHUP_WITH_PRIVATE_LOGS,
                tracker(),
                [this]() {
                    catch_up_with_private_logs(partition_status::PS_PARTITION_SPLIT);
                    _split_states.async_learn_task = nullptr;
                },
                get_gpid().thread_hash());
            return;
        }
    }

    ddebug_replica("child catch up parent states, goal decree={}, local decree={}",
                   _prepare_list->last_committed_decree(),
                   _app->last_committed_decree());
    _split_states.is_caught_up = true;

    child_notify_catch_up();
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica::child_notify_catch_up() // on child partition
{
    FAIL_POINT_INJECT_F("replica_child_notify_catch_up", [](dsn::string_view) {});

    std::unique_ptr<notify_catch_up_request> request = make_unique<notify_catch_up_request>();
    request->parent_gpid = _split_states.parent_gpid;
    request->child_gpid = get_gpid();
    request->child_ballot = get_ballot();
    request->child_address = _stub->_primary_address;

    ddebug_replica("send notification to primary: {}@{}, ballot={}",
                   _split_states.parent_gpid,
                   _config.primary.to_string(),
                   get_ballot());

    notify_catch_up_rpc rpc(std::move(request), RPC_SPLIT_NOTIFY_CATCH_UP);
    rpc.call(_config.primary,
             tracker(),
             [this, rpc](error_code ec) mutable {
                 const auto &response = rpc.response();
                 if (ec == ERR_TIMEOUT) {
                     dwarn_replica("notify primary catch up timeout, please wait and retry");
                     tasking::enqueue(LPC_PARTITION_SPLIT,
                                      tracker(),
                                      std::bind(&replica::child_notify_catch_up, this),
                                      get_gpid().thread_hash(),
                                      std::chrono::seconds(1));
                     return;
                 }
                 if (ec != ERR_OK || response.err != ERR_OK) {
                     error_code err = (ec == ERR_OK) ? response.err : ec;
                     dwarn_replica("failed to notify primary catch up, error={}", err.to_string());
                     _stub->split_replica_error_handler(
                         _split_states.parent_gpid,
                         std::bind(&replica::parent_cleanup_split_context, std::placeholders::_1));
                     child_handle_split_error("notify_primary_split_catch_up");
                     return;
                 }
                 ddebug_replica("notify primary catch up succeed");
             },
             _split_states.parent_gpid.thread_hash());
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica::parent_handle_child_catch_up(const notify_catch_up_request &request,
                                           notify_cacth_up_response &response) // on primary parent
{
    if (status() != partition_status::PS_PRIMARY) {
        derror_replica("status is {}", enum_to_string(status()));
        response.err = ERR_INVALID_STATE;
        return;
    }

    if (request.child_ballot != get_ballot()) {
        derror_replica("receive out-date request, request ballot = {}, local ballot = {}",
                       request.child_ballot,
                       get_ballot());
        response.err = ERR_INVALID_STATE;
        return;
    }

    if (request.child_gpid != _child_gpid) {
        derror_replica(
            "receive wrong child request, request child_gpid = {}, local child_gpid = {}",
            request.child_gpid,
            _child_gpid);
        response.err = ERR_INVALID_STATE;
        return;
    }

    response.err = ERR_OK;
    ddebug_replica("receive catch_up request from {}@{}, current ballot={}",
                   request.child_gpid,
                   request.child_address.to_string(),
                   request.child_ballot);

    _primary_states.caught_up_children.insert(request.child_address);
    // _primary_states.statuses is a map structure: rpc address -> partition_status
    // it stores replica's rpc address and partition_status of this replica group
    for (auto &iter : _primary_states.statuses) {
        if (_primary_states.caught_up_children.find(iter.first) ==
            _primary_states.caught_up_children.end()) {
            // there are child partitions not caught up its parent
            return;
        }
    }

    ddebug_replica("all child partitions catch up");
    _primary_states.caught_up_children.clear();
    _primary_states.sync_send_write_request = true;

    // sync_point is the first decree after parent send write request to child synchronously
    // when sync_point commit, parent consider child has all data it should have during async-learn
    decree sync_point = _prepare_list->max_decree() + 1;
    if (!_options->empty_write_disabled) {
        // empty wirte here to commit sync_point
        mutation_ptr mu = new_mutation(invalid_decree);
        mu->add_client_request(RPC_REPLICATION_WRITE_EMPTY, nullptr);
        init_prepare(mu, false);
        dassert_replica(sync_point == mu->data.header.decree,
                        "sync_point should be equal to mutation's decree, {} vs {}",
                        sync_point,
                        mu->data.header.decree);
    };

    // check if sync_point has been committed
    tasking::enqueue(LPC_PARTITION_SPLIT,
                     tracker(),
                     std::bind(&replica::parent_check_sync_point_commit, this, sync_point),
                     get_gpid().thread_hash(),
                     std::chrono::seconds(1));
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica::parent_check_sync_point_commit(decree sync_point) // on primary parent
{
    FAIL_POINT_INJECT_F("replica_parent_check_sync_point_commit", [](dsn::string_view) {});
    ddebug_replica("sync_point = {}, app last_committed_decree = {}",
                   sync_point,
                   _app->last_committed_decree());
    if (_app->last_committed_decree() >= sync_point) {
        // TODO(heyuchen): TBD
        // update child replica group partition_count
    } else {
        dwarn_replica("sync_point has not been committed, please wait and retry");
        tasking::enqueue(LPC_PARTITION_SPLIT,
                         tracker(),
                         std::bind(&replica::parent_check_sync_point_commit, this, sync_point),
                         get_gpid().thread_hash(),
                         std::chrono::seconds(1));
    }
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica::register_child_on_meta(ballot b) // on primary parent
{
    if (status() != partition_status::PS_PRIMARY) {
        dwarn_replica("failed to register child, status = {}", enum_to_string(status()));
        return;
    }

    if (_primary_states.reconfiguration_task != nullptr) {
        dwarn_replica("under reconfiguration, delay and retry to register child");
        _primary_states.register_child_task =
            tasking::enqueue(LPC_PARTITION_SPLIT,
                             tracker(),
                             std::bind(&replica::register_child_on_meta, this, b),
                             get_gpid().thread_hash(),
                             std::chrono::seconds(1));
        return;
    }

    partition_configuration child_config = _primary_states.membership;
    child_config.ballot++;
    child_config.last_committed_decree = 0;
    child_config.last_drops.clear();
    child_config.pid.set_partition_index(_app_info.partition_count +
                                         get_gpid().get_partition_index());

    register_child_request request;
    request.app = _app_info;
    request.child_config = child_config;
    request.parent_config = _primary_states.membership;
    request.primary_address = _stub->_primary_address;

    // reject client request
    update_local_configuration_with_no_ballot_change(partition_status::PS_INACTIVE);
    set_inactive_state_transient(true);
    _partition_version = -1;

    parent_send_register_request(request);
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica::parent_send_register_request(
    const register_child_request &request) // on primary parent
{
    FAIL_POINT_INJECT_F("replica_parent_send_register_request", [](dsn::string_view) {});

    dcheck_eq_replica(status(), partition_status::PS_INACTIVE);
    ddebug_replica(
        "send register child({}) request to meta_server, current ballot = {}, child ballot = {}",
        request.child_config.pid,
        request.parent_config.ballot,
        request.child_config.ballot);

    rpc_address meta_address(_stub->_failure_detector->get_servers());
    std::unique_ptr<register_child_request> req = make_unique<register_child_request>(request);
    register_child_rpc rpc(std::move(req), RPC_CM_REGISTER_CHILD_REPLICA);
    _primary_states.register_child_task =
        rpc.call(meta_address,
                 tracker(),
                 [this, rpc](error_code ec) mutable {
                     on_register_child_on_meta_reply(ec, rpc.request(), rpc.response());
                 },
                 _split_states.parent_gpid.thread_hash());
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica::on_register_child_on_meta_reply(
    dsn::error_code ec,
    const register_child_request &request,
    const register_child_response &response) // on primary parent
{
    FAIL_POINT_INJECT_F("replica_on_register_child_on_meta_reply", [](dsn::string_view) {});

    _checker.only_one_thread_access();

    // primary parent is under reconfiguration, whose status should be PS_INACTIVE
    if (partition_status::PS_INACTIVE != status() || !_stub->is_connected()) {
        dwarn_replica("status wrong or stub is not connected, status = {}",
                      enum_to_string(status()));
        _primary_states.register_child_task = nullptr;
        // TODO(heyuchen): TBD - clear other split tasks in primary context
        return;
    }

    dsn::error_code err = ec == ERR_OK ? response.err : ec;
    if (err != ERR_OK) {
        dwarn_replica(
            "register child({}) failed, error = {}, request child ballot = {}, local ballot = {}",
            request.child_config.pid,
            err.to_string(),
            request.child_config.ballot,
            get_ballot());

        // register request is out-of-dated
        if (err == ERR_INVALID_VERSION) {
            return;
        }

        // we need not resend register request if child has been registered
        if (err != ERR_CHILD_REGISTERED) {
            _primary_states.register_child_task =
                tasking::enqueue(LPC_DELAY_UPDATE_CONFIG,
                                 tracker(),
                                 std::bind(&replica::parent_send_register_request, this, request),
                                 get_gpid().thread_hash(),
                                 std::chrono::seconds(1));
            return;
        }
    }

    if (err == ERR_OK) {
        ddebug_replica("register child({}) succeed, response parent ballot = {}, local ballot = "
                       "{}, local status = {}",
                       response.child_config.pid,
                       response.parent_config.ballot,
                       get_ballot(),
                       enum_to_string(status()));

        dcheck_eq_replica(_app_info.partition_count * 2, response.app.partition_count);
        _stub->split_replica_exec(LPC_PARTITION_SPLIT,
                                  response.child_config.pid,
                                  std::bind(&replica::child_partition_active,
                                            std::placeholders::_1,
                                            response.child_config));

        // TODO(heyuchen): TBD - update parent group partition_count
    }

    // parent register child succeed or child partition has already resgitered
    // in both situation, we should reset resgiter child task and child_gpid
    _primary_states.register_child_task = nullptr;
    _child_gpid.set_app_id(0);
    if (response.parent_config.ballot >= get_ballot()) {
        ddebug_replica("response ballot = {}, local ballot = {}, should update configuration",
                       response.parent_config.ballot,
                       get_ballot());
        update_configuration(response.parent_config);
    }
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica::child_partition_active(const partition_configuration &config) // on child
{
    ddebug_replica("child partition become active");
    _primary_states.last_prepare_decree_on_new_primary = _prepare_list->max_decree();
    update_configuration(config);
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica::parent_cleanup_split_context() // on parent partition
{
    _child_gpid.set_app_id(0);
    _child_init_ballot = 0;
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica::child_handle_split_error(const std::string &error_msg) // on child partition
{
    if (status() != partition_status::PS_ERROR) {
        dwarn_replica("partition split failed because {}", error_msg);
        // TODO(heyuchen):
        // convert child partition_status from PS_PARTITION_SPLIT to PS_ERROR in further pull
        // request
    }
}

// ThreadPool: THREAD_POOL_REPLICATION_LONG
void replica::child_handle_async_learn_error() // on child partition
{
    _stub->split_replica_error_handler(
        _split_states.parent_gpid,
        std::bind(&replica::parent_cleanup_split_context, std::placeholders::_1));
    child_handle_split_error("meet error when execute child_learn_states");
    _split_states.async_learn_task = nullptr;
}

} // namespace replication
} // namespace dsn
