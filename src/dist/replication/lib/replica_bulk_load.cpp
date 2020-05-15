// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "replica.h"
#include "replica_stub.h"

#include <fstream>

#include <dsn/dist/block_service.h>
#include <dsn/dist/fmt_logging.h>
#include <dsn/dist/replication/replication_app_base.h>
#include <dsn/utility/fail_point.h>
#include <dsn/utility/filesystem.h>

namespace dsn {
namespace replication {

typedef rpc_holder<group_bulk_load_request, group_bulk_load_response> group_bulk_load_rpc;

// ThreadPool: THREAD_POOL_REPLICATION
void replica::on_bulk_load(const bulk_load_request &request, /*out*/ bulk_load_response &response)
{
    _checker.only_one_thread_access();

    response.pid = request.pid;
    response.app_name = request.app_name;
    response.err = ERR_OK;

    if (status() != partition_status::PS_PRIMARY) {
        dwarn_replica("receive bulk load request with wrong status {}", enum_to_string(status()));
        response.err = ERR_INVALID_STATE;
        return;
    }

    if (request.ballot != get_ballot()) {
        dwarn_replica(
            "receive bulk load request with wrong version, remote ballot={}, local ballot={}",
            request.ballot,
            get_ballot());
        response.err = ERR_INVALID_STATE;
        return;
    }

    ddebug_replica(
        "receive bulk load request, remote provider = {}, cluster_name = {}, app_name = {}, "
        "meta_bulk_load_status = {}, local bulk_load_status = {}",
        request.remote_provider_name,
        request.cluster_name,
        request.app_name,
        enum_to_string(request.meta_bulk_load_status),
        enum_to_string(get_bulk_load_status()));

    error_code ec = do_bulk_load(request.app_name,
                                 request.meta_bulk_load_status,
                                 request.cluster_name,
                                 request.remote_provider_name);
    if (ec != ERR_OK) {
        response.err = ec;
        response.primary_bulk_load_status = get_bulk_load_status();
        return;
    }

    report_bulk_load_states_to_meta(
        request.meta_bulk_load_status, request.query_bulk_load_metadata, response);
    if (response.err != ERR_OK) {
        return;
    }

    broadcast_group_bulk_load(request);
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica::broadcast_group_bulk_load(const bulk_load_request &meta_req)
{
    if (!_primary_states.learners.empty()) {
        dwarn_replica("has learners, skip broadcast group bulk load request");
        return;
    }

    if (!_primary_states.group_bulk_load_pending_replies.empty()) {
        dwarn_replica("{} group bulk_load replies are still pending, cancel it firstly",
                      _primary_states.group_bulk_load_pending_replies.size());
        for (auto &kv : _primary_states.group_bulk_load_pending_replies) {
            CLEANUP_TASK_ALWAYS(kv.second);
        }
        _primary_states.group_bulk_load_pending_replies.clear();
    }

    ddebug_replica("start to broadcast group bulk load");

    for (const auto &addr : _primary_states.membership.secondaries) {
        if (addr == _stub->_primary_address)
            continue;

        auto request = make_unique<group_bulk_load_request>();
        request->app_name = _app_info.app_name;
        request->target_address = addr;
        _primary_states.get_replica_config(partition_status::PS_SECONDARY, request->config);
        request->cluster_name = meta_req.cluster_name;
        request->provider_name = meta_req.remote_provider_name;
        request->meta_bulk_load_status = meta_req.meta_bulk_load_status;

        ddebug_replica("send group_bulk_load_request to {}", addr.to_string());

        group_bulk_load_rpc rpc(
            std::move(request), RPC_GROUP_BULK_LOAD, 0_ms, 0, get_gpid().thread_hash());
        auto callback_task = rpc.call(addr, tracker(), [this, rpc](error_code err) mutable {
            on_group_bulk_load_reply(err, rpc.request(), rpc.response());
        });
        _primary_states.group_bulk_load_pending_replies[addr] = callback_task;
    }
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica::on_group_bulk_load(const group_bulk_load_request &request,
                                 /*out*/ group_bulk_load_response &response)
{
    _checker.only_one_thread_access();

    response.err = ERR_OK;

    if (request.config.ballot < get_ballot()) {
        response.err = ERR_VERSION_OUTDATED;
        dwarn_replica(
            "receive outdated group_bulk_load request, request ballot({}) VS local ballot({})",
            request.config.ballot,
            get_ballot());
        return;
    }
    if (request.config.ballot > get_ballot()) {
        response.err = ERR_INVALID_STATE;
        dwarn_replica("receive group_bulk_load request, local ballot is outdated, request "
                      "ballot({}) VS local ballot({})",
                      request.config.ballot,
                      get_ballot());
        return;
    }
    if (status() != request.config.status) {
        response.err = ERR_INVALID_STATE;
        dwarn_replica("status changed, status should be {}, but {}",
                      enum_to_string(request.config.status),
                      enum_to_string(status()));
        return;
    }

    ddebug_replica("receive group_bulk_load request, primary address = {}, ballot = {}, "
                   "meta bulk_load_status = {}, local bulk_load_status = {}",
                   request.config.primary.to_string(),
                   request.config.ballot,
                   enum_to_string(request.meta_bulk_load_status),
                   enum_to_string(get_bulk_load_status()));

    error_code ec = do_bulk_load(request.app_name,
                                 request.meta_bulk_load_status,
                                 request.cluster_name,
                                 request.provider_name);
    if (ec != ERR_OK) {
        response.err = ec;
        response.status = get_bulk_load_status();
        return;
    }

    report_bulk_load_states_to_primary(request.meta_bulk_load_status, response);
}

void replica::on_group_bulk_load_reply(error_code err,
                                       const group_bulk_load_request &req,
                                       const group_bulk_load_response &resp)
{
    _checker.only_one_thread_access();

    if (partition_status::PS_PRIMARY != status()) {
        derror_replica("replica status={}, should be {}",
                       enum_to_string(status()),
                       enum_to_string(partition_status::PS_PRIMARY));
        return;
    }

    _primary_states.group_bulk_load_pending_replies.erase(req.target_address);

    // TODO(heyuchen): TBD
    // if error happened, reset secondary bulk_load_state
    // otherwise, set secondary bulk_load_states from resp
}

// ThreadPool: THREAD_POOL_REPLICATION
error_code replica::do_bulk_load(const std::string &app_name,
                                 bulk_load_status::type meta_status,
                                 const std::string &cluster_name,
                                 const std::string &provider_name)
{
    // TODO(heyuchen): TBD
    return ERR_OK;
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica::report_bulk_load_states_to_meta(bulk_load_status::type remote_status,
                                              bool report_metadata,
                                              /*out*/ bulk_load_response &response)
{
    // TODO(heyuchen): TBD
}

void replica::report_bulk_load_states_to_primary(bulk_load_status::type remote_status,
                                                 /*out*/ group_bulk_load_response &response)
{
    // TODO(heyuchen): TBD
}

} // namespace replication
} // namespace dsn
