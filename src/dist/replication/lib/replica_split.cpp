// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <dsn/dist/fmt_logging.h>
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
void replica::init_child_replica(gpid parent_gpid,
                                 rpc_address primary_address,
                                 ballot init_ballot) // on child partition
{
    FAIL_POINT_INJECT_F("replica_init_child_replica",
                        [](dsn::string_view) { ddebug_f("mock init_child_replica succeed"); });

    if (status() != partition_status::PS_INACTIVE) {
        dwarn_replica("wrong status {}", enum_to_string(status()));
        _stub->split_replica_error_handler(parent_gpid,
                                           [](replica_ptr r) { r->_child_gpid.set_app_id(0); });
        return;
    }

    // update replica config
    _config.ballot = init_ballot;
    _config.primary = primary_address;
    _config.status = partition_status::PS_PARTITION_SPLIT;

    // init split states
    _split_states.parent_gpid = parent_gpid;

    ddebug_replica("init ballot is {}, parent gpid is ({})", init_ballot, parent_gpid);
}

void replica::clean_up_parent_split_context()
{
    _child_gpid.set_app_id(0);
    _child_init_ballot = 0;
}

} // namespace replication
} // namespace dsn
