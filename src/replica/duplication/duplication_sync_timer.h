// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <atomic>

#include "replica/replica_stub.h"

#include <dsn/dist/replication/duplication_common.h>
#include <dsn/utility/chrono_literals.h>

namespace dsn {
namespace replication {

using namespace literals::chrono_literals;

constexpr int DUPLICATION_SYNC_PERIOD_SECOND = 10;

// Per-server(replica_stub)-instance.
class duplication_sync_timer
{
public:
    explicit duplication_sync_timer(replica_stub *stub);

    ~duplication_sync_timer();

    void start();

    void close();

    struct replica_dup_state
    {
        gpid id;
        bool duplicating{false};
        decree not_duplicated{0};
        decree not_confirmed{0};
        duplication_fail_mode::type fail_mode{duplication_fail_mode::FAIL_SLOW};
    };
    std::multimap<dupid_t, replica_dup_state> get_dup_states(int app_id, /*out*/ bool *app_found);

private:
    // replica server periodically uploads current confirm points to meta server by sending
    // `duplication_sync_request`.
    // if success, meta server will respond with `duplication_sync_response`, which contains
    // the entire set of duplications on this server.
    void run();

    /// \param dup_map: <appid -> list<dup_entry>>
    void
    update_duplication_map(const std::map<app_id, std::map<dupid_t, duplication_entry>> &dup_map);

    void on_duplication_sync_reply(error_code err, const duplication_sync_response &resp);

    std::vector<replica_ptr> get_all_primaries();

    std::vector<replica_ptr> get_all_replicas();

private:
    friend class duplication_sync_timer_test;

    replica_stub *_stub{nullptr};

    task_ptr _timer_task;
    task_ptr _rpc_task;
    mutable zlock _lock; // protect _rpc_task
};

} // namespace replication
} // namespace dsn
