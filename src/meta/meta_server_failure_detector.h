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

#pragma once

#include <stdint.h>
#include <atomic>
#include <map>
#include <string>
#include <vector>

#include "failure_detector/failure_detector.h"
#include "runtime/api_layer1.h"
#include "runtime/rpc/rpc_address.h"
#include "runtime/task/task.h"
#include "utils/fmt_logging.h"
#include "utils/zlocks.h"

namespace dsn {
namespace dist {
class distributed_lock_service;
} // namespace dist
namespace fd {
class beacon_ack;
class beacon_msg;
} // namespace fd
template <typename TResponse>
class rpc_replier;

namespace replication {

class fd_suboptions;
class meta_service;

namespace test {
class test_checker;
}
class meta_server_failure_detector : public fd::failure_detector
{
public:
    struct worker_stability
    {
        int64_t last_start_time_ms;
        int unstable_restart_count;
        worker_stability(int64_t lst, int urc)
            : last_start_time_ms(lst), unstable_restart_count(urc)
        {
        }
    };
    typedef std::map<dsn::rpc_address, worker_stability> stability_map;

public:
    meta_server_failure_detector(meta_service *svc);
    virtual ~meta_server_failure_detector();

    // get the meta-server's leader
    // leader: the leader's address. Invalid if no leader selected
    //         if leader==nullptr, then the new leader won't be returned
    // ret true if i'm the current leader; false if not.
    bool get_leader(/*output*/ dsn::rpc_address *leader);

    // return if acquire the leader lock, or-else blocked forever
    void acquire_leader_lock();

    void reset_stability_stat(const dsn::rpc_address &node);

    // _fd_opts is initialized in constructor with a fd_suboption stored in meta_service.
    // so usually you don't need to call this.
    // the function is mainly for a test module, in which the fd object is created without the
    // "meta_service", please make sure that options's lifetime is longer than the fd object
    void set_options(fd_suboptions *options) { _fd_opts = options; }

    // client side
    virtual void on_master_disconnected(const std::vector<rpc_address> &)
    {
        CHECK(false, "unsupported method");
    }
    virtual void on_master_connected(rpc_address) { CHECK(false, "unsupported method"); }

    // server side
    // it is in the protection of failure_detector::_lock
    virtual void on_worker_disconnected(const std::vector<rpc_address> &nodes) override;
    // it is in the protection of failure_detector::_lock
    virtual void on_worker_connected(rpc_address node) override;
    virtual bool is_worker_connected(rpc_address node) const override
    {
        // we treat all nodes not in the worker list alive in the first grace period.
        // For the reason, please consider this situation:
        // 1. a RS connected to a meta M1
        // 2. M1 crashed, then M2 selected as new leader, before the first beacon of RS sent
        //    to M2, RS is not in the worker_map of M2.
        // 3. If M2 claims RS is not alive, then the perfect-FD's constraint will be broken.
        //    Coz RS will find itself dead after the leader-periods.
        if (_election_moment.load() + get_grace_ms() < dsn_now_ms()) {
            return true;
        }
        return failure_detector::is_worker_connected(node);
    }
    virtual void on_ping(const fd::beacon_msg &beacon, rpc_replier<fd::beacon_ack> &reply) override;

private:
    // return value: return true if beacon.from_addr is stable; or-else, false
    bool update_stability_stat(const fd::beacon_msg &beacon);
    void leader_initialize(const std::string &lock_service_owner);

private:
    // meta_service need to visit the failure_detector's lock
    friend class meta_service;

    friend class test::test_checker;

    // initialize in the constructor
    meta_service *_svc;
    dist::distributed_lock_service *_lock_svc;
    std::string _primary_lock_id;
    const fd_suboptions *_fd_opts;

    // initialize in acquire_leader_lock
    task_ptr _lock_grant_task;
    task_ptr _lock_expire_task;
    std::atomic_bool _is_leader;
    std::atomic<uint64_t> _election_moment;

    // record the start time of a replica-server, check if it crashed frequently
    mutable zlock _map_lock;
    stability_map _stablity;

public:
    /* these two functions are for test */
    meta_server_failure_detector(rpc_address leader_address, bool is_myself_leader);
    void set_leader_for_test(rpc_address leader_address, bool is_myself_leader);
    stability_map *get_stability_map_for_test();
};
}
}
