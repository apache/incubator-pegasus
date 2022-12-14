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
 *     interface for a perfect failure detector
 *
 * Revision history:
 *     Mar., 2015, @imzhenyu (Zhenyu Guo), first version
 *     Dec., 2015, @shengofsun (Weijie Sun), make zlock preoteced,
 *                 give the subClasses flexibility
 *     xxxx-xx-xx, author, fix bug about xxx
 */

/*
 * Notes on the failure detector:
 *
 * 1. Due to the fact that we can only check the liveness inside check-all-records call,
 *    which happens every "check_interval_seconds" seconds, worker may disconnect from master
 *    in the period earlier than the lease_seconds to ensure the perfect FD.
 *    In the worst case, workers may disconnect themselves
 *    after "lease"-"check_interval_seconds" seconds;
 *
 *    Similarily, master may claim a worker dead more slowly even the workers are dead
 *    for longer than grace_seconds. In the worst case, it will be
 *    "grace"+"check_interval_seconds" seconds.
 *
 * 2. In practice, your should set check_interval_seconds a small value for a fine-grained FD.
 *    For client, you may set it as 2 second as it usually connect to a small number of masters.
 *    For master, you may set it as 5 or 10 seconds.
 *
 * 3. We should always use dedicated thread pools for THREAD_POOL_FD,
 *    and set thread priority to being highest so as to minimize the performance
 *    interference with other workloads.
 *
 * 4. The lease_periods must be less than the grace_periods, as required by prefect FD.
 *
 */
#pragma once

#include "failure_detector/fd.client.h"
#include "failure_detector/fd.server.h"
#include "perf_counter/perf_counter_wrapper.h"
#include "utils/zlocks.h"

namespace dsn {
namespace fd {

DEFINE_THREAD_POOL_CODE(THREAD_POOL_FD)
DEFINE_TASK_CODE(LPC_BEACON_CHECK, TASK_PRIORITY_HIGH, THREAD_POOL_FD)
DEFINE_TASK_CODE(LPC_BEACON_SEND, TASK_PRIORITY_HIGH, THREAD_POOL_FD)

class failure_detector_callback
{
public:
    virtual ~failure_detector_callback() {}

    // worker side
    virtual void on_master_disconnected(const std::vector<::dsn::rpc_address> &nodes) = 0;
    virtual void on_master_connected(::dsn::rpc_address node) = 0;

    // master side
    virtual void on_worker_disconnected(const std::vector<::dsn::rpc_address> &nodes) = 0;
    virtual void on_worker_connected(::dsn::rpc_address node) = 0;
};

class failure_detector : public failure_detector_service,
                         public failure_detector_client,
                         public failure_detector_callback
{
public:
    failure_detector();
    virtual ~failure_detector();

    virtual void on_ping(const beacon_msg &beacon, ::dsn::rpc_replier<beacon_ack> &reply);

    virtual void end_ping(::dsn::error_code err, const beacon_ack &ack, void *context);

    virtual void register_ctrl_commands();

public:
    error_code start(uint32_t check_interval_seconds,
                     uint32_t beacon_interval_seconds,
                     uint32_t lease_seconds,
                     uint32_t grace_seconds,
                     bool use_allow_list = false);

    // TODO(yingchun): can it be removed ?
    void stop();

    uint32_t get_lease_ms() const { return _lease_milliseconds; }
    uint32_t get_grace_ms() const { return _grace_milliseconds; }

    void register_master(::dsn::rpc_address target);

    bool switch_master(::dsn::rpc_address from, ::dsn::rpc_address to, uint32_t delay_milliseconds);

    bool unregister_master(::dsn::rpc_address node);

    virtual bool is_master_connected(::dsn::rpc_address node) const;

    // ATTENTION: be very careful to set is_connected to false as
    // workers are always considered *connected* initially which is ok even when workers think
    // master is disconnected
    // Considering workers *disconnected* initially is *dangerous* coz it may violate the invariance
    // when workers think they are online
    void register_worker(::dsn::rpc_address node, bool is_connected = true);

    bool unregister_worker(::dsn::rpc_address node);

    void clear_workers();

    virtual bool is_worker_connected(::dsn::rpc_address node) const;

    void add_allow_list(::dsn::rpc_address node);

    bool remove_from_allow_list(::dsn::rpc_address node);

    void set_allow_list(const std::vector<std::string> &replica_addrs);

    std::string get_allow_list(const std::vector<std::string> &args) const;

    int worker_count() const { return static_cast<int>(_workers.size()); }

    int master_count() const { return static_cast<int>(_masters.size()); }

protected:
    void on_ping_internal(const beacon_msg &beacon, /*out*/ beacon_ack &ack);

    // return false when the ack is not applicable
    bool end_ping_internal(::dsn::error_code err, const beacon_ack &ack);

    bool is_time_greater_than(uint64_t ts, uint64_t base);

    void report(::dsn::rpc_address node, bool is_master, bool is_connected);

private:
    void check_all_records();

private:
    class master_record
    {
    public:
        ::dsn::rpc_address node;
        uint64_t last_send_time_for_beacon_with_ack;
        bool is_alive;
        bool rejected;
        task_ptr send_beacon_timer;

        // masters are always considered *disconnected* initially which is ok even when master
        // thinks workers are connected
        master_record(::dsn::rpc_address n, uint64_t last_send_time_for_beacon_with_ack_)
        {
            node = n;
            last_send_time_for_beacon_with_ack = last_send_time_for_beacon_with_ack_;
            is_alive = false;
            rejected = false;
        }
    };

    class worker_record
    {
    public:
        ::dsn::rpc_address node;
        uint64_t last_beacon_recv_time;
        bool is_alive;

        // workers are always considered *connected* initially which is ok even when workers think
        // master is disconnected
        worker_record(::dsn::rpc_address node, uint64_t last_beacon_recv_time)
        {
            this->node = node;
            this->last_beacon_recv_time = last_beacon_recv_time;
            is_alive = true;
        }
    };

private:
    typedef std::unordered_map<::dsn::rpc_address, master_record> master_map;
    typedef std::unordered_map<::dsn::rpc_address, worker_record> worker_map;

    // allow list are set on machine name (port can vary)
    typedef std::unordered_set<::dsn::rpc_address> allow_list;

    master_map _masters;
    worker_map _workers;

    uint32_t _check_interval_milliseconds;
    uint32_t _beacon_interval_milliseconds;
    uint32_t _beacon_timeout_milliseconds;
    uint32_t _lease_milliseconds;
    uint32_t _grace_milliseconds;
    bool _is_started;
    ::dsn::task_ptr _check_task;

    bool _use_allow_list;
    allow_list _allow_list;

    perf_counter_wrapper _recent_beacon_fail_count;

    std::unique_ptr<command_deregister> _get_allow_list;

protected:
    mutable zlock _lock;
    dsn::task_tracker _tracker;

    // subClass can rewrite these method.
    virtual void send_beacon(::dsn::rpc_address node, uint64_t time);
};
}
} // end namespace
