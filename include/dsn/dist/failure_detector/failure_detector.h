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
 *     Dec., 2015, @shengofsun (Weijie Sun), make service::zlock preoteced, give the subClasses flexibility
 *     xxxx-xx-xx, author, fix bug about xxx
 */

# pragma once

# include <dsn/dist/failure_detector/fd.client.h>
# include <dsn/dist/failure_detector/fd.server.h>

namespace dsn { namespace fd {

DEFINE_THREAD_POOL_CODE(THREAD_POOL_FD)
DEFINE_TASK_CODE(LPC_BEACON_CHECK, TASK_PRIORITY_HIGH, THREAD_POOL_FD)

class failure_detector_callback
{
public:
    virtual ~failure_detector_callback() {}

    // client side
    virtual void on_master_disconnected( const std::vector< ::dsn::rpc_address>& nodes ) = 0;
    virtual void on_master_connected( ::dsn::rpc_address node) = 0;

    // server side
    virtual void on_worker_disconnected( const std::vector< ::dsn::rpc_address>& nodes ) = 0;
    virtual void on_worker_connected( ::dsn::rpc_address node ) = 0;
};

class failure_detector : 
    public failure_detector_service,
    public failure_detector_client, 
    public failure_detector_callback
{
public:
    failure_detector();
    virtual ~failure_detector() {}

    virtual void on_ping(const beacon_msg& beacon, ::dsn::rpc_replier<beacon_ack>& reply);

    virtual void end_ping(::dsn::error_code err, const beacon_ack& ack, void* context);

public:
    error_code start(
        uint32_t check_interval_seconds,
        uint32_t beacon_interval_seconds,
        uint32_t lease_seconds,
        uint32_t grace_seconds,
        bool use_allow_list = false
        );

    error_code stop();

    void register_master(::dsn::rpc_address target);

    bool switch_master(::dsn::rpc_address from, ::dsn::rpc_address to);

    bool unregister_master( ::dsn::rpc_address node);

    bool is_master_connected( ::dsn::rpc_address node) const;

    // ATTENTION: be very careful to set is_connected to false as
    // workers are always considered *connected* initially which is ok even when workers think master is disconnected
    // Considering workers *disconnected* initially is *dangerous* coz it may violate the invariance when workers think they are online 
    void register_worker( ::dsn::rpc_address node, bool is_connected = true);

    bool unregister_worker( ::dsn::rpc_address node);

    void clear_workers();

    bool is_worker_connected( ::dsn::rpc_address node) const;

    void add_allow_list( ::dsn::rpc_address node);

    bool remove_from_allow_list( ::dsn::rpc_address node);

    int  worker_count() const { return static_cast<int>(_workers.size()); }

    int  master_count() const { return static_cast<int>(_masters.size()); }
    
protected:
    void on_ping_internal(const beacon_msg& beacon, /*out*/ beacon_ack& ack);

    // return false when the ack is not applicable
    bool end_ping_internal(::dsn::error_code err, const beacon_ack& ack);

    bool is_time_greater_than(uint64_t ts, uint64_t base); 

    void report(::dsn::rpc_address node, bool is_master, bool is_connected);

private:
    void process_all_records();

private:
    class master_record
    {
    public:
        ::dsn::rpc_address       node;
        uint64_t        last_send_time_for_beacon_with_ack;
        uint64_t        next_beacon_time;
        bool            is_alive;
        bool            rejected;

        // masters are always considered *disconnected* initially which is ok even when master thinks workers are connected
        master_record(::dsn::rpc_address n, uint64_t last_send_time_for_beacon_with_ack_, uint64_t next_beacon_time_)
        {
            node = n;
            last_send_time_for_beacon_with_ack = last_send_time_for_beacon_with_ack_;
            next_beacon_time = next_beacon_time_;
            is_alive = false;
            rejected = false;
        }
    };

    class worker_record
    {
    public:
        ::dsn::rpc_address       node;
        uint64_t        last_beacon_recv_time;
        bool            is_alive;

        // workers are always considered *connected* initially which is ok even when workers think master is disconnected
        worker_record(::dsn::rpc_address node, uint64_t last_beacon_recv_time)
        {
            this->node = node;
            this->last_beacon_recv_time = last_beacon_recv_time;
            is_alive = true;
        }
    };

private:    
    typedef std::unordered_map< ::dsn::rpc_address, master_record>    master_map;
    typedef std::unordered_map< ::dsn::rpc_address, worker_record>    worker_map;

    // allow list are set on machine name (port can vary)
    typedef std::unordered_set< ::dsn::rpc_address>   allow_list;

    master_map            _masters;
    worker_map            _workers;

    uint32_t             _beacon_interval_milliseconds;
    uint32_t             _check_interval_milliseconds;
    uint32_t             _lease_milliseconds;
    uint32_t             _grace_milliseconds;
    bool                 _is_started;
    ::dsn::task_ptr      _current_task;

    bool                 _use_allow_list;
    allow_list           _allow_list;

protected:
    mutable service::zlock _lock;
    // subClass can rewrite these method.
    virtual void send_beacon(::dsn::rpc_address node, uint64_t time);


    // subClass can not rewrite these method.
    virtual void end_ping2(::dsn::error_code err,
                           std::shared_ptr< ::dsn::fd::beacon_msg>& beacon,
                           std::shared_ptr< ::dsn::fd::beacon_ack>& resp) override;
};

}} // end namespace
