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

#pragma once

# include <dsn/dist/failure_detector.h>
# include <dsn/dist/distributed_lock_service.h>
# include "replication_common.h"

using namespace dsn;
using namespace dsn::service;
using namespace dsn::replication;
using namespace dsn::fd;

class server_state;
class meta_service;

namespace dsn {
    namespace replication{
        class replication_checker;
        namespace test {
            class test_checker;
        }
    }
}

class meta_server_failure_detector : public failure_detector
{
public:
    meta_server_failure_detector(server_state* state, meta_service* svc);

    /* these two functions are for test */
    meta_server_failure_detector(rpc_address leader_address, bool is_myself_leader);
    void set_leader_for_test(rpc_address leader_address, bool is_myself_leader);

    virtual ~meta_server_failure_detector();

    bool is_primary() const { return _is_primary; }

    rpc_address get_primary()
    {
        dsn::utils::auto_lock<zlock> l(_primary_address_lock);
        return _primary_address;
    }
    
    void acquire_leader_lock();
    
    // client side
    virtual void on_master_disconnected(const std::vector< ::dsn::rpc_address>& nodes)
    {
        dassert (false, "unsupported method");
    }

    virtual void on_master_connected(::dsn::rpc_address node)
    {
        dassert (false, "unsupported method");
    }

    // server side
    virtual void on_worker_disconnected(const std::vector< ::dsn::rpc_address>& nodes);
    virtual void on_worker_connected(::dsn::rpc_address node);

    virtual void on_ping(const fd::beacon_msg& beacon, ::dsn::rpc_replier<fd::beacon_ack>& reply);

private:
    void set_primary(rpc_address primary);
    void query_leader_callback();

private:
    friend class ::dsn::replication::replication_checker;
    friend class ::dsn::replication::test::test_checker;

    volatile bool _is_primary;

    zlock         _primary_address_lock;
    rpc_address   _primary_address;

    server_state  *_state;
    meta_service  *_svc;

    ::dsn::dist::distributed_lock_service *_lock_svc;
    task_ptr    _lock_grant_task;
    task_ptr    _lock_expire_task;
    std::string _primary_lock_id;
    std::string _local_owner_id;
};

