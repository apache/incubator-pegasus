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

#include "replication_common.h"
# include <dsn/dist/failure_detector.h>

namespace dsn { namespace replication {

class replica_stub;
class replication_failure_detector  : public dsn::fd::failure_detector
{
public:
    replication_failure_detector(replica_stub* stub, std::vector<::dsn::rpc_address>& meta_servers);
    ~replication_failure_detector(void);

    virtual void end_ping(::dsn::error_code err, const fd::beacon_ack& ack, void* context);

     // client side
    virtual void on_master_disconnected( const std::vector<::dsn::rpc_address>& nodes );
    virtual void on_master_connected( ::dsn::rpc_address node);

    // server side
    virtual void on_worker_disconnected( const std::vector<::dsn::rpc_address>& nodes ) { dassert (false, ""); }
    virtual void on_worker_connected( ::dsn::rpc_address node )  { dassert (false, ""); }

    ::dsn::rpc_address current_server_contact() const { zauto_lock l(_meta_lock); return dsn_group_get_leader(_meta_servers.group_handle()); }
    ::dsn::rpc_address get_servers() const  { return _meta_servers; }

private:
    mutable zlock            _meta_lock;
    dsn::rpc_address         _meta_servers;
    replica_stub             *_stub;
};

}} // end namespace

