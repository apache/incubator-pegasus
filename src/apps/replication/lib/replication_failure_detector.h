/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation, Robust Distributed System Nucleus(rDSN)

 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.

 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
#pragma once

#include "replication_common.h"
#include "failure_detector.h"


namespace dsn { namespace replication {

class replica_stub;
class replication_failure_detector  : public dsn::fd::failure_detector
{
public:
    replication_failure_detector(replica_stub* stub, std::vector<end_point>& meta_servers);
    ~replication_failure_detector(void);

    virtual void end_ping(
        ::dsn::error_code err,
        std::shared_ptr<fd::beacon_msg> beacon,
        std::shared_ptr<fd::beacon_ack> ack);

     // client side
    virtual void on_master_disconnected( const std::vector<end_point>& nodes );
    virtual void on_master_connected( const end_point& node);

    // server side
    virtual void on_worker_disconnected( const std::vector<end_point>& nodes ) { dassert (false, ""); }
    virtual void on_worker_connected( const end_point& node )  { dassert (false, ""); }

    end_point current_server_contact() const { zauto_lock l(_meta_lock); return _current_meta_server; }
    std::vector<end_point> get_servers() const  { zauto_lock l(_meta_lock); return _meta_servers; }

private:
    end_point find_next_meta_server(end_point current);

private:
    typedef std::set<end_point, end_point_comparor> end_points;

    mutable zlock             _meta_lock;
    end_point               _current_meta_server;

    std::vector<end_point>  _meta_servers;
    replica_stub               *_stub;
};

}} // end namespace

