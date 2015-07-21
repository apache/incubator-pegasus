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
#include "replication_failure_detector.h"
#include "replica_stub.h"

namespace dsn { namespace replication {


replication_failure_detector::replication_failure_detector(replica_stub* stub, std::vector<dsn_address_t>& meta_servers)
{
    _stub = stub;
    _meta_servers = meta_servers;
    _current_meta_server = _meta_servers[random32(0, 100) % _meta_servers.size()];
}

replication_failure_detector::~replication_failure_detector(void)
{

}

dsn_address_t replication_failure_detector::find_next_meta_server(dsn_address_t current)
{
    if (dsn_endpoint_invalid == current)
        return _meta_servers[random32(0, 100) % _meta_servers.size()];
    else
    {
        auto it = std::find(_meta_servers.begin(), _meta_servers.end(), current);
        dassert (it != _meta_servers.end(), "");
        it++;
        if (it != _meta_servers.end())
            return *it;
        else
            return _meta_servers.at(0);
    }
}

void replication_failure_detector::end_ping(::dsn::error_code err, const fd::beacon_ack& ack, void* context)
{
    failure_detector::end_ping(err, ack, context);

    zauto_lock l(_meta_lock);
    
    if (ack.this_node == _current_meta_server)
    {
        if (err != ERR_OK)
        {
            dsn_address_t node = find_next_meta_server(ack.this_node);
            if (ack.this_node != node)
            {
                switch_master(ack.this_node, node);
            }
        }
        else if (ack.is_master == false)
        {
            if (dsn_endpoint_invalid != ack.primary_node)
            {
                switch_master(ack.this_node, ack.primary_node);
            }
        }
    }

    else
    {
        if (err != ERR_OK)
        {
            // nothing to do
        }
        else if (ack.is_master == false)
        {
            if (dsn_endpoint_invalid != ack.primary_node)
            {
                switch_master(ack.this_node, ack.primary_node);
            }
        }
        else 
        {
            _current_meta_server = ack.this_node;
        }
    }
}

// client side
void replication_failure_detector::on_master_disconnected( const std::vector<dsn_address_t>& nodes )
{
    bool primaryDisconnected = false;

    {
    zauto_lock l(_meta_lock);
    for (auto it = nodes.begin(); it != nodes.end(); it++)
    {
        if (_current_meta_server == *it)
            primaryDisconnected = true;
    }
    }

    if (primaryDisconnected)
    {
        _stub->on_meta_server_disconnected();
    }
}

void replication_failure_detector::on_master_connected( const dsn_address_t& node)
{
    bool is_primary = false;

    {
    zauto_lock l(_meta_lock);
    is_primary = (node == _current_meta_server);
    }

    if (is_primary)
    {
        _stub->on_meta_server_connected();
    }
}

}} // end namespace

