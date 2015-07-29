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
# pragma once

# include <dsn/internal/task.h>
# include <dsn/internal/network.h>
# include <dsn/internal/synchronize.h>
# include <dsn/internal/global_config.h>

namespace dsn {

class service_node;
class rpc_engine
{
public:
    rpc_engine(configuration_ptr config, service_node* node);

    //
    // management routines
    //
    ::dsn::error_code start(const service_app_spec& spec);

    //
    // rpc registrations
    //
    bool  register_rpc_handler(rpc_handler_ptr& handler);
    rpc_handler_ptr unregister_rpc_handler(dsn_task_code_t rpc_code);

    //
    // rpc routines
    //
    void call(message_ex* request, rpc_response_task* call);
    void on_recv_request(message_ex* msg, int delay_ms);
    static void reply(message_ex* response);

    //
    // information inquery
    //
    service_node* node() const { return _node; }
    const dsn_address_t& primary_address() const { return _local_primary_address; }

private:
    network* create_network(const network_server_config& netcs, bool client_only);

private:
    configuration_ptr                     _config;    
    service_node                          *_node;
    std::vector<std::vector<network*>>    _client_nets; // <format, <CHANNEL, network*>>
    std::unordered_map<int, std::vector<network*>>  _server_nets; // <port, <CHANNEL, network*>>
    dsn_address_t                             _local_primary_address;

    typedef std::unordered_map<std::string, rpc_handler_ptr> rpc_handlers;
    rpc_handlers                  _handlers;
    utils::rw_lock_nr             _handlers_lock;
    
    bool                          _is_running;

    static bool                   _message_crc_required;
};

} // end namespace

