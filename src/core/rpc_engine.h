/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation

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
# pragma once

# include <dsn/internal/task.h>
# include <dsn/internal/network.h>
# include <dsn/internal/synchronize.h>
# include <dsn/internal/global_config.h>

namespace dsn {

class rpc_client_matcher : public std::enable_shared_from_this<rpc_client_matcher>
{
public:
    void on_call(message_ptr& request, rpc_response_task_ptr& call, rpc_client_session_ptr& client);
    bool on_recv_reply(uint64_t key, message_ptr& reply, int delay_handling_milliseconds = 0);
    
private:
    friend class rpc_timeout_task;
    void on_rpc_timeout(uint64_t key, task_spec* spec);
    int32_t get_timeout_ms(int32_t timeout_ms, task_spec* spec) const;

private:
    struct match_entry
    {
        rpc_response_task_ptr resp_task;
        task_ptr              timeout_task;
        rpc_client_session_ptr client;
    };
    typedef std::map<uint64_t, match_entry> rpc_requests;
    rpc_requests         _requests;
    std::recursive_mutex _requests_lock;
};

class service_node;
class rpc_engine
{
public:
    rpc_engine(configuration_ptr config, service_node* node);

    //
    // management routines
    //
    error_code start(const service_spec& spec, int port = 0);

    //
    // rpc registrations
    //
    bool register_rpc_handler(rpc_handler_ptr& handler);
    bool unregister_rpc_handler(task_code rpc_code) ;    

    //
    // rpc routines
    //
    void call(message_ptr& request, rpc_response_task_ptr& call);
    static void reply(message_ptr& response);
    
    //
    // information inquery
    //
    const end_point& address() const { return _address; }
    service_node* node() const { return _node; }

private:
    friend class rpc_server_session;    
    void on_recv_request(message_ptr& msg, int delay_handling_milliseconds = 0);
            
private:
    configuration_ptr                     _config;    
    service_node                          *_node;
    std::vector<std::vector<network*>>    _networks; // std::vector<CHANNEL:std::vector<PORT>>
    std::shared_ptr<rpc_client_matcher>   _matcher;


    typedef std::map<std::string, rpc_handler_ptr> rpc_handlers;
    rpc_handlers                  _handlers;
    utils::rw_lock                _handlers_lock;
    
    bool                          _is_running;
    end_point                     _address;

    static bool                   _message_crc_required;
    static int                    _max_udp_package_size;    
};

} // end namespace

