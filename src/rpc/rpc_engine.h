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

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "network.h"
#include "rpc/rpc_address.h"
#include "rpc/rpc_host_port.h"
#include "rpc/rpc_message.h"
#include "runtime/api_task.h"
#include "task/task.h"
#include "task/task_code.h"
#include "task/task_spec.h"
#include "utils/autoref_ptr.h"
#include "utils/error_code.h"
#include "utils/fmt_logging.h"
#include "utils/synchronize.h"

namespace dsn {

class rpc_engine;
class service_node;

#define MAX_CLIENT_PORT 1023
struct network_server_config;
struct service_app_spec;

//
// client matcher for matching RPC request and RPC response, and handling timeout
// (1) the whole network may share a single client matcher,
// (2) or we usually prefere each <src, dst> pair use a client matcher to have better inquery
// performance
// (3) or we have certain cases we want RPC responses from node which is not the initial target node
//     the RPC request message is sent to. In this case, a shared rpc_engine level matcher is used.
//
// WE NOW USE option (3) so as to enable more features and the performance should not be degraded
// (due to less std::shared_ptr<rpc_client_matcher> operations in rpc_timeout_task)
//
#define MATCHER_BUCKET_NR 13
class rpc_client_matcher : public ref_counter
{
public:
    rpc_client_matcher(rpc_engine *engine) : _engine(engine) {}

    ~rpc_client_matcher();

    //
    // when a two-way RPC call is made, register the requst id and the callback
    // which also registers a timer for timeout tracking
    //
    void on_call(message_ex *request, const rpc_response_task_ptr &call);

    //
    // when a RPC response is received, call this function to trigger calback
    //  key - message.header.id
    //  reply - rpc response message
    //  delay_ms - sometimes we want to delay the delivery of the message for certain purposes
    //
    // we may receive an empty reply to early terminate the rpc
    //
    bool on_recv_reply(network *net, uint64_t key, message_ex *reply, int delay_ms);

private:
    friend class rpc_timeout_task;
    void on_rpc_timeout(uint64_t key);

private:
    rpc_engine *_engine;
    struct match_entry
    {
        rpc_response_task_ptr resp_task;
        task_ptr timeout_task;
        uint64_t timeout_ts_ms; // > 0 for auto-resent msgs
    };
    typedef std::unordered_map<uint64_t, match_entry> rpc_requests;
    rpc_requests _requests[MATCHER_BUCKET_NR];
    ::dsn::utils::ex_lock_nr_spin _requests_lock[MATCHER_BUCKET_NR];
};

class rpc_server_dispatcher
{
public:
    rpc_server_dispatcher();
    ~rpc_server_dispatcher();

    bool register_rpc_handler(task_code code, const char *extra_name, const rpc_request_handler &h);
    bool unregister_rpc_handler(task_code rpc_code);
    rpc_request_task *on_request(message_ex *msg, service_node *node);
    int handler_count() const
    {
        utils::auto_read_lock l(_handlers_lock);
        return static_cast<int>(_handlers.size());
    }

private:
    struct handler_entry
    {
        task_code code;
        std::string extra_name;
        rpc_request_handler h;
    };

    mutable utils::rw_lock_nr _handlers_lock;
    // there are 2 pairs for each rpc handler: code_name->hander_entry*, extra_name->hander_entry*
    // the hander_entry pointers are the same for these 2 pairs, and the pointer is owned by
    // _vhandlers[code_index]->first
    //
    // we support an extra name for compatibility to
    // rpc client of other framework like thrift or grpc
    std::unordered_map<std::string, handler_entry *> _handlers;

    // there is one entry for each rpc code
    std::vector<std::pair<std::unique_ptr<handler_entry>, utils::rw_lock_nr> *> _vhandlers;
};

class rpc_engine
{
public:
    explicit rpc_engine(service_node *node);

    //
    // management routines
    //
    ::dsn::error_code start(const service_app_spec &spec);
    void start_serving() { _is_serving = true; }
    void stop_serving() { _is_serving = false; }

    //
    // rpc registrations
    //
    bool
    register_rpc_handler(dsn::task_code code, const char *extra_name, const rpc_request_handler &h);
    bool unregister_rpc_handler(dsn::task_code rpc_code);

    //
    // rpc routines
    //
    void call(message_ex *request, const rpc_response_task_ptr &call);
    void on_recv_request(network *net, message_ex *msg, int delay_ms);
    void reply(message_ex *response, error_code err = ERR_OK);
    void forward(message_ex *request, rpc_address address);

    //
    // information inquery
    //
    service_node *node() const { return _node; }
    ::dsn::rpc_address primary_address() const { return _local_primary_address; }
    host_port primary_host_port() const { return _local_primary_host_port; }
    rpc_client_matcher *matcher() { return &_rpc_matcher; }

    // call with group address only
    void call_group(rpc_address addr, message_ex *request, const rpc_response_task_ptr &call);

    // call with ip address only
    void call_ip(rpc_address addr,
                 message_ex *request,
                 const rpc_response_task_ptr &call,
                 bool reset_request_id = false,
                 bool set_forwarded = false);

    // call with explicit address
    void call_address(rpc_address addr, message_ex *request, const rpc_response_task_ptr &call);

private:
    network *create_network(const network_server_config &netcs,
                            bool client_only,
                            network_header_format client_hdr_format);

private:
    service_node *_node;
    std::vector<std::vector<std::unique_ptr<network>>>
        _client_nets; // <format, <CHANNEL, network*>>
    std::unordered_map<int, std::vector<std::unique_ptr<network>>>
        _server_nets; // <port, <CHANNEL, network*>>
    ::dsn::rpc_address _local_primary_address;
    host_port _local_primary_host_port;
    rpc_client_matcher _rpc_matcher;
    rpc_server_dispatcher _rpc_dispatcher;

    volatile bool _is_running;
    volatile bool _is_serving;
};

// ------------------------ inline implementations --------------------

inline void
rpc_engine::call_address(rpc_address addr, message_ex *request, const rpc_response_task_ptr &call)
{
    switch (addr.type()) {
    case HOST_TYPE_IPV4:
        call_ip(addr, request, call);
        break;
    case HOST_TYPE_GROUP:
        call_group(addr, request, call);
        break;
    default:
        CHECK(false, "invalid target address type {}", request->server_address.type());
        break;
    }
}

} // namespace dsn
