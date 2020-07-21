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

#include <sys/socket.h>
#include <netdb.h>
#include <ifaddrs.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "rpc_engine.h"
#include "runtime/service_engine.h"
#include <dsn/utility/factory_store.h>
#include <dsn/tool-api/group_address.h>
#include <dsn/tool-api/task_queue.h>
#include <dsn/tool-api/async_calls.h>
#include <dsn/cpp/serialization.h>
#include <dsn/utility/rand.h>
#include <set>

namespace dsn {

DEFINE_TASK_CODE(LPC_RPC_TIMEOUT, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)

class rpc_timeout_task : public task
{
public:
    rpc_timeout_task(rpc_client_matcher *matcher, uint64_t id, service_node *node)
        : task(LPC_RPC_TIMEOUT, 0, node)
    {
        _matcher = matcher;
        _id = id;
    }

    virtual void exec() { _matcher->on_rpc_timeout(_id); }

private:
    // use the following if the matcher is per rpc session
    // rpc_client_matcher_ptr _matcher;

    rpc_client_matcher *_matcher;
    uint64_t _id;
};

rpc_client_matcher::~rpc_client_matcher()
{
    for (int i = 0; i < MATCHER_BUCKET_NR; i++) {
        dassert(_requests[i].size() == 0,
                "all rpc entries must be removed before the matcher ends");
    }
}

bool rpc_client_matcher::on_recv_reply(network *net, uint64_t key, message_ex *reply, int delay_ms)
{
    rpc_response_task_ptr call;
    task_ptr timeout_task;
    int bucket_index = key % MATCHER_BUCKET_NR;

    {
        utils::auto_lock<::dsn::utils::ex_lock_nr_spin> l(_requests_lock[bucket_index]);
        auto it = _requests[bucket_index].find(key);
        if (it != _requests[bucket_index].end()) {
            call = std::move(it->second.resp_task);
            timeout_task = std::move(it->second.timeout_task);
            _requests[bucket_index].erase(it);
        } else {
            if (reply) {
                dassert(reply->get_count() == 0,
                        "reply should not be referenced by anybody so far");
                delete reply;
            }
            return false;
        }
    }

    dbg_dassert(call != nullptr, "rpc response task cannot be empty");
    dbg_dassert(timeout_task != nullptr, "rpc timeout task cannot be empty");

    if (timeout_task != task::get_current_task()) {
        timeout_task->cancel(false); // no need to wait
    }

    auto req = call->get_request();
    auto spec = task_spec::get(req->local_rpc_code);

    // if rpc is early terminated with empty reply
    if (nullptr == reply) {
        if (req->server_address.type() == HOST_TYPE_GROUP && spec->grpc_mode == GRPC_TO_LEADER &&
            req->server_address.group_address()->is_update_leader_automatically()) {
            req->server_address.group_address()->leader_forward();
        }

        call->set_delay(delay_ms);
        call->enqueue(ERR_NETWORK_FAILURE, reply);
        return true;
    }

    // normal reply
    auto err = reply->error();

    // if this is pure client (no server port assigned), we can only do fake forwarding,
    // in this case, the server will return ERR_FORWARD_TO_OTHERS
    if (err == ERR_FORWARD_TO_OTHERS) {
        rpc_address addr;
        ::dsn::unmarshall((dsn::message_ex *)reply, addr);

        // handle the case of forwarding to itself where addr == req->to_address.
        dbg_dassert(addr != req->to_address,
                    "impossible forwarding to myself as this only happens when i'm pure client so "
                    "i don't get a named to_address %s",
                    addr.to_string());

        // server address side effect
        switch (req->server_address.type()) {
        case HOST_TYPE_GROUP:
            switch (spec->grpc_mode) {
            case GRPC_TO_LEADER:
                if (req->server_address.group_address()->is_update_leader_automatically()) {
                    req->server_address.group_address()->set_leader(addr);
                }
                break;
            default:
                break;
            }
            break;
        default:
            dassert(false, "not implemented");
            break;
        }

        // do fake forwarding, reset request_id
        // TODO(qinzuoyan): reset timeout to new value
        _engine->call_ip(addr, req, call, true);

        dassert(reply->get_count() == 0, "reply should not be referenced by anybody so far");
        delete reply;
    } else {
        // server address side effect
        if (reply->header->context.u.is_forwarded) {
            switch (req->server_address.type()) {
            case HOST_TYPE_GROUP:
                switch (spec->grpc_mode) {
                case GRPC_TO_LEADER:
                    if (err == ERR_OK &&
                        req->server_address.group_address()->is_update_leader_automatically()) {
                        req->server_address.group_address()->set_leader(
                            reply->header->from_address);
                    }
                    break;
                default:
                    break;
                }
                break;
            default:
                dassert(false, "not implemented");
                break;
            }
        }

        call->set_delay(delay_ms);

        // failure injection applied
        if (!call->enqueue(err, reply)) {
            ddebug("rpc reply %s is dropped (fault inject), trace_id = %016" PRIx64,
                   reply->header->rpc_name,
                   reply->header->trace_id);

            // call network failure model
            net->inject_drop_message(reply, false);
        }
    }

    return true;
}

void rpc_client_matcher::on_rpc_timeout(uint64_t key)
{
    rpc_response_task_ptr call;
    int bucket_index = key % MATCHER_BUCKET_NR;
    uint64_t timeout_ts_ms;
    bool resend = false;

    {
        utils::auto_lock<::dsn::utils::ex_lock_nr_spin> l(_requests_lock[bucket_index]);
        auto it = _requests[bucket_index].find(key);
        if (it != _requests[bucket_index].end()) {
            timeout_ts_ms = it->second.timeout_ts_ms;
            call = it->second.resp_task;
            if (timeout_ts_ms == 0) {
                _requests[bucket_index].erase(it);
            }

            // resend is enabled
            else {
                // do it in next check so we can do expensive things
                // outside of the lock
                resend = true;
            }
        } else {
            return;
        }
    }

    dbg_dassert(call != nullptr, "rpc response task is missing for rpc request %" PRIu64, key);

    // if timeout
    if (!resend) {
        call->enqueue(ERR_TIMEOUT, nullptr);
        return;
    }

    // prepare resend context and check again
    uint64_t now_ts_ms = dsn_now_ms();

    // resend when timeout is not yet, and the call is not cancelled
    // TODO: time overflow
    resend = (now_ts_ms < timeout_ts_ms && call->state() == TASK_STATE_READY);

    // TODO: memory pool for this task
    task_ptr new_timeout_task;
    if (resend) {
        new_timeout_task = new rpc_timeout_task(this, key, call->node());
    }

    {
        utils::auto_lock<::dsn::utils::ex_lock_nr_spin> l(_requests_lock[bucket_index]);
        auto it = _requests[bucket_index].find(key);
        if (it != _requests[bucket_index].end()) {
            // timeout
            if (!resend) {
                _requests[bucket_index].erase(it);
            }

            // resend
            else {
                // reset timeout task
                it->second.timeout_task = new_timeout_task;
            }
        }

        // response is received
        else {
            resend = false;
        }
    }

    if (resend) {
        auto req = call->get_request();
        dinfo("resend request message for rpc trace_id = %016" PRIx64 ", key = %" PRIu64,
              req->header->trace_id,
              key);

        // resend without handling rpc_matcher, use the same request_id
        _engine->call_ip(req->to_address, req, nullptr);

        // use rest of the timeout to resend once only
        new_timeout_task->set_delay(static_cast<int>(timeout_ts_ms - now_ts_ms));
        new_timeout_task->enqueue();
    }
}

void rpc_client_matcher::on_call(message_ex *request, const rpc_response_task_ptr &call)
{
    message_header &hdr = *request->header;
    int bucket_index = hdr.id % MATCHER_BUCKET_NR;
    auto sp = task_spec::get(request->local_rpc_code);
    int timeout_ms = hdr.client.timeout_ms;
    uint64_t timeout_ts_ms = 0;

    // reset timeout when resend is enabled
    if (sp->rpc_request_resend_timeout_milliseconds > 0 &&
        timeout_ms > sp->rpc_request_resend_timeout_milliseconds) {
        timeout_ts_ms = dsn_now_ms() + timeout_ms; // non-zero for resend
        timeout_ms = sp->rpc_request_resend_timeout_milliseconds;
    }

    dbg_dassert(call != nullptr, "rpc response task cannot be empty");
    task *timeout_task(new rpc_timeout_task(this, hdr.id, call->node()));

    {
        utils::auto_lock<::dsn::utils::ex_lock_nr_spin> l(_requests_lock[bucket_index]);
        auto pr =
            _requests[bucket_index].emplace(hdr.id, match_entry{call, timeout_task, timeout_ts_ms});
        dassert(pr.second, "the message is already on the fly!!!");
    }

    timeout_task->set_delay(timeout_ms);
    timeout_task->enqueue();
}

//----------------------------------------------------------------------------------------------
rpc_server_dispatcher::rpc_server_dispatcher()
{
    _vhandlers.resize(dsn::task_code::max() + 1);
    for (auto &h : _vhandlers) {
        h = new std::pair<std::unique_ptr<handler_entry>, utils::rw_lock_nr>();
    }
    _handlers.clear();
}

rpc_server_dispatcher::~rpc_server_dispatcher()
{
    for (auto &h : _vhandlers) {
        delete h;
    }
    _vhandlers.clear();
    _handlers.clear();
    dassert(_handlers.size() == 0,
            "please make sure all rpc handlers are unregistered at this point");
}

bool rpc_server_dispatcher::register_rpc_handler(dsn::task_code code,
                                                 const char *extra_name,
                                                 const rpc_request_handler &h)
{
    std::unique_ptr<handler_entry> ctx(new handler_entry{code, extra_name, h});

    utils::auto_write_lock l(_handlers_lock);
    auto it = _handlers.find(code.to_string());
    auto it2 = _handlers.find(extra_name);
    if (it == _handlers.end() && it2 == _handlers.end()) {
        _handlers[code.to_string()] = ctx.get();
        _handlers[ctx->extra_name] = ctx.get();

        {
            utils::auto_write_lock l(_vhandlers[code.code()]->second);
            _vhandlers[code.code()]->first = std::move(ctx);
        }
        return true;
    } else {
        dassert(false, "rpc registration confliction for '%s' '%s'", code.to_string(), extra_name);
        return false;
    }
}

bool rpc_server_dispatcher::unregister_rpc_handler(dsn::task_code rpc_code)
{
    {
        utils::auto_write_lock l(_handlers_lock);
        auto it = _handlers.find(rpc_code.to_string());
        if (it == _handlers.end())
            return false;

        handler_entry *ctx = it->second;
        _handlers.erase(it);
        _handlers.erase(ctx->extra_name);

        {
            utils::auto_write_lock l(_vhandlers[rpc_code]->second);
            _vhandlers[rpc_code]->first.reset();
        }
    }

    return true;
}

rpc_request_task *rpc_server_dispatcher::on_request(message_ex *msg, service_node *node)
{
    rpc_request_handler handler;

    if (TASK_CODE_INVALID != msg->local_rpc_code) {
        utils::auto_read_lock l(_vhandlers[msg->local_rpc_code]->second);
        handler_entry *ctx = _vhandlers[msg->local_rpc_code]->first.get();
        if (ctx != nullptr) {
            handler = ctx->h;
        }
    } else {
        utils::auto_read_lock l(_handlers_lock);
        auto it = _handlers.find(msg->header->rpc_name);
        if (it != _handlers.end()) {
            msg->local_rpc_code = it->second->code;
            handler = it->second->h;
        }
    }

    if (handler) {
        auto r = new rpc_request_task(msg, std::move(handler), node);
        r->spec().on_task_create.execute(task::get_current_task(), r);
        return r;
    } else
        return nullptr;
}

//----------------------------------------------------------------------------------------------
rpc_engine::rpc_engine(service_node *node) : _node(node), _rpc_matcher(this)
{
    dassert(_node != nullptr, "");
    _is_running = false;
    _is_serving = false;
}

//
// management routines
//
network *rpc_engine::create_network(const network_server_config &netcs,
                                    bool client_only,
                                    network_header_format client_hdr_format)
{
    network *net = utils::factory_store<network>::create(
        netcs.factory_name.c_str(), ::dsn::PROVIDER_TYPE_MAIN, this, nullptr);
    net->reset_parser_attr(client_hdr_format, netcs.message_buffer_block_size);

    // start the net
    error_code ret = net->start(netcs.channel, netcs.port, client_only);
    if (ret == ERR_OK) {
        return net;
    } else {
        // mem leak, don't care as it halts the program
        dassert(false, "create network failed, error_code: %s", ret.to_string());
        return nullptr;
    }
}

error_code rpc_engine::start(const service_app_spec &aspec)
{
    if (_is_running) {
        return ERR_SERVICE_ALREADY_RUNNING;
    }

    // start client networks
    _client_nets.resize(network_header_format::max_value() + 1);

    // for each format
    for (int i = NET_HDR_INVALID + 1; i <= network_header_format::max_value(); i++) {
        std::vector<std::unique_ptr<network>> &pnet = _client_nets[i];
        pnet.resize(rpc_channel::max_value() + 1);
        auto client_hdr_format = network_header_format(network_header_format::to_string(i));

        // for each channel
        for (int j = 0; j <= rpc_channel::max_value(); j++) {
            rpc_channel c = rpc_channel(rpc_channel::to_string(j));
            std::string factory;
            int blk_size;

            auto it1 = aspec.network_client_confs.find(c);
            if (it1 != aspec.network_client_confs.end()) {
                factory = it1->second.factory_name;
                blk_size = it1->second.message_buffer_block_size;
            } else {
                dwarn("network client for channel %s not registered, assuming not used further",
                      c.to_string());
                continue;
            }

            network_server_config cs(aspec.id, c);
            cs.factory_name = factory;
            cs.message_buffer_block_size = blk_size;

            auto net = create_network(cs, true, client_hdr_format);
            if (!net)
                return ERR_NETWORK_INIT_FAILED;
            pnet[j].reset(net);

            ddebug("[%s] network client started at port %u, channel = %s, fmt = %s ...",
                   node()->full_name(),
                   (uint32_t)(cs.port),
                   cs.channel.to_string(),
                   client_hdr_format.to_string());
        }
    }

    // start server networks
    for (auto &sp : aspec.network_server_confs) {
        int port = sp.second.port;

        std::vector<std::unique_ptr<network>> *pnets;
        auto it = _server_nets.find(port);

        if (it == _server_nets.end()) {
            auto pr = _server_nets.emplace(port, std::vector<std::unique_ptr<network>>{});
            pnets = &pr.first->second;
            pnets->resize(rpc_channel::max_value() + 1);
        } else {
            pnets = &it->second;
        }

        auto net = create_network(sp.second, false, NET_HDR_DSN);
        if (net == nullptr) {
            return ERR_NETWORK_INIT_FAILED;
        }

        (*pnets)[sp.second.channel].reset(net);

        dwarn("[%s] network server started at port %u, channel = %s, ...",
              node()->full_name(),
              (uint32_t)(port),
              sp.second.channel.to_string());
    }

    _local_primary_address = _client_nets[NET_HDR_DSN][0]->address();
    _local_primary_address.set_port(aspec.ports.size() > 0 ? *aspec.ports.begin() : aspec.id);

    ddebug("=== service_node=[%s], primary_address=[%s] ===",
           _node->full_name(),
           _local_primary_address.to_string());

    _is_running = true;
    return ERR_OK;
}

bool rpc_engine::register_rpc_handler(dsn::task_code code,
                                      const char *extra_name,
                                      const rpc_request_handler &h)
{
    return _rpc_dispatcher.register_rpc_handler(code, extra_name, h);
}

bool rpc_engine::unregister_rpc_handler(dsn::task_code rpc_code)
{
    return _rpc_dispatcher.unregister_rpc_handler(rpc_code);
}

void rpc_engine::on_recv_request(network *net, message_ex *msg, int delay_ms)
{
    if (!_is_serving) {
        dwarn("recv message with rpc name %s from %s when rpc engine is not serving, trace_id = "
              "%" PRIu64,
              msg->header->rpc_name,
              msg->header->from_address.to_string(),
              msg->header->trace_id);

        dassert(msg->get_count() == 0, "request should not be referenced by anybody so far");
        delete msg;
        return;
    }

    auto code = msg->rpc_code();

    if (code != ::dsn::TASK_CODE_INVALID) {
        rpc_request_task *tsk = nullptr;

        // handle replication
        if (msg->header->gpid.get_app_id() > 0) {
            tsk = _node->generate_intercepted_request_task(msg);
        }

        if (tsk == nullptr) {
            tsk = _rpc_dispatcher.on_request(msg, _node);
        }

        if (tsk != nullptr) {
            // injector
            if (tsk->spec().on_rpc_request_enqueue.execute(tsk, true)) {
                // we set a default delay if it isn't generated by fault-injector
                if (tsk->delay_milliseconds() == 0)
                    tsk->set_delay(delay_ms);
                tsk->enqueue();
            }

            // release the task when necessary
            else {
                ddebug("rpc request %s is dropped (fault inject), trace_id = %016" PRIx64,
                       msg->header->rpc_name,
                       msg->header->trace_id);

                // call network failure model when network is present
                net->inject_drop_message(msg, false);

                // because (1) initially, the ref count is zero
                //         (2) upper apps may call add_ref already
                tsk->add_ref();
                tsk->release_ref();
            }
        } else {
            dwarn("recv message with unhandled rpc name %s from %s, trace_id = %016" PRIx64,
                  msg->header->rpc_name,
                  msg->header->from_address.to_string(),
                  msg->header->trace_id);

            dassert(msg->get_count() == 0, "request should not be referenced by anybody so far");
            msg->add_ref();
            dsn_rpc_reply(msg->create_response(), ::dsn::ERR_HANDLER_NOT_FOUND);
            msg->release_ref();
        }
    } else {
        dwarn("recv message with unknown rpc name %s from %s, trace_id = %016" PRIx64,
              msg->header->rpc_name,
              msg->header->from_address.to_string(),
              msg->header->trace_id);

        dassert(msg->get_count() == 0, "request should not be referenced by anybody so far");
        msg->add_ref();
        dsn_rpc_reply(msg->create_response(), ::dsn::ERR_HANDLER_NOT_FOUND);
        msg->release_ref();
    }
}

void rpc_engine::call(message_ex *request, const rpc_response_task_ptr &call)
{
    auto &hdr = *request->header;
    hdr.from_address = primary_address();
    hdr.trace_id = rand::next_u64(std::numeric_limits<decltype(hdr.trace_id)>::min(),
                                  std::numeric_limits<decltype(hdr.trace_id)>::max());

    call_address(request->server_address, request, call);
}

void rpc_engine::call_group(rpc_address addr,
                            message_ex *request,
                            const rpc_response_task_ptr &call)
{
    dbg_dassert(addr.type() == HOST_TYPE_GROUP, "only group is now supported");

    auto sp = task_spec::get(request->local_rpc_code);
    switch (sp->grpc_mode) {
    case GRPC_TO_LEADER:
        call_ip(request->server_address.group_address()->possible_leader(), request, call);
        break;
    case GRPC_TO_ANY:
        // TODO: performance optimization
        call_ip(request->server_address.group_address()->random_member(), request, call);
        break;
    case GRPC_TO_ALL:
        dassert(false, "to be implemented");
        break;
    default:
        dassert(false, "invalid group rpc mode %d", (int)(sp->grpc_mode));
    }
}

void rpc_engine::call_ip(rpc_address addr,
                         message_ex *request,
                         const rpc_response_task_ptr &call,
                         bool reset_request_id,
                         bool set_forwarded)
{
    dbg_dassert(addr.type() == HOST_TYPE_IPV4, "only IPV4 is now supported");
    dbg_dassert(addr.port() > MAX_CLIENT_PORT, "only server address can be called");
    dassert(!request->header->from_address.is_invalid(),
            "from address must be set before call call_ip");

    while (!request->dl.is_alone()) {
        dwarn("msg request %s (trace_id = %016" PRIx64 ") is in sending queue, try to pick out ...",
              request->header->rpc_name,
              request->header->trace_id);
        auto s = request->io_session;
        if (s.get() != nullptr) {
            s->cancel(request);
        }
    }

    request->to_address = addr;

    auto sp = task_spec::get(request->local_rpc_code);
    auto &hdr = *request->header;

    network *net = _client_nets[request->hdr_format][sp->rpc_call_channel].get();
    dassert(nullptr != net,
            "network not present for rpc channel '%s' with format '%s' used by rpc %s",
            sp->rpc_call_channel.to_string(),
            sp->rpc_call_header_format.to_string(),
            hdr.rpc_name);

    dinfo("rpc_name = %s, remote_addr = %s, header_format = %s, channel = %s, seq_id = %" PRIu64
          ", trace_id = %016" PRIx64,
          hdr.rpc_name,
          addr.to_string(),
          request->hdr_format.to_string(),
          sp->rpc_call_channel.to_string(),
          hdr.id,
          hdr.trace_id);

    if (reset_request_id) {
        hdr.id = message_ex::new_id();
    }

    if (set_forwarded && request->header->context.u.is_forwarded == false) {
        request->header->context.u.is_forwarded = true;
    }

    // join point and possible fault injection
    if (!sp->on_rpc_call.execute(task::get_current_task(), request, call, true)) {
        ddebug("rpc request %s is dropped (fault inject), trace_id = %016" PRIx64,
               request->header->rpc_name,
               request->header->trace_id);

        // call network failure model
        net->inject_drop_message(request, true);

        if (call != nullptr) {
            call->set_delay(hdr.client.timeout_ms);
            call->enqueue(ERR_TIMEOUT, nullptr);
        } else {
            // as ref_count for request may be zero
            request->add_ref();
            request->release_ref();
        }

        return;
    }

    if (call != nullptr) {
        _rpc_matcher.on_call(request, call);
    }

    net->send_message(request);
}

void rpc_engine::reply(message_ex *response, error_code err)
{
    // when a message doesn't need to reply, we don't do the on_rpc_reply hooks to avoid mistakes
    // for example, the profiler may be mistakenly calculated
    auto s = response->io_session.get();
    if (s == nullptr && response->to_address.is_invalid()) {
        dinfo("rpc reply %s is dropped (invalid to-address), trace_id = %016" PRIx64,
              response->header->rpc_name,
              response->header->trace_id);
        response->add_ref();
        response->release_ref();
        return;
    }

    strncpy(response->header->server.error_name,
            err.to_string(),
            sizeof(response->header->server.error_name) - 1);
    response->header->server.error_name[sizeof(response->header->server.error_name) - 1] = '\0';
    response->header->server.error_code.local_code = err;
    response->header->server.error_code.local_hash = message_ex::s_local_hash;

    // response rpc code may be TASK_CODE_INVALID when request rpc code is not exist
    auto sp = response->local_rpc_code == TASK_CODE_INVALID
                  ? nullptr
                  : task_spec::get(response->local_rpc_code);

    bool no_fail = true;
    if (sp) {
        // current task may be nullptr when this method is directly invoked from rpc_engine.
        task *cur_task = task::get_current_task();
        if (cur_task) {
            no_fail = sp->on_rpc_reply.execute(cur_task, response, true);
        }
    }

    // connection oriented network, we have bound session
    if (s != nullptr) {
        // not forwarded, we can use the original rpc session
        if (!response->header->context.u.is_forwarded) {
            if (no_fail) {
                s->send_message(response);
            } else {
                s->net().inject_drop_message(response, true);
            }
        }

        // request is forwarded, we cannot use the original rpc session,
        // so use client session to send response.
        else {
            dbg_dassert(response->to_address.port() > MAX_CLIENT_PORT,
                        "target address must have named port in this case");

            // use the header format recorded in the message
            auto rpc_channel = sp ? sp->rpc_call_channel : RPC_CHANNEL_TCP;
            network *net = _client_nets[response->hdr_format][rpc_channel].get();
            dassert(
                nullptr != net,
                "client network not present for rpc channel '%s' with format '%s' used by rpc %s",
                RPC_CHANNEL_TCP.to_string(),
                response->hdr_format.to_string(),
                response->header->rpc_name);

            if (no_fail) {
                net->send_message(response);
            } else {
                net->inject_drop_message(response, true);
            }
        }
    }

    // not connection oriented network, we always use the named network to send msgs
    else {
        dbg_dassert(response->to_address.port() > MAX_CLIENT_PORT,
                    "target address must have named port in this case");

        auto rpc_channel = sp ? sp->rpc_call_channel : RPC_CHANNEL_TCP;
        network *net = _server_nets[response->header->from_address.port()][rpc_channel].get();

        dassert(nullptr != net,
                "server network not present for rpc channel '%s' on port %u used by rpc %s",
                RPC_CHANNEL_UDP.to_string(),
                response->header->from_address.port(),
                response->header->rpc_name);

        if (no_fail) {
            net->send_message(response);
        } else {
            net->inject_drop_message(response, true);
        }
    }

    if (!no_fail) {
        // because (1) initially, the ref count is zero
        //         (2) upper apps may call add_ref already
        response->add_ref();
        response->release_ref();
    }
}

void rpc_engine::forward(message_ex *request, rpc_address address)
{
    dassert(request->header->context.u.is_request, "only rpc request can be forwarded");
    dassert(request->header->context.u.is_forward_supported,
            "rpc msg %s (trace_id = %016" PRIx64 ") does not support being forwared",
            task_spec::get(request->local_rpc_code)->name.c_str(),
            request->header->trace_id);
    dassert(address != primary_address(),
            "cannot forward msg %s (trace_id = %016" PRIx64 ") to the local node",
            task_spec::get(request->local_rpc_code)->name.c_str(),
            request->header->trace_id);

    // msg is from pure client (no server port assigned)
    // in this case, we have no way to directly post a message
    // to it but reusing the current server connection
    // we therefore cannot really do the forwarding but fake it
    if (request->header->from_address.port() <= MAX_CLIENT_PORT) {
        auto resp = request->create_response();
        ::dsn::marshall(resp, address);
        ::dsn::task::get_current_rpc()->reply(resp, ::dsn::ERR_FORWARD_TO_OTHERS);
    }

    // do real forwarding, not reset request_id, but set forwarded flag
    // if forwarding failed for non-timeout reason (such as connection denied),
    // we will consider this as msg lost from the client side's perspective as
    else {
        auto copied_request = request->copy_and_prepare_send(false);
        call_ip(address, copied_request, nullptr, false, true);
    }
}

} // namespace dsn
