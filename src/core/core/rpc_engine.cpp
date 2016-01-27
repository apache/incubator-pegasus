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
 *     rpc engine implementation
 *
 * Revision history:
 *     Mar., 2015, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

# ifdef _WIN32
# include <WinSock2.h>
# else
# include <sys/socket.h>
# include <netdb.h>
# include <ifaddrs.h>
# include <netinet/in.h>
# include <arpa/inet.h>
# endif

# include "rpc_engine.h"
# include "service_engine.h"
# include "group_address.h"
# include <dsn/internal/perf_counters.h>
# include <dsn/internal/factory_store.h>
# include <dsn/internal/task_queue.h>
# include <dsn/cpp/serialization.h>
# include <set>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "rpc.engine"

namespace dsn {
    
    DEFINE_TASK_CODE(LPC_RPC_TIMEOUT, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)

    class rpc_timeout_task : public task
    {
    public:
        rpc_timeout_task(rpc_client_matcher* matcher, uint64_t id, service_node* node) 
            : task(LPC_RPC_TIMEOUT, nullptr, nullptr, 0, node)
        {
            _matcher = matcher;
            _id = id;
        }

        virtual void exec()
        {
            _matcher->on_rpc_timeout(_id);
        }

    private:
        // use the following if the matcher is per rpc session
        // rpc_client_matcher_ptr _matcher;

        rpc_client_matcher* _matcher;
        uint64_t            _id;
    };

    rpc_client_matcher::~rpc_client_matcher()
    {
        for (int i = 0; i < MATCHER_BUCKET_NR; i++)
        {
            dassert(_requests[i].size() == 0, "all rpc entries must be removed before the matcher ends");
        }
    }

    bool rpc_client_matcher::on_recv_reply(network* net, uint64_t key, message_ex* reply, int delay_ms)
    {       
        rpc_response_task* call;
        task* timeout_task;
        int bucket_index = key % MATCHER_BUCKET_NR;

        {
            utils::auto_lock< ::dsn::utils::ex_lock_nr_spin> l(_requests_lock[bucket_index]);
            auto it = _requests[bucket_index].find(key);
            if (it != _requests[bucket_index].end())
            {
                call = it->second.resp_task;
                timeout_task = it->second.timeout_task;
                timeout_task->add_ref(); // released below in the same function
                _requests[bucket_index].erase(it);
            }
            else
            {
                if (reply)
                {
                    dassert(reply->get_count() == 0,
                        "reply should not be referenced by anybody so far");
                    delete reply;
                }
                return false;
            }
        }
                
        dbg_dassert(call != nullptr, "rpc response task cannot be empty");
        if (timeout_task != task::get_current_task())
        {
            timeout_task->cancel(false); // no need to wait
        }
        timeout_task->release_ref(); // added above in the same function

        // if rpc is early terminated with empty reply
        if (nullptr == reply)
        {
            call->set_delay(delay_ms);
            call->enqueue(ERR_TIMEOUT, reply);
            call->release_ref(); // added in on_call
            return true;
        }

        // normal reply        
        if (reply->error() == ERR_FORWARD_TO_OTHERS)
        {
            rpc_address addr;
            ::unmarshall((dsn_message_t)reply, addr);
            _engine->call_ip(addr, call->get_request(), call, true);

            dassert(reply->get_count() == 0,
                "reply should not be referenced by anybody so far");
            delete reply;
        }
        else
        {
            auto err = reply->error();

            // server address side effect
            auto req = call->get_request();
            auto sp = task_spec::get(req->local_rpc_code);
            if (reply->from_address != req->to_address)
            {
                switch (req->server_address.type())
                {
                case HOST_TYPE_GROUP:
                    
                    switch (sp->grpc_mode)
                    {
                    case GRPC_TO_LEADER:
                        if (err == ERR_OK)
                        {
                            req->server_address.group_address()->set_leader(reply->from_address);
                        }
                        break;
                        // TODO:
                    }
                    break;
                case HOST_TYPE_URI:
                    // TODO:
                    break;
                }
            }

            if (sp->on_rpc_response_enqueue.execute(call, true))
            {
                call->set_delay(delay_ms);
                call->enqueue(err, reply);
            }

            // release the task when necessary
            else
            {
                dassert(reply->get_count() == 0,
                    "reply should not be referenced by anybody so far");
                delete reply;

                // call network failure model implementation to make the above failure real
                net->inject_drop_message(reply, true, false);

                // because (1) initially, the ref count is zero
                //         (2) upper apps may call add_ref already
                call->add_ref();
                call->release_ref();
            }
        }

        call->release_ref(); // added in on_call
        return true;
    }

    void rpc_client_matcher::on_rpc_timeout(uint64_t key)
    {
        rpc_response_task* call;
        int bucket_index = key % MATCHER_BUCKET_NR;        
        uint64_t timeout_ts_ms;
        bool resend = false;

        {
            utils::auto_lock< ::dsn::utils::ex_lock_nr_spin> l(_requests_lock[bucket_index]);
            auto it = _requests[bucket_index].find(key);
            if (it != _requests[bucket_index].end())
            {
                timeout_ts_ms = it->second.timeout_ts_ms;
                call = it->second.resp_task;
                if (timeout_ts_ms == 0)
                {
                    _requests[bucket_index].erase(it);
                }

                // resend is enabled
                else
                {
                    // do it in next check so we can do expensive things
                    // outside of the lock

                    // call may be eliminated from this container and deleted after its execution
                    // we therefore add_ref here
                    call->add_ref();  // released after re-send
                    resend = true;
                }
            }
            else
            {
                return;
            }
        }

        dbg_dassert(call != nullptr,
            "rpc response task is missing for rpc request %" PRIu64, key);

        // if timeout
        if (!resend)
        {
            call->enqueue(ERR_TIMEOUT, nullptr);
            call->release_ref(); // added in on_call
            return;
        }

        // prepare resend context and check again
        uint64_t now_ts_ms = dsn_now_ms();

        // resend when timeout is not yet, and the call is not cancelled
        // TODO: time overflow
        resend = (now_ts_ms < timeout_ts_ms && call->state() == TASK_STATE_READY);

        // TODO: memory pool for this task
        task* new_timeout_task = resend ? new rpc_timeout_task(this, key, call->node()) : nullptr;

        {
            utils::auto_lock< ::dsn::utils::ex_lock_nr_spin> l(_requests_lock[bucket_index]);
            auto it = _requests[bucket_index].find(key);
            if (it != _requests[bucket_index].end())
            {
                // timeout                
                if (!resend)
                {
                    _requests[bucket_index].erase(it);
                }

                // resend
                else
                {
                    // reset timeout task
                    it->second.timeout_task = new_timeout_task;
                }
            }

            // response is received
            else
            {
                resend = false;
            }
        }

        if (resend)
        {
            auto req = call->get_request();
            dinfo("resend reqeust message for rpc %" PRIx64 ", key = %" PRIu64,
                req->header->rpc_id, key);

            // resend without handling rpc_matcher and reset rquest id
            _engine->call_ip(req->to_address, req, nullptr, false);

            // use rest of the timeout to resend once only
            new_timeout_task->set_delay(timeout_ts_ms - now_ts_ms);
            new_timeout_task->enqueue();
        }
        else
        {
            if (new_timeout_task)
            {
                delete new_timeout_task;
            }
        }

        call->release_ref(); // added inside the first check of resend
    }
    
    void rpc_client_matcher::on_call(message_ex* request, rpc_response_task* call)
    {
        task* timeout_task;
        message_header& hdr = *request->header;
        int bucket_index = hdr.id % MATCHER_BUCKET_NR;
        auto sp = task_spec::get(request->local_rpc_code);
        int timeout_ms = hdr.client.timeout_ms;
        uint64_t timeout_ts_ms = 0;
        
        // reset timeout when resend is enabled
        if (sp->rpc_request_resend_timeout_milliseconds > 0 && 
            timeout_ms > sp->rpc_request_resend_timeout_milliseconds
            )
        {
            timeout_ts_ms = dsn_now_ms() + timeout_ms; // non-zero for resend
            timeout_ms = sp->rpc_request_resend_timeout_milliseconds;            
        }

        dbg_dassert(call != nullptr, "rpc response task cannot be empty");
        timeout_task = (new rpc_timeout_task(this, hdr.id, call->node()));

        {
            utils::auto_lock< ::dsn::utils::ex_lock_nr_spin> l(_requests_lock[bucket_index]);
            auto pr = _requests[bucket_index].emplace(hdr.id, match_entry { call, timeout_task, timeout_ts_ms });
            dassert (pr.second, "the message is already on the fly!!!");
        }

        timeout_task->set_delay(timeout_ms);
        timeout_task->enqueue();

        call->add_ref(); // released in on_rpc_timeout or on_recv_reply
    }

    //----------------------------------------------------------------------------------------------
    bool rpc_server_dispatcher::register_rpc_handler(rpc_handler_ptr& handler)
    {
        auto name = std::string(dsn_task_code_to_string(handler->code));

        utils::auto_write_lock l(_handlers_lock);
        auto it = _handlers.find(name);
        auto it2 = _handlers.find(handler->name);
        if (it == _handlers.end() && it2 == _handlers.end())
        {
            _handlers[name] = handler;
            _handlers[handler->name] = handler;
            return true;
        }
        else
        {
            dassert(false, "rpc registration confliction for '%s'", name.c_str());
            return false;
        }
    }

    rpc_handler_ptr rpc_server_dispatcher::unregister_rpc_handler(dsn_task_code_t rpc_code)
    {
        utils::auto_write_lock l(_handlers_lock);
        auto it = _handlers.find(dsn_task_code_to_string(rpc_code));
        if (it == _handlers.end())
            return nullptr;

        auto ret = it->second;
        std::string name = it->second->name;
        _handlers.erase(it);
        _handlers.erase(name);

        return ret;
    }

    rpc_request_task* rpc_server_dispatcher::on_request(message_ex* msg, service_node* node)
    {
        rpc_request_task* tsk = nullptr;
        {
            utils::auto_read_lock l(_handlers_lock);
            auto it = _handlers.find(msg->header->rpc_name);
            if (it != _handlers.end())
            {
                msg->local_rpc_code = (uint16_t)it->second->code;
                tsk = new rpc_request_task(msg, it->second, node);
            }
        }
        return tsk;
    }

    //----------------------------------------------------------------------------------------------
    /*static*/ bool rpc_engine::_message_crc_required;

    rpc_engine::rpc_engine(configuration_ptr config, service_node* node)
        : _config(config), _node(node), _rpc_matcher(this)
    {
        dassert (_node != nullptr, "");
        dassert (_config != nullptr, "");

        _is_running = false;
        _message_crc_required = config->get_value<bool>(
            "network", "message_crc_required", false,
            "whether crc is enabled for network messages");
    }
    
    //
    // management routines
    //
    network* rpc_engine::create_network(
        const network_server_config& netcs, 
        bool client_only,
        io_modifer& ctx
        )
    {
        const service_spec& spec = service_engine::fast_instance().spec();
        auto net = utils::factory_store<network>::create(
            netcs.factory_name.c_str(), ::dsn::PROVIDER_TYPE_MAIN, this, nullptr);
        net->reset_parser(netcs.hdr_format, netcs.message_buffer_block_size);

        for (auto it = spec.network_aspects.begin();
            it != spec.network_aspects.end();
            it++)
        {
            net = utils::factory_store<network>::create(it->c_str(), ::dsn::PROVIDER_TYPE_ASPECT, this, net);
        }

        // start the net
        error_code ret = net->start(netcs.channel, netcs.port + ctx.port_shift_value, client_only, ctx); 
        if (ret == ERR_OK)
        {
            return net;
        }
        else
        {
            // mem leak, don't care as it halts the program
            return nullptr;
        }   
    }

    error_code rpc_engine::start(
        const service_app_spec& aspec, 
        io_modifer& ctx
        )
    {
        if (_is_running)
        {
            return ERR_SERVICE_ALREADY_RUNNING;
        }
    
        // local cache for shared networks with same provider and message format and port
        std::map<std::string, network*> named_nets; // factory##fmt##port -> net

        // start client networks
        _client_nets.resize(network_header_format::max_value() + 1);

        // for each format
        for (int i = 0; i <= network_header_format::max_value(); i++)
        {
            std::vector<network*>& pnet = _client_nets[i];
            pnet.resize(rpc_channel::max_value() + 1);

            // for each channel
            for (int j = 0; j <= rpc_channel::max_value(); j++)
            {
                rpc_channel c = rpc_channel(rpc_channel::to_string(j));
                std::string factory;
                int blk_size;

                auto it1 = aspec.network_client_confs.find(c);
                if (it1 != aspec.network_client_confs.end())
                {
                    factory = it1->second.factory_name;
                    blk_size = it1->second.message_buffer_block_size;
                }
                else
                {
                    dwarn("network client for channel %s not registered, assuming not used further", c.to_string());
                    continue;
                }

                network_server_config cs(aspec.id, c);

                cs.factory_name = factory;
                cs.message_buffer_block_size = blk_size;
                cs.hdr_format = network_header_format(network_header_format::to_string(i));

                auto net = create_network(cs, true, ctx);
                if (!net) return ERR_NETWORK_INIT_FAILED;
                pnet[j] = net;

                if (ctx.queue)
                {
                    ddebug("[%s.%s] network client started at port %u, channel = %s, fmt = %s ...",
                        node()->name(),
                        ctx.queue->get_name().c_str(),
                        (uint32_t)(cs.port + ctx.port_shift_value),
                        cs.channel.to_string(),
                        cs.hdr_format.to_string()
                        );
                }
                else
                {
                    ddebug("[%s] network client started at port %u, channel = %s, fmt = %s ...",
                        node()->name(),
                        (uint32_t)(cs.port + ctx.port_shift_value),
                        cs.channel.to_string(),
                        cs.hdr_format.to_string()
                        );
                }
            }
        }
        
        // start server networks
        for (auto& sp : aspec.network_server_confs)
        {
            int port = sp.second.port;

            std::vector<network*>* pnets;
            auto it = _server_nets.find(port);

            if (it == _server_nets.end())
            {
                std::vector<network*> nets;
                auto pr = _server_nets.insert(std::map<int, std::vector<network*>>::value_type(port, nets));
                pnets = &pr.first->second;
                pnets->resize(rpc_channel::max_value() + 1);
            }
            else
            {
                pnets = &it->second;
            }

            auto net = create_network(sp.second, false, ctx);
            if (net == nullptr)
            {
                return ERR_NETWORK_INIT_FAILED;
            }

            (*pnets)[sp.second.channel] = net;

            if (ctx.queue)
            {
                dwarn("[%s.%s] network server started at port %u, channel = %s, fmt = %s ...",
                    node()->name(),
                    ctx.queue->get_name().c_str(),
                    (uint32_t)(port + ctx.port_shift_value),
                    sp.second.channel.to_string(),
                    sp.second.hdr_format.to_string()
                    );
            }
            else
            {
                dwarn("[%s] network server started at port %u, channel = %s, fmt = %s ...",
                    node()->name(),
                    (uint32_t)(port + ctx.port_shift_value),
                    sp.second.channel.to_string(),
                    sp.second.hdr_format.to_string()
                    );
            }
        }

        _local_primary_address = _client_nets[0][0]->address();
        _local_primary_address.c_addr_ptr()->u.v4.port = aspec.ports.size() > 0 ? *aspec.ports.begin() : aspec.id + ctx.port_shift_value;

        ddebug("=== service_node=[%s], primary_address=[%s] ===",
            _node->name(), _local_primary_address.to_string());

        _is_running = true;
        return ERR_OK;
    }

    bool rpc_engine::register_rpc_handler(rpc_handler_ptr& handler, uint64_t vnid)
    {
        if (0 == vnid)
        {
            return _rpc_dispatcher.register_rpc_handler(handler);
        }
        else
        {
            utils::auto_write_lock l(_vnodes_lock);
            auto it = _vnodes.find(vnid);
            if (it == _vnodes.end())
            {
                auto dispatcher = new rpc_server_dispatcher();
                return _vnodes.insert(
                    std::unordered_map<uint64_t, rpc_server_dispatcher* >::value_type(vnid, dispatcher))
                    .first->second->register_rpc_handler(handler);
            }
            else
            {
                return it->second->register_rpc_handler(handler);
            }
        }
    }

    rpc_handler_ptr rpc_engine::unregister_rpc_handler(dsn_task_code_t rpc_code, uint64_t vnid)
    {
        if (0 == vnid)
        {
            return _rpc_dispatcher.unregister_rpc_handler(rpc_code);
        }
        else
        {
            utils::auto_write_lock l(_vnodes_lock);
            auto it = _vnodes.find(vnid);
            if (it == _vnodes.end())
            {
                return nullptr;
            }
            else
            {
                auto r = it->second->unregister_rpc_handler(rpc_code);
                if (0 == it->second->handler_count())
                {
                    delete it->second;
                    _vnodes.erase(it);
                }
                return r;
            }
        }
    }
    
    void rpc_engine::on_recv_request(message_ex* msg, int delay_ms)
    {
        rpc_request_task* tsk;
        if (msg->header->vnid == 0)
            tsk = _rpc_dispatcher.on_request(msg, _node);
        else
        {
            utils::auto_read_lock l(_vnodes_lock);
            auto it = _vnodes.find(msg->header->vnid);
            if (it != _vnodes.end())
            {
                tsk = it->second->on_request(msg, _node);
            }
            else
            {
                tsk = nullptr;
            }
        }

        if (tsk != nullptr)
        {
            tsk->set_delay(delay_ms);
            tsk->enqueue();
        }
        else
        {
            dwarn(
                "recv unknown message with type %s from %s, rpc_id = %016llx",
                msg->header->rpc_name,
                msg->from_address.to_string(),
                msg->header->rpc_id
                );

            delete msg;
        }
    }

    void rpc_engine::call(message_ex* request, rpc_response_task* call)
    {
        auto sp = task_spec::get(request->local_rpc_code);
        auto& hdr = *request->header;

        hdr.client.port = primary_address().port();
        hdr.rpc_id = dsn_random64(
            std::numeric_limits<decltype(hdr.rpc_id)>::min(),
            std::numeric_limits<decltype(hdr.rpc_id)>::max()
            );
        request->seal(_message_crc_required);
        
        switch (request->server_address.type())
        {
        case HOST_TYPE_IPV4:
            call_ip(request->server_address, request, call);
            break;
        case HOST_TYPE_URI:
            dassert(false, "uri as host support is to be implemented");
            break;
        case HOST_TYPE_GROUP:
            switch (sp->grpc_mode)
            {
            case GRPC_TO_LEADER:
                // TODO: auto-changed leader
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
            break;
        default:
            dassert(false, "invalid target address type %d", (int)request->server_address.type());
            break;
        }
        return;
    }

    void rpc_engine::call_ip(rpc_address addr, message_ex* request, rpc_response_task* call, bool reset_request_id)
    {
        dbg_dassert(addr.type() == HOST_TYPE_IPV4, "only IPV4 is now supported");
        dbg_dassert(addr.port() >= 1024, "only server address can be called");

        while (!request->dl.is_alone())
        {
            dwarn("msg request %s (%" PRIx64 ") is in sending queue, try to pick out ...",
                request->header->rpc_name,
                request->header->rpc_id
                );
            auto s = request->io_session;
            if (s.get() != nullptr)
            {
                s->cancel(request);
            }
        }
        
        request->from_address = primary_address();
        request->to_address = addr;

        auto sp = task_spec::get(request->local_rpc_code);
        auto& hdr = *request->header; 
        auto& named_nets = _client_nets[sp->rpc_call_header_format];
        network* net = named_nets[sp->rpc_call_channel];
        
        dassert(nullptr != net, "network not present for rpc channel '%s' with format '%s' used by rpc %s",
            sp->rpc_call_channel.to_string(),
            sp->rpc_call_header_format.to_string(),
            hdr.rpc_name
            );

        if (reset_request_id)
        {
            hdr.id = message_ex::new_id();
            request->seal(_message_crc_required);
        }

        // join point and possible fault injection
        if (!sp->on_rpc_call.execute(task::get_current_task(), request, call, true))
        {
            ddebug("rpc request %s is dropped (fault inject), rpc_id = %016llx",
                request->header->rpc_name,
                request->header->rpc_id
                );

            // call network failure model
            net->inject_drop_message(request, true, true);

            if (call != nullptr)
            {
                call->set_delay(hdr.client.timeout_ms);
                call->enqueue(ERR_TIMEOUT, nullptr);
            }
            else
            {
                // as ref_count for request may be zero
                request->add_ref();
                request->release_ref();
            }

            return;
        }
            
        if (call != nullptr)
        {
            _rpc_matcher.on_call(request, call);
        }

        net->send_message(request);
    }

    void rpc_engine::reply(message_ex* response, error_code err)
    {
        response->header->server.error = err;
        response->seal(_message_crc_required);

        auto sp = task_spec::get(response->local_rpc_code);
        bool no_fail = sp->on_rpc_reply.execute(task::get_current_task(), response, true);
        
        auto s = response->io_session.get();
        if (s != nullptr) 
        {
            if (no_fail)
            {
                s->send_message(response); 
            }
            else
            {
                s->net().inject_drop_message(response, false, true);
            }
        }
        else
        {
            auto sp = task_spec::get(response->local_rpc_code);
            auto &net = _server_nets[response->from_address.port()][sp->rpc_call_channel];
            if (no_fail)
            {
                net->send_message(response); 
            }
            else
            {
                net->inject_drop_message(response, false, true);
            }
        }

        if (!no_fail)
        {
            // do not delete following add and release here for cancellation
            // as response may initially have ref_count == 0
            response->add_ref();
            response->release_ref();
            return;
        }
    }
}
