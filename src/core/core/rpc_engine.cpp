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
# include "uri_address.h"
# include <dsn/internal/perf_counters.h>
# include <dsn/internal/factory_store.h>
# include <dsn/internal/task_queue.h>
# include <dsn/cpp/serialization.h>
# include <set>
# include <dsn/dist/layer2_handler.h>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "rpc.engine"

namespace dsn {
    
    DEFINE_TASK_CODE(LPC_RPC_TIMEOUT, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)

    class rpc_timeout_task : public task, public transient_object
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
            // TODO(qinzuoyan): maybe set err as ERR_NETWORK_FAILURE to differ with ERR_TIMEOUT
            call->enqueue(ERR_TIMEOUT, reply);
            call->release_ref(); // added in on_call
            return true;
        }

        // normal reply
        auto err = reply->error();
        auto req = call->get_request();
        auto sp = task_spec::get(req->local_rpc_code);

        // if this is pure client (no server port assigned), we can only do fake forwarding,
        // in this case, the server will return ERR_FORWARD_TO_OTHERS
        if (err == ERR_FORWARD_TO_OTHERS)
        {
            rpc_address addr;
            ::unmarshall((dsn_message_t)reply, addr);

            // TODO(qinzuoyan): handle the case of forwarding to itself where addr == req->to_address.

            // server address side effect
            switch (req->server_address.type())
            {
            case HOST_TYPE_GROUP:
                switch (sp->grpc_mode)
                {
                case GRPC_TO_LEADER:
                    if (req->server_address.group_address()->is_update_leader_on_rpc_forward())
                    {
                        req->server_address.group_address()->set_leader(addr);
                    }
                    break;
                case GRPC_TO_ANY:
                case GRPC_TO_ALL:
                    break;
                }
                break;
            case HOST_TYPE_URI:
                dassert(false, "not implemented");
                break;
            }

            // do fake forwarding, reset request_id
            // TODO(qinzuoyan): reset timeout to new value
            _engine->call_ip(addr, req, call, true);

            dassert(reply->get_count() == 0,
                "reply should not be referenced by anybody so far");
            delete reply;
        }
        else
        {
            // server address side effect
            if (reply->header->context.u.is_forwarded)
            {
                switch (req->server_address.type())
                {
                case HOST_TYPE_GROUP:
                    switch (sp->grpc_mode)
                    {
                    case GRPC_TO_LEADER:
                        if (err == ERR_OK && req->server_address.group_address()->is_update_leader_on_rpc_forward())
                        {
                            req->server_address.group_address()->set_leader(reply->header->from_address);
                        }
                        break;
                    case GRPC_TO_ANY:
                    case GRPC_TO_ALL:
                        break;
                    }
                    break;
                case HOST_TYPE_URI:
                    dassert(false, "not implemented");
                    break;
                }
            }

            // injector
            if (sp->on_rpc_response_enqueue.execute(call, true))
            {
                call->set_delay(delay_ms);

                if (ERR_OK == err)
                    call->enqueue(err, reply);
                else
                {
                    call->enqueue(err, nullptr);

                    // because (1) initially, the ref count is zero
                    //         (2) upper apps may call add_ref already
                    call->add_ref();
                    call->release_ref();
                }
            }

            // release the task when necessary
            else
            {
                ddebug("rpc reply %s is dropped (fault inject), rpc_id = %016llx",
                    reply->header->rpc_name,
                    reply->header->rpc_id
                    );

                // call network failure model
                net->inject_drop_message(reply, false);

                dassert(reply->get_count() == 0,
                    "reply should not be referenced by anybody so far");
                delete reply;

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

            // resend without handling rpc_matcher, use the same request_id
            _engine->call_ip(req->to_address, req, nullptr);

            // use rest of the timeout to resend once only
            new_timeout_task->set_delay(static_cast<int>(timeout_ts_ms - now_ts_ms));
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
    rpc_server_dispatcher::rpc_server_dispatcher()
    {
        _vhandlers.resize(dsn_task_code_max() + 1);
        for (auto& h : _vhandlers)
        {
            h = new std::pair<rpc_handler_info*, utils::rw_lock_nr>();
            h->first = nullptr;
        }
    }

    bool rpc_server_dispatcher::register_rpc_handler(rpc_handler_info* handler)
    {
        auto name = std::string(dsn_task_code_to_string(handler->code));

        utils::auto_write_lock l(_handlers_lock);
        auto it = _handlers.find(name);
        auto it2 = _handlers.find(handler->name);
        if (it == _handlers.end() && it2 == _handlers.end())
        {
            _handlers[name] = handler;
            _handlers[handler->name] = handler;   

            {
                utils::auto_write_lock l(_vhandlers[handler->code]->second);
                _vhandlers[handler->code]->first = handler;
            }
            return true;
        }
        else
        {
            dassert(false, "rpc registration confliction for '%s'", name.c_str());
            return false;
        }
    }

    rpc_handler_info* rpc_server_dispatcher::unregister_rpc_handler(dsn_task_code_t rpc_code)
    {
        rpc_handler_info* ret;
        {
            utils::auto_write_lock l(_handlers_lock);
            auto it = _handlers.find(dsn_task_code_to_string(rpc_code));
            if (it == _handlers.end())
                return nullptr;

            ret = it->second;
            std::string name = it->second->name;
            _handlers.erase(it);
            _handlers.erase(name);

            {
                utils::auto_write_lock l(_vhandlers[rpc_code]->second);
                _vhandlers[rpc_code]->first = nullptr;
            }
        }

        ret->unregister();
        return ret;
    }

    rpc_request_task* rpc_server_dispatcher::on_request(message_ex* msg, service_node* node)
    {
        rpc_handler_info* handler = nullptr;
        
        if (TASK_CODE_INVALID != msg->local_rpc_code)
        {
            utils::auto_read_lock l(_vhandlers[msg->local_rpc_code]->second);
            handler = _vhandlers[msg->local_rpc_code]->first;
            if (nullptr != handler)
            {
                handler->add_ref();
            }
        }
        else
        {
            utils::auto_read_lock l(_handlers_lock);
            auto it = _handlers.find(msg->header->rpc_name);
            if (it != _handlers.end())
            {
                msg->local_rpc_code = it->second->code;
                handler = it->second;
                handler->add_ref();
            }
        }

        return handler ? new rpc_request_task(msg, handler, node) : nullptr;
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
            dassert(false, "create network failed, error_code: %s", ret.to_string());
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

        _uri_resolver_mgr.reset(new uri_resolver_manager(
            service_engine::fast_instance().spec().config.get()));

        _local_primary_address = _client_nets[0][0]->address();
        _local_primary_address.c_addr_ptr()->u.v4.port = aspec.ports.size() > 0 ? *aspec.ports.begin() : aspec.id + ctx.port_shift_value;

        ddebug("=== service_node=[%s], primary_address=[%s] ===",
            _node->name(), _local_primary_address.to_string());

        _is_running = true;
        return ERR_OK;
    }

    bool rpc_engine::register_rpc_handler(rpc_handler_info* handler)
    {
        return _rpc_dispatcher.register_rpc_handler(handler);
    }

    rpc_handler_info* rpc_engine::unregister_rpc_handler(dsn_task_code_t rpc_code)
    {
        return _rpc_dispatcher.unregister_rpc_handler(rpc_code);
    }

    void rpc_engine::on_recv_request(network* net, message_ex* msg, int delay_ms)
    {
        uint32_t code = 0;
        auto binary_hash = msg->header->rpc_name_fast.local_hash;
        if (binary_hash == ::dsn::message_ex::s_local_hash && binary_hash != 0)
        {
            code = msg->header->rpc_name_fast.local_rpc_id;
        }
        else
        {
            code = dsn_task_code_from_string(msg->header->rpc_name, ::dsn::TASK_CODE_INVALID);
        }

        if (code != ::dsn::TASK_CODE_INVALID)
        {
            msg->local_rpc_code = code;

            // handle replication
            auto sp = task_spec::get(code);
            if (sp->rpc_request_layer2_handler_required)
            {
                _node->handle_l2_rpc_request(msg->header->gpid, sp->rpc_request_is_write_operation, (dsn_message_t)(msg), delay_ms);
                return;
            }

            rpc_request_task* tsk = _rpc_dispatcher.on_request(msg, _node);

            if (tsk != nullptr)
            {
                // injector
                if (tsk->spec().on_rpc_request_enqueue.execute(tsk, true))
                {
                    tsk->set_delay(delay_ms);
                    tsk->enqueue();
                }

                // release the task when necessary
                else
                {
                    ddebug("rpc request %s is dropped (fault inject), rpc_id = %016llx",
                        msg->header->rpc_name,
                        msg->header->rpc_id
                        );

                    // call network failure model when network is present
                    net->inject_drop_message(msg, false);

                    // because (1) initially, the ref count is zero
                    //         (2) upper apps may call add_ref already
                    tsk->add_ref();
                    tsk->release_ref();
                }
                return;
            }
        }

        dwarn(
            "recv unknown message with type %s from %s, rpc_id = %016llx",
            msg->header->rpc_name,
            msg->header->from_address.to_string(),
            msg->header->rpc_id
            );

        dassert(msg->get_count() == 0,
            "request should not be referenced by anybody so far");
        delete msg;
    }

    void rpc_engine::call(message_ex* request, rpc_response_task* call)
    {
        auto& hdr = *request->header;

        hdr.from_address = primary_address();
        hdr.rpc_id = dsn_random64(
            std::numeric_limits<decltype(hdr.rpc_id)>::min(),
            std::numeric_limits<decltype(hdr.rpc_id)>::max()
            );
        request->seal(_message_crc_required);

        call_address(request->server_address, request, call);
    }
    
    void rpc_engine::call_uri(rpc_address addr, message_ex* request, rpc_response_task* call)
    {
        dbg_dassert(addr.type() == HOST_TYPE_URI, "only URI is now supported");
        auto& hdr = *request->header;
        auto partition_hash = hdr.context.u.parameter_type == MSG_PARAM_PARTITION_HASH ?
            hdr.context.u.parameter :
            0;

        auto resolver = request->server_address.uri_address()->get_resolver();
        if (nullptr == resolver)
        {
            if (call != nullptr)
            {
                call->enqueue(ERR_SERVICE_NOT_FOUND, nullptr);
            }
            else
            {
                // as ref_count for request may be zero
                request->add_ref();
                request->release_ref();
            }
        }
        else
        {
            if (call)
            {
                call->add_hook(
                    [](dsn_error_t err, dsn_message_t req, dsn_message_t, void*)
                    {
                        auto req2 = (message_ex*)(req);
                        if (req2->header->gpid.value != 0 && err != ERR_OK && err != ERR_HANDLER_NOT_FOUND)
                        {
                            auto resolver = req2->server_address.uri_address()->get_resolver();
                            if (nullptr != resolver)
                            {
                                resolver->on_access_failure(req2->header->gpid.u.partition_index, err);
                            }
                        }
                    },
                    nullptr,
                    true
                    );
            }

            resolver->resolve(
                partition_hash,
                [=](dist::partition_resolver::resolve_result&& result) mutable
                {
                    if (result.err == ERR_OK)
                    {
                        // update gpid when necessary
                        auto& hdr2 = request->header;
                        if (*(uint64_t*)&hdr2->gpid != *(uint64_t*)&result.gpid)
                        {
                            hdr2->gpid = result.gpid;
                            hdr2->client.hash = dsn_gpid_to_hash(result.gpid);
                            request->seal(_message_crc_required);
                        }

                        call_address(result.address, request, call);
                    }
                    else
                    {                        
                        if (call != nullptr)
                        {
                            call->enqueue(result.err, nullptr);
                        }
                        else
                        {
                            // as ref_count for request may be zero
                            request->add_ref();
                            request->release_ref();
                        }
                    }
                },
                hdr.client.timeout_ms
                );
        }
    }

    void rpc_engine::call_group(rpc_address addr, message_ex* request, rpc_response_task* call)
    {
        dbg_dassert(addr.type() == HOST_TYPE_GROUP, "only group is now supported");

        auto sp = task_spec::get(request->local_rpc_code);
        switch (sp->grpc_mode)
        {
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

    void rpc_engine::call_ip(rpc_address addr, message_ex* request, rpc_response_task* call, bool reset_request_id, bool set_forwarded)
    {
        dbg_dassert(addr.type() == HOST_TYPE_IPV4, "only IPV4 is now supported");
        dbg_dassert(addr.port() > MAX_CLIENT_PORT, "only server address can be called");
        dassert(!request->header->from_address.is_invalid(), "from address must be set before call call_ip");

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

        bool need_seal = false;
        if (reset_request_id)
        {
            hdr.id = message_ex::new_id();
            need_seal = true;
        }
        if (set_forwarded && request->header->context.u.is_forwarded == false)
        {
            request->header->context.u.is_forwarded = true;
            need_seal = true;
        }
        if (need_seal)
        {
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
            net->inject_drop_message(request, true);

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

        // connetion oriented network, we have bound session
        if (s != nullptr)
        {
            // not forwarded
            if (!response->header->context.u.is_forwarded)
            {
                if (no_fail)
                {
                    s->send_message(response);
                }
                else
                {
                    s->net().inject_drop_message(response, true);
                }
            }

            // request is forwarded, we cannot use the original rpc session
            else
            {
                dbg_dassert(response->to_address.port() > MAX_CLIENT_PORT, 
                    "target address must have named port in this case");

                auto sp = task_spec::get(response->local_rpc_code);
                auto& hdr = *response->header;
                auto& named_nets = _client_nets[sp->rpc_call_header_format];
                network* net = named_nets[sp->rpc_call_channel];

                dassert(nullptr != net, "network not present for rpc channel '%s' with format '%s' used by rpc %s",
                    sp->rpc_call_channel.to_string(),
                    sp->rpc_call_header_format.to_string(),
                    hdr.rpc_name
                    );

                if (no_fail)
                {
                    net->send_message(response);
                }
                else
                {
                    net->inject_drop_message(response, true);
                }
            }            
        }

        // not connetion oriented network, we always use the named network to send msgs
        else
        {
            dbg_dassert(response->to_address.port() > MAX_CLIENT_PORT,
                "target address must have named port in this case");

            auto sp = task_spec::get(response->local_rpc_code);
            auto &net = _server_nets[response->header->from_address.port()][sp->rpc_call_channel];
            if (no_fail)
            {
                net->send_message(response);
            }
            else
            {
                net->inject_drop_message(response, true);
            }
        }

        if (!no_fail)
        {
            // because (1) initially, the ref count is zero
            //         (2) upper apps may call add_ref already
            response->add_ref();
            response->release_ref();
        }
    }

    void rpc_engine::forward(message_ex * request, rpc_address address)
    {
        dassert(request->header->context.u.is_request, "only rpc request can be forwarded");

        // msg is from pure client (no server port assigned)
        // in this case, we have no way to directly post a message
        // to it but reusing the current server connection
        // we therefore cannot really do the forwarding but fake it
        if (request->header->from_address.port() <= MAX_CLIENT_PORT)
        {
            auto resp = request->create_response();
            ::marshall(resp, address);
            ::dsn::task::get_current_rpc()->reply(resp, ::dsn::ERR_FORWARD_TO_OTHERS);
        }

        // do real forwarding, not reset request_id, but set forwarded flag
        // TODO(qinzuoyan): reply to client if forwarding failed for non-timeout reason (such as connection denied).
        else
        {
            auto copied_request = request->copy_and_prepare_send();
            call_ip(address, copied_request, nullptr, false, true);
        }
    }
}
