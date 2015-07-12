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
# ifdef _WIN32
# include <WinSock2.h>
# else
# include <sys/socket.h>
# include <netdb.h>
# endif

# include "rpc_engine.h"
# include "service_engine.h"
# include <dsn/service_api.h>
# include <dsn/internal/perf_counters.h>
# include <dsn/internal/factory_store.h>
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
        rpc_timeout_task(rpc_client_matcher* matcher, uint64_t id) 
            : task(LPC_RPC_TIMEOUT)
        {
            _matcher = matcher;
            _id = id;
        }

        virtual void exec()
        {
            _matcher->on_rpc_timeout(_id);
        }

    private:
        rpc_client_matcher_ptr _matcher;
        uint64_t               _id;
    };

    rpc_client_matcher::~rpc_client_matcher()
    {
        dassert(_requests.size() == 0, "all rpc enries must be removed before the matcher ends");
    }

    bool rpc_client_matcher::on_recv_reply(uint64_t key, message_ptr& reply, int delay_ms)
    {
        dassert(reply != nullptr, "cannot receive an empty reply message");

        rpc_response_task_ptr call;
        task_ptr timeout_task;

        {
            utils::auto_lock<::dsn::utils::ex_lock_nr_spin> l(_requests_lock);
            auto it = _requests.find(key);
            if (it != _requests.end())
            {
                call = it->second.resp_task;
                timeout_task = std::move(it->second.timeout_task);
                _requests.erase(it);
            }
            else
            {
                return false;
            }
        }

        if (call != nullptr)
        {
            if (timeout_task != task::get_current_task())
            {
                timeout_task->cancel(true);
            }
            
            call->set_delay(delay_ms);
            call->enqueue(reply->error(), reply);
        }

        return true;
    }

    void rpc_client_matcher::on_rpc_timeout(uint64_t key)
    {
        rpc_response_task_ptr call;

        {
            utils::auto_lock<::dsn::utils::ex_lock_nr_spin> l(_requests_lock);
            auto it = _requests.find(key);
            if (it != _requests.end())
            {
                call = std::move(it->second.resp_task);
                _requests.erase(it);
            }
            else
            {
                return;
            }
        }

        message_ptr null_msg(nullptr);
        call->enqueue(ERR_TIMEOUT, null_msg);
    }
    
    void rpc_client_matcher::on_call(message_ptr& request, rpc_response_task_ptr& call)
    {
        message* msg = request.get();
        task* timeout_task;
        message_header& hdr = msg->header();

        timeout_task = (new rpc_timeout_task(this, hdr.id));

        {
            utils::auto_lock<::dsn::utils::ex_lock_nr_spin> l(_requests_lock);
            auto pr = _requests.insert(rpc_requests::value_type(hdr.id, match_entry()));
            dassert (pr.second, "the message is already on the fly!!!");
            pr.first->second.resp_task = call;
            pr.first->second.timeout_task = timeout_task;
        }

        timeout_task->set_delay(msg->header().client.timeout_ms);
        timeout_task->enqueue();
    }

    //------------------------
    /*static*/ bool rpc_engine::_message_crc_required;

    rpc_engine::rpc_engine(configuration_ptr config, service_node* node)
        : _config(config), _node(node)
    {
        dassert (_node != nullptr, "");
        dassert (_config != nullptr, "");

        _is_running = false;
        _local_primary_address = end_point::INVALID;
        _message_crc_required = config->get_value<bool>("network", "message_crc_required", false);
    }
    
    //
    // management routines
    //
    network* rpc_engine::create_network(const network_server_config& netcs, bool client_only)
    {
        const service_spec& spec = service_engine::instance().spec();
        auto net = utils::factory_store<network>::create(
            netcs.factory_name.c_str(), PROVIDER_TYPE_MAIN, this, nullptr);
        net->reset_parser(netcs.hdr_format, netcs.message_buffer_block_size);

        for (auto it = spec.network_aspects.begin();
            it != spec.network_aspects.end();
            it++)
        {
            net = utils::factory_store<network>::create(it->c_str(), PROVIDER_TYPE_ASPECT, this, net);
        }

        // start the net
        error_code ret = net->start(netcs.channel, netcs.port, client_only);
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

    error_code rpc_engine::start(const service_app_spec& aspec)
    {
        if (_is_running)
        {
            return ERR_SERVICE_ALREADY_RUNNING;
        }
    
        // local cache for shared networks with same provider and message format and port
        std::map<std::string, network*> named_nets; // factory##fmt##port -> net

        // start client networks
        _client_nets.resize(network_header_format::max_value() + 1);

        const service_spec& spec = service_engine::instance().spec();

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

                auto net = create_network(cs, true);
                if (!net) return ERR_NETWORK_INIT_FALED;
                pnet[j] = net;
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

            auto net = create_network(sp.second, false);
            if (net == nullptr)
            {
                return ERR_NETWORK_INIT_FALED;
            }

            (*pnets)[sp.second.channel] = net;

            dinfo("network started at port %u, channel = %s, fmt = %s ...",
                (uint32_t)port,
                sp.second.channel.to_string(),
                sp.second.hdr_format.to_string()
                );
        }

        _local_primary_address = _client_nets[0][0]->address();
        _local_primary_address.port = aspec.ports.size() > 0 ? *aspec.ports.begin() : aspec.id;

        _is_running = true;
        return ERR_OK;
    }
    
    bool rpc_engine::register_rpc_handler(rpc_handler_ptr& handler)
    {
        utils::auto_write_lock l(_handlers_lock);
        auto it = _handlers.find(handler->code.to_string());
        auto it2 = _handlers.find(handler->name);
        if (it == _handlers.end() && it2 == _handlers.end())
        {
            _handlers[handler->code.to_string()] = handler;
            _handlers[handler->name] = handler;
            return true;
        }
        else
        {
            dassert(false, "rpc registration confliction for '%s'", handler->code.to_string());
            return false;
        }
    }

    bool rpc_engine::unregister_rpc_handler(task_code rpc_code)
    {
        utils::auto_write_lock l(_handlers_lock);
        auto it = _handlers.find(rpc_code.to_string());
        if (it == _handlers.end())
            return false;

        std::string name = it->second->name;
        _handlers.erase(it);
        _handlers.erase(name);
        return true;
    }

    void rpc_engine::on_recv_request(message_ptr& msg, int delay_ms)
    {
        rpc_handler_ptr handler;
        {
            utils::auto_read_lock l(_handlers_lock);
            auto it = _handlers.find(msg->header().rpc_name);
            if (it != _handlers.end())
            {
                handler = it->second;
            }
        }

        if (handler != nullptr)
        {
            msg->header().local_rpc_code = (uint16_t)handler->code;
            rpc_request_task_ptr tsk = handler->handler->new_request_task(msg, node());
            tsk->set_delay(delay_ms);
            tsk->enqueue(_node);
        }
        else
        {
            // TODO: warning about this msg
            dwarn(
                "recv unknown message with type %s from %s:%d",
                msg->header().rpc_name,
                msg->header().from_address.name.c_str(),
                static_cast<int>(msg->header().from_address.port)
                );
        }

        //counters.RpcServerQps->increment();
    }

    void rpc_engine::call(message_ptr& request, rpc_response_task_ptr& call)
    {
        message* msg = request.get();
        
        auto sp = task_spec::get(msg->header().local_rpc_code);
        auto nts_us = ::dsn::service::env::now_us();
        auto& named_nets = _client_nets[sp->rpc_call_header_format];
        network* net = named_nets[sp->rpc_call_channel];

        dassert(nullptr != net, "network not present for rpc channel '%s' with format '%s' used by rpc %s",
            sp->rpc_call_channel.to_string(),
            sp->rpc_call_header_format.to_string(),
            msg->header().rpc_name
            );

        msg->header().client.port = primary_address().port;
        msg->header().from_address = primary_address();
        msg->header().new_rpc_id();
        msg->seal(_message_crc_required);

        if (!sp->on_rpc_call.execute(task::get_current_task(), msg, call.get(), true))
        {
            if (call != nullptr)
            {
                message_ptr nil;
                call->set_delay(msg->header().client.timeout_ms);
                call->enqueue(ERR_TIMEOUT, nil);
            }   
            return;
        }

        net->call(request, call);
    }

    void rpc_engine::reply(message_ptr& response)
    {
        auto s = response->server_session().get();
        if (s == nullptr)
            return;

        message* msg = response.get();
        msg->seal(_message_crc_required);

        auto sp = task_spec::get(msg->header().local_rpc_code);
        if (!sp->on_rpc_reply.execute(task::get_current_task(), msg, true))
            return;
                
        s->send(response);
    }
}
