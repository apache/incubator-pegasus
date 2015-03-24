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
# ifdef _WIN32
# include <WinSock2.h>
# else
# include <sys/socket.h>
# include <netdb.h>
# endif

# include "rpc_engine.h"
# include "service_engine.h"
# include <dsn/internal/perf_counters.h>
# include <dsn/internal/factory_store.h>
# include <set>

# define __TITLE__ "rpc.engine"

namespace dsn {
    
    DEFINE_TASK_CODE(LPC_RPC_TIMEOUT, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)

    class rpc_timeout_task : public task
    {
    public:
        rpc_timeout_task(std::shared_ptr<rpc_client_matcher> se, uint64_t id, task_spec* s) : task(LPC_RPC_TIMEOUT)
        {
            _s = se;
            _id = id;
            _spec = s;
        }

        virtual void exec()
        {
            _s->on_rpc_timeout(_id, _spec);
        }

    private:
        std::shared_ptr<rpc_client_matcher> _s;
        uint64_t     _id;
        task_spec  *_spec;
    };

    bool rpc_client_matcher::on_recv_reply(uint64_t key, message_ptr& reply, int delay_ms)
    {
        error_code sys_err = (reply != nullptr) ? reply->error() : ERR_TIMEOUT;
        rpc_response_task_ptr call;
        task_ptr timeout_tsk;
        bool ret;

        {
            utils::auto_lock l(_requests_lock);
            auto it = _requests.find(key);
            if (it != _requests.end())
            {
                call = it->second.resp_task;
                timeout_tsk = it->second.timeout_task;
                _requests.erase(it);
                ret = true;
            }
            else
            {
                ret = false;
            }
        }

        if (call != nullptr)
        {
            if (timeout_tsk != task::get_current_task())
            {
                timeout_tsk->cancel(true);
            }
            
            call->set_delay(delay_ms);
            call->enqueue(sys_err, reply);
        }

        return ret;
    }

    void rpc_client_matcher::on_call(message_ptr& request, rpc_response_task_ptr& call, rpc_client_session_ptr& client)
    {
        message* msg = request.get();
        task_ptr timeout_tsk;
        task_spec* spec = task_spec::get(msg->header().local_rpc_code);
        message_header& hdr = msg->header();

        timeout_tsk = (new rpc_timeout_task(shared_from_this(), hdr.id, spec));

        {
            utils::auto_lock l(_requests_lock);
            auto pr = _requests.insert(rpc_requests::value_type(hdr.id, match_entry()));
            dassert (pr.second, "the message is already on the fly!!!");
            pr.first->second.resp_task = call;
            pr.first->second.timeout_task = timeout_tsk;
            pr.first->second.client = client;
            //{call, timeout_tsk, client }
        }

        int timeout_milliseconds = get_timeout_ms(hdr.client.timeout_milliseconds, spec);
        timeout_tsk->set_delay(timeout_milliseconds);
        timeout_tsk->enqueue();
        msg->add_elapsed_timeout_milliseconds(timeout_milliseconds);

    }

    int32_t rpc_client_matcher::get_timeout_ms(int32_t timeout_ms, task_spec* spec) const
    {
        if (timeout_ms >= spec->rpc_retry_interval_milliseconds * 2)
            return spec->rpc_retry_interval_milliseconds;
        else
            return timeout_ms;
    }

    void rpc_client_matcher::on_rpc_timeout(uint64_t key, task_spec* spec)
    {
        rpc_response_task_ptr call;
        rpc_client_session_ptr client;

        {
            utils::auto_lock l(_requests_lock);
            auto it = _requests.find(key);
            if (it == _requests.end())
                return;

            call = it->second.resp_task;
            client = it->second.client;
        }

        message_ptr& msg = call->get_request();
        int remain_time_ms = msg->header().client.timeout_milliseconds - msg->elapsed_timeout_milliseconds();

        if (remain_time_ms <= 0)
        {
            message_ptr reply(nullptr);
            on_recv_reply(key, reply, 0);
        }
        else
        {
            task_ptr timeout_tsk(new rpc_timeout_task(shared_from_this(), key, spec));
            int timeout_ms = get_timeout_ms(remain_time_ms, spec);
            msg->add_elapsed_timeout_milliseconds(timeout_ms);
            timeout_tsk->set_delay(timeout_ms);
            timeout_tsk->enqueue();

            {
                utils::auto_lock l(_requests_lock);
                auto it = _requests.find(key);
                if (it == _requests.end())
                    return;

                it->second.timeout_task = timeout_tsk;
            }

            client->send(msg);
        }
    }

    //------------------------
    /*static*/ bool rpc_engine::_message_crc_required;
    /*static*/ int  rpc_engine::_max_udp_package_size;

    rpc_engine::rpc_engine(configuration_ptr config, service_node* node)
        : _config(config), _node(node)
    {
        _is_running = false;
        _message_crc_required = false;
        _max_udp_package_size = 63459; /* SO_MAX_MSG_SIZE(65507) - 2KB */
    
        if (config != nullptr)
        {
            _max_udp_package_size = config->get_value<long>("network", "max_udp_package_size", _max_udp_package_size);
            _message_crc_required = config->get_value<bool>("network", "message_crc_required", _message_crc_required);
        }
    }
    
    //
    // management routines
    //
    error_code rpc_engine::start(const service_spec& spec, int port/* = 0*/)
    {
        if (_is_running)
        {
            return ERR_SERVICE_ALREADY_RUNNING;
        }
    
        // local cache for shared networks with same provider and message format and port
        std::map<std::string, network*> named_nets; // factory##fmt##port -> net

        // start to create networks
        network* net = nullptr;
        _networks.resize(rpc_channel::max_value() + 1);
        for (int i = 0; i <= rpc_channel::max_value(); i++)
        {
            auto& fmt_nets = _networks[i];
            fmt_nets.resize(network_formats::instance().max_value() + 1);

            for (int j = 0; j <= network_formats::instance().max_value(); j++)
            {
                network_config_spec cs;
                cs.channel = rpc_channel(rpc_channel::to_string(i));
                cs.message_format = network_formats::instance().get_name(j);

                // find factory and message size
                auto itnc = spec.network_configs.find(cs);
                if (itnc == spec.network_configs.end())
                {
                    dwarn("network [%s.%s] not designated, may result fault when demanded",
                        rpc_channel::to_string(i),
                        network_formats::instance().get_name(j)
                        );
                    continue;
                }
                std::stringstream ss;
                ss << itnc->second.factory_name
                    << "##"
                    << cs.message_format
                    << "##"
                    << port + j
                    ;
                std::string nname = ss.str();

                // create net                
                if (named_nets.find(nname) != named_nets.end())
                    net = named_nets[nname];
                else
                {   
                    net = utils::factory_store<network>::create(
                        itnc->second.factory_name.c_str(), PROVIDER_TYPE_MAIN, this, nullptr);
                    net->reset_parser(itnc->second.message_format, itnc->second.message_buffer_block_size);

                    for (auto it = spec.network_aspects.begin();
                        it != spec.network_aspects.end();
                        it++)
                    {
                        net = utils::factory_store<network>::create(it->c_str(), PROVIDER_TYPE_ASPECT, this, net);
                    }
                                        
                    // start the net
                    error_code ret = net->start(port + j, port + j <= network::max_faked_port_for_client_only_node);
                    if (ret != ERR_SUCCESS)
                    {
                        return ret;
                    }

                    named_nets[nname] = net;
                }

                // put net into _networks;
                fmt_nets[j] = net;
            }
        }

        // report
        for (int i = 0; i <= rpc_channel::max_value(); i++)
        {
            for (int j = 0; j <= network_formats::instance().max_value(); j++)
            {
                auto& fmt_nets = _networks[i];
                if (fmt_nets[j] != nullptr)
                {
                    if (fmt_nets[j]->address().port <= network::max_faked_port_for_client_only_node)
                    {
                        dinfo("network [%s.%s] started at port %u (client only) ...",
                            rpc_channel::to_string(i),
                            network_formats::instance().get_name(j),
                            (uint32_t)fmt_nets[j]->address().port
                            );
                    }
                    else
                    {
                        dinfo("network [%s.%s] started at port %u ...",
                            rpc_channel::to_string(i),
                            network_formats::instance().get_name(j),
                            (uint32_t)fmt_nets[j]->address().port
                            );
                    }
                }
            }
        }

        if (net)
            _address = net->address();
        _address.port = port;  
    
        _is_running = true;
        return ERR_SUCCESS;
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
            auto tsk = handler->handler->new_request_task(msg, node());
            tsk->set_delay(delay_ms);
            tsk->enqueue(_node);
        }
        else
        {
            // TODO: warning about this msg
            dwarn(
                "recv unknown message with type %s from %s:%u",
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
        if (sp->rpc_message_header_format_id == -1)
        {
            auto idx = network_formats::instance().get_id(sp->rpc_message_header_format.c_str());
            dassert(idx != -1, "invalid message format specified '%s' for rpc '%s'",
                sp->rpc_message_header_format.c_str(),
                sp->name                
                );
            sp->rpc_message_header_format_id = idx;
        }

        auto& named_nets = _networks[sp->rpc_message_channel];
        network* net = named_nets[sp->rpc_message_header_format_id];

        dassert(nullptr != net, "network not present for rpc channel '%s' with format '%s' used by rpc %s",
            sp->rpc_message_channel.to_string(),
            sp->rpc_message_header_format.c_str(),
            msg->header().rpc_name
            );

        msg->header().client.port = net->address().port;
        msg->header().from_address = net->address();
        msg->header().new_rpc_id();
        msg->seal(_message_crc_required);

        if (!sp->on_rpc_call.execute(task::get_current_task(), msg, call.get(), true))
        {
            if (call != nullptr)
            {
                message_ptr nil;
                call->set_delay(msg->header().client.timeout_milliseconds);
                call->enqueue(ERR_TIMEOUT, nil);
            }   
            return;
        }

        net->call(request, call);
    }

    void rpc_engine::reply(message_ptr& response)
    {
        message* msg = response.get();
        msg->seal(_message_crc_required);

        auto sp = task_spec::get(msg->header().local_rpc_code);
        if (!sp->on_rpc_reply.execute(task::get_current_task(), msg, true))
            return;
                
        auto s = response->server_session().get();
        dassert (s != nullptr, "rpc server session missing for sending response msg");
        s->send(response);
    }

}
