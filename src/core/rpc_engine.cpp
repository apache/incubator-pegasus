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
        dassert(reply != nullptr, "cannot recieve an empty reply message");

        error_code sys_err = reply->error();
        rpc_response_task_ptr call;
        task_ptr timeout_task;
        bool ret;

        {
            utils::auto_lock l(_requests_lock);
            auto it = _requests.find(key);
            if (it != _requests.end())
            {
                call = it->second.resp_task;
                timeout_task = it->second.timeout_task;
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
            if (timeout_task != task::get_current_task())
            {
                timeout_task->cancel(true);
            }
            
            call->set_delay(delay_ms);
            call->enqueue(sys_err, reply);
        }

        return ret;
    }

    void rpc_client_matcher::on_rpc_timeout(uint64_t key, task_spec* spec)
    {
        rpc_response_task_ptr call;
        network* net;

        {
            utils::auto_lock l(_requests_lock);
            auto it = _requests.find(key);
            if (it != _requests.end())
            {
                call = it->second.resp_task;
                net = it->second.net;
                _requests.erase(it);
            }
            else
            {
                return;
            }
        }

        message_ptr& msg = call->get_request();
        auto nts = ::dsn::service::env::now_us();
        
        // timeout already
        if (nts >= msg->header().client.timeout_ts_us)
        {
            message_ptr null_msg(nullptr);
            call->enqueue(ERR_TIMEOUT, null_msg);
        }
        else
        {
            net->call(msg, call);
        }
    }


    void rpc_client_matcher::on_call(message_ptr& request, rpc_response_task_ptr& call, network* net)
    {
        message* msg = request.get();
        task_ptr timeout_task;
        task_spec* spec = task_spec::get(msg->header().local_rpc_code);
        message_header& hdr = msg->header();

        timeout_task = (new rpc_timeout_task(shared_from_this(), hdr.id, spec));

        {
            utils::auto_lock l(_requests_lock);
            auto pr = _requests.insert(rpc_requests::value_type(hdr.id, match_entry()));
            dassert (pr.second, "the message is already on the fly!!!");
            pr.first->second.resp_task = call;
            pr.first->second.timeout_task = timeout_task;
            pr.first->second.net = net;
            //{call, timeout_task, net }
        }

        auto nts = ::dsn::service::env::now_us();
        auto tts = msg->header().client.timeout_ts_us;

        int timeout_ms = static_cast<int>(tts > nts ? (tts - nts) : 0)/1000;
        if (timeout_ms >= spec->rpc_retry_interval_milliseconds * 2)
            timeout_ms = spec->rpc_retry_interval_milliseconds;

        timeout_task->set_delay(timeout_ms);
        timeout_task->enqueue();
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
        _local_primary_address = end_point::INVALID;

        if (config != nullptr)
        {
            _max_udp_package_size = config->get_value<long>("network", "max_udp_package_size", _max_udp_package_size);
            _message_crc_required = config->get_value<bool>("network", "message_crc_required", _message_crc_required);
        }
    }
    
    //
    // management routines
    //
    network* rpc_engine::create_network(const network_config_spec& netcs, bool client_only)
    {
        const service_spec& spec = service_engine::instance().spec();
        auto net = utils::factory_store<network>::create(
            netcs.factory_name.c_str(), PROVIDER_TYPE_MAIN, this, nullptr);
        net->reset_parser(netcs.message_format, netcs.message_buffer_block_size);

        for (auto it = spec.network_aspects.begin();
            it != spec.network_aspects.end();
            it++)
        {
            net = utils::factory_store<network>::create(it->c_str(), PROVIDER_TYPE_ASPECT, this, net);
        }

        // start the net
        error_code ret = net->start(netcs.port, client_only);
        if (ret == ERR_SUCCESS)
        {
            return net;
        }
        else
        {
            // mem leak, don't care as it halts the program
            return nullptr;
        }   
    }

    bool rpc_engine::start_server_port(int port)
    {
        // exsiting servers
        if (_server_nets.find(port) != _server_nets.end())
            return false;

        std::vector<network*>* pnets;
        std::vector<network*> nets;
        auto pr = _server_nets.insert(std::map<int, std::vector<network*>>::value_type(port, nets));
        pnets = &pr.first->second;

        pnets->resize(rpc_channel::max_value() + 1);
        const service_spec& spec = service_engine::instance().spec();
        for (int i = 0; i <= rpc_channel::max_value(); i++)
        {
            network_config_spec cs(port, rpc_channel(rpc_channel::to_string(i)));
            network* net = nullptr;

            auto it = spec.network_configs.find(cs);
            if (it != spec.network_configs.end())
            {
                net = create_network(it->second, false);
            }

            // create default when for TCP
            else if (i == RPC_CHANNEL_TCP)
            {
                net = create_network(cs, false);
            }

            (*pnets)[i] = net;

            // report
            if (net)
            {
                dinfo("network started at port %u, channel = %s, fmt = %s ...",                    
                    (uint32_t)port,
                    rpc_channel::to_string(i),
                    cs.message_format.c_str()
                    );
            }
        }
        return true;
    }

    error_code rpc_engine::start(int app_id, const std::vector<int>& ports)
    {
        if (_is_running)
        {
            return ERR_SERVICE_ALREADY_RUNNING;
        }
    
        // local cache for shared networks with same provider and message format and port
        std::map<std::string, network*> named_nets; // factory##fmt##port -> net

        // start client networks
        bool r;
        _client_nets.resize(network_formats::instance().max_value() + 1);
        for (int i = 0; i <= network_formats::instance().max_value(); i++)
        {
            std::vector<network*>& pnet = _client_nets[i];
            pnet.resize(rpc_channel::max_value() + 1);
            for (int j = 0; j <= rpc_channel::max_value(); j++)
            {
                network_config_spec cs(app_id, rpc_channel(rpc_channel::to_string(j)));
                auto net = create_network(cs, true);
                if (!net) return ERR_NETWORK_INIT_FALED;
                pnet[j] = net;
            }
        }
        
        // start server networks
        for (auto& p : ports)
        {
            r = start_server_port(p);
            if (!r) return ERR_NETWORK_INIT_FALED;
        }

        _local_primary_address = _client_nets[0][0]->address();
        _local_primary_address.port = ports.size() > 0 ? *ports.begin() : app_id;

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
        if (sp->rpc_call_header_format == -1)
        {
            auto idx = network_formats::instance().get_id(sp->rpc_call_header_format_name.c_str());
            dassert(idx != -1, "invalid message format specified '%s' for rpc '%s'",
                sp->rpc_call_header_format_name.c_str(),
                sp->name                
                );
            sp->rpc_call_header_format = idx;
        }

        auto& named_nets = _client_nets[sp->rpc_call_header_format];
        network* net = named_nets[sp->rpc_call_channel];

        dassert(nullptr != net, "network not present for rpc channel '%s' with format '%s' used by rpc %s",
            sp->rpc_call_channel.to_string(),
            sp->rpc_call_header_format_name.c_str(),
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
                auto nts = ::dsn::service::env::now_us();
                int delay_ms = static_cast<int>((msg->header().client.timeout_ts_us - nts) / 1000);

                message_ptr nil;
                call->set_delay(delay_ms);
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
