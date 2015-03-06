# ifdef _WIN32
# include <WinSock2.h>
# else
# include <sys/socket.h>
# include <netdb.h>
# endif

# include "rpc_engine.h"
# include "service_engine.h"
# include <rdsn/internal/perf_counters.h>

# define __TITLE__ "rpc.engine"

namespace rdsn {
    
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

    bool rpc_client_matcher::on_recv_reply(uint64_t key, message_ptr& reply, int delay_handling_milliseconds)
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

            call->enqueue(sys_err, reply, delay_handling_milliseconds);
        }

        return ret;
    }

    void rpc_client_matcher::on_call(message_ptr& request, rpc_response_task_ptr& call, std::shared_ptr<rpc_client_session>& client)
    {
        message* msg = request.get();
        task_ptr timeout_tsk;
        task_spec* spec = task_spec::get(msg->header().local_rpc_code);
        message_header& hdr = msg->header();

        timeout_tsk = (new rpc_timeout_task(shared_from_this(), hdr.id, spec));

        {
            utils::auto_lock l(_requests_lock);
            auto pr = _requests.insert(rpc_requests::value_type(hdr.id, match_entry()));
			rdsn_assert (pr.second, "the message is already on the fly!!!");
			pr.first->second.resp_task = call;
			pr.first->second.timeout_task = timeout_tsk;
			pr.first->second.client = client;
			//{call, timeout_tsk, client }
        }

        int timeout_milliseconds = hdr.timeout_milliseconds > spec->rpc_min_timeout_milliseconds_for_retry ? spec->rpc_retry_interval_milliseconds : hdr.timeout_milliseconds;
        timeout_tsk->enqueue(timeout_milliseconds);
        msg->add_elapsed_timeout_milliseconds(timeout_milliseconds);

    }

    void rpc_client_matcher::on_rpc_timeout(uint64_t key, task_spec* spec)
    {
        rpc_response_task_ptr call;
        std::shared_ptr<rpc_client_session> client;

        {
            utils::auto_lock l(_requests_lock);
            auto it = _requests.find(key);
            if (it == _requests.end())
                return;

            call = it->second.resp_task;
            client = it->second.client;
        }

        message_ptr& msg = call->get_request();
        int remainTime = msg->header().timeout_milliseconds - msg->elapsed_timeout_milliseconds();

        if (remainTime <= 0)
        {
            message_ptr reply(nullptr);
            on_recv_reply(key, reply);
        }
        else
        {
            task_ptr timeout_tsk(new rpc_timeout_task(shared_from_this(), key, spec));
            int timeout = remainTime > spec->rpc_min_timeout_milliseconds_for_retry ? spec->rpc_retry_interval_milliseconds : remainTime;
            msg->add_elapsed_timeout_milliseconds(timeout);
            timeout_tsk->enqueue(timeout);

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

    rpc_engine::rpc_engine(configuration_ptr config, service_node* node)
        : _config(config), _node(*node)
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
    error_code rpc_engine::start(std::map<rpc_channel, network*>& networks, int port/* = 0*/)
    {
        if (_is_running)
        {
            return ERR_SERVICE_ALREADY_RUNNING;
        }
    
        _networks.resize(rpc_channel::max_value() + 1); 
        for (auto& kv : networks)
        {
            _networks[kv.first] = kv.second;
        }            

        for (int i = 0; i <= rpc_channel::max_value(); i++)
        {
            if (_networks[i] == nullptr)
            {
                rdsn_warn("network factory for %s not designated, may result fault when demanded",
                    rpc_channel::to_string(i)
                    );
            }
        }

        bool addr_used_here = false;
        network* net = nullptr;
        for (auto& kv : networks)
        {
            error_code ret = kv.second->start(port);
            if (ret != ERR_SUCCESS)
            {
                if (ret == ERR_ADDRESS_ALREADY_USED && addr_used_here)
                {
                    // reuse the same network
                    delete kv.second;
                    kv.second = net;
                }
                else
                    return ret;
            }
            else if (!addr_used_here)
            {
                addr_used_here = true;
                net = kv.second;
            }
        }
        _address = _networks[RPC_CHANNEL_TCP]->address();
    
        rdsn_debug("rpc server started, listen on port %u...", (int)address().port);
    
        _is_running = true;
        return ERR_SUCCESS;
    }


    bool rpc_engine::register_rpc_handler(rpc_handler_ptr& handler)
    {
        utils::auto_write_lock l(_handlers_lock);
        auto it = _handlers.find(handler->code.to_string());
        if (it == _handlers.end())
        {
            _handlers[handler->code.to_string()] = handler;
            return true;
        }
        else
        {
            return false;
        }
    }

    bool rpc_engine::unregister_rpc_handler(task_code rpc_code)
    {
        utils::auto_write_lock l(_handlers_lock);
        return _handlers.erase(rpc_code.to_string()) > 0;
    }

    void rpc_engine::on_recv_request(message_ptr& msg, int delay_handling_milliseconds)
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
            auto tsk = handler->handler->new_request_task(msg);
            tsk->enqueue(delay_handling_milliseconds, &_node);
        }
        else
        {
            // TODO: warning about this msg
            rdsn_warn(
                "recv unknown message with type %s from %s:%u",
                msg->header().rpc_name,
                msg->header().from_address.name.c_str(),
                (int)msg->header().from_address.port
                );
        }

        //counters.RpcServerQps->increment();
    }

    void rpc_engine::call(message_ptr& request, rpc_response_task_ptr& call)
    {
        message* msg = request.get();
        
        if (call != nullptr)
        {
            msg->header().is_response_expected = true;
        }
        
        msg->header().new_rpc_id();  
        msg->seal(_message_crc_required);

        auto sp = task_spec::get(msg->header().local_rpc_code);

        if (!sp->on_rpc_call.execute(task::get_current_task(), msg, call.get(), true))
        {
            if (call != nullptr)
            {
                message_ptr nil;
                call->enqueue(ERR_TIMEOUT, nil, msg->header().timeout_milliseconds);
            }   
            return;
        }

        network* net = _networks[sp->rpc_message_channel];
        rdsn_assert(nullptr != net, "network not present for rpc channel %s used by rpc %s",
            sp->rpc_message_channel.to_string(),
            msg->header().rpc_name
            );

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
        rdsn_assert(s != nullptr, "rpc server session missing for sending response msg");
        s->send(response);
    }

}
