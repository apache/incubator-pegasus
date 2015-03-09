# pragma once

# include <rdsn/internal/task.h>
# include <rdsn/internal/network.h>
# include <rdsn/internal/synchronize.h>

namespace rdsn {

class rpc_client_matcher : public std::enable_shared_from_this<rpc_client_matcher>
{
public:
    void on_call(message_ptr& request, rpc_response_task_ptr& call, rpc_client_session_ptr& client);
    bool on_recv_reply(uint64_t key, message_ptr& reply, int delay_handling_milliseconds = 0);
    
private:
    friend class rpc_timeout_task;
    void on_rpc_timeout(uint64_t key, task_spec* spec);

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
    error_code start(std::map<rpc_channel, network*>& networks, int port = 0);

    //
    // rpc registrations
    //
    bool register_rpc_handler(rpc_handler_ptr& handler);
    bool unregister_rpc_handler(task_code rpc_code) ;    

    //
    // rpc routines
    //
    void call(message_ptr& request, rpc_response_task_ptr& call);
    void reply(message_ptr& response);
    
    //
    // information inquery
    //
    const end_point& address() const { return _address; }

private:
    friend class rpc_server_session;    
    void on_recv_request(message_ptr& msg, int delay_handling_milliseconds = 0);
        
private:
    configuration_ptr             _config;    
    service_node                  &_node;
    std::vector<network*>         _networks;
    std::shared_ptr<rpc_client_matcher>   _matcher;


    typedef std::map<std::string, rpc_handler_ptr> rpc_handlers;
    rpc_handlers                  _handlers;
    utils::rw_lock                _handlers_lock;
    
    bool                          _is_running;
    bool                          _message_crc_required;
    int                           _max_udp_package_size;
    end_point                     _address;
};

} // end namespace

