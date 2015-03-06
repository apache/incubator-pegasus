# pragma once

# include <rdsn/internal/rdsn_types.h>
# include <rdsn/internal/singleton.h>
# include <rdsn/internal/end_point.h>
# include <rdsn/internal/global_config.h>
# include <rdsn/internal/error_code.h>

namespace rdsn { 

class task_engine;
class rpc_engine;
class disk_engine;
class env_provider;
class logging_provider;

class service_node
{
public:    
    service_node();
    
    task_engine* computation() const { return _computation; }
    rpc_engine*  rpc() const { return _rpc; }
    disk_engine* disk() const { return _disk; }
    
    error_code start(const service_spec& spec);   

    const std::string& identity() const { return _id; }
    
private:
    std::string  _id;
    task_engine* _computation;
    rpc_engine*  _rpc;
    disk_engine* _disk;
};

class service_engine : public utils::singleton<service_engine>
{
public:
    service_engine();

    //ServiceMode Mode() const { return _spec.Mode; }
    const service_spec& spec() const { return _spec; }
    env_provider* env() const { return _env; }
    logging_provider* logging() const { return _logging; }
    const end_point& primary_address() const;
    service_node* get_node(uint16_t port) const;
    service_node* primary_node() const { return _primary_node; }
    
    void prepare_minimum_providers_for_toollets(const service_spec& spec);

    error_code start(const service_spec& spec);    
    void configuration_changed(configuration_ptr configuration);

    service_node* start_secondary(uint16_t port);

private:
    service_spec                    _spec;
    bool                           _is_running;
    env_provider*                   _env;
    logging_provider*               _logging;
    service_node*                   _primary_node;

    // <port, servicenode>
    typedef std::map<uint16_t, service_node*> node_engines;
    node_engines                    _engines;
};

// ------------ inline impl ---------------------
inline service_node* service_engine::get_node(uint16_t port) const
{
    auto it = _engines.find(port);
    if (it != _engines.end())
        return it->second;
    else
        return nullptr;
}

} // end namespace
