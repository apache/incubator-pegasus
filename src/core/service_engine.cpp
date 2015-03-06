# include "service_engine.h"
# include "task_engine.h"
# include "disk_engine.h"
# include "rpc_engine.h"
# include <rdsn/internal/env_provider.h>
# include <rdsn/internal/perf_counters.h>
# include <rdsn/internal/factory_store.h>
# include <rdsn/internal/logging.h>
# include <rdsn/tool_api.h>

#define __TITLE__ "service_engine"

using namespace rdsn::utils;

namespace rdsn {

service_node::service_node(void)
{
    _computation = nullptr;
    _rpc = nullptr;
    _disk = nullptr;
}

error_code service_node::start(const service_spec& spec)
{
    char port[6];
    sprintf(port, "%u", spec.port);
    _id = port;
    
    // init task engine
    _computation = new task_engine(this);
    _computation->start(spec.threadpool_specs);    
    rdsn_assert (_computation->is_started(), "task engine must be started at this point");

    // init disk engine
    _disk = new disk_engine(this);
    aio_provider* aio = factory_store<aio_provider>::create(spec.aio_factory_name.c_str(), PROVIDER_TYPE_MAIN, _disk, nullptr);
    for (auto it = spec.aio_aspects.begin();
        it != spec.aio_aspects.end();
        it++)
    {
        aio = factory_store<aio_provider>::create(it->c_str(), PROVIDER_TYPE_ASPECT, _disk, aio);
    }
    _disk->start(aio);
    
    // init rpc engine
    _rpc = new rpc_engine(spec.config, this);

    // init all networks
    std::map<rpc_channel, network*> nets;
    
    for (auto& kv : spec.network_factory_names)
    {
        network* net = factory_store<network>::create(kv.second.c_str(), PROVIDER_TYPE_MAIN, _rpc, nullptr);
        for (auto it = spec.network_aspects.begin();
            it != spec.network_aspects.end();
            it++)
        {
            net = factory_store<network>::create(it->c_str(), PROVIDER_TYPE_ASPECT, _rpc, net);
        }
        nets[kv.first] = net;
    }

    error_code err = _rpc->start(nets, spec.port);
    return err;
}

//////////////////////////////////////////////////////////////////////////////////////////

service_engine::service_engine(void)
{
    _is_running = false;
    _env = nullptr;
    _logging = nullptr;
}

void service_engine::prepare_minimum_providers_for_toollets(const service_spec& spec)
{
    _logging = factory_store<logging_provider>::create(spec.logging_factory_name.c_str(), PROVIDER_TYPE_MAIN, nullptr);

    // init perf counter provider
    perf_counters::instance().register_factory(factory_store<perf_counter>::get_factory<perf_counter_factory>(spec.perf_counter_factory_name.c_str(), PROVIDER_TYPE_MAIN));
}

error_code service_engine::start(const service_spec& spec)
{
    rdsn_assert (!_is_running, "cannot start as it is running!");

    std::string factoryName;
    
    _spec = spec;
        
    // init log provider    
    if (nullptr != _logging) delete _logging;
    _logging = factory_store<logging_provider>::create(spec.logging_factory_name.c_str(), PROVIDER_TYPE_MAIN, nullptr);

    //std::srand(_spec.failure_options.GetCurrentRandomSeed());

    // init perf counter provider
    perf_counters::instance().register_factory(factory_store<perf_counter>::get_factory<perf_counter_factory>(_spec.perf_counter_factory_name.c_str(), PROVIDER_TYPE_MAIN));
    
    // init env provider
    _env = factory_store<env_provider>::create(spec.env_factory_name.c_str(), PROVIDER_TYPE_MAIN, nullptr);
    for (auto it = spec.env_aspects.begin();
        it != spec.env_aspects.end();
        it++)
    {
        _env = factory_store<env_provider>::create(it->c_str(), PROVIDER_TYPE_ASPECT, _env);
    }      
    
    // init primary rpc/disk/tasking
    _primary_node = new service_node();    
    error_code err = _primary_node->start(spec);
    _engines[_primary_node->rpc()->address().port] = _primary_node;

    _is_running = true;
    return err;
}

service_node* service_engine::start_secondary(uint16_t port)
{
    if (0 == port)
    {
        return _primary_node;
    }

    auto it = _engines.find(port);
    if (it != _engines.end())
    {
        return it->second;
    }
    else
    {
        service_spec spec = _spec;
        spec.port = port;

        auto node = new service_node();
        error_code err = node->start(spec);
        rdsn_assert (err == 0, "service node start failed, err = %s", err.to_string());
        _engines[node->rpc()->address().port] = node;

        return node;
    }
}

const end_point& service_engine::primary_address() const
{ 
    return _primary_node->rpc()->address(); 
}

void service_engine::configuration_changed(configuration_ptr configuration)
{
    task_spec::init(configuration);
}

} // end namespace
