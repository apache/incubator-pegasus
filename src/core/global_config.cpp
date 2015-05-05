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
# include <dsn/internal/global_config.h>
# include <thread>
# include <dsn/internal/logging.h>
# include <dsn/internal/task_code.h>
# include <dsn/internal/network.h>

#define __TITLE__ "ConfigFile"

namespace dsn {

threadpool_spec::threadpool_spec(const threadpool_spec& source)
    : pool_code(source.pool_code)
{
    *this = source;
}

threadpool_spec& threadpool_spec::operator=(const threadpool_spec& source)
{
    name = source.name;
    pool_code.reset(source.pool_code);
    run = source.run;
    worker_count = source.worker_count;
    worker_priority = source.worker_priority;
    worker_share_core = source.worker_share_core;
    worker_affinity_mask = source.worker_affinity_mask;
    max_input_queue_length = source.max_input_queue_length;
    partitioned = source.partitioned;
    
    queue_factory_name = source.queue_factory_name;
    worker_factory_name = source.worker_factory_name;
    queue_aspects = source.queue_aspects;
    worker_aspects = source.worker_aspects;

    admission_controller_factory_name = source.admission_controller_factory_name;
    admission_controller_arguments = source.admission_controller_arguments;

    return *this;
}

bool threadpool_spec::init(configuration_ptr& config, __out_param std::vector<threadpool_spec>& specs)
{
    /*
    [threadpool.default]
    worker_count = 4
    worker_priority = THREAD_xPRIORITY_NORMAL
    max_input_queue_length = 10000
    partitioned = false
    queue_aspects = xxx
    worker_aspects = xxx
    admission_controller_factory_name = xxx
    admission_controller_arguments = xxx

    [threadpool.THREAD_POOL_REPLICATION]
    name = Thr.replication
    run = true
    worker_count = 4
    worker_priority = THREAD_xPRIORITY_NORMAL
    max_input_queue_length = 10000
    partitioned = false
    queue_aspects = xxx
    worker_aspects = xxx
    admission_controller_factory_name = xxx
    admission_controller_arguments = xxx
    */

    threadpool_spec default_spec("placeholder");
    default_spec.worker_priority = enum_from_string(config->get_string_value("threadpool.default", "worker_priority", "THREAD_xPRIORITY_NORMAL").c_str(), THREAD_xPRIORITY_INVALID);
    if (default_spec.worker_priority == THREAD_xPRIORITY_INVALID)
    {
        dlog(log_level_ERROR, __TITLE__,  "invalid worker priority in [threadpool.default]");
        return false;
    }
    default_spec.worker_share_core = config->get_value<bool>("threadpool.default", "worker_share_core", true);
    default_spec.worker_affinity_mask = static_cast<uint64_t>(config->get_value<int64_t>("threadpool.default", "worker_affinity_mask", 0));
    if (false == default_spec.worker_share_core && 0 == default_spec.worker_affinity_mask)
    {
        default_spec.worker_affinity_mask = (1 << std::thread::hardware_concurrency()) - 1;
    }
    
    default_spec.run = false;
    default_spec.worker_count = config->get_value<int>("threadpool.default", "worker_count", 1);
    default_spec.max_input_queue_length = config->get_value<int>("threadpool.default", "max_input_queue_length", 0xFFFFFFFFUL);
    default_spec.partitioned = config->get_value<bool>("threadpool.default", "partitioned", false);
    default_spec.queue_aspects = config->get_string_value_list("threadpool.default", "queue_aspects", ',');
    default_spec.worker_aspects = config->get_string_value_list("threadpool.default", "worker_aspects", ',');
    default_spec.admission_controller_factory_name = config->get_string_value("threadpool.default", "admission_controller_factory_name", "");
    default_spec.admission_controller_arguments = config->get_string_value("threadpool.default", "admission_controller_arguments", "");
    
    for (int code = 0; code <= threadpool_code::max_value(); code++)
    {
        if (code == THREAD_POOL_INVALID || code == threadpool_code::from_string("placeholder", THREAD_POOL_INVALID))
            continue;

        std::string section_name = std::string("threadpool.") + std::string(threadpool_code::to_string(code));
        threadpool_spec spec(default_spec);
        spec.pool_code.reset(threadpool_code::from_string(threadpool_code::to_string(code), THREAD_POOL_INVALID));
        spec.name = std::string(threadpool_code::to_string(code));
        spec.run = true;
        
        if (config->has_section(section_name.c_str()))
        {
            spec.name = config->get_string_value(section_name.c_str(), "name", threadpool_code::to_string(code));
            spec.run = config->get_value<bool>(section_name.c_str(), "run", true);
            spec.worker_count = config->get_value<int>(section_name.c_str(), "worker_count", default_spec.worker_count);
            spec.max_input_queue_length = config->get_value<int>(section_name.c_str(), "max_input_queue_length", default_spec.max_input_queue_length);
            spec.partitioned = config->get_value<bool>(section_name.c_str(), "partitioned", default_spec.partitioned);
            spec.queue_aspects = config->get_string_value_list(section_name.c_str(), "queue_aspects", ',');
            spec.worker_priority = enum_from_string(config->get_string_value(section_name.c_str(), "worker_priority", "THREAD_xPRIORITY_NORMAL").c_str(), THREAD_xPRIORITY_INVALID);
            
            spec.worker_share_core = config->get_value<bool>(section_name.c_str(), "worker_share_core", true);
            spec.worker_affinity_mask = static_cast<uint64_t>(config->get_value<int64_t>(section_name.c_str(), "worker_affinity_mask", 0));
            if (false == spec.worker_share_core && 0 == spec.worker_affinity_mask)
            {
                spec.worker_affinity_mask = (1 << std::thread::hardware_concurrency()) - 1;
            }
            
            if (spec.queue_aspects.size() == 0)
            {
                spec.queue_aspects = default_spec.queue_aspects;
            }

            spec.worker_aspects = config->get_string_value_list(section_name.c_str(), "worker_aspects", ',');
            if (spec.worker_aspects.size() == 0)
            {
                spec.worker_aspects = default_spec.worker_aspects;
            }

            spec.admission_controller_factory_name = config->get_string_value(section_name.c_str(), "admission_controller_factory_name", default_spec.admission_controller_factory_name.c_str());
            spec.admission_controller_arguments = config->get_string_value(section_name.c_str(), "admission_controller_arguments", default_spec.admission_controller_arguments.c_str());
        }

        if (spec.run)
        {
            specs.push_back(spec);
        }
    }

    return true;
}

service_app_spec::service_app_spec(const service_app_spec& r)
{
    id = r.id;
    name = r.name;
    type = r.type;
    arguments = r.arguments;
    ports = r.ports;
    delay_seconds = r.delay_seconds;
    run = r.run;
}

bool service_app_spec::init(const char* section, configuration_ptr config)
{
    id = 0;
    name = config->get_string_value(section, "name", "");
    type = config->get_string_value(section, "type", "");
    arguments = config->get_string_value(section, "arguments", "");

    ports.clear();
    std::list<std::string> ports_str = config->get_string_value_list(section, "ports", ',');
    for (auto& s : ports_str)
    {
        int p = atoi(s.c_str());
        if (p != 0)
        {
            dassert(p > 1024, "specified port is either 0 (no listen port) or greater than 1024");
            ports.push_back(p);
        }
    }
    std::sort(ports.begin(), ports.end());

    delay_seconds = config->get_value<int>(section, "delay_seconds", 0);    
    run = config->get_value<bool>(section, "run", true);

    return true;
}

network_config_spec::network_config_spec(const network_config_spec& r)
: channel(r.channel)
{
    port = r.port;
    message_format = r.message_format;
    factory_name = r.factory_name;
    message_buffer_block_size = r.message_buffer_block_size;
}

network_config_spec::network_config_spec(int p, rpc_channel c)
    : channel(c)
{
    port = p;

    message_format = "dsn";
    factory_name = "dsn::tools::asio_network_provider";
    message_buffer_block_size = 65536;
}

bool network_config_spec::operator < (const network_config_spec& r) const
{
    return port < r.port || (port == r.port && channel < r.channel);
}

bool service_spec::register_network(const network_config_spec& netcs, bool force)
{
    if (force)
    {
        network_configs[netcs] = netcs;
        network_formats::instance().register_id(netcs.message_format.c_str());
        return true;
    }
    else
    {
        auto it = network_configs.find(netcs);
        if (it == network_configs.end())
        {
            network_configs[netcs] = netcs;
            network_formats::instance().register_id(netcs.message_format.c_str());
            return true;
        }
        else
            return false;
    }    
}

bool service_spec::init(configuration_ptr c)
{
    std::vector<std::string> poolIds;

    config = c;
    tool = config->get_string_value("core", "tool", "");
    toollets = config->get_string_value_list("core", "toollets", ',');
    coredump_dir = config->get_string_value("core", "coredump_dir", "./coredump");
    
    aio_factory_name = config->get_string_value("core", "aio_factory_name", "");
    env_factory_name = config->get_string_value("core", "env_factory_name", "");
    lock_factory_name = config->get_string_value("core", "lock_factory_name", "");
    rwlock_factory_name = config->get_string_value("core", "rwlock_factory_name", "");
    semaphore_factory_name = config->get_string_value("core", "semaphore_factory_name", "");
    nfs_factory_name = config->get_string_value("core", "nfs_factory_name", "");

    network_aspects = config->get_string_value_list("core", "network_aspects", ',');
    aio_aspects = config->get_string_value_list("core", "aio_aspects", ',');
    env_aspects = config->get_string_value_list("core", "env_aspects", ',');

    lock_aspects = config->get_string_value_list("core", "lock_aspects", ',');
    rwlock_aspects = config->get_string_value_list("core", "rwlock_aspects", ',');
    semaphore_aspects = config->get_string_value_list("core", "semaphore_aspects", ',');
    
    perf_counter_factory_name = config->get_string_value("core", "perf_counter_factory_name", "");
    logging_factory_name = config->get_string_value("core", "logging_factory_name", "");

    // init thread pools
    threadpool_spec::init(config, threadpool_specs);

    // init task specs
    task_spec::init(config);

    // init service apps
    std::vector<std::string> allSectionNames;
    config->get_all_sections(allSectionNames);
    
    int app_id = 0;
    for (auto it = allSectionNames.begin(); it != allSectionNames.end(); it++)
    {
        if (it->substr(0, strlen("apps.")) == std::string("apps."))
        {
            service_app_spec app;
            app.init((*it).c_str(), config);

            auto ports = app.ports;            
            auto gap = ports.size() > 0 ? (*ports.rbegin() + 1 - *ports.begin()) : 0;            
            int count = config->get_value<int>((*it).c_str(), "count", 1);
            std::string name = app.name;
            for (int i = 1; i <= count; i++)
            {
                char buf[16];
                sprintf(buf, ".%u", i);
                app.name = (count > 1 ? (name + buf) : name);
                app.id = ++app_id;

                // network configs
                for (auto& p : ports)
                {
                    if (1 == i)
                    {
                        if (!build_network_spec(p))
                            return false;
                    }
                    else
                    {
                        for (auto& cs : network_configs)
                        {
                            if (cs.first.port == p)
                            {
                                auto csc = cs.first;
                                csc.port = p + i * gap;

                                if (!register_network(csc, false))
                                {
                                    printf("register network configuration confliction for port %d used by %s.%d\n",
                                        csc.port,
                                        app.name.c_str(),
                                        i
                                        );
                                    return false;
                                }
                            }
                        }
                    }
                }

                // add app
                app_specs.push_back(app);

                // for next instance
                app.ports.clear();
                for (auto& p : ports)
                {
                    app.ports.push_back(p + i * gap);
                }
            }
        }
    }

    return true;
}

bool service_spec::build_network_spec(int port)
{
    /*
    [network.27001]
    ;channel = message_format,network_provider_name,buffer_block_size
    RPC_CHANNEL_TCP = dsn,dsn::tools::asio_network_provider,65536
    RPC_CHANNEL_UDP = dsn,dsn::tools::asio_network_provider,65536
    */
    std::stringstream ss;
    ss << "network." << port;
    std::string s = ss.str();

    if (!config->has_section(s.c_str()))
    {
        // use default settings
        return true;
    }
       
    
    std::vector<std::string> cs;
    config->get_all_keys(s.c_str(), cs);

    for (auto& c : cs)
    {
        if (!rpc_channel::is_exist(c.c_str()))
        {
            printf("invalid rpc channel type '%s', please following the example below to define new channel:"
                "\t\tDEFINE_CUSTOMIZED_ID(rpc_channel, RPC_CHANNEL_NEW_TYPE)"
                "currently regisered rpc channels types are:\n", c.c_str());

            for (int i = 0; i <= rpc_channel::max_value(); i++)
            {
                printf("\t\t%s (%u)\n", rpc_channel::to_string(i), i);
            }
            return false;
        }

        network_config_spec ns(port, rpc_channel(c.c_str()));

        // dsn,dsn::tools::asio_network_provider,65536
        std::list<std::string> vs;
        std::string v = config->get_string_value(s.c_str(), c.c_str(), "");
        utils::split_args(v.c_str(), vs, ',');

        if (vs.size() != 3)
        {
            printf("invalid network specification '%s', should be '$message-format, $network-factory,$msg-buffer-size'\n",
                s.c_str()
                );
            return false;
        }

        ns.message_format = *vs.begin();
        network_formats::instance().register_id(ns.message_format.c_str());

        ns.factory_name = *(++vs.begin());
        ns.message_buffer_block_size = atoi(vs.rbegin()->c_str());
        if (ns.message_buffer_block_size == 0)
        {
            printf("invalid message buffer size specified: '%s'\n", vs.rbegin()->c_str());
            return false;
        }

        if (!register_network(ns, false))
        {
            printf("register network configuration confliction for port %d\n", port);
            return false;
        }
    }
    return true;
}


} // end namespace dsn
