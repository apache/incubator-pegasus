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
# include <rdsn/internal/global_config.h>
# include <thread>
# include <rdsn/internal/logging.h>
# include <rdsn/internal/task_code.h>
# include <rdsn/internal/network.h>

#define __TITLE__ "ConfigFile"

namespace rdsn {

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
    max_input_queue_length = -1
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
    max_input_queue_length = -1
    partitioned = false
    queue_aspects = xxx
    worker_aspects = xxx
    admission_controller_factory_name = xxx
    admission_controller_arguments = xxx
    */

    threadpool_spec defaultSpec("placeholder");
    defaultSpec.worker_priority = enum_from_string(config->get_string_value("threadpool.default", "worker_priority", "THREAD_xPRIORITY_NORMAL").c_str(), THREAD_xPRIORITY_INVALID);
    if (defaultSpec.worker_priority == THREAD_xPRIORITY_INVALID)
    {
        rlog(log_level_ERROR, __TITLE__,  "invalid worker priority in [threadpool.default]");
        return false;
    }
    defaultSpec.worker_share_core = config->get_value<bool>("threadpool.default", "worker_share_core", true);
    defaultSpec.worker_affinity_mask = static_cast<uint64_t>(config->get_value<int64_t>("threadpool.default", "worker_affinity_mask", 0));
    if (false == defaultSpec.worker_share_core && 0 == defaultSpec.worker_affinity_mask)
    {
        defaultSpec.worker_affinity_mask = (1 << std::thread::hardware_concurrency()) - 1;
    }
    
    defaultSpec.run = false;
    defaultSpec.worker_count = (int)config->get_value<long>("threadpool.default", "worker_count", 1);
    defaultSpec.max_input_queue_length = (int)config->get_value<long>("threadpool.default", "max_input_queue_length", 10000);
    defaultSpec.partitioned = config->get_value<bool>("threadpool.default", "partitioned", false);
    defaultSpec.queue_aspects = config->get_string_value_list("threadpool.default", "queue_aspects", ',');
    defaultSpec.worker_aspects = config->get_string_value_list("threadpool.default", "worker_aspects", ',');
    defaultSpec.admission_controller_factory_name = config->get_string_value("threadpool.default", "admission_controller_factory_name", "");
    defaultSpec.admission_controller_arguments = config->get_string_value("threadpool.default", "admission_controller_arguments", "");
    
    for (int code = 0; code <= threadpool_code::max_value(); code++)
    {
        if (code == THREAD_POOL_INVALID || code == threadpool_code::from_string("placeholder", THREAD_POOL_INVALID))
            continue;

        std::string section_name = std::string("threadpool.") + std::string(threadpool_code::to_string(code));
        threadpool_spec spec(threadpool_code::to_string(code));
        spec.name = std::string(threadpool_code::to_string(code));
        
        if (config->has_section(section_name.c_str()))
        {
            spec.name = config->get_string_value(section_name.c_str(), "name", threadpool_code::to_string(code));
            spec.run = config->get_value<bool>(section_name.c_str(), "run", true);
            spec.worker_count = (int)config->get_value<long>(section_name.c_str(), "worker_count", defaultSpec.worker_count);
            spec.max_input_queue_length = (int)config->get_value<long>(section_name.c_str(), "max_input_queue_length", defaultSpec.max_input_queue_length);
            spec.partitioned = config->get_value<bool>(section_name.c_str(), "partitioned", defaultSpec.partitioned);
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
                spec.queue_aspects = defaultSpec.queue_aspects;
            }

            spec.worker_aspects = config->get_string_value_list(section_name.c_str(), "worker_aspects", ',');
            if (spec.worker_aspects.size() == 0)
            {
                spec.worker_aspects = defaultSpec.worker_aspects;
            }

            spec.admission_controller_factory_name = config->get_string_value(section_name.c_str(), "admission_controller_factory_name", defaultSpec.admission_controller_factory_name.c_str());
            spec.admission_controller_arguments = config->get_string_value(section_name.c_str(), "admission_controller_arguments", defaultSpec.admission_controller_arguments.c_str());
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
    name = r.name;
    type = r.type;
    arguments = r.arguments;
    port = r.port;
    delay_seconds = r.delay_seconds;
    run = r.run;
}

bool service_app_spec::init(const char* section, configuration_ptr config)
{
    name = config->get_string_value(section, "name", "");
    type = config->get_string_value(section, "type", "");
    arguments = config->get_string_value(section, "arguments", "");
    port = config->get_value<int>(section, "port", 0);    
    delay_seconds = config->get_value<int>(section, "delay_seconds", 0);    
    run = config->get_value<bool>(section, "run", true);
    return true;
}

bool service_spec::init(configuration_ptr c)
{
    std::vector<std::string> poolIds;

    config = c;
    tool = config->get_string_value("core", "tool", "");
    toollets = config->get_string_value_list("core", "toollets", ',');
    port = 0;   
    
    std::vector<std::string> cs;
    config->get_all_keys("network.factory", cs);
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

        rpc_channel rc(c.c_str());
        std::string s = config->get_string_value("network.factory", c.c_str(), "");
        network_factory_names[rc] = s;
    }

    aio_factory_name = config->get_string_value("core", "aio_factory_name", "");
    env_factory_name = config->get_string_value("core", "env_factory_name", "");
    lock_factory_name = config->get_string_value("core", "lock_factory_name", "");
    rwlock_factory_name = config->get_string_value("core", "rwlock_factory_name", "");
    semaphore_factory_name = config->get_string_value("core", "semaphore_factory_name", "");

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
    for (auto it = allSectionNames.begin(); it != allSectionNames.end(); it++)
    {
        if (it->substr(0, strlen("apps.")) == std::string("apps."))
        {
            service_app_spec app;
            app.init((*it).c_str(), config);
            rassert(app.port == 0 || app.port > 1024, "specified port is either 0 (no listen port) or greater than 1024");

            int lport = app.port;
            int count = config->get_value<int>((*it).c_str(), "count", 1);
            std::string name = app.name;
            for (int i = 1; i <= count; i++)
            {
                char buf[16];
                sprintf(buf, ".%u", i);
                app.name = (count > 1 ? (name + buf) : name);

                if (lport == 0)
                {
                    app.port = ++network::max_faked_port_for_client_only_node;
                    rassert(app.port <= 1024, "faked port for client nodes only must not exceed 1024");
                    app_specs.push_back(app);
                }
                else
                {
                    app_specs.push_back(app);
                    app.port++;
                }
            }
        }
    }

    return true;
}


} // end namespace rdsn
