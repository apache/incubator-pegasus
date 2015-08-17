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
# pragma once

# include <string>
# include <dsn/service_api_c.h>
# include <dsn/internal/configuration.h>
# include <dsn/internal/task_spec.h>
# include <map>

namespace dsn {

//
// channel and header format are specified per task-code
// port is specified per RPC call
//
struct network_client_config
{
    std::string factory_name;
    int         message_buffer_block_size;
};

typedef std::map<rpc_channel, network_client_config> network_client_configs;

struct network_server_config
{
    // [ key
    int         port;
    rpc_channel channel;
    // ]

    network_header_format hdr_format;
    std::string           factory_name;
    int                   message_buffer_block_size;

    network_server_config(const network_server_config& r);
    network_server_config() : channel(RPC_CHANNEL_TCP), hdr_format(NET_HDR_DSN) {}
    network_server_config(int p, rpc_channel c);
    bool operator < (const network_server_config& r) const;
};

// <port,channel> => config
typedef std::map<network_server_config, network_server_config> network_server_configs;

typedef struct service_app_role
{
    std::string   name; // type name
    dsn_app_create create;
    dsn_app_start start;
    dsn_app_destroy destroy;

} service_app_role;

struct service_app_spec
{
    int                  id;    // global for all roles
    int                  index; // local index for the current role (1,2,3,...)
    std::string          config_section; //[apps.$role]
    std::string          name;  // $role.$count
    std::string          type;  // registered type_name
    std::string          arguments;
    std::vector<int>     ports;
    std::list<dsn_threadpool_code_t> pools;
    int                  delay_seconds;
    bool                 run;
    int                  count; // index = 1,2,...,count
    std::string          dmodule; // when the service is a dynamcially loaded module

    service_app_role     role;

    network_client_configs network_client_confs;
    network_server_configs network_server_confs;

    service_app_spec() {}
    service_app_spec(const service_app_spec& r);
    bool init(const char* section, 
        const char* role, 
        service_app_spec* default_value,
        network_client_configs* default_client_nets = nullptr,
        network_server_configs* default_server_nets = nullptr
        );
};

CONFIG_BEGIN(service_app_spec)
    CONFIG_FLD_STRING(type, "", "app type name, as given when registering by dsn_register_app_role")
    CONFIG_FLD_STRING(arguments, "", "arguments for the app instances")
    CONFIG_FLD_STRING(dmodule, "", "path of a dynamic library which implement this app role, and register itself upon loaded")    
    CONFIG_FLD_INT_LIST(ports, "RPC server listening ports needed for this app")
    CONFIG_FLD_ID_LIST(threadpool_code2, pools, "thread pools need to be started")
    CONFIG_FLD(int, uint64, delay_seconds, 0, "delay seconds for when the apps should be started")
    CONFIG_FLD(int, uint64, count, 1, "count of app instances for this type (ports are automatically calculated accordingly to avoid confliction)")
    CONFIG_FLD(bool, bool, run, true, "whether to run the app instances or not")
CONFIG_END

struct service_spec
{
    configuration_ptr            config; // config file

    std::string                  tool;   // the main tool (only 1 is allowed for a time)
    std::list<std::string>       toollets; // toollets enabled compatible to the main tool
    std::string                  coredump_dir; // to store core dump files
    bool                         start_nfs;
    
    std::string                  timer_factory_name;
    std::string                  aio_factory_name;
    std::string                  env_factory_name;
    std::string                  lock_factory_name;
    std::string                  lock_nr_factory_name;
    std::string                  rwlock_nr_factory_name;
    std::string                  semaphore_factory_name;
    std::string                  nfs_factory_name;
    std::string                  perf_counter_factory_name;
    std::string                  logging_factory_name;
    std::string                  memory_factory_name; // for upper applications
    std::string                  tools_memory_factory_name; // for rDSN itself and lower tools

    std::list<std::string>       network_aspects; // toollets compatible to the above network main providers in network configs
    std::list<std::string>       aio_aspects; // toollets compatible to main aio provider
    std::list<std::string>       env_aspects;
    std::list<std::string>       timer_aspects;
    std::list<std::string>       lock_aspects;
    std::list<std::string>       lock_nr_aspects;
    std::list<std::string>       rwlock_nr_aspects;
    std::list<std::string>       semaphore_aspects;
        
    network_client_configs        network_default_client_cfs; // default network configed by tools
    network_server_configs        network_default_server_cfs; // default network configed by tools
    std::vector<threadpool_spec>  threadpool_specs;
    std::vector<service_app_spec> app_specs;

    service_spec() {}
    bool init();
    bool init_app_specs();
};

CONFIG_BEGIN(service_spec)
    CONFIG_FLD_STRING(tool, "", "use what tool to run this process, e.g., native or simulator")
    CONFIG_FLD_STRING_LIST(toollets, "use what toollets, e.g., tracer, profiler, fault_injector")
    CONFIG_FLD_STRING(coredump_dir, "./coredump", "where to put the core dump files")
    CONFIG_FLD(bool, bool, start_nfs, false, "whether to start nfs")
    CONFIG_FLD_STRING(timer_factory_name, "", "timer service provider")
    CONFIG_FLD_STRING(aio_factory_name, "", "asynchonous file system provider")
    CONFIG_FLD_STRING(env_factory_name, "", "environment provider")
    CONFIG_FLD_STRING(lock_factory_name, "", "recursive exclusive lock provider")
    CONFIG_FLD_STRING(lock_nr_factory_name, "", "non-recurisve exclusive lock provider")
    CONFIG_FLD_STRING(rwlock_nr_factory_name, "", "non-recurisve rwlock provider")
    CONFIG_FLD_STRING(semaphore_factory_name, "", "semaphore provider")
    CONFIG_FLD_STRING(nfs_factory_name, "", "nfs provider")
    CONFIG_FLD_STRING(perf_counter_factory_name, "", "peformance counter provider")
    CONFIG_FLD_STRING(logging_factory_name, "", "logging provider")
    CONFIG_FLD_STRING(memory_factory_name, "", "memory management provider")
    CONFIG_FLD_STRING(tools_memory_factory_name, "", "memory management provider for tools")

    CONFIG_FLD_STRING_LIST(network_aspects, "network aspect providers, usually for tooling purpose")
    CONFIG_FLD_STRING_LIST(aio_aspects, "aio aspect providers, usually for tooling purpose")
    CONFIG_FLD_STRING_LIST(timer_aspects, "timer service aspect providers, usually for tooling purpose")
    CONFIG_FLD_STRING_LIST(env_aspects, "environment aspect providers, usually for tooling purpose")
    CONFIG_FLD_STRING_LIST(lock_aspects, "recursive lock aspect providers, usually for tooling purpose")
    CONFIG_FLD_STRING_LIST(lock_nr_aspects, "non-recurisve lock aspect providers, usually for tooling purpose")
    CONFIG_FLD_STRING_LIST(rwlock_nr_aspects, "non-recursive rwlock aspect providers, usually for tooling purpose")
    CONFIG_FLD_STRING_LIST(semaphore_aspects, "semaphore aspect providers, usually for tooling purpose")
CONFIG_END

enum sys_exit_type
{
    SYS_EXIT_NORMAL,
    SYS_EXIT_BREAK, // Ctrl-C/Break,Shutdown,LogOff, see SetConsoleCtrlHandler
    SYS_EXIT_EXCEPTION,

    SYS_EXIT_INVALID
};

ENUM_BEGIN(sys_exit_type, SYS_EXIT_INVALID)
    ENUM_REG(SYS_EXIT_NORMAL)
    ENUM_REG(SYS_EXIT_BREAK)
    ENUM_REG(SYS_EXIT_EXCEPTION)
ENUM_END(sys_exit_type)

}

