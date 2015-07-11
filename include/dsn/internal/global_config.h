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
# include <dsn/internal/configuration.h>
# include <dsn/internal/threadpool_code.h>
# include <dsn/internal/task_code.h>
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

struct service_app_spec
{
    int                  id;    // global for all roles
    int                  index; // local index for the current role (1,2,3,...)
    std::string          role;
    std::string          config_section;
    std::string          name; 
    std::string          type;
    std::string          arguments;
    std::vector<int>     ports;
    std::list<threadpool_code> pools;
    int                  delay_seconds;
    bool                 run;
    int                  count; // index = 1,2,...,count

    network_client_configs network_client_confs;
    network_server_configs network_server_confs;

    service_app_spec() {}
    service_app_spec(const service_app_spec& r);
    bool init(const char* section, 
        const char* role, 
        configuration_ptr& config, 
        service_app_spec* default_value,
        network_client_configs* default_client_nets = nullptr,
        network_server_configs* default_server_nets = nullptr
        );
};

CONFIG_BEGIN(service_app_spec)
    CONFIG_FLD(std::string, name, "")
    CONFIG_FLD(std::string, type, "")
    CONFIG_FLD(std::string, arguments, "")
    CONFIG_FLD_INT_LIST(ports)
    CONFIG_FLD_ID_LIST(threadpool_code, pools)
    CONFIG_FLD(int, delay_seconds, 0)
    CONFIG_FLD(int, count, 1)
    CONFIG_FLD(bool, run, true)
CONFIG_END

struct service_spec
{
    configuration_ptr            config; // config file

    std::string                  tool;   // the main tool (only 1 is allowed for a time)
    std::list<std::string>       toollets; // toollets enabled compatible to the main tool
    std::string                  coredump_dir; // to store core dump files
    
    std::string                  aio_factory_name;
    std::string                  env_factory_name;
    std::string                  lock_factory_name;
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
    std::list<std::string>       lock_aspects;
    std::list<std::string>       rwlock_aspects;
    std::list<std::string>       semaphore_aspects;
        
    network_client_configs        network_default_client_cfs; // default network configed by tools
    network_server_configs        network_default_server_cfs; // default network configed by tools
    std::vector<threadpool_spec>  threadpool_specs;
    std::vector<service_app_spec> app_specs;

    service_spec() {}
    bool init(configuration_ptr config);
    bool init_app_specs(configuration_ptr c);
};

CONFIG_BEGIN(service_spec)
    CONFIG_FLD(std::string, tool, "")
    CONFIG_FLD_STRING_LIST(toollets)
    CONFIG_FLD(std::string, coredump_dir, "./coredump")
    CONFIG_FLD(std::string, aio_factory_name, "")
    CONFIG_FLD(std::string, env_factory_name, "")
    CONFIG_FLD(std::string, lock_factory_name, "")
    CONFIG_FLD(std::string, rwlock_nr_factory_name, "")
    CONFIG_FLD(std::string, semaphore_factory_name, "")
    CONFIG_FLD(std::string, nfs_factory_name, "")
    CONFIG_FLD(std::string, perf_counter_factory_name, "")
    CONFIG_FLD(std::string, logging_factory_name, "")
    CONFIG_FLD(std::string, memory_factory_name, "")
    CONFIG_FLD(std::string, tools_memory_factory_name, "")

    CONFIG_FLD_STRING_LIST(network_aspects)
    CONFIG_FLD_STRING_LIST(aio_aspects)
    CONFIG_FLD_STRING_LIST(env_aspects)
    CONFIG_FLD_STRING_LIST(lock_aspects)
    CONFIG_FLD_STRING_LIST(rwlock_aspects)
    CONFIG_FLD_STRING_LIST(semaphore_aspects)
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

