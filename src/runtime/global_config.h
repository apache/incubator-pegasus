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

/// Attention: There are some types which are defined in dsn_runtime being used in this file,
/// so this file is coupled with dsn_runtime. If you want to add some variables/types here or
/// include this file, please make sure whether you want to couple with dsn_runtime or not.

#pragma once

#include <algorithm>
#include <list>
#include <map>
#include <string>
#include <vector>

#include "runtime/task/task_spec.h"
#include "utils/config_api.h"
#include "utils/config_helper.h"
#include "utils/customizable_id.h"
#include "utils/threadpool_code.h"
#include "utils/threadpool_spec.h"

namespace dsn {

//
// channel and header format are specified per task-code
// port is specified per RPC call
//
struct network_client_config
{
    std::string factory_name;
    int message_buffer_block_size;

    network_client_config();
};

typedef std::map<rpc_channel, network_client_config> network_client_configs;

struct network_server_config
{
    // [ key
    int port;
    rpc_channel channel;
    // ]

    std::string factory_name;
    int message_buffer_block_size;

    network_server_config();
    network_server_config(int p, rpc_channel c);
    network_server_config(const network_server_config &r);
    bool operator<(const network_server_config &r) const;
};

// <port,channel> => config
typedef std::map<network_server_config, network_server_config> network_server_configs;

// Terms used in rDSN:
//  - app_id
//  - app_name/role_name
//  - role_index
//  - app_full_name
//  - app_type
struct service_app_spec
{
    int id;    // global id for all roles, assigned by rDSN automatically
    int index; // local index for the current role (1,2,3,...), also named as "role_index"
    std::string data_dir;       // data dir for the app. it is auto-set as
                                // ${service_spec.data_dir}/${service_app_spec.name}.
    std::string config_section; // [apps.${role_name}]
    std::string role_name;      // role name of [apps.${role_name}], also named as "app_name"

    // combined by role_name and role_index, also named as "app_full_name"
    // e.g., if role_name = meta and role_index = 1, then app_full_name = meta1
    // specially, if role count is 1, then app_full_name equals to role_name
    // it is usually used for printing log
    std::string full_name;
    std::string type; // registered type name, alse named as "app_type"
    std::string arguments;
    std::vector<int> ports;
    std::list<dsn::threadpool_code> pools;
    int delay_seconds;
    bool run;
    int count;     // index = 1,2,...,count
    int ports_gap; // when count > 1

    network_client_configs network_client_confs;
    network_server_configs network_server_confs;

    service_app_spec() {}
    /*service_app_spec(const service_app_spec& r);*/
    bool init(const char *section,
              const char *role_name_,
              service_app_spec *default_value,
              network_client_configs *default_client_nets = nullptr,
              network_server_configs *default_server_nets = nullptr);
};

CONFIG_BEGIN(service_app_spec)
CONFIG_FLD_STRING(type, "", "app type name, as given when registering by dsn_register_app")
CONFIG_FLD_STRING(arguments, "", "arguments for the app instances")
CONFIG_FLD_INT_LIST(ports, "RPC server listening ports needed for this app")
CONFIG_FLD_ID_LIST(threadpool_code, pools, "thread pools need to be started")
CONFIG_FLD(int, uint64, delay_seconds, 0, "delay seconds for when the apps should be started")
CONFIG_FLD(int,
           uint64,
           count,
           1,
           "count of app instances for this type (ports are automatically "
           "calculated accordingly to avoid confliction)")
CONFIG_FLD(bool, bool, run, true, "whether to run the app instances or not")
CONFIG_END

struct service_spec
{
    std::string tool;                // the main tool (only 1 is allowed for a time)
    std::list<std::string> toollets; // toollets enabled compatible to the main tool
    std::string data_dir;            // to store all data/log/coredump etc.

    //
    // we allow multiple apps in the same process in rDSN, and each app (service_app_spec)
    // has its own rpc/thread/disk engines etc..
    // when a rDSN call is made in a thread not belonging to any rDSN app,
    // developers need to call dsn_mimic_app to designated which app this call and subsequent
    // calls belong to.
    // this is kinds of tedious sometimes, we therefore introduce enable_default_app_mimic
    // option here, which automatically starts an internal app which does nothing but serves
    // those external calls only. This will release the developers from writing dsn_mimic_app
    // when they write certain codes, esp. client side code.
    //
    bool enable_default_app_mimic;

    std::string timer_factory_name;
    std::string env_factory_name;
    std::string lock_factory_name;
    std::string lock_nr_factory_name;
    std::string rwlock_nr_factory_name;
    std::string semaphore_factory_name;
    std::string logging_factory_name;

    network_client_configs network_default_client_cfs; // default network configed by tools
    network_server_configs network_default_server_cfs; // default network configed by tools
    std::vector<threadpool_spec> threadpool_specs;
    std::vector<service_app_spec> app_specs;

    // auto-set
    std::string dir_log;

    service_spec() {}
    bool init();
    bool init_app_specs();
};

CONFIG_BEGIN(service_spec)
CONFIG_FLD_STRING(tool, "", "use what tool to run this process, e.g., native or simulator")
CONFIG_FLD_STRING_LIST(toollets, "use what toollets, e.g., tracer, profiler, fault_injector")
CONFIG_FLD_STRING(data_dir, "./data", "where to put the all the data/log/coredump, etc..")
CONFIG_FLD(
    bool,
    bool,
    enable_default_app_mimic,
    false,
    "whether to start a default service app for serving the rDSN calls made in\n"
    "; non-rDSN threads, so that developers do not need to write dsn_mimic_app call before them\n"
    "; in this case, a [apps.mimic] section must be defined in config files");

CONFIG_FLD_STRING(timer_factory_name, "", "timer service provider")
CONFIG_FLD_STRING(env_factory_name, "", "environment provider")
CONFIG_FLD_STRING(lock_factory_name, "", "recursive exclusive lock provider")
CONFIG_FLD_STRING(lock_nr_factory_name, "", "non-recurisve exclusive lock provider")
CONFIG_FLD_STRING(rwlock_nr_factory_name, "", "non-recurisve rwlock provider")
CONFIG_FLD_STRING(semaphore_factory_name, "", "semaphore provider")
CONFIG_FLD_STRING(logging_factory_name, "", "logging provider")
CONFIG_END
} // namespace dsn
