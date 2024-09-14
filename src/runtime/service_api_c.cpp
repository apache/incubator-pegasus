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

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <chrono>
#include <fstream> // IWYU pragma: keep
#include <list>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#ifdef DSN_ENABLE_GPERF
#include <gperftools/malloc_extension.h>
#endif

#include "fmt/core.h"
#include "fmt/format.h"
#include "perf_counter/perf_counters.h"
#include "rpc/rpc_address.h"
#include "rpc/rpc_engine.h"
#include "rpc/rpc_host_port.h"
#include "rpc/rpc_message.h"
#include "runtime/api_layer1.h"
#include "runtime/api_task.h"
#include "runtime/app_model.h"
#include "runtime/global_config.h"
#include "runtime/service_app.h"
#include "runtime/service_engine.h"
#include "task/task.h"
#include "task/task_code.h"
#include "task/task_engine.h"
#include "task/task_spec.h"
#include "task/task_worker.h"
#include "runtime/tool_api.h"
#include "security/init.h"
#include "security/negotiation_manager.h"
#include "utils/api_utilities.h"
#include "utils/command_manager.h"
#include "utils/config_api.h"
#include "utils/coredump.h"
#include "utils/error_code.h"
#include "utils/factory_store.h"
#include "utils/filesystem.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/join_point.h"
#include "utils/logging_provider.h"
#include "utils/process_utils.h"
#include "utils/string_conv.h"
#include "utils/strings.h"
#include "utils/sys_exit_hook.h"
#include "utils/threadpool_spec.h"

DSN_DEFINE_bool(
    core,
    pause_on_start,
    false,
    "Whether to pause during startup to wait for interactive input, often for debugging purpose");
#ifdef DSN_ENABLE_GPERF
DSN_DEFINE_double(core,
                  tcmalloc_release_rate,
                  1.,
                  "the memory releasing rate of tcmalloc, default "
                  "is 1.0 in gperftools, value range is "
                  "[0.0, 10.0]");
#endif

DSN_DECLARE_bool(enable_auth);
DSN_DECLARE_bool(enable_zookeeper_kerberos);

//
// global state
//
static struct _all_info_
{
    unsigned int magic;
    bool engine_ready;
    bool config_completed;
    std::unique_ptr<::dsn::tools::tool_app> tool;
    ::dsn::service_engine *engine;
    std::vector<::dsn::task_spec *> task_specs;

    bool is_config_completed() const { return magic == 0xdeadbeef && config_completed; }

    bool is_engine_ready() const { return magic == 0xdeadbeef && engine_ready; }

} dsn_all;

std::unique_ptr<dsn::command_deregister> dump_log_cmd;

volatile int *dsn_task_queue_virtual_length_ptr(dsn::task_code code, int hash)
{
    return dsn::task::get_current_node()->computation()->get_task_queue_virtual_length_ptr(code,
                                                                                           hash);
}

bool dsn_task_is_running_inside(dsn::task *t) { return ::dsn::task::get_current_task() == t; }

void dsn_coredump()
{
    ::dsn::utils::coredump::write();
    ::abort();
}

//------------------------------------------------------------------------------
//
// rpc
//
//------------------------------------------------------------------------------

// rpc calls
dsn::rpc_address dsn_primary_address() { return ::dsn::task::get_current_rpc()->primary_address(); }

dsn::host_port dsn_primary_host_port()
{
    return ::dsn::task::get_current_rpc()->primary_host_port();
}

bool dsn_rpc_register_handler(dsn::task_code code,
                              const char *extra_name,
                              const dsn::rpc_request_handler &cb)
{
    return ::dsn::task::get_current_node()->rpc_register_handler(code, extra_name, cb);
}

bool dsn_rpc_unregiser_handler(dsn::task_code code)
{
    return ::dsn::task::get_current_node()->rpc_unregister_handler(code);
}

void dsn_rpc_call(dsn::rpc_address server, dsn::rpc_response_task *rpc_call)
{
    CHECK_EQ_MSG(rpc_call->spec().type, TASK_TYPE_RPC_RESPONSE, "invalid task_type");

    auto msg = rpc_call->get_request();
    msg->server_address = server;
    msg->server_host_port = dsn::host_port::from_address(msg->server_address);
    ::dsn::task::get_current_rpc()->call(msg, dsn::rpc_response_task_ptr(rpc_call));
}

dsn::message_ex *dsn_rpc_call_wait(dsn::rpc_address server, dsn::message_ex *request)
{
    auto msg = ((::dsn::message_ex *)request);
    msg->server_address = server;
    msg->server_host_port = dsn::host_port::from_address(msg->server_address);

    ::dsn::rpc_response_task *rtask = new ::dsn::rpc_response_task(msg, nullptr, 0);
    rtask->add_ref();
    ::dsn::task::get_current_rpc()->call(msg, dsn::rpc_response_task_ptr(rtask));
    rtask->wait();
    if (rtask->error() == ::dsn::ERR_OK) {
        auto msg = rtask->get_response();
        msg->add_ref();       // released by callers
        rtask->release_ref(); // added above
        return msg;
    } else {
        rtask->release_ref(); // added above
        return nullptr;
    }
}

void dsn_rpc_call_one_way(dsn::rpc_address server, dsn::message_ex *request)
{
    auto msg = ((::dsn::message_ex *)request);
    msg->server_address = server;
    msg->server_host_port = dsn::host_port::from_address(server);

    ::dsn::task::get_current_rpc()->call(msg, nullptr);
}

void dsn_rpc_reply(dsn::message_ex *response, dsn::error_code err)
{
    auto msg = ((::dsn::message_ex *)response);
    ::dsn::task::get_current_rpc()->reply(msg, err);
}

void dsn_rpc_forward(dsn::message_ex *request, dsn::rpc_address addr)
{
    ::dsn::task::get_current_rpc()->forward((::dsn::message_ex *)(request),
                                            ::dsn::rpc_address(addr));
}

//------------------------------------------------------------------------------
//
// system
//
//------------------------------------------------------------------------------

static bool
run(const char *config_file, const char *config_arguments, bool is_server, std::string &app_list);

bool dsn_run_config(const char *config, bool is_server)
{
    std::string name;
    return run(config, nullptr, is_server, name);
}

[[noreturn]] void dsn_exit(int code)
{
    dump_log_cmd.reset();

    printf("dsn exit with code %d\n", code);
    fflush(stdout);
    ::dsn::tools::sys_exit.execute(::dsn::SYS_EXIT_NORMAL);

    _exit(code);
}

bool dsn_mimic_app(const char *app_role, int index)
{
    auto worker = ::dsn::task::get_current_worker2();
    CHECK(worker == nullptr, "cannot call dsn_mimic_app in rDSN threads");

    auto cnode = ::dsn::task::get_current_node2();
    if (cnode != nullptr) {
        const std::string &name = cnode->spec().full_name;
        if (cnode->spec().role_name == std::string(app_role) && cnode->spec().index == index) {
            return true;
        } else {
            LOG_ERROR("current thread is already attached to another rDSN app {}", name);
            return false;
        }
    }

    const auto &nodes = dsn::service_engine::instance().get_all_nodes();
    for (const auto &n : nodes) {
        if (n.second->spec().role_name == std::string(app_role) &&
            n.second->spec().index == index) {
            ::dsn::task::set_tls_dsn_context(n.second.get(), nullptr);
            return true;
        }
    }

    LOG_ERROR("cannot find host app {} with index {}", app_role, index);
    return false;
}

//
// run the system with arguments
//   config [-cargs k1=v1;k2=v2] [-app_list app_name1@index1;app_name2@index]
// e.g., config.ini -app_list replica@1 to start the first replica as a new process
//       config.ini -app_list replica to start ALL replicas (count specified in config) as a new
//       process
//       config.ini -app_list replica -cargs replica-port=34556 to start ALL replicas with given
//       port variable specified in config.ini
//       config.ini to start ALL apps as a new process
//
void dsn_run(int argc, char **argv, bool is_server)
{
    if (argc < 2) {
        printf(
            "invalid options for dsn_run\n"
            "// run the system with arguments\n"
            "//   config [-cargs k1=v1;k2=v2] [-app_list app_name1@index1;app_name2@index]\n"
            "// e.g., config.ini -app_list replica@1 to start the first replica as a new process\n"
            "//       config.ini -app_list replica to start ALL replicas (count specified in "
            "config) as a new process\n"
            "//       config.ini -app_list replica -cargs replica-port=34556 to start with "
            "%%replica-port%% var in config.ini\n"
            "//       config.ini to start ALL apps as a new process\n");
        dsn_exit(1);
        return;
    }

    char *config = argv[1];
    std::string config_args = "";
    std::string app_list = "";

    for (int i = 2; i < argc;) {
        if (dsn::utils::equals(argv[i], "-cargs")) {
            if (++i < argc) {
                config_args = std::string(argv[i++]);
            }
        }

        else if (dsn::utils::equals(argv[i], "-app_list")) {
            if (++i < argc) {
                app_list = std::string(argv[i++]);
            }
        } else {
            printf("unknown arguments %s\n", argv[i]);
            dsn_exit(1);
            return;
        }
    }

    if (!run(config, config_args.size() > 0 ? config_args.c_str() : nullptr, is_server, app_list)) {
        printf("run the system failed\n");
        dsn_exit(-1);
        return;
    }
}

namespace dsn {
namespace tools {

bool is_engine_ready() { return dsn_all.is_engine_ready(); }

tool_app *get_current_tool() { return dsn_all.tool.get(); }

} // namespace tools
} // namespace dsn

extern void dsn_core_init();

inline void dsn_global_init()
{
    // make perf_counters destructed after service_engine,
    // because service_engine relies on the former to monitor
    // task queues length.
    dsn::perf_counters::instance();
    dsn::service_engine::instance();
}

static std::string dsn_log_prefixed_message_func()
{
    const int tid = dsn::utils::get_current_tid();
    const auto t = dsn::task::get_current_task_id();
    if (t) {
        if (nullptr != dsn::task::get_current_worker2()) {
            return fmt::format("{}.{}{}.{:016}: ",
                               dsn::task::get_current_node_name(),
                               dsn::task::get_current_worker2()->pool_spec().name,
                               dsn::task::get_current_worker2()->index(),
                               t);
        } else {
            return fmt::format(
                "{}.io-thrd.{}.{:016}: ", dsn::task::get_current_node_name(), tid, t);
        }
    } else {
        if (nullptr != dsn::task::get_current_worker2()) {
            return fmt::format("{}.{}{}: ",
                               dsn::task::get_current_node_name(),
                               dsn::task::get_current_worker2()->pool_spec().name,
                               dsn::task::get_current_worker2()->index());
        } else {
            return fmt::format("{}.io-thrd.{}: ", dsn::task::get_current_node_name(), tid);
        }
    }
}

bool run(const char *config_file,
         const char *config_arguments,
         bool is_server,
         std::string &app_list)
{
    // We put the loading of configuration at the beginning of this func.
    // Because in dsn_global_init(), it calls perf_counters::instance(), which calls
    // shared_io_service::instance(). And in the cstor of shared_io_service, it calls
    // dsn_config_get_value_uint64() to load the corresponding configs. That will make
    // dsn_config_get_value_uint64() get wrong value if we put dsn_config_load at behind of
    // dsn_global_init()
    if (!dsn_config_load(config_file, config_arguments)) {
        printf("Fail to load config file %s\n", config_file);
        return false;
    }
    dsn::flags_initialize();

    dsn_global_init();
    dsn_core_init();
    ::dsn::task::set_tls_dsn_context(nullptr, nullptr);

    dsn_all.engine_ready = false;
    dsn_all.config_completed = false;
    dsn_all.tool = nullptr;
    dsn_all.engine = &::dsn::service_engine::instance();
    dsn_all.magic = 0xdeadbeef;

    // pause when necessary
    if (FLAGS_pause_on_start) {
        printf("\nPause for debugging (pid = %d)...\n", static_cast<int>(getpid()));
        getchar();
    }

    for (int i = 0; i <= dsn::task_code::max(); i++) {
        dsn_all.task_specs.push_back(::dsn::task_spec::get(i));
    }

    // initialize global specification from config file
    ::dsn::service_spec spec;
    if (!spec.init()) {
        printf("error in config file %s, exit ...\n", config_file);
        return false;
    }

    dsn_all.config_completed = true;

    // setup data dir
    auto &data_dir = spec.data_dir;
    CHECK(!dsn::utils::filesystem::file_exists(data_dir), "{} should not be a file.", data_dir);
    if (!dsn::utils::filesystem::directory_exists(data_dir)) {
        CHECK(dsn::utils::filesystem::create_directory(data_dir), "Fail to create {}", data_dir);
    }
    std::string cdir;
    CHECK(dsn::utils::filesystem::get_absolute_path(data_dir, cdir),
          "Fail to get absolute path from {}",
          data_dir);
    spec.data_dir = cdir;

    ::dsn::utils::coredump::init();

    // Setup log directory.
    // If log_dir is not set, use data_dir/log instead.
    if (spec.log_dir.empty()) {
        spec.log_dir = ::dsn::utils::filesystem::path_combine(spec.data_dir, "log");
        fmt::print(stdout, "log_dir is not set, use '{}' instead\n", spec.log_dir);
    }
    // Validate log_dir.
    if (!dsn::utils::filesystem::is_absolute_path(spec.log_dir)) {
        fmt::print(stderr, "log_dir({}) should be set with an absolute path\n", spec.log_dir);
        return false;
    }
    dsn::utils::filesystem::create_directory(spec.log_dir);

    // Initialize tools.
    dsn_all.tool.reset(::dsn::utils::factory_store<::dsn::tools::tool_app>::create(
        spec.tool.c_str(), ::dsn::PROVIDER_TYPE_MAIN, spec.tool.c_str()));
    dsn_all.tool->install(spec);

    // Initialize app specs.
    if (!spec.init_app_specs()) {
        fmt::print(stderr, "error in config file {}, exit ...\n", config_file);
        return false;
    }

#ifdef DSN_ENABLE_GPERF
    ::MallocExtension::instance()->SetMemoryReleaseRate(FLAGS_tcmalloc_release_rate);
#endif

    // Extract app_names.
    std::list<std::string> app_names_and_indexes;
    ::dsn::utils::split_args(app_list.c_str(), app_names_and_indexes, ';');
    std::vector<std::string> app_names;
    for (const auto &app_name_and_index : app_names_and_indexes) {
        std::vector<std::string> name_and_index;
        ::dsn::utils::split_args(app_name_and_index.c_str(), name_and_index, '@');
        if (name_and_index.empty()) {
            fmt::print(stderr, "app_name should be specified in '{}'", app_name_and_index);
            return false;
        }
        app_names.push_back(name_and_index[0]);
    }

    // Initialize logging.
    dsn_log_init(spec.logging_factory_name,
                 spec.log_dir,
                 fmt::format("{}", fmt::join(app_names, ".")),
                 dsn_log_prefixed_message_func);

    // Prepare the minimum necessary.
    ::dsn::service_engine::instance().init_before_toollets(spec);

    LOG_INFO("process({}) start: {}, date: {}",
             getpid(),
             dsn::utils::process_start_millis(),
             dsn::utils::process_start_date_time_mills());

    // Initialize toollets.
    for (const auto &toollet_name : spec.toollets) {
        auto tlet = dsn::tools::internal_use_only::get_toollet(toollet_name.c_str(),
                                                               ::dsn::PROVIDER_TYPE_MAIN);
        CHECK_NOTNULL(tlet, "toolet not found");
        tlet->install(spec);
    }

    // init provider specific system inits
    dsn::tools::sys_init_before_app_created.execute();

    // TODO: register sys_exit execution

    // init runtime
    ::dsn::service_engine::instance().init_after_toollets();

    dsn_all.engine_ready = true;

    // init security if FLAGS_enable_auth == true
    if (FLAGS_enable_auth) {
        if (!dsn::security::init(is_server)) {
            return false;
        }
        // if FLAGS_enable_auth is false but FLAGS_enable_zookeeper_kerberos, we should init
        // kerberos for it separately
        // include two steps:
        // 1) apply kerberos ticket and keep it valid
        // 2) complete sasl init for client(use FLAGS_sasl_plugin_path)
    } else if (FLAGS_enable_zookeeper_kerberos && app_list == "meta") {
        if (!dsn::security::init_for_zookeeper_client()) {
            return false;
        }
    }

    // init apps
    for (auto &sp : spec.app_specs) {
        if (!sp.run) {
            continue;
        }

        bool create_it = false;
        // create all apps
        if (app_list.empty()) {
            create_it = true;
        } else {
            for (const auto &app_name_and_index : app_names_and_indexes) {
                std::vector<std::string> name_and_index;
                ::dsn::utils::split_args(app_name_and_index.c_str(), name_and_index, '@');
                CHECK(!name_and_index.empty(),
                      "app_name should be specified in '{}'",
                      app_name_and_index);
                if (std::string("apps.") + name_and_index.front() == sp.config_section) {
                    if (name_and_index.size() < 2) {
                        create_it = true;
                    } else {
                        int32_t index = 0;
                        const auto index_str = name_and_index.back();
                        CHECK(dsn::buf2int32(index_str, index),
                              "'{}' is not a valid index",
                              index_str);
                        create_it = (index == sp.index);
                    }
                    break;
                }
            }
        }

        if (create_it) {
            ::dsn::service_engine::instance().start_node(sp);
        }
    }

    if (dsn::service_engine::instance().get_all_nodes().empty()) {
        fmt::print(stderr,
                   "no app are created, usually because \n"
                   "app_name is not specified correctly, should be 'xxx' in [apps.xxx]\n"
                   "or app_index (1-based) is greater than specified count in config file\n");
        exit(1);
    }

    dump_log_cmd = dsn::command_manager::instance().register_single_command(
        "config-dump",
        "Dump all configurations to a server local path or to stdout",
        "[target_file]",
        [](const std::vector<std::string> &args) {
            std::ostringstream oss;
            std::ofstream off;
            std::ostream *os = &oss;
            if (args.size() > 0) {
                off.open(args[0]);
                os = &off;

                oss << "config dump to file " << args[0] << std::endl;
            }

            dsn_config_dump(*os);
            return oss.str();
        });

    // invoke customized init after apps are created
    dsn::tools::sys_init_after_app_created.execute();

    // start the tool
    dsn_all.tool->run();

    if (is_server) {
        while (true) {
            std::this_thread::sleep_for(std::chrono::hours(1));
        }
    }

    // add this to allow mimic app call from this thread.
    memset((void *)&dsn::tls_dsn, 0, sizeof(dsn::tls_dsn));

    return true;
}

namespace dsn {
service_app *service_app::new_service_app(const std::string &type,
                                          const dsn::service_app_info *info)
{
    return dsn::utils::factory_store<service_app>::create(
        type.c_str(), dsn::PROVIDER_TYPE_MAIN, info);
}

service_app::service_app(const dsn::service_app_info *info) : _info(info), _started(false)
{
    security::negotiation_manager::instance().open_service();
}

const service_app_info &service_app::info() const { return *_info; }

const service_app_info &service_app::current_service_app_info()
{
    return tls_dsn.node->get_service_app_info();
}

void service_app::get_all_service_apps(std::vector<service_app *> *apps)
{
    const service_nodes_by_app_id &nodes = dsn_all.engine->get_all_nodes();
    for (const auto &kv : nodes) {
        const service_node *node = kv.second.get();
        apps->push_back(const_cast<service_app *>(node->get_service_app()));
    }
}

} // namespace dsn
