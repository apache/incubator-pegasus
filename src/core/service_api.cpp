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
# include <dsn/service_api.h>
# include <dsn/tool_api.h>
# include "service_engine.h"
# include "task_engine.h"
# include "rpc_engine.h"
# include "disk_engine.h"
# include <dsn/internal/coredump.h>
# include <dsn/internal/env_provider.h>
# include <dsn/internal/factory_store.h>
# include <dsn/internal/nfs.h>
# include <boost/filesystem.hpp>
# include "command_manager.h"
# include <thread>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "service.api"

namespace dsn 
{
    namespace service 
    {
        class service_app_helper
        {
        public:
            service_app_helper(::dsn::service::service_app* app, ::dsn::service_node* node)
            {
                app->set_service_node(node);
            }
        };
    }
    
    namespace internal_only 
    {
        class service_apps : public utils::singleton<service_apps>
        {
        public:
            void add(::dsn::service::service_app* app)
            {
                bool r = _apps.find(app->name()) == _apps.end();
                dassert(r, "apps cannot have the same name for %s", app->name().c_str());

                _apps[app->name()] = app;


                if (app->id() > static_cast<int>(_apps_by_id.size()))
                {
                    _apps_by_id.resize(app->id());
                }
                dassert(_apps_by_id[app->id() - 1] == nullptr, "apps cannot have the same id %d for %s", app->id(), app->name().c_str());
                _apps_by_id[app->id() - 1] = app;
            }

            ::dsn::service::service_app* get(const char* name) const
            {
                auto it = _apps.find(name);
                if (it != _apps.end())
                    return it->second;
                else
                    return nullptr;
            }

            ::dsn::service::service_app* operator [] (int id) const
            {
                dassert(id >= 1 && id <= static_cast<int>(_apps_by_id.size()), "invalid app id %d", id);
                return _apps_by_id[id - 1];
            }

            const std::map<std::string, ::dsn::service::service_app*>& get_all_apps() const { return _apps; }

        private:
            std::map<std::string, ::dsn::service::service_app*> _apps;
            std::vector<::dsn::service::service_app*> _apps_by_id;
        };

        static struct _all_info_
        {
            bool                                                      engine_ready;
            ::dsn::tools::tool_app                                    *tool;
            const std::map<std::string, ::dsn::service::service_app*> *apps;
            configuration_ptr                                         config;
            service_engine                                            *engine;
            std::vector<task_spec*>                                   task_specs;
            ::dsn::memory_provider                                    *memory;
        } dsn_all;

        static bool run(const char* config_file, bool sleep_after_init, std::string& app_name, int app_index = -1)
        {
            dsn_all.engine_ready = false;
            dsn_all.tool = nullptr;
            dsn_all.apps = &service_apps::instance().get_all_apps();
            dsn_all.engine = &service_engine::instance();
            dsn_all.config.reset(new configuration(config_file));
            dsn_all.memory = nullptr;

            for (int i = 0; i <= task_code::max_value(); i++)
            {
                dsn_all.task_specs.push_back(task_spec::get(i));
            }

            service_spec spec;
            if (!spec.init(dsn_all.config))
            {
                printf("error in config file %s, exit ...\n", config_file);
                return false;
            }

            // pause when necessary
            if (dsn_all.config->get_value<bool>("core", "pause_on_start", false))
            {
#if defined(_WIN32)
                printf("\nPause for debugging (pid = %d)...\n", static_cast<int>(::GetCurrentProcessId()));
#else
                printf("\nPause for debugging (pid = %d)...\n", static_cast<int>(getpid()));
#endif
                getchar();
            }

            // setup coredump
            if (!boost::filesystem::exists(spec.coredump_dir))
            {
                boost::filesystem::create_directory(spec.coredump_dir);
            }
            std::string cdir = boost::filesystem::canonical(boost::filesystem::path(spec.coredump_dir)).string();
            utils::coredump::init(cdir.c_str());

            // init tools
            dsn_all.tool = utils::factory_store<::dsn::tools::tool_app>::create(spec.tool.c_str(), 0, spec.tool.c_str());
            dsn_all.tool->install(spec);

            // init app specs
            if (!spec.init_app_specs(dsn_all.config))
            {
                printf("error in config file %s, exit ...\n", config_file);
                return false;
            }
            
            // init tool memory
            dsn_all.memory = ::dsn::utils::factory_store<::dsn::memory_provider>::create(spec.tools_memory_factory_name.c_str(), PROVIDER_TYPE_MAIN);

            // prepare minimum necessary
            service_engine::instance().init_before_toollets(spec);

            // init logging
            log_init(dsn_all.config);

            // init toollets
            for (auto it = spec.toollets.begin(); it != spec.toollets.end(); it++)
            {
                auto tlet = dsn::tools::internal_use_only::get_toollet(it->c_str(), 0);
                dassert(tlet, "toolet not found");
                tlet->install(spec);
            }

            // init provider specific system inits
            dsn::tools::sys_init_before_app_created.execute(service_engine::instance().spec().config);

            // TODO: register sys_exit execution

            // init runtime
            service_engine::instance().init_after_toollets();

            dsn_all.engine_ready = true;

            // init apps
            std::list<::dsn::service::service_app*> apps;
            for (auto it = spec.app_specs.begin(); it != spec.app_specs.end(); it++)
            {
                if (it->run)
                {
                    auto app = utils::factory_store<::dsn::service::service_app>::create(it->type.c_str(), 0, &(*it));
                    dassert(app != nullptr, "Cannot create service app with type name '%s'", it->type.c_str());
                    apps.push_back(app);
                }
            }

            for (auto it = apps.begin(); it != apps.end(); it++)
            {
                ::dsn::service::service_app* app = *it;
                bool create_it = false;

                if (app_name == "") // create all apps
                {
                    create_it = true;
                }   
                else if (app_name == app->spec().role)
                {
                    if (app_index == -1)
                        create_it = true;
                    else
                    {
                        create_it = (app_index == app->spec().index);
                    }
                }
                else
                    create_it = false;

                if (create_it)
                {
                    service_apps::instance().add(app);

                    auto node = service_engine::instance().start_node(app);
                    ::dsn::service::service_app_helper h(app, node);
                }
            }

            if (service_apps::instance().get_all_apps().size() == 0)
            {
                printf("no app are created, usually because \n"
                    "app_name is not specified correctly, should be 'xxx' in [apps.xxx]\n"
                    "or app_index (1-based) is greater than specified count in config file\n"
                    );
                exit(1);
            }

            // start cli if necessary
            if (dsn_all.config->get_value<bool>("core", "cli_local", true))
            {
                ::dsn::command_manager::instance().start_local_cli();
            }

            if (dsn_all.config->get_value<bool>("core", "cli_remote", true))
            {
                ::dsn::command_manager::instance().start_remote_cli();
            }

            // invoke customized init after apps are created
            dsn::tools::sys_init_after_app_created.execute(service_engine::instance().spec().config);

            // start the tool
            dsn_all.tool->run();

            //
            if (sleep_after_init)
            {
                while (true)
                {
                    std::this_thread::sleep_for(std::chrono::hours(1));
                }
            }

            return true;
        }


        //
        // run the system with arguments
        //   config [app_name [app_index]]
        // e.g., config.ini replica 1 to start the first replica as a new process
        //       config.ini replica to start ALL replicas (count specified in config) as a new process
        //       config.ini to start ALL apps as a new process
        //
        static void run(int argc, char** argv, bool sleep_after_init)
        {
            if (argc < 1)
            {
                printf("invalid options for system::run\n"
                    "   config [app_name [app_index]]\n"
                    "   e.g., config.ini replica 1 to start the first replica as a new process\n"
                    "       config.ini replica to start ALL replicas (count specified in config) as a new process\n"
                    "       config.ini to start ALL apps as a new process\n"
                    );
                exit(1);
                return;
            }

            char* config = argv[0];
            std::string app_name = "";
            int app_index = -1;

            if (argc >= 2)
            {
                app_name = argv[1];
            }

            if (argc >= 3)
            {
                app_index = atoi(argv[2]);
            }

            run(config, sleep_after_init, app_name, app_index);
        }
    }

    namespace service 
    {
        namespace system
        {
            namespace internal_use_only
            {
                bool register_service(const char* name, service_app_factory factory)
                {
                    return utils::factory_store<service_app>::register_factory(name, factory, 0);
                }
            }

            bool run(const char* config_file, bool sleep_after_init)
            {
                std::string name;
                return ::dsn::internal_only::run(config_file, sleep_after_init, name);
            }

            void run(int argc, char** argv, bool sleep_after_init)
            {
                return ::dsn::internal_only::run(argc, argv, sleep_after_init);
            }

            bool is_ready()
            {
                return ::dsn::internal_only::dsn_all.engine_ready;
            }


            configuration_ptr config()
            {
                return ::dsn::internal_only::dsn_all.config;
            }

            extern const service_spec& spec()
            {
                return service_engine::instance().spec();
            }

            service_app* get_current_app()
            {
                auto tsk = task::get_current_task();
                dassert(tsk != nullptr, "this function can only be invoked inside tasks");

                return ::dsn::internal_only::service_apps::instance()[tsk->node()->id()];
            }

            const std::map<std::string, service_app*>& get_all_apps()
            {
                return ::dsn::internal_only::service_apps::instance().get_all_apps();
            }
        }

        namespace rpc
        {
            const end_point& primary_address()
            {
                auto tsk = task::get_current_task();
                dassert(tsk != nullptr, "this function can only be invoked inside tasks");

                return tsk->node()->rpc()->primary_address();
            }

            bool register_rpc_handler(task_code code, const char* name, rpc_server_handler* handler)
            {
                auto tsk = task::get_current_task();
                dassert(tsk != nullptr, "this function can only be invoked inside tasks");

                rpc_handler_ptr h(new rpc_handler_info(code));
                h->name = std::string(name);
                h->handler = handler;

                return tsk->node()->rpc()->register_rpc_handler(h);
            }

            bool unregister_rpc_handler(task_code code)
            {
                auto tsk = task::get_current_task();
                dassert(tsk != nullptr, "this function can only be invoked inside tasks");

                return tsk->node()->rpc()->unregister_rpc_handler(code);
            }

            void call(const end_point& server, message_ptr& request, rpc_response_task_ptr& callback)
            {
                auto tsk = task::get_current_task();
                dassert(tsk != nullptr, "this function can only be invoked inside tasks");
                dassert(nullptr != callback, "callback cannot be empty");

                rpc_engine* rpc = tsk->node()->rpc();
                request->header().to_address = server;
                rpc->call(request, callback);
            }

            rpc_response_task_ptr call(const end_point& server, message_ptr& request)
            {
                auto tsk = task::get_current_task();
                dassert(tsk != nullptr, "this function can only be invoked inside tasks");

                rpc_response_task_ptr callback(new rpc_response_task_empty(request));
                rpc_engine* rpc = tsk->node()->rpc();
                request->header().to_address = server;
                rpc->call(request, callback);
                return std::move(callback);
            }

            void call_one_way(const end_point& server, message_ptr& request)
            {
                auto tsk = task::get_current_task();
                dassert(tsk != nullptr, "this function can only be invoked inside tasks");

                rpc_response_task_ptr nil;
                rpc_engine* rpc = tsk->node()->rpc();
                request->header().to_address = server;
                rpc->call(request, nil);
            }

            void reply(message_ptr& response)
            {
                rpc_engine::reply(response);
            }
        }

        namespace file
        {
            handle_t open(const char* file_name, int flag, int pmode)
            {
                auto tsk = task::get_current_task();
                dassert(tsk != nullptr, "this function can only be invoked inside tasks");

                return tsk->node()->disk()->open(file_name, flag, pmode);
            }

            void read(handle_t hFile, char* buffer, int count, uint64_t offset, aio_task_ptr& callback)
            {
                auto tsk = task::get_current_task();
                dassert(tsk != nullptr, "this function can only be invoked inside tasks");

                callback->aio()->buffer = buffer;
                callback->aio()->buffer_size = count;
                callback->aio()->engine = nullptr;
                callback->aio()->file = hFile;
                callback->aio()->file_offset = offset;
                callback->aio()->type = AIO_Read;

                tsk->node()->disk()->read(callback);
            }

            void write(handle_t hFile, const char* buffer, int count, uint64_t offset, aio_task_ptr& callback)
            {
                auto tsk = task::get_current_task();
                dassert(tsk != nullptr, "this function can only be invoked inside tasks");

                callback->aio()->buffer = (char*)buffer;
                callback->aio()->buffer_size = count;
                callback->aio()->engine = nullptr;
                callback->aio()->file = hFile;
                callback->aio()->file_offset = offset;
                callback->aio()->type = AIO_Write;

                tsk->node()->disk()->write(callback);
            }

            error_code close(handle_t hFile)
            {
                auto tsk = task::get_current_task();
                dassert(tsk != nullptr, "this function can only be invoked inside tasks");

                return tsk->node()->disk()->close(hFile);
            }

            void copy_remote_files(
                const end_point& remote,
                const std::string& source_dir,
                std::vector<std::string>& files,  // empty for all
                const std::string& dest_dir,
                bool overwrite,
                aio_task_ptr& callback
                )
            {
                std::shared_ptr<remote_copy_request> rci(new remote_copy_request());
                rci->source = remote;
                rci->source_dir = source_dir;
                rci->files = files;
                rci->dest_dir = dest_dir;
                rci->overwrite = overwrite;

                auto tsk = task::get_current_task();
                dassert(tsk != nullptr, "this function can only be invoked inside tasks");

                return tsk->node()->nfs()->call(rci, callback);
            }
        }

        namespace env
        {
            // since Epoch (1970-01-01 00:00:00 +0000 (UTC))
            uint64_t now_ns()
            {
                return service_engine::instance().env()->now_ns();
            }

            // generate random number [min, max]
            uint64_t random64(uint64_t min, uint64_t max)
            {
                return service_engine::instance().env()->random64(min, max);
            }
        }

        namespace memory
        {
            void* allocate(size_t sz)
            {
                return service_engine::instance().memory()->allocate(sz);
            }

            void* reallocate(void* ptr, size_t sz)
            {
                return service_engine::instance().memory()->reallocate(ptr, sz);
            }

            void  deallocate(void* ptr)
            {
                return service_engine::instance().memory()->deallocate(ptr);
            }
        }

    }
} // end namespace dsn::service


namespace dsn 
{
    namespace tools 
    {
        tool_app* get_current_tool()
        {
            return ::dsn::internal_only::dsn_all.tool;
        }
        
        namespace memory
        {
            void* allocate(size_t sz)
            {
                return ::dsn::internal_only::dsn_all.memory->allocate(sz);
            }

            void* reallocate(void* ptr, size_t sz)
            {
                return ::dsn::internal_only::dsn_all.memory->reallocate(ptr, sz);
            }

            void  deallocate(void* ptr)
            {
                return ::dsn::internal_only::dsn_all.memory->deallocate(ptr);
            }
        }
    }
}
