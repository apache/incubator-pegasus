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
# include <dsn/tool_api.h>
# include <dsn/internal/service_app.h>
# include "service_engine.h"
# include <dsn/internal/factory_store.h>
# include <dsn/internal/singleton_store.h>

namespace dsn { 

    DEFINE_TASK_CODE(LPC_CONTROL_SERVICE_APP, TASK_PRIORITY_HIGH, THREAD_POOL_DEFAULT)

    class service_control_task : public task
    {
    public:
        service_control_task(service::service_app* app, bool start)
            : _app(app), task(LPC_CONTROL_SERVICE_APP, 0, app->node()), _start(start)
        {
        }

        void exec()
        {
            if (_start)
            {
                error_code err;
                if (_app->node()->nfs())
                {
                    err = _app->node()->nfs()->start();
                    dassert(err == ERR_OK, "start nfs failed, err = %s", err.to_string());
                }

                err = _app->start(_app->arg_count(), _app->args());
                dassert(err == ERR_OK, "start app failed, err = %s", err.to_string());
                _app->_started = true;
            }
            else
            {
                _app->stop();
                _app->_started = false;
            }
        }

    private:
        service::service_app* _app;
        bool     _start; // false for stop
    };


    namespace tools {

        tool_base::tool_base(const char* name)
        {
            _name = name;
        }

        toollet::toollet(const char* name)
            : tool_base(name)
        {
        }

        tool_app::tool_app(const char* name)
            : tool_base(name)
        {
        }

        void tool_app::start_all_service_apps()
        {
            auto apps = service::system::get_all_apps();
            for (auto it = apps.begin(); it != apps.end(); it++)
            {
                task_ptr t(new service_control_task(it->second, true));
                t->set_delay(1000 * it->second->spec().delay_seconds);
                t->enqueue();
            }
        }


        void tool_app::stop_all_service_apps()
        {
            auto apps = service::system::get_all_apps();
            for (auto it = apps.begin(); it != apps.end(); it++)
            {
                task_ptr t(new service_control_task(it->second, false));
                t->enqueue();
            }
        }

        const service_spec& tool_app::get_service_spec()
        {
            return service_engine::instance().spec();
        }

        configuration_ptr config()
        {
            return service_engine::instance().spec().config;
        }

        join_point<void, configuration_ptr> sys_init_before_app_created("system.init.1");
        join_point<void, configuration_ptr> sys_init_after_app_created("system.init.2");
        join_point<void, sys_exit_type> sys_exit("system.exit");

        namespace internal_use_only
        {
            bool register_toollet(const char* name, toollet_factory f, int type)
            {
                return dsn::utils::factory_store<toollet>::register_factory(name, f, type);
            }

            bool register_tool(const char* name, tool_app_factory f, int type)
            {
                return dsn::utils::factory_store<tool_app>::register_factory(name, f, type);
            }

            bool register_component_provider(const char* name, task_queue_factory f, int type)
            {
                return dsn::utils::factory_store<task_queue>::register_factory(name, f, type);
            }

            bool register_component_provider(const char* name, task_worker_factory f, int type)
            {
                return dsn::utils::factory_store<task_worker>::register_factory(name, f, type);
            }

            bool register_component_provider(const char* name, admission_controller_factory f, int type)
            {
                return dsn::utils::factory_store<admission_controller>::register_factory(name, f, type);
            }

            bool register_component_provider(const char* name, lock_factory f, int type)
            {
                return dsn::utils::factory_store<lock_provider>::register_factory(name, f, type);
            }

            bool register_component_provider(const char* name, read_write_lock_factory f, int type)
            {
                return dsn::utils::factory_store<rwlock_nr_provider>::register_factory(name, f, type);
            }

            bool register_component_provider(const char* name, semaphore_factory f, int type)
            {
                return dsn::utils::factory_store<semaphore_provider>::register_factory(name, f, type);
            }

            bool register_component_provider(const char* name, network_factory f, int type)
            {
                return dsn::utils::factory_store<network>::register_factory(name, f, type);
            }

            bool register_component_provider(const char* name, aio_factory f, int type)
            {
                return dsn::utils::factory_store<aio_provider>::register_factory(name, f, type);
            }

            bool register_component_provider(const char* name, env_factory f, int type)
            {
                return dsn::utils::factory_store<env_provider>::register_factory(name, f, type);
            }

            bool register_component_provider(const char* name, perf_counter_factory f, int type)
            {
                return dsn::utils::factory_store<perf_counter>::register_factory(name, f, type);
            }

            bool register_component_provider(const char* name, nfs_factory f, int type)
            {
                return dsn::utils::factory_store<nfs_node>::register_factory(name, f, type);
            }

            bool register_component_provider(const char* name, logging_factory f, int type)
            {
                return dsn::utils::factory_store<logging_provider>::register_factory(name, f, type);
            }

            bool register_component_provider(const char* name, memory_factory f, int type)
            {
                return dsn::utils::factory_store<memory_provider>::register_factory(name, f, type);
            }

            bool register_component_provider(const char* name, message_parser_factory f, int type)
            {
                return dsn::utils::factory_store<message_parser>::register_factory(name, f, type);
            }

            toollet* get_toollet(const char* name, int type)
            {
                toollet* tlt = nullptr;
                if (utils::singleton_store<std::string, toollet*>::instance().get(name, tlt))
                    return tlt;
                else
                {
                    tlt = utils::factory_store<toollet>::create(name, type, name);
                    utils::singleton_store<std::string, toollet*>::instance().put(name, tlt);
                    return tlt;
                }
            }

            configuration_ptr config()
            {
                return ::dsn::service::system::config();
            }
        }
    }
} // end namespace dsn::tool_api
