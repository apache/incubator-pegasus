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

/*
 * Description:
 *     What is this file about?
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */
 
# include "service_engine.h"
# include "rpc_engine.h"
# include <dsn/utility/singleton_store.h>

DSN_API bool dsn_register_app(dsn_app* app_type)
{
    dsn_app* app;
    auto& store = ::dsn::utils::singleton_store<std::string, dsn_app*>::instance();
    if (store.get(app_type->type_name, app))
    {
        dassert(false, "app type %s is already registered", app_type->type_name);
        return false;
    }

    app = new dsn_app();
    *app = *app_type;
    auto r = store.put(app_type->type_name, app);
    dassert(r, "app type %s is already registered", app_type->type_name);
    return r;
}

DSN_API bool dsn_get_app_callbacks(const char* name, /* out */ dsn_app_callbacks* callbacks)
{
    dsn_app* lapp;
    auto& store = ::dsn::utils::singleton_store<std::string, dsn_app*>::instance();
    if (store.get(name, lapp))
    {
        *callbacks = lapp->layer2.apps;
        return true;
    }
    else
    {
        dwarn("application model '%s' is not found, make sure it is registered", name);
        return false;
    }
}

DSN_API dsn_error_t dsn_hosted_app_create(
    const char* type,
    dsn_gpid gpid,
    const char* data_dir,
    /*our*/ void** app_context_for_downcalls,
    /*out*/void** app_context_for_callbacks
    )
{
    return ::dsn::task::get_current_node2()->get_l2_handler().create_app(type, gpid, data_dir,
        app_context_for_downcalls, app_context_for_callbacks);
}

DSN_API dsn_error_t dsn_hosted_app_start(void* app_context, int argc, char** argv)
{
    return ::dsn::task::get_current_node2()->get_l2_handler().start_app(app_context, argc, argv);
}

DSN_API dsn_error_t dsn_hosted_app_destroy(void* app_context, bool cleanup)
{
    return ::dsn::task::get_current_node2()->get_l2_handler().destroy_app(app_context, cleanup);
}

DSN_API void dsn_hosted_app_commit_rpc_request(void* app_context, dsn_message_t msg, bool exec_inline)
{
    auto app = (::dsn::app_manager::app_internal*)(app_context);

    if (exec_inline)
    {
        app->server_dispatcher.on_request_with_inline_execution((::dsn::message_ex*)(msg), ::dsn::task::get_current_node2());
    }
    else
    {
        auto tsk = app->server_dispatcher.on_request((::dsn::message_ex*)(msg), ::dsn::task::get_current_node2());
        if (tsk)
            tsk->enqueue();
        else
        {
            dassert(false, "to be handled");
        }
    }
}


namespace dsn 
{
    app_manager::app_manager(service_node* node)
        : _owner_node(node)
    {
    }

    error_code app_manager::create_app(
        const char* type, 
        dsn_gpid gpid, 
        const char* data_dir,
        /*out*/ void** app_context,
        /*out*/ void** app_context_for_callbacks)
    {
        app_internal* app = nullptr;
        dsn_app* app_model = nullptr;

        auto& store = ::dsn::utils::singleton_store<std::string, dsn_app*>::instance();
        if (!store.get(type, app_model))
        {
            derror("app model '%s' is not registered yet", type);
            return ERR_OBJECT_NOT_FOUND;
        }

        {
            utils::auto_write_lock l(_apps_lock);

            auto it = _apps.find(gpid.value);
            if (it != _apps.end())
            {
                *app_context = it->second.get();
                *app_context_for_callbacks = it->second.get()->app_context;
                return ERR_SERVICE_ALREADY_EXIST;
            }
            else
            {
                app = new app_manager::app_internal();
                app->gpid = gpid;
                app->model = app_model;

                memset(&app->info, 0, sizeof(app->info));
                app->info.app_id = gpid.u.app_id;
                app->info.index = gpid.u.partition_index;
                strncpy(app->info.role, _owner_node->spec().role_name.c_str(), sizeof(app->info.role));
                strncpy(app->info.name, _owner_node->spec().name.c_str(), sizeof(app->info.name));
                strncpy(app->info.data_dir, data_dir, sizeof(app->info.data_dir));

                _apps.emplace(gpid.value,
                    std::unique_ptr<app_manager::app_internal>(app));
            }
        }

        app->app_context = app->model->layer1.create(_owner_node->spec().role->type_name, gpid);
        app->info.app.app_context_ptr = app->app_context;
        *app_context = app;
        *app_context_for_callbacks = app->app_context;
        return ERR_OK;
    }

    error_code app_manager::start_app(void* app_context, int argc, char** argv)
    {
        auto app = (::dsn::app_manager::app_internal*)(app_context);
        return app->model->layer1.start(
            app->app_context,
            argc,
            argv
            );
    }

    error_code app_manager::destroy_app(void* app_context, bool cleanup)
    {
        auto app = (app_manager::app_internal*)(app_context);
        error_code err = app->model->layer1.destroy(app->app_context, cleanup);

        if (err == ERR_OK)
        {
            utils::auto_write_lock l(_apps_lock);
            auto it = _apps.find(app->gpid.value);
            if (it != _apps.end())
            {
                _apps.erase(it);
            }
            else
            {
                dassert(false,
                    "framework hosted app is missing, gpid = %d.%d!",
                    app->gpid.u.app_id,
                    app->gpid.u.partition_index
                    );
            }
        }
        else
        {
            derror("destroy framework hosted app %s failed, err = %s",
                app->info.name,
                err.to_string()
                );
        }

        return err;
    }

    bool app_manager::rpc_register_handler(dsn_gpid gpid, rpc_handler_info* handler)
    {
        app_internal* app;
        {
            utils::auto_read_lock l(_apps_lock);
            auto it = _apps.find(gpid.value);
            if (it != _apps.end())
            {
                app = it->second.get();
            }
            else
                return false;
        }

        return app->server_dispatcher.register_rpc_handler(handler);
    }

    rpc_handler_info* app_manager::rpc_unregister_handler(dsn_gpid gpid, dsn_task_code_t rpc_code)
    {
        app_internal* app;
        {
            utils::auto_read_lock l(_apps_lock);
            auto it = _apps.find(gpid.value);
            if (it != _apps.end())
            {
                app = it->second.get();
            }
            else
                return nullptr;
        }

        return app->server_dispatcher.unregister_rpc_handler(rpc_code);
    }

    dsn_app_info* app_manager::get_app_info(dsn_gpid gpid)
    {
        utils::auto_read_lock l(_apps_lock);
        auto it = _apps.find(gpid.value);
        if (it != _apps.end())
        {
            return &it->second->info;
        }
        else
            return nullptr;
    }

}

