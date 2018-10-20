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
 *     define the interface for implementing and plug-in the tools &
 *     runtime components into rDSN.
 *     In rDSN, both developement tools and runtime libraries
 *     (e.g., high performance components) are considered tools.
 *
 * Revision history:
 *     Mar., 2015, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

/*!
@defgroup tool-api-hooks Join Points
@ingroup tool-api

Join points are hooks that allow for system monitoring and manipulation

@defgroup tool-api-providers Component Providers
@ingroup tool-api

Component providers define the interface for the local components (e.g., network, lock)
*/

#pragma once

// providers
#include <dsn/tool-api/global_config.h>
#include <dsn/utility/factory_store.h>
#include <dsn/tool-api/task_queue.h>
#include <dsn/tool-api/task_worker.h>
#include <dsn/tool-api/admission_controller.h>
#include <dsn/tool-api/network.h>
#include <dsn/tool-api/aio_provider.h>
#include <dsn/tool-api/env_provider.h>
#include <dsn/tool-api/message_parser.h>
#include <dsn/tool-api/logging_provider.h>
#include <dsn/tool-api/timer_service.h>

namespace dsn {
namespace tools {

/*!
@addtogroup tool-api-providers
@{
 */
class tool_base
{
public:
    virtual ~tool_base() {}

    DSN_API explicit tool_base(const char *name);

    const std::string &name() const { return _name; }

protected:
    std::string _name;
};

class toollet : public tool_base
{
public:
    template <typename T>
    static toollet *create(const char *name)
    {
        return new T(name);
    }

    typedef toollet *(*factory)(const char *);

public:
    DSN_API toollet(const char *name);

    virtual void install(service_spec &spec) = 0;
};

class tool_app : public tool_base
{
public:
    template <typename T>
    static tool_app *create(const char *name)
    {
        return new T(name);
    }

    typedef tool_app *(*factory)(const char *);

public:
    DSN_API tool_app(const char *name);

    virtual void install(service_spec &spec) = 0;

    // this routine will be invoked in the main thread as the tool driver (if necessary for the
    // tool, e.g., model checking)
    virtual void run() { start_all_apps(); }

public:
    DSN_API virtual void start_all_apps();
    DSN_API virtual void stop_all_apps(bool cleanup);

    DSN_API static const service_spec &get_service_spec();
};

namespace internal_use_only {
DSN_API bool
register_component_provider(const char *name, timer_service::factory f, ::dsn::provider_type type);
DSN_API bool
register_component_provider(const char *name, task_queue::factory f, ::dsn::provider_type type);
DSN_API bool
register_component_provider(const char *name, task_worker::factory f, ::dsn::provider_type type);
DSN_API bool register_component_provider(const char *name,
                                         admission_controller::factory f,
                                         ::dsn::provider_type type);
DSN_API bool
register_component_provider(const char *name, network::factory f, ::dsn::provider_type type);
DSN_API bool
register_component_provider(const char *name, aio_provider::factory f, ::dsn::provider_type type);
DSN_API bool
register_component_provider(const char *name, env_provider::factory f, ::dsn::provider_type type);
DSN_API bool register_component_provider(const char *name,
                                         logging_provider::factory f,
                                         ::dsn::provider_type type);
DSN_API bool register_component_provider(network_header_format fmt,
                                         const std::vector<const char *> &signatures,
                                         message_parser::factory f,
                                         size_t sz);
DSN_API bool register_toollet(const char *name, toollet::factory f, ::dsn::provider_type type);
DSN_API bool register_tool(const char *name, tool_app::factory f, ::dsn::provider_type type);
DSN_API toollet *get_toollet(const char *name, ::dsn::provider_type type);
}

/*!
@addtogroup tool-api-hooks
@{
*/
DSN_API extern join_point<void> sys_init_before_app_created;
DSN_API extern join_point<void> sys_init_after_app_created;
DSN_API extern join_point<void, sys_exit_type> sys_exit;
/*@}*/

template <typename T>
bool register_component_provider(const char *name)
{
    return internal_use_only::register_component_provider(
        name, T::template create<T>, ::dsn::PROVIDER_TYPE_MAIN);
}
template <typename T>
bool register_component_aspect(const char *name)
{
    return internal_use_only::register_component_provider(
        name, T::template create<T>, ::dsn::PROVIDER_TYPE_ASPECT);
}
template <typename T>
bool register_message_header_parser(network_header_format fmt,
                                    const std::vector<const char *> &signatures);

template <typename T>
bool register_toollet(const char *name)
{
    return internal_use_only::register_toollet(
        name, toollet::template create<T>, ::dsn::PROVIDER_TYPE_MAIN);
}
template <typename T>
bool register_tool(const char *name)
{
    return internal_use_only::register_tool(
        name, tool_app::template create<T>, ::dsn::PROVIDER_TYPE_MAIN);
}
template <typename T>
T *get_toollet(const char *name)
{
    return (T *)internal_use_only::get_toollet(name, ::dsn::PROVIDER_TYPE_MAIN);
}
DSN_API tool_app *get_current_tool();
DSN_API const service_spec &spec();
DSN_API const char *get_service_node_name(service_node *node);
DSN_API bool is_engine_ready();

/*
 @}
 */

// --------- inline implementation -----------------------------
template <typename T>
bool register_message_header_parser(network_header_format fmt,
                                    const std::vector<const char *> &signatures)
{
    return internal_use_only::register_component_provider(
        fmt, signatures, T::template create<T>, sizeof(T));
}
}
} // end namespace dsn::tools
