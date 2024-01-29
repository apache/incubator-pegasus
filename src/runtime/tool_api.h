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

/*!
@defgroup tool-api-hooks Join Points
@ingroup tool-api

Join points are hooks that allow for system monitoring and manipulation

@defgroup tool-api-providers Component Providers
@ingroup tool-api

Component providers define the interface for the local components (e.g., network, lock)
*/

#pragma once

#include <stddef.h>
#include <string>
#include <vector>

#include "runtime/env_provider.h"
#include "runtime/rpc/message_parser.h"
#include "runtime/rpc/network.h"
#include "runtime/task/task_queue.h"
#include "runtime/task/task_spec.h"
#include "runtime/task/task_worker.h"
#include "runtime/task/timer_service.h"
// providers
#include "utils/factory_store.h"
#include "utils/join_point.h"
#include "utils/logging_provider.h" // IWYU pragma: keep

namespace dsn {
class service_node;
struct service_spec;

namespace tools {

// Define the interface for implementing and plug-in the tools & runtime components into rDSN.
// In rDSN, both developement tools and runtime libraries (e.g., high performance components) are
// considered tools.

/*!
@addtogroup tool-api-providers
@{
 */
class tool_base
{
public:
    virtual ~tool_base() {}

    explicit tool_base(const char *name);

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
    toollet(const char *name);

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
    tool_app(const char *name);

    virtual void install(service_spec &spec) = 0;

    // this routine will be invoked in the main thread as the tool driver (if necessary for the
    // tool, e.g., model checking)
    virtual void run() { start_all_apps(); }

public:
    virtual void start_all_apps();
    virtual void stop_all_apps(bool cleanup);

    static const service_spec &get_service_spec();
};

namespace internal_use_only {
bool register_component_provider(const char *name,
                                 timer_service::factory f,
                                 ::dsn::provider_type type);
bool register_component_provider(const char *name,
                                 task_queue::factory f,
                                 ::dsn::provider_type type);
bool register_component_provider(const char *name,
                                 task_worker::factory f,
                                 ::dsn::provider_type type);
bool register_component_provider(const char *name, network::factory f, ::dsn::provider_type type);
bool register_component_provider(const char *name,
                                 env_provider::factory f,
                                 ::dsn::provider_type type);
bool register_component_provider(network_header_format fmt,
                                 const std::vector<const char *> &signatures,
                                 message_parser::factory f,
                                 size_t sz);
bool register_toollet(const char *name, toollet::factory f, ::dsn::provider_type type);
bool register_tool(const char *name, tool_app::factory f, ::dsn::provider_type type);
toollet *get_toollet(const char *name, ::dsn::provider_type type);
} // namespace internal_use_only

/*!
@addtogroup tool-api-hooks
@{
*/
extern join_point<void> sys_init_before_app_created;
extern join_point<void> sys_init_after_app_created;
/*@}*/

template <typename T>
bool register_component_provider(const char *name)
{
    return internal_use_only::register_component_provider(
        name, T::template create<T>, ::dsn::PROVIDER_TYPE_MAIN);
}

template <typename T>
struct component_provider_registerer
{
    component_provider_registerer(const char *name) { register_component_provider<T>(name); }
};

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
tool_app *get_current_tool();
const service_spec &spec();
const char *get_service_node_name(service_node *node);
bool is_engine_ready();

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
} // namespace tools

#define DSN_REGISTER_COMPONENT_PROVIDER(type, name)                                                \
    static tools::component_provider_registerer<type> COMPONENT_PROVIDER_REG_##type(name)

} // namespace dsn
