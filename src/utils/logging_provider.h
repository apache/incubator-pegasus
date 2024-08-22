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

#pragma once

#include "utils/api_utilities.h"
#include "utils/command_manager.h"
#include "utils/factory_store.h"

namespace dsn {

/*!
@addtogroup tool-api-providers
@{
*/
class logging_provider
{
public:
    template <typename T>
    static logging_provider *create(const char *log_dir, const char *role_name)
    {
        return new T(log_dir, role_name);
    }

    typedef logging_provider *(*factory)(const char *, const char *);

public:
    virtual ~logging_provider() = default;

    // singleton
    static logging_provider *instance();

    // not thread-safe
    static void set_logger(logging_provider *logger);

    virtual void log(const char *file,
                     const char *function,
                     const int line,
                     log_level_t log_level,
                     const char *str) = 0;

    virtual void flush() = 0;

protected:
    static std::unique_ptr<logging_provider> _logger;

    static logging_provider *create_default_instance();

    logging_provider(log_level_t stderr_start_level) : _stderr_start_level(stderr_start_level) {}

    const log_level_t _stderr_start_level;
};

void set_log_prefixed_message_func(std::function<std::string()> func);
extern std::function<std::string()> log_prefixed_message_func;

namespace tools {
namespace internal_use_only {
bool register_component_provider(const char *name,
                                 logging_provider::factory f,
                                 ::dsn::provider_type type);
} // namespace internal_use_only
} // namespace tools
} // namespace dsn

extern void dsn_log_init(const std::string &logging_factory_name,
                         const std::string &log_dir,
                         const std::string &role_name,
                         const std::function<std::string()> &dsn_log_prefixed_message_func);
