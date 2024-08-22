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

#include <algorithm>
#include <functional>
#include <memory>
#include <string>
#include <utility>

#include "runtime/tool_api.h"
#include "simple_logger.h"
#include "utils/api_utilities.h"
#include "utils/factory_store.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/join_point.h"
#include "utils/logging_provider.h"
#include "utils/sys_exit_hook.h"

DSN_DEFINE_string(core,
                  logging_start_level,
                  "LOG_LEVEL_INFO",
                  "Logs with level larger than or equal to this level be logged");

DSN_DEFINE_bool(core,
                logging_flush_on_exit,
                true,
                "Whether to flush the logs when the process exits");

log_level_t log_start_level = LOG_LEVEL_INFO;

namespace dsn {

using namespace tools;
DSN_REGISTER_COMPONENT_PROVIDER(screen_logger, "dsn::tools::screen_logger");
DSN_REGISTER_COMPONENT_PROVIDER(simple_logger, "dsn::tools::simple_logger");

std::function<std::string()> log_prefixed_message_func = []() -> std::string { return ": "; };

void set_log_prefixed_message_func(std::function<std::string()> func)
{
    log_prefixed_message_func = std::move(func);
}
} // namespace dsn

static void log_on_sys_exit(::dsn::sys_exit_type)
{
    dsn::logging_provider *logger = dsn::logging_provider::instance();
    logger->flush();
}

void dsn_log_init(const std::string &logging_factory_name,
                  const std::string &log_dir,
                  const std::string &role_name,
                  const std::function<std::string()> &dsn_log_prefixed_message_func)
{
    log_start_level = enum_from_string(FLAGS_logging_start_level, LOG_LEVEL_INVALID);

    CHECK_NE_MSG(
        log_start_level, LOG_LEVEL_INVALID, "invalid [core] logging_start_level specified");

    // register log flush on exit
    if (FLAGS_logging_flush_on_exit) {
        ::dsn::tools::sys_exit.put_back(log_on_sys_exit, "log.flush");
    }

    dsn::logging_provider *logger = dsn::utils::factory_store<dsn::logging_provider>::create(
        logging_factory_name.c_str(), dsn::PROVIDER_TYPE_MAIN, log_dir.c_str(), role_name.c_str());
    dsn::logging_provider::set_logger(logger);

    if (dsn_log_prefixed_message_func != nullptr) {
        dsn::set_log_prefixed_message_func(dsn_log_prefixed_message_func);
    }
}

log_level_t get_log_start_level() { return log_start_level; }

void set_log_start_level(log_level_t level) { log_start_level = level; }

void global_log(
    const char *file, const char *function, const int line, log_level_t log_level, const char *str)
{
    dsn::logging_provider *logger = dsn::logging_provider::instance();
    logger->log(file, function, line, log_level, str);
}

namespace dsn {

std::unique_ptr<logging_provider> logging_provider::_logger;

logging_provider *logging_provider::instance()
{
    static std::unique_ptr<logging_provider> default_logger(create_default_instance());
    return _logger ? _logger.get() : default_logger.get();
}

logging_provider *logging_provider::create_default_instance()
{
    return new tools::screen_logger(nullptr, nullptr);
}

void logging_provider::set_logger(logging_provider *logger) { _logger.reset(logger); }

namespace tools {
namespace internal_use_only {
bool register_component_provider(const char *name,
                                 logging_provider::factory f,
                                 ::dsn::provider_type type)
{
    return dsn::utils::factory_store<logging_provider>::register_factory(name, f, type);
}
} // namespace internal_use_only
} // namespace tools
} // namespace dsn
