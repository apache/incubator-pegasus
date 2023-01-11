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

#include "utils/logging_provider.h"

#include "common/gpid.h"
#include "runtime/task/task_code.h"
#include "utils/command_manager.h"
#include "utils/error_code.h"
#include "utils/flags.h"
#include "utils/smart_pointers.h"
#include "utils/threadpool_code.h"
#include "simple_logger.h"

dsn_log_level_t dsn_log_start_level = dsn_log_level_t::LOG_LEVEL_INFO;
DSN_DEFINE_string(core,
                  logging_start_level,
                  "LOG_LEVEL_INFO",
                  "logs with level below this will not be logged");

DSN_DEFINE_bool(core, logging_flush_on_exit, true, "flush log when exit system");

namespace dsn {

using namespace tools;
DSN_REGISTER_COMPONENT_PROVIDER(screen_logger, "dsn::tools::screen_logger");
DSN_REGISTER_COMPONENT_PROVIDER(simple_logger, "dsn::tools::simple_logger");

std::function<std::string()> log_prefixed_message_func = []() -> std::string { return ": "; };

void set_log_prefixed_message_func(std::function<std::string()> func)
{
    log_prefixed_message_func = func;
}
} // namespace dsn

static void log_on_sys_exit(::dsn::sys_exit_type)
{
    dsn::logging_provider *logger = dsn::logging_provider::instance();
    logger->flush();
}

void dsn_log_init(const std::string &logging_factory_name,
                  const std::string &dir_log,
                  std::function<std::string()> dsn_log_prefixed_message_func)
{
    dsn_log_start_level =
        enum_from_string(FLAGS_logging_start_level, dsn_log_level_t::LOG_LEVEL_INVALID);

    CHECK_NE_MSG(dsn_log_start_level,
                 dsn_log_level_t::LOG_LEVEL_INVALID,
                 "invalid [core] logging_start_level specified");

    // register log flush on exit
    if (FLAGS_logging_flush_on_exit) {
        ::dsn::tools::sys_exit.put_back(log_on_sys_exit, "log.flush");
    }

    dsn::logging_provider *logger = dsn::utils::factory_store<dsn::logging_provider>::create(
        logging_factory_name.c_str(), dsn::PROVIDER_TYPE_MAIN, dir_log.c_str());
    dsn::logging_provider::set_logger(logger);

    if (dsn_log_prefixed_message_func != nullptr) {
        dsn::set_log_prefixed_message_func(dsn_log_prefixed_message_func);
    }
}

dsn_log_level_t dsn_log_get_start_level() { return dsn_log_start_level; }

void dsn_log_set_start_level(dsn_log_level_t level) { dsn_log_start_level = level; }

void dsn_logv(const char *file,
              const char *function,
              const int line,
              dsn_log_level_t log_level,
              const char *fmt,
              va_list args)
{
    dsn::logging_provider *logger = dsn::logging_provider::instance();
    logger->dsn_logv(file, function, line, log_level, fmt, args);
}

void dsn_logf(const char *file,
              const char *function,
              const int line,
              dsn_log_level_t log_level,
              const char *fmt,
              ...)
{
    va_list ap;
    va_start(ap, fmt);
    dsn_logv(file, function, line, log_level, fmt, ap);
    va_end(ap);
}

void dsn_log(const char *file,
             const char *function,
             const int line,
             dsn_log_level_t log_level,
             const char *str)
{
    dsn::logging_provider *logger = dsn::logging_provider::instance();
    logger->dsn_log(file, function, line, log_level, str);
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
    return new tools::screen_logger(true);
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
