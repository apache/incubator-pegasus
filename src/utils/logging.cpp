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

#include <dsn/tool-api/command_manager.h>
#include <dsn/tool-api/logging_provider.h>
#include <dsn/tool-api/auto_codes.h>
#include <dsn/utility/flags.h>
#include <dsn/utility/smart_pointers.h>
#include "simple_logger.h"

DSN_API dsn_log_level_t dsn_log_start_level = dsn_log_level_t::LOG_LEVEL_INFORMATION;
DSN_DEFINE_string("core",
                  logging_start_level,
                  "LOG_LEVEL_INFORMATION",
                  "logs with level below this will not be logged");

DSN_DEFINE_bool("core", logging_flush_on_exit, true, "flush log when exit system");

namespace dsn {

using namespace tools;
DSN_REGISTER_COMPONENT_PROVIDER(screen_logger, "dsn::tools::screen_logger");
DSN_REGISTER_COMPONENT_PROVIDER(simple_logger, "dsn::tools::simple_logger");

std::function<std::string()> log_prefixed_message_func = []() {
    static thread_local std::string prefixed_message;

    static thread_local std::once_flag flag;
    std::call_once(flag, [&]() {
        prefixed_message.resize(23);
        int tid = dsn::utils::get_current_tid();
        sprintf(const_cast<char *>(prefixed_message.c_str()), "unknown.io-thrd.%05d: ", tid);
    });

    return prefixed_message;
};

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

    dassert(dsn_log_start_level != dsn_log_level_t::LOG_LEVEL_INVALID,
            "invalid [core] logging_start_level specified");

    // register log flush on exit
    if (FLAGS_logging_flush_on_exit) {
        ::dsn::tools::sys_exit.put_back(log_on_sys_exit, "log.flush");
    }

    dsn::logging_provider *logger = dsn::utils::factory_store<dsn::logging_provider>::create(
        logging_factory_name.c_str(), dsn::PROVIDER_TYPE_MAIN, dir_log.c_str());
    dsn::logging_provider::set_logger(logger);

    // register command for logging
    ::dsn::command_manager::instance().register_command(
        {"flush-log"},
        "flush-log - flush log to stderr or log file",
        "flush-log",
        [](const std::vector<std::string> &args) {
            dsn::logging_provider *logger = dsn::logging_provider::instance();
            logger->flush();
            return "Flush done.";
        });
    ::dsn::command_manager::instance().register_command(
        {"reset-log-start-level"},
        "reset-log-start-level - reset the log start level",
        "reset-log-start-level [INFORMATION | DEBUG | WARNING | ERROR | FATAL]",
        [](const std::vector<std::string> &args) {
            dsn_log_level_t start_level;
            if (args.size() == 0) {
                start_level =
                    enum_from_string(FLAGS_logging_start_level, dsn_log_level_t::LOG_LEVEL_INVALID);
            } else {
                std::string level_str = "LOG_LEVEL_" + args[0];
                start_level =
                    enum_from_string(level_str.c_str(), dsn_log_level_t::LOG_LEVEL_INVALID);
                if (start_level == dsn_log_level_t::LOG_LEVEL_INVALID) {
                    return "ERROR: invalid level '" + args[0] + "'";
                }
            }
            dsn_log_set_start_level(start_level);
            return std::string("OK, current level is ") + enum_to_string(start_level);
        });

    if (dsn_log_prefixed_message_func != nullptr) {
        dsn::set_log_prefixed_message_func(dsn_log_prefixed_message_func);
    }
}

DSN_API dsn_log_level_t dsn_log_get_start_level() { return dsn_log_start_level; }

DSN_API void dsn_log_set_start_level(dsn_log_level_t level) { dsn_log_start_level = level; }

DSN_API void dsn_logv(const char *file,
                      const char *function,
                      const int line,
                      dsn_log_level_t log_level,
                      const char *fmt,
                      va_list args)
{
    dsn::logging_provider *logger = dsn::logging_provider::instance();
    logger->dsn_logv(file, function, line, log_level, fmt, args);
}

DSN_API void dsn_logf(const char *file,
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

DSN_API void dsn_log(const char *file,
                     const char *function,
                     const int line,
                     dsn_log_level_t log_level,
                     const char *str)
{
    dsn::logging_provider *logger = dsn::logging_provider::instance();
    logger->dsn_log(file, function, line, log_level, str);
}

namespace dsn {

std::unique_ptr<logging_provider> logging_provider::_logger =
    std::unique_ptr<logging_provider>(nullptr);

logging_provider *logging_provider::instance()
{
    static std::unique_ptr<logging_provider> default_logger =
        std::unique_ptr<logging_provider>(create_default_instance());
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
