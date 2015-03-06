# pragma once

# include <exception>
# include <stdarg.h>

#ifdef _WIN32
__pragma(warning(disable:4127))
#endif

namespace rdsn {

enum logging_level
{
    log_level_INFORMATION,
    log_level_DEBUG,
    log_level_WARNING,
    log_level_ERROR,
    log_level_FATAL
};

extern void logv(const char *file, const char *function, const int line, logging_level logLevel, const char* title, const char* fmt, va_list args);

extern void logv(const char *file, const char *function, const int line, logging_level logLevel, const char* title, const char* fmt, ...);

extern void logv(const char *file, const char *function, const int line, logging_level logLevel, const char* title);
} // end namespace

#define rdsn_log(level, title, ...) rdsn::logv(__FILE__, __FUNCTION__, __LINE__, level, title, __VA_ARGS__)
#define rdsn_info(...)  rdsn_log(rdsn::log_level_INFORMATION, __TITLE__, __VA_ARGS__)
#define rdsn_debug(...) rdsn_log(rdsn::log_level_DEBUG, __TITLE__, __VA_ARGS__)
#define rdsn_warn(...)  rdsn_log(rdsn::log_level_WARNING, __TITLE__, __VA_ARGS__)
#define rdsn_error(...) rdsn_log(rdsn::log_level_ERROR, __TITLE__, __VA_ARGS__)
#define rdsn_fatal(...) rdsn_log(rdsn::log_level_FATAL, __TITLE__, __VA_ARGS__)
#define rdsn_assert(x, ...) do { if (!(x)) {rdsn_log(rdsn::log_level_FATAL, #x, __VA_ARGS__); *((int*)0x1) = 0; } } while (false)

#ifdef _DEBUG
#define rdsn_debug_assert rdsn_assert
#else
#define rdsn_debug_assert(x, ...) 
#endif
