# pragma once

# include <rdsn/internal/logging.h>
# include <stdarg.h>

namespace rdsn {

class logging_provider
{
public:
    template <typename T> static logging_provider* create(const char *parameter)
    {
        return new T(parameter);
    }

public:
    logging_provider(const char *parameter) {}

    virtual ~logging_provider(void) { }
    
    virtual void logv(const char *file, const char *function, const int line, logging_level logLevel, const char* title, const char* fmt, va_list args) = 0;
};


// ----------------------- inline implementation ---------------------------------------
} // end namespace
