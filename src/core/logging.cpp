# include <rdsn/internal/logging_provider.h>
# include "service_engine.h"

namespace rdsn {

    void logv(const char *file, const char *function, const int line, logging_level logLevel, const char* title, const char* fmt, va_list args)
    {
        logging_provider* logger = service_engine::instance().logging();
        if (logger != nullptr)
        {
            logger->logv(file, function, line, logLevel, title, fmt, args);
        }        
    }

    void logv(const char *file, const char *function, const int line, logging_level logLevel, const char* title, const char* fmt, ...)
    {
        va_list ap;
        va_start(ap, fmt);
        logv(file, function, line, logLevel, title, fmt, ap);
        va_end(ap);
    }

    void logv(const char *file, const char *function, const int line, logging_level logLevel, const char* title)
    {
        // TODO: using logging provider
        printf ("assertion at %s:%u in %s\n", function, line, file);
    }

} // end name
