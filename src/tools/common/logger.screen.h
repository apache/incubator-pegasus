# pragma once

# include <rdsn/tool_api.h>
# include <rdsn/service_api.h>

namespace rdsn { namespace tools {

class screen_logger : public logging_provider
{
public:
    screen_logger(const char *parameter) : logging_provider(parameter) { }
    virtual ~screen_logger(void) { }

    virtual void logv(const char *file, 
            const char *function, 
            const int line,         
            logging_level logLevel, 
            const char* title, 
            const char *fmt, 
            va_list args
            )
    {   

        const char* wn = "unknown";
        if (task::get_current_worker())
        {
            wn = (const char*)task::get_current_worker()->name().c_str();
        }            

        utils::auto_lock l(_lock); 
        if (logLevel >= log_level_WARNING)
        {
            // TODO: console color output
            printf("(%016llx) %llu ms @ %s ", task::get_current_task_id(), ::rdsn::service::env::now_ns() / 1000000, wn);
            vprintf(fmt, args);
            //printf(" [%s(%d) %s]\n", file, line, function);
            printf("\n");
        }
        else
        {
            printf("(%016llx) %llu ms @ %s ", task::get_current_task_id(), ::rdsn::service::env::now_ns() / 1000000, wn);
            vprintf(fmt, args);
            //printf(" [%s(%d) %s]\n", file, line, function);
            printf("\n");
        }
    }

private:
    std::recursive_mutex _lock;
};

}}