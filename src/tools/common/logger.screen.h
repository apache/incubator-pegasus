/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation

 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.

 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
# pragma once

# include <dsn/tool_api.h>
# include <dsn/service_api.h>

namespace dsn { namespace tools {

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
            printf("(%016llx) %llu ms @ %s ", task::get_current_task_id(), ::dsn::service::env::now_ns() / 1000000, wn);
            vprintf(fmt, args);
            //printf(" [%s(%d) %s]\n", file, line, function);
            printf("\n");
        }
        else
        {
            printf("(%016llx) %llu ms @ %s ", task::get_current_task_id(), ::dsn::service::env::now_ns() / 1000000, wn);
            vprintf(fmt, args);
            //printf(" [%s(%d) %s]\n", file, line, function);
            printf("\n");
        }
    }

private:
    std::recursive_mutex _lock;
};

}}
