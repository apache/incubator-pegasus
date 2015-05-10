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
# include "simple_logger.h"
# include <boost/lexical_cast.hpp>
# include <boost/thread.hpp>

namespace dsn {
    namespace tools {

        static void print_header(FILE* fp)
        {
            uint64_t ts = 0;
            if (::dsn::service::system::is_ready())
                ts = ::dsn::service::env::now_ms();

            char str[24];
            ::dsn::utils::time_ms_to_string(ts, str);

            fprintf(fp, "%s ", str);

            task* t = task::get_current_task();
            if (t)
            {
                if (nullptr != task::get_current_worker())
                {
                    fprintf(fp, "%6s.%7s%u.%016llx: ",
                        t->node_name(),
                        task::get_current_worker()->pool_spec().name.c_str(),
                        task::get_current_worker()->index(),
                        t->id()
                        );
                }
                else
                {
                    std::string tid = boost::lexical_cast<std::string>(boost::this_thread::get_id());

                    fprintf(fp, "%6s.%7s.%s.%016llx: ",
                        t->node_name(),
                        "io-thrd",
                        tid.c_str(),
                        t->id()
                        );
                }
            }
            else
            {
                std::string tid = boost::lexical_cast<std::string>(boost::this_thread::get_id());
                fprintf(fp, "%6s.%7s.%s: ",
                    "system",
                    "io-thrd",
                    tid.c_str()
                    );
            }
        }

        void screen_logger::logv(const char *file,
            const char *function,
            const int line,
            logging_level logLevel,
            const char* title,
            const char *fmt,
            va_list args
            )
        {
            utils::auto_lock l(_lock);

            print_header(stdout);
            vprintf(fmt, args);
            printf("\n");
        }

        simple_logger::simple_logger(const char *parameter) 
            : logging_provider(parameter) 
        {
            _index = 0;
            _lines = 0;
            _log = nullptr;

            create_log_file();
        }

        void simple_logger::create_log_file()
        {
            if (_log != nullptr)
                fclose(_log);

            _lines = 0;
            std::stringstream str;
            str << "log." << ++_index << ".txt";
            _log = fopen(str.str().c_str(), "w+");            
        }

        simple_logger::~simple_logger(void) 
        { 
            fclose(_log);
        }

        void simple_logger::logv(const char *file,
            const char *function,
            const int line,
            logging_level logLevel,
            const char* title,
            const char *fmt,
            va_list args
            )
        {
            utils::auto_lock l(_lock);
         
            print_header(_log);
            vfprintf(_log, fmt, args);
            fprintf(_log, "\n");
            if (logLevel >= log_level_ERROR)
                fflush(_log);

            if (logLevel >= log_level_WARNING)
            {
                print_header(stdout);
                vprintf(fmt, args);
                printf("\n");
            }

            if (++_lines >= 200000)
                create_log_file();
        }
    }
}
