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

/*
 * Description:
 *     Repli util main.
 *
 * Revision history:
 *     Nov., 2015, @qinzuoyan (Zuoyan Qin), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

# include "repli.app.h"
# include <dsn/cpp/utils.h>
# include <iostream>
# include <thread>
# if !defined (_WIN32)
# include "unistd.h"
# endif

bool g_done = false;

int main(int argc, char** argv)
{
    if (argc < 2)
    {
        std::cerr << "USAGE: " << argv[0] << " <command> <params...>" << std::endl;
        dsn::service::repli_app::usage();
        return -1;
    }

    std::string conf;
    char buf[4096];
    int slen;

# if defined(_WIN32)
    slen = ::GetModuleFileNameA(NULL, buf, sizeof(buf));
# else
    slen = readlink("/proc/self/exe", buf, sizeof(buf));
# endif
    
    if (slen != -1)
    {
        std::string dir = dsn::utils::filesystem::remove_file_name(buf);
        conf = dir + "/config.ini";
        if (!dsn::utils::filesystem::file_exists(conf))
        {
            std::cerr << "ERROR: config file not found: " << conf << std::endl;
            dsn::service::repli_app::usage();
            return -1;
        }
    }

    // register all possible service apps
    dsn::register_app< ::dsn::service::repli_app>("repli");

    dsn::service::repli_app::set_args(argc - 1, argv + 1);

    // specify what services and tools will run in config file, then run
    dsn_run_config(conf.c_str(), false);

    while (!g_done)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    return 0;
}
