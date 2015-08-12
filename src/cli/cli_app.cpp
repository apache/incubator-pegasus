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

# include "cli_app.h"
# include <iostream>

namespace dsn {
    namespace service {

        cli::cli(service_app_spec* s)
        : service_app(s)
        {
            _timeout_seconds = 10; // 10 seconds by default
        }

        void usage()
        {
            std::cout << "------------ rcli commands ------" << std::endl;
            std::cout << "help:   show this message" << std::endl;
            std::cout << "exit:   exit the console" << std::endl;
            std::cout << "remote: set cli target by 'remote %machine% %port% %timeout_seconds%" << std::endl;
            std::cout << "all other commands are sent to remote target %machine%:%port%" << std::endl;
            std::cout << "---------------------------------" << std::endl;
        }

        error_code cli::start(int argc, char** argv)
        {
            
            std::cout << "dsn remote cli begin ..." << std::endl;
            usage();

            while (true)
            {
                std::string cmdline;
                std::cout << ">";
                std::getline(std::cin, cmdline);

                std::string scmd = cmdline;
                std::vector<std::string> args;

                utils::split_args(scmd.c_str(), args, ' ');

                if (args.size() < 1)
                    continue;

                std::string cmd = args[0];
                if (cmd == "help")
                {
                    usage();
                    continue;
                }
                else if (cmd == "exit")
                {
                    exit(0);
                    continue;
                }
                else if (cmd == "remote")
                {
                    if (args.size() < 4)
                    {
                        std::cout << "invalid parameters for remote command, try help" << std::endl;
                        continue;
                    }
                    else
                    {
                        std::string machine = args[1];
                        int port = atoi(args[2].c_str());
                        _timeout_seconds = atoi(args[3].c_str());
                        _target = end_point(machine.c_str(), port);
                        std::cout << "remote target is set to " << machine << ":" << port << ", timeout = " << _timeout_seconds << " seconds" <<std::endl;
                        continue;
                    }
                }
                else
                {

                    command rcmd;
                    rcmd.cmd = cmd;
                    for (size_t i = 1; i < args.size(); i++)
                    {
                        rcmd.arguments.push_back(args[i]);
                    }

                    std::cout << "CALL " << _target.name << ":" << _target.port << " ..." << std::endl;
                    std::string result;
                    auto err = _client.call(rcmd, result, _timeout_seconds * 1000, 0, &_target);
                    if (err == ERR_OK)
                    {
                        std::cout << result << std::endl;
                    }
                    else
                    {
                        std::cout << "remote cli failed, err = " << err.to_string() << std::endl;
                    }
                    continue;
                }
            }
            return ERR_OK;
        }

        void cli::stop(bool cleanup)
        {
            
        } 

    }
}
