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
# include <fstream>
# include <sstream>

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

        DEFINE_TASK_CODE(LPC_JSON_FILE_REQUEST_TIMER, ::dsn::TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT);
        std::string cli::trans_to_json_file(std::string src, int nowreq)
        {
            std::stringstream ss, ss_label, ss_data;
            std::vector<std::string> args;
            utils::split_args(src.c_str(), args, ' ');

            ss_label << "    \"label\" :[";
            ss_data << "    \"newdata\" : [";
            ss << "{" << std::endl << "    \"type\" : \"time." << _pjs_request[nowreq].chart_type << "\"," << std::endl;
            ss << "    \"view\" : \"" << _pjs_request[nowreq].profile_view << "\"," << std::endl;
            if (args.size() > 0)
            {
                std::vector<std::string> val;
                utils::split_args(args[0].c_str(), val, ':');
                ss << "    \"target\" : \"" << val[_pjs_request[nowreq].profile_view != "task"] << "\"," << std::endl;
                ss << "    \"percentile\" : \"" << val[2] << "\"," << std::endl;
            }

            for (size_t i = 0; i < args.size(); i++)
            {
                std::vector<std::string> val;
                utils::split_args(args[i].c_str(), val, ':');
                ss_label << "\"" << val[_pjs_request[nowreq].profile_view == "task"] << "\"";
                ss_data << "{\"time\": 0,  \"y\": " << val[3] << "}";
                if (i < args.size() - 1)
                {
                    ss_label << ", ";
                    ss_data << ", ";
                }
            }
            ss << ss_label.str() << "]," << std::endl << ss_data.str() << "]" << std::endl << "}" << std::endl;
            return ss.str();
        }

        bool cli::get_pjs_top_data(int nowreq)
        {
            std::string result;
            std::vector<std::string> args;
            auto err = _client.call(_pjs_request[nowreq].topcmd, result, _timeout_seconds * 1000, 0, &_target);
            if (err == ERR_OK)
            {
                utils::split_args(result.c_str(), args, ' ');
                bool flag = false;
                for (auto key : _pjs_request[nowreq].rcmd.arguments)
                {
                    bool diff = true;
                    for (auto neo : args)
                    {
                        if (key == neo)
                        {
                            diff = false;
                            break;
                        }
                    }
                    if (diff)
                    {
                        flag = true;
                        break;
                    }
                }
                if ((flag) || (_pjs_request[nowreq].rcmd.arguments.size() == 0))
                    _pjs_request[nowreq].rcmd.arguments = args;
                return 1;
            }
            else
            {
                std::cout << "remote cli failed, err = " << err.to_string() << std::endl;
                return 0;
            }
        }

        void cli::get_pjs_json_file(int nowreq)
        {
            if (_pjs_request[nowreq].profile_view == "top")
            {
                _pjs_request[nowreq].count++;
                if (_pjs_request[nowreq].count == _pjs_request[nowreq].interval)
                {
                    _pjs_request[nowreq].count = 0;
                    get_pjs_top_data(nowreq);
                }
            }

            std::string result;
            auto err = _client.call(_pjs_request[nowreq].rcmd, result, _timeout_seconds * 1000, 0, &_target);
            if (err == ERR_OK)
            {
                std::stringstream filepath;
                filepath << "../../../../profiler_javascript/data" << nowreq + 1 << ".json";
                if (result == "")
                {
                    std::cout << "invalid parameters for remote command, try help" << std::endl;
                    _pjs_request[nowreq].timer->cancel(true);
                    return;
                }
                std::ofstream out(filepath.str().c_str(), std::ios::out);
                if (out.is_open())
                {
                    out << trans_to_json_file(result, nowreq);
                    out.close();
                }
                else
                {
                    std::cout << "Can not open file" << std::endl;
                }
            }
            else
            {
                std::cout << "remote cli failed, err = " << err.to_string() << std::endl;
            }
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
                        std::cout << "remote target is set to " << machine << ":" << port << ", timeout = " << _timeout_seconds << " seconds" << std::endl;
                        continue;
                    }
                }
                else if (cmd == "pjsc")
                {
                    if (args.size() < 2)
                    {
                        std::cout << "invalid parameters for remote command, try help" << std::endl;
                        continue;
                    }
                    int k = atoi(args[1].c_str()) - 1;
                    if ((k != -1) || (_pjs_request[k].timer != NULL))
                    {
                        _pjs_request[k].timer->cancel(true);
                        _pjs_request[k].timer = NULL;
                        _pjs_request[k].rcmd.cmd = "";
                        _pjs_request[k].rcmd.arguments.clear();
                        std::cout << "Request " << k + 1 << " has been canceled" << std::endl;
                    }
                    continue;
                }
                else if (cmd == "pjs")
                {
                    if (args.size() < 2)
                    {
                        std::cout << "invalid parameters for remote command, try help" << std::endl;
                        continue;
                    }
                    int nowreq = atoi(args[1].c_str()) - 1;
                    size_t k = 1 + (nowreq != -1);

                    if (k + 2 >= args.size())
                    {
                        std::cout << "invalid parameters for remote command, try help" << std::endl;
                        continue;
                    }

                    //Find File NO
                    if (nowreq == -1)
                    {
                        for (nowreq++; (nowreq < MAX_PJS_REQUEST) && (_pjs_request[nowreq].timer != NULL); nowreq++);
                        if (nowreq >= MAX_PJS_REQUEST)
                        {
                            std::cout << "Requests are full, please cancel one first" << std::endl;
                            continue;
                        }
                    }

                    //If Exist, Cancel
                    if (_pjs_request[nowreq].timer != NULL)
                    {
                        _pjs_request[nowreq].timer->cancel(true);
                        _pjs_request[nowreq].timer = NULL;
                        _pjs_request[nowreq].rcmd.cmd = "";
                        _pjs_request[nowreq].rcmd.arguments.clear();
                    }
                    _pjs_request[nowreq].rcmd.cmd = "pd";

                    //Get chart type
                    if ((args[k] == "line") || (args[k] == "l"))
                    {
                        _pjs_request[nowreq].chart_type = "line";
                    }
                    else if ((args[k] == "area") || (args[k] == "a"))
                    {
                        _pjs_request[nowreq].chart_type = "area";
                    }
                    else
                    {
                        std::cout << "invalid parameters for remote command, try help" << std::endl;
                        continue;
                    }

                    //Get main command
                    if (args[k + 1] == "top")
                    {
                        if (k + 3 >= args.size())
                        {
                            std::cout << "invalid parameters for remote command, try help" << std::endl;
                            continue;
                        }

                        _pjs_request[nowreq].profile_view = "top";
                        _pjs_request[nowreq].interval = 60;
                        _pjs_request[nowreq].topcmd.cmd = "pd";
                        _pjs_request[nowreq].topcmd.arguments.push_back(_pjs_request[nowreq].profile_view);
                        _pjs_request[nowreq].topcmd.arguments.push_back(args[k + 2]);
                        _pjs_request[nowreq].topcmd.arguments.push_back(args[k + 3]);

                        std::string counter_percentile_type;
                        if ((k + 4 >= args.size()) || (atoi(args[k + 4].c_str()) == 0))
                        {
                            counter_percentile_type = "50";
                        }
                        else
                        {
                            counter_percentile_type = args[k + 4];
                        }
                        _pjs_request[nowreq].topcmd.arguments.push_back(counter_percentile_type);
                        if (get_pjs_top_data(nowreq) == false)
                        {
                            std::cout << "invalid parameters for remote command, try help" << std::endl;
                            continue;
                        }
                    }
                    else if ((args[k + 1] == "task") || (args[k + 1] == "t") || (args[k + 1] == "counter") || (args[k + 1] == "c"))
                    {
                        _pjs_request[nowreq].profile_view = ((args[k + 1] == "task") || (args[k + 1] == "t")) ? "task" : "counter";
                        int counter_percentile_type;
                        int offset_percentile = 1;
                        if ((k + 3 <= args.size()) || ((counter_percentile_type = atoi(args[k + 3].c_str())) == 0))
                        {
                            counter_percentile_type = 50;
                            offset_percentile = 0;
                        }

                        if ((_pjs_request[nowreq].profile_view == "task") && (k + 3 + offset_percentile >= args.size()))
                        {
                            std::stringstream ss;
                            ss << args[k + 2] << ":AllPercentile:" << counter_percentile_type;
                            _pjs_request[nowreq].rcmd.arguments.push_back(ss.str());
                        }
                        for (size_t i = k + 3 + offset_percentile; i < args.size(); i++)
                        {
                            std::stringstream ss;
                            ss << args[_pjs_request[nowreq].profile_view == "counter" ? i : k + 2] << ":" << args[_pjs_request[nowreq].profile_view == "counter" ? k + 2 : i] << ":" << counter_percentile_type;
                            _pjs_request[nowreq].rcmd.arguments.push_back(ss.str());
                        }
                    }
                    else
                    {
                        std::cout << "invalid parameters for remote command, try help" << std::endl;
                        continue;
                    }

                    std::cout << "Profile begin, dup to file data" << nowreq + 1 << ".json" << std::endl;
                    _pjs_request[nowreq].rcmd.cmd = "pd";
                    _pjs_request[nowreq].count = 0;
                    _pjs_request[nowreq].timer = ::dsn::service::tasking::enqueue(LPC_JSON_FILE_REQUEST_TIMER, this, std::bind(&cli::get_pjs_json_file, this, nowreq), 0, 0, 1000);
                    get_pjs_json_file(nowreq);
                    continue;
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
