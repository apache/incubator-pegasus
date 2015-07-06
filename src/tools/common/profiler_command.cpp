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

#include <dsn/toollet/profiler.h>
#include "profiler_header.h"

namespace dsn {
    namespace tools {
        static int find_task_id(const std::string &name)
        {
            auto code = task_code::from_string(name.c_str(), TASK_CODE_INVALID);
            if (code == TASK_CODE_INVALID)
            {
                return -2;
            }
            return code;
        }

        static counter_percentile_type find_percentail_type(const std::string &name)
        {
            int num = atoi(name.c_str());
            if (num == 0)
                return COUNTER_PERCENTILE_INVALID;
            switch (num)
            {
            case 50:
                return COUNTER_PERCENTILE_50;
            case 90:
                return COUNTER_PERCENTILE_90;
            case 95:
                return COUNTER_PERCENTILE_95;
            case 99:
                return COUNTER_PERCENTILE_99;
            case 999:
                return COUNTER_PERCENTILE_999;
            default:
                return COUNTER_PERCENTILE_INVALID;
            }
        }

        static perf_counter_ptr_type find_counter_type(const std::string &name)
        {
            auto it = counter_info::pointer_type.find(std::string(name));
            if (it == counter_info::pointer_type.end())
            {
                return PREF_COUNTER_INVALID;
            }
            return it->second;
        }

        std::string profiler_output_handler(const std::vector<std::string>& args)
        {
            std::stringstream ss;
            int id = -1, num;
            
            if (args.size() < 1)
            {
                ss << "unenough arguments" << std::endl;
                return ss.str();
            }

            if ((args[0] == "task") || (args[0] == "t"))
            {
                if ((args[1] == "dependency") || (args[1] == "dep"))
                {
                    if (args.size() <= 2)
                    {
                        ss << "unenough arguments" << std::endl;
                        return ss.str();
                    }

                    if (args[2] == "matrix")
                    {
                        profiler_output_dependency_matrix(ss);
                        return ss.str();
                    }
                    if (args[2] == "list")
                    {
                        if (args.size() <= 3)
                        {
                            profiler_output_dependency_list_caller(ss, -1);
                            return ss.str();
                        }

                        if (args[3] == "caller")
                        {
                            profiler_output_dependency_list_caller(ss, -1);
                            return ss.str();
                        }
                        if (args[3] == "callee")
                        {
                            profiler_output_dependency_list_callee(ss, -1);
                            return ss.str();
                        }

                        id = find_task_id(args[3]);
                        if (id == -2)
                        {
                            ss << "no such task" << std::endl;
                            return ss.str();
                        }

                        if ((args.size() > 4) && (args[4] == "callee"))
                        {
                            profiler_output_dependency_list_callee(ss, id);
                        }
                        else
                        {
                            profiler_output_dependency_list_caller(ss, id);
                        }

                        return ss.str();

                    }

                    ss << "wrong arguments" << std::endl;
                    return ss.str();
                }
                else if (args[1] == "info")
                {
                    if (args.size() <= 1)
                    {
                        ss << "unenough arguments" << std::endl;
                        return ss.str();
                    }

                    if ((args.size() > 2) && (args[2] != "all"))
                    {
                        id = find_task_id(args[2]);
                    }
                    if (id == -2)
                    {
                        ss << "no such task" << std::endl;
                        return ss.str();
                    }

                    profiler_output_information_table(ss, id, (args.size() > 2) && (args[2] == "all"));
                }
                else if (args[1] == "top")
                {
                    if (args.size() < 4)
                    {
                        ss << "unenough arguments" << std::endl;
                        return ss.str();
                    }

                    num = atoi(args[2].c_str());
                    if (num == 0)
                    {
                        ss << "not a legal value" << std::endl;
                        return ss.str();
                    }
                    perf_counter_ptr_type counter_type = find_counter_type(args[3]);
                    if (counter_type == PREF_COUNTER_INVALID)
                    {
                        ss << "no such counter type" << std::endl;
                        return ss.str();
                    }

                    counter_percentile_type type = COUNTER_PERCENTILE_50;
                    if ((args.size() > 4) && (find_percentail_type(args[4]) != COUNTER_PERCENTILE_INVALID))
                    {
                        type = find_percentail_type(args[4]);
                    }

                    profiler_output_top(ss, counter_type, type, num);
                }
                else
                    ss << "wrong arguments" << std::endl;
            }
            else 
                ss << "wrong arguments" << std::endl;

            
            return ss.str();
        }
    }
}
