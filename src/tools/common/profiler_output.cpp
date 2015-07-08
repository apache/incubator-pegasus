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

#include <iomanip>
#include <algorithm>
#include <dsn/toollet/profiler.h>
#include "profiler_header.h"
#include <iostream>

namespace dsn {
    namespace tools {
        struct sort_node
        {
            int id;
            double val;

            sort_node() : val(-1.0) {}
        };

        extern task_spec_profiler* s_spec_profilers;
        extern counter_info* counter_info_ptr[PREF_COUNTER_COUNT];
        static const std::string percentail_counter_string[COUNTER_PERCENTILE_COUNT] = { "50%", "90%", "95%", "99%", "999%" };
        profiler_output_data_type* profiler_output_data = new profiler_output_data_type(taskname_width, data_width, call_width);

        static inline bool cmp(const sort_node &x, const sort_node &y)
        {
            if (x.val == y.val)
            {
                return x.id < y.id;
            }
            return x.val > y.val;
        }

        void profiler_output_top(std::stringstream &ss, const perf_counter_ptr_type counter_type, const counter_percentile_type type, const int num)
        {
            sort_node *_tmp = new sort_node[task_code::max_value() + 3];
            int tmp_num = num >= task_code::max_value() + 1 ? task_code::max_value() + 1 : num;

            for (int i = 0; i <= task_code::max_value(); _tmp[i].id = i, i++)
            {
                if ((i == TASK_CODE_INVALID) || (!s_spec_profilers[i].is_profile) || (s_spec_profilers[i].ptr[counter_type] == nullptr))
                    continue;

                if (counter_info_ptr[counter_type]->type == COUNTER_TYPE_NUMBER_PERCENTILES)
                {
                    _tmp[i].val = s_spec_profilers[i].ptr[counter_type]->get_percentile(type);
                }
                else
                {
                    _tmp[i].val = s_spec_profilers[i].ptr[counter_type]->get_value();
                }
            }

            std::sort(_tmp, _tmp + 1 + task_code::max_value(), cmp);

            ss << profiler_output_data->separate_line_top << std::endl;
            ss << "|PROFILE|" << profiler_output_data->view_task_type << std::setw(data_width) << counter_info_ptr[counter_type]->title << "|" << std::endl;
            ss << profiler_output_data->separate_line_top << std::endl;
            for (int k = 0; k < tmp_num; k++)
            {
                int i = _tmp[k].id;

                if ((i == TASK_CODE_INVALID) || (s_spec_profilers[i].is_profile == false) || (_tmp[k].val == -1))
                    continue;

                ss << "|" << std::setw(4) << i << "   |" << std::setiosflags(std::ios::left) << std::setw(taskname_width) << std::string(task_code::to_string(i)) << std::resetiosflags(std::ios::left);
                if (s_spec_profilers[i].ptr[counter_type] != nullptr)
                {
                    ss << "|" << std::setw(data_width) << _tmp[k].val << "|" << std::endl;
                }
                else
                {
                    ss << profiler_output_data->none << std::endl;
                }

                ss << profiler_output_data->separate_line_top << std::endl;
            }
            delete[] _tmp;
        }

        void profiler_output_infomation_line(std::stringstream &ss, const int request, counter_percentile_type type, const bool flag)
        {
            task_spec* spec = task_spec::get(request);

            if (flag == true)
            {
                ss << "|" << std::setw(4) << request << "   |" << std::setiosflags(std::ios::left) << std::setw(taskname_width) << std::string(task_code::to_string(request))
                    << std::resetiosflags(std::ios::left) << "|" << std::setw(7) << percentail_counter_string[type] << "|";
            }
            else
            {
                ss << "|       |" << profiler_output_data->blank_name << std::setw(7) << percentail_counter_string[type] << "|";
            }
            for (int i = 0; i < PREF_COUNTER_COUNT; i++)
            {
                if (flag == true)
                {
                    if (s_spec_profilers[request].ptr[i] == nullptr)
                    {
                        ss << profiler_output_data->none;
                    }
                    else if (counter_info_ptr[i]->type == COUNTER_TYPE_NUMBER_PERCENTILES)
                    {
                        ss << std::setw(data_width) << s_spec_profilers[request].ptr[i]->get_percentile(type) << "|";
                    }
                    else
                    {
                        ss << std::setw(data_width) << s_spec_profilers[request].ptr[i]->get_value() << "|";
                    }
                }
                else
                if ((counter_info_ptr[i]->type == COUNTER_TYPE_NUMBER_PERCENTILES) && (s_spec_profilers[request].ptr[i] != nullptr))
                {
                    ss << std::setw(data_width) << s_spec_profilers[request].ptr[i]->get_percentile(type) << "|";
                }
                else
                {
                    ss << profiler_output_data->blank_data;
                }
            }
            ss << std::endl;
        }

        void profiler_output_information_table(std::stringstream &ss, const int request, const bool flag)
        {
            ss << profiler_output_data->separate_line_info << std::endl;
            ss << "|PROFILE|" << profiler_output_data->view_task_type << "PERCENT|";
            for (int i = 0; i < PREF_COUNTER_COUNT; i++)
            {
                ss << std::setw(data_width) << counter_info_ptr[i]->title << "|";
            }
            ss << std::endl;
            ss << profiler_output_data->separate_line_info << std::endl;
            
            for (int i = 0; i <= task_code::max_value(); i++)
            {
                if ((i == TASK_CODE_INVALID) || (s_spec_profilers[i].is_profile == false))
                    continue;

                if ((request >= 0) && (request != i))
                    continue;

                if ((request < 0) && (flag == false) && (s_spec_profilers[i].ptr[TASK_EXEC_TIME_NS]->get_percentile(COUNTER_PERCENTILE_999) < 0.0))
                    continue;

                task_spec* spec = task_spec::get(i);
                for (int j = 0; j < COUNTER_PERCENTILE_COUNT; j++)
                {
                    profiler_output_infomation_line(ss, i, (counter_percentile_type)j, j == COUNTER_PERCENTILE_COUNT / 2);
                }
                ss << profiler_output_data->separate_line_info << std::endl;
            }
        }

        void profiler_output_dependency_matrix(std::stringstream &ss)
        {
            ss << profiler_output_data->separate_line_depmatrix;
            for (int i = 0; i <= task_code::max_value(); i++)
            {
                if ((i != TASK_CODE_INVALID) && (s_spec_profilers[i].is_profile != false))
                {
                    ss << profiler_output_data->separate_line_call_times;
                }
            }
            ss << std::endl;

            ss << "|" << profiler_output_data->view_task_type << profiler_output_data->blank_matrix;
            for (int i = 0; i <= task_code::max_value(); i++)
            {
                if ((i != TASK_CODE_INVALID) && (s_spec_profilers[i].is_profile != false))
                {
                    ss << std::setw(call_width) << i << "|";
                }
            }
            ss << std::endl;

            ss << profiler_output_data->separate_line_depmatrix;
            for (int i = 0; i <= task_code::max_value(); i++)
            {
                if ((i != TASK_CODE_INVALID) && (s_spec_profilers[i].is_profile != false))
                {
                    ss << profiler_output_data->separate_line_call_times;
                }
            }
            ss << std::endl;

            for (int i = 0; i <= task_code::max_value(); i++)
            {
                if ((i != TASK_CODE_INVALID) && (s_spec_profilers[i].is_profile != false))
                {
                    ss << "|" << std::setiosflags(std::ios::left) << std::setw(taskname_width) << std::string(task_code::to_string(i)) << std::resetiosflags(std::ios::left) << "|" << std::setw(call_width) << i << "|";
                    for (int j = 0; j <= task_code::max_value(); j++)
                    {
                        if ((j != TASK_CODE_INVALID) && (s_spec_profilers[j].is_profile != false))
                        {
                            ss << std::setw(call_width) << s_spec_profilers[i].call_counts[j] << "|";
                        }
                    }
                    ss << std::endl;

                    ss << profiler_output_data->separate_line_depmatrix;
                    for (int j = 0; j <= task_code::max_value(); j++)
                    {
                        if ((j != TASK_CODE_INVALID) && (s_spec_profilers[j].is_profile != false))
                        {
                            ss << profiler_output_data->separate_line_call_times;
                        }
                    }
                    ss << std::endl;
                }
            }
        }

        void profiler_output_dependency_list_caller(std::stringstream &ss, const int request)
        {
            ss << profiler_output_data->separate_line_deplist << std::endl;
            ss << "|" << profiler_output_data->view_caller_type << profiler_output_data->view_callee_type << profiler_output_data->view_call_times << std::endl;
            ss << profiler_output_data->separate_line_deplist << std::endl;

            for (int i = 0; i <= task_code::max_value(); i++)
            {
                if ((i != TASK_CODE_INVALID) && (s_spec_profilers[i].is_profile != false) && (s_spec_profilers[i].collect_call_count != false) && ((request < 0) || (request == i)))
                {
                    for (int j = 0; j <= task_code::max_value(); j++)
                    {
                        if ((j != TASK_CODE_INVALID) && (s_spec_profilers[j].is_profile != false) && (s_spec_profilers[i].call_counts[j] != 0))
                        {
                            ss << "|" << std::setiosflags(std::ios::left) << std::setw(taskname_width) << std::string(task_code::to_string(i));
                            ss << "|" << std::setw(taskname_width) << std::string(task_code::to_string(j)) << std::resetiosflags(std::ios::left);
                            ss << "|" << std::setw(call_width) << s_spec_profilers[i].call_counts[j] << "|" << std::endl;
                            ss << profiler_output_data->separate_line_deplist << std::endl;
                        }
                    }
                }
            }
        }

        void profiler_output_dependency_list_callee(std::stringstream &ss, const int request)
        {
            ss << profiler_output_data->separate_line_deplist << std::endl;
            ss << "|" << profiler_output_data->view_callee_type << profiler_output_data->view_caller_type << "  TIMES  |" << std::endl;
            ss << profiler_output_data->separate_line_deplist << std::endl;
            for (int i = 0; i <= task_code::max_value(); i++)
            {
                if ((i != TASK_CODE_INVALID) && (s_spec_profilers[i].is_profile != false) && (s_spec_profilers[i].collect_call_count != false) && ((request < 0) || (request == i)))
                {
                    for (int j = 0; j <= task_code::max_value(); j++)
                    {
                        if ((j != TASK_CODE_INVALID) && (s_spec_profilers[j].is_profile != false) && (s_spec_profilers[j].call_counts[i] != 0))
                        {
                            ss << "|" << std::setiosflags(std::ios::left) << std::setw(taskname_width) << std::string(task_code::to_string(i));
                            ss << "|" << std::setw(taskname_width) << std::string(task_code::to_string(j)) << std::resetiosflags(std::ios::left);
                            ss << "|" << std::setw(9) << s_spec_profilers[j].call_counts[i] << "|" << std::endl;
                            ss << profiler_output_data->separate_line_deplist << std::endl;
                        }
                    }
                }
            }
        }
    }
}
