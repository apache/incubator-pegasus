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
 *     Output handler for command 'profile' and 'profiledata'
 *
 * Revision history:
 *     2015-06-01, zjc95, first version
 *     2015-06-01, zjc95, deleted pdh part
 *     2015-06-02, zjc95, revised License
 *     2015-06-02, zjc95, revised format of tab and brace
 *     2015-08-11, zjc95, revised format of variable name
 *     2015-08-11, zjc95, added function 'profiler_data_top'
 *     2015-11-24, zjc95, revised the decription
 *
 */

#include <iomanip>
#include <iostream>
#include <algorithm>
#include <dsn/toollet/profiler.h>
#include <dsn/utility/smart_pointers.h>
#include "profiler_header.h"

namespace dsn {
namespace tools {
struct sort_node
{
    int id;
    double val;

    sort_node() : val(-1.0) {}
};

extern std::unique_ptr<task_spec_profiler[]> s_spec_profilers;
extern counter_info *counter_info_ptr[PERF_COUNTER_COUNT];
auto profiler_output_data =
    make_unique<profiler_output_data_type>(taskname_width, data_width, call_width);

static inline bool cmp(const sort_node &x, const sort_node &y)
{
    if (x.val == y.val) {
        return x.id < y.id;
    }
    return x.val > y.val;
}

void profiler_output_top(std::stringstream &ss,
                         const perf_counter_ptr_type counter_type,
                         const dsn_perf_counter_percentile_type_t percentile_type,
                         const int num)
{
    sort_node *_tmp = new sort_node[dsn::task_code::max() + 3];
    int tmp_num = (num >= dsn::task_code::max() + 1) ? dsn::task_code::max() + 1 : num;

    // Load data
    for (int i = 0; i <= dsn::task_code::max(); _tmp[i].id = i, i++) {
        if ((i == TASK_CODE_INVALID) || (!s_spec_profilers[i].is_profile) ||
            (s_spec_profilers[i].ptr[counter_type].get() == nullptr))
            continue;

        if (counter_info_ptr[counter_type]->type == COUNTER_TYPE_NUMBER_PERCENTILES) {
            _tmp[i].val = s_spec_profilers[i].ptr[counter_type]->get_percentile(percentile_type);
        } else {
            _tmp[i].val = s_spec_profilers[i].ptr[counter_type]->get_value();
        }
    }

    std::sort(_tmp, _tmp + 1 + dsn::task_code::max(), cmp);

    ss << profiler_output_data->separate_line_top << std::endl;
    ss << "|PROFILE|" << profiler_output_data->view_task_type << std::setw(data_width)
       << counter_info_ptr[counter_type]->title << "|" << std::endl;
    ss << profiler_output_data->separate_line_top << std::endl;

    // Print sorted data
    for (int k = 0; k < tmp_num; k++) {
        int i = _tmp[k].id;

        if ((i == TASK_CODE_INVALID) || (s_spec_profilers[i].is_profile == false) ||
            (_tmp[k].val == -1))
            continue;

        ss << "|" << std::setw(4) << i << "   |" << std::setiosflags(std::ios::left)
           << std::setw(taskname_width) << std::string(dsn::task_code(i).to_string())
           << std::resetiosflags(std::ios::left);
        if (_tmp[k].val != -1) {
            ss << "|" << std::setw(data_width) << _tmp[k].val << "|" << std::endl;
        } else {
            ss << profiler_output_data->none << std::endl;
        }

        ss << profiler_output_data->separate_line_top << std::endl;
    }
    delete[] _tmp;
}

void profiler_output_infomation_line(std::stringstream &ss,
                                     const int task_id,
                                     dsn_perf_counter_percentile_type_t percentile_type,
                                     const bool full_data)
{
    // Print the table infrom
    if (full_data == true) {
        ss << "|" << std::setw(4) << task_id << "   |" << std::setiosflags(std::ios::left)
           << std::setw(taskname_width) << std::string(dsn::task_code(task_id).to_string())
           << std::resetiosflags(std::ios::left) << "|" << std::setw(7)
           << percentail_counter_string[percentile_type] << "|";
    } else {
        ss << "|       |" << profiler_output_data->blank_name << std::setw(7)
           << percentail_counter_string[percentile_type] << "|";
    }

    // Print the data
    for (int i = 0; i < PERF_COUNTER_COUNT; i++) {
        // The middle line, print the data of all counter type
        if (full_data == true) {
            if (s_spec_profilers[task_id].ptr[i].get() == nullptr) {
                ss << profiler_output_data->none;
            } else if (counter_info_ptr[i]->type == COUNTER_TYPE_NUMBER_PERCENTILES) {
                ss << std::setw(data_width)
                   << s_spec_profilers[task_id].ptr[i]->get_percentile(percentile_type) << "|";
            } else {
                ss << std::setw(data_width) << s_spec_profilers[task_id].ptr[i]->get_value() << "|";
            }
        }
        // Other line, just print the number_percentile_type data
        else {
            if (counter_info_ptr[i]->type != COUNTER_TYPE_NUMBER_PERCENTILES) {
                ss << profiler_output_data->blank_data;
            } else if (s_spec_profilers[task_id].ptr[i].get() != nullptr) {
                ss << std::setw(data_width)
                   << s_spec_profilers[task_id].ptr[i]->get_percentile(percentile_type) << "|";
            } else {
                ss << profiler_output_data->none;
            }
        }
    }
    ss << std::endl;
}

void profiler_output_information_table(std::stringstream &ss, const int task_id)
{
    ss << profiler_output_data->separate_line_info << std::endl;
    ss << "|PROFILE|" << profiler_output_data->view_task_type << "PERCENT|";
    for (int i = 0; i < PERF_COUNTER_COUNT; i++) {
        ss << std::setw(data_width) << counter_info_ptr[i]->title << "|";
    }
    ss << std::endl;
    ss << profiler_output_data->separate_line_info << std::endl;

    // Select the task
    if (task_id >= 0) {
        if ((task_id == TASK_CODE_INVALID) || (!s_spec_profilers[task_id].is_profile))
            return;

        // Print all percentile type
        for (int j = 0; j < COUNTER_PERCENTILE_COUNT; j++) {
            profiler_output_infomation_line(ss,
                                            task_id,
                                            (dsn_perf_counter_percentile_type_t)j,
                                            j == COUNTER_PERCENTILE_COUNT / 2);
        }
        ss << profiler_output_data->separate_line_info << std::endl;
        return;
    }

    // Print all task infrom
    for (int i = 0; i <= dsn::task_code::max(); i++) {
        if ((i == TASK_CODE_INVALID) || (s_spec_profilers[i].is_profile == false))
            continue;

        // Print all percentile type
        for (int j = 0; j < COUNTER_PERCENTILE_COUNT; j++) {
            profiler_output_infomation_line(
                ss, i, (dsn_perf_counter_percentile_type_t)j, j == COUNTER_PERCENTILE_COUNT / 2);
        }

        ss << profiler_output_data->separate_line_info << std::endl;
    }
}

void profiler_output_dependency_matrix(std::stringstream &ss)
{
    // Print the separate line
    ss << profiler_output_data->separate_line_depmatrix;
    for (int i = 0; i <= dsn::task_code::max(); i++) {
        if ((i != TASK_CODE_INVALID) && (s_spec_profilers[i].is_profile != false)) {
            ss << profiler_output_data->separate_line_call_times;
        }
    }
    ss << std::endl;

    // Print the title of table
    ss << "|" << profiler_output_data->view_task_type << profiler_output_data->blank_matrix;
    for (int i = 0; i <= dsn::task_code::max(); i++) {
        if ((i != TASK_CODE_INVALID) && (s_spec_profilers[i].is_profile != false)) {
            ss << std::setw(call_width) << i << "|";
        }
    }
    ss << std::endl;

    // Print the separate line
    ss << profiler_output_data->separate_line_depmatrix;
    for (int i = 0; i <= dsn::task_code::max(); i++) {
        if ((i != TASK_CODE_INVALID) && (s_spec_profilers[i].is_profile != false)) {
            ss << profiler_output_data->separate_line_call_times;
        }
    }
    ss << std::endl;

    // Print the calling table
    for (int i = 0; i <= dsn::task_code::max(); i++) {
        if ((i != TASK_CODE_INVALID) && (s_spec_profilers[i].is_profile != false)) {
            ss << "|" << std::setiosflags(std::ios::left) << std::setw(taskname_width)
               << std::string(dsn::task_code(i).to_string()) << std::resetiosflags(std::ios::left)
               << "|" << std::setw(call_width) << i << "|";
            for (int j = 0; j <= dsn::task_code::max(); j++) {
                if ((j != TASK_CODE_INVALID) && (s_spec_profilers[j].is_profile != false)) {
                    ss << std::setw(call_width) << s_spec_profilers[i].call_counts[j] << "|";
                }
            }
            ss << std::endl;

            // Print the separate line
            ss << profiler_output_data->separate_line_depmatrix;
            for (int j = 0; j <= dsn::task_code::max(); j++) {
                if ((j != TASK_CODE_INVALID) && (s_spec_profilers[j].is_profile != false)) {
                    ss << profiler_output_data->separate_line_call_times;
                }
            }
            ss << std::endl;
        }
    }
}

void profiler_output_dependency_list_caller(std::stringstream &ss, const int task_id)
{
    // Print the title of table
    ss << profiler_output_data->separate_line_deplist << std::endl;
    ss << "|" << profiler_output_data->view_caller_type << profiler_output_data->view_callee_type
       << profiler_output_data->view_call_times << std::endl;
    ss << profiler_output_data->separate_line_deplist << std::endl;

    // Select the task
    if (task_id >= 0) {
        if ((task_id == TASK_CODE_INVALID) || (!s_spec_profilers[task_id].is_profile) ||
            (!s_spec_profilers[task_id].collect_call_count))
            return;

        for (int j = 0; j <= dsn::task_code::max(); j++) {
            if ((j != TASK_CODE_INVALID) && (s_spec_profilers[j].is_profile) &&
                (s_spec_profilers[task_id].call_counts[j] > 0)) {
                ss << "|" << std::setiosflags(std::ios::left) << std::setw(taskname_width)
                   << std::string(dsn::task_code(task_id).to_string());
                ss << "|" << std::setw(taskname_width) << std::string(dsn::task_code(j).to_string())
                   << std::resetiosflags(std::ios::left);
                ss << "|" << std::setw(call_width) << s_spec_profilers[task_id].call_counts[j]
                   << "|" << std::endl;
                ss << profiler_output_data->separate_line_deplist << std::endl;
            }
        }
        return;
    }

    // Print all task calling infrom
    for (int i = 0; i <= dsn::task_code::max(); i++) {
        if ((i != TASK_CODE_INVALID) && (s_spec_profilers[i].is_profile) &&
            (s_spec_profilers[i].collect_call_count)) {
            for (int j = 0; j <= dsn::task_code::max(); j++) {
                if ((j != TASK_CODE_INVALID) && (s_spec_profilers[j].is_profile) &&
                    (s_spec_profilers[i].call_counts[j] > 0)) {
                    ss << "|" << std::setiosflags(std::ios::left) << std::setw(taskname_width)
                       << std::string(dsn::task_code(i).to_string());
                    ss << "|" << std::setw(taskname_width)
                       << std::string(dsn::task_code(j).to_string())
                       << std::resetiosflags(std::ios::left);
                    ss << "|" << std::setw(call_width) << s_spec_profilers[i].call_counts[j] << "|"
                       << std::endl;
                    ss << profiler_output_data->separate_line_deplist << std::endl;
                }
            }
        }
    }
}

void profiler_output_dependency_list_callee(std::stringstream &ss, const int task_id)
{
    // Print the title of table
    ss << profiler_output_data->separate_line_deplist << std::endl;
    ss << "|" << profiler_output_data->view_callee_type << profiler_output_data->view_caller_type
       << "  TIMES  |" << std::endl;
    ss << profiler_output_data->separate_line_deplist << std::endl;

    // Select the task
    if (task_id >= 0) {
        if ((task_id == TASK_CODE_INVALID) || (!s_spec_profilers[task_id].is_profile) ||
            (!s_spec_profilers[task_id].collect_call_count))
            return;

        for (int j = 0; j <= dsn::task_code::max(); j++) {
            if ((j != TASK_CODE_INVALID) && (s_spec_profilers[j].is_profile) &&
                (s_spec_profilers[j].collect_call_count) &&
                (s_spec_profilers[j].call_counts[task_id] > 0)) {
                ss << "|" << std::setiosflags(std::ios::left) << std::setw(taskname_width)
                   << std::string(dsn::task_code(task_id).to_string());
                ss << "|" << std::setw(taskname_width) << std::string(dsn::task_code(j).to_string())
                   << std::resetiosflags(std::ios::left);
                ss << "|" << std::setw(call_width) << s_spec_profilers[j].call_counts[task_id]
                   << "|" << std::endl;
                ss << profiler_output_data->separate_line_deplist << std::endl;
            }
        }
        return;
    }

    // Print all task calling infrom
    for (int i = 0; i <= dsn::task_code::max(); i++) {
        if ((i != TASK_CODE_INVALID) && (s_spec_profilers[i].is_profile)) {
            for (int j = 0; j <= dsn::task_code::max(); j++) {
                if ((j != TASK_CODE_INVALID) && (s_spec_profilers[j].is_profile) &&
                    (s_spec_profilers[j].collect_call_count) &&
                    (s_spec_profilers[j].call_counts[i] > 0)) {
                    ss << "|" << std::setiosflags(std::ios::left) << std::setw(taskname_width)
                       << std::string(dsn::task_code(i).to_string());
                    ss << "|" << std::setw(taskname_width)
                       << std::string(dsn::task_code(j).to_string())
                       << std::resetiosflags(std::ios::left);
                    ss << "|" << std::setw(call_width) << s_spec_profilers[j].call_counts[i] << "|"
                       << std::endl;
                    ss << profiler_output_data->separate_line_deplist << std::endl;
                }
            }
        }
    }
}

void profiler_data_top(std::stringstream &ss,
                       const perf_counter_ptr_type counter_type,
                       const dsn_perf_counter_percentile_type_t percentile_type,
                       const int num)
{
    sort_node *_tmp = new sort_node[dsn::task_code::max() + 3];
    int tmp_num = num >= dsn::task_code::max() + 1 ? dsn::task_code::max() + 1 : num;

    // Load data
    for (int i = 0; i <= dsn::task_code::max(); _tmp[i].id = i, i++) {
        if ((i == TASK_CODE_INVALID) || (!s_spec_profilers[i].is_profile) ||
            (s_spec_profilers[i].ptr[counter_type].get() == nullptr))
            continue;

        if (counter_info_ptr[counter_type]->type == COUNTER_TYPE_NUMBER_PERCENTILES) {
            _tmp[i].val = s_spec_profilers[i].ptr[counter_type]->get_percentile(percentile_type);
        } else {
            _tmp[i].val = s_spec_profilers[i].ptr[counter_type]->get_value();
        }
    }

    std::sort(_tmp, _tmp + 1 + dsn::task_code::max(), cmp);

    // Output sorted data
    for (int k = 0; k < tmp_num; k++) {
        int i = _tmp[k].id;

        if ((i == TASK_CODE_INVALID) || (s_spec_profilers[i].is_profile == false) ||
            (_tmp[k].val == -1))
            continue;

        ss << dsn::task_code(i).to_string() << ":" << counter_info_ptr[counter_type]->keys[0] << ":"
           << percentail_counter_int[percentile_type] << " ";
    }
    delete[] _tmp;
}
} // namespace tools
} // namespace dsn
