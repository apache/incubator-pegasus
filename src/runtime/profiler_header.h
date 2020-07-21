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

#pragma once
#include <iomanip>
#include <dsn/perf_counter/perf_counter_wrapper.h>

namespace dsn {
namespace tools {

const int data_width = 15;
const int taskname_width = 30;
const int call_width = 15;
static const std::string percentail_counter_string[COUNTER_PERCENTILE_COUNT] = {
    "50%", "90%", "95%", "99%", "999%"};
static const int percentail_counter_int[COUNTER_PERCENTILE_COUNT] = {50, 90, 95, 99, 999};

enum perf_counter_ptr_type
{
    TASK_QUEUEING_TIME_NS,
    TASK_EXEC_TIME_NS,
    TASK_THROUGHPUT,
    TASK_CANCELLED,
    AIO_LATENCY_NS,
    RPC_SERVER_LATENCY_NS,
    RPC_SERVER_SIZE_PER_REQUEST_IN_BYTES,
    RPC_SERVER_SIZE_PER_RESPONSE_IN_BYTES,
    RPC_CLIENT_NON_TIMEOUT_LATENCY_NS,
    RPC_CLIENT_TIMEOUT_THROUGHPUT,
    TASK_IN_QUEUE,

    PERF_COUNTER_COUNT,
    PERF_COUNTER_INVALID
};

class counter_info
{
public:
    counter_info(const std::vector<const char *> &command_keys,
                 perf_counter_ptr_type _index,
                 dsn_perf_counter_type_t _type,
                 const char *_title,
                 const char *_unit)
        : type(_type), title(_title), unit_name(_unit)
    {
        for (auto key : command_keys) {
            if (key != nullptr) {
                keys.push_back(key);
                auto it = pointer_type.find(std::string(key));
                dassert(it == pointer_type.end(), "command '%s' already regisered", key);
                pointer_type[std::string(key)] = _index;
            }
        }
    }

    static std::map<std::string, perf_counter_ptr_type> pointer_type;
    std::vector<const char *> keys;
    dsn_perf_counter_type_t type;
    const char *title;
    const char *unit_name;
};

class profiler_output_data_type
{
public:
    profiler_output_data_type(int name_width, int data_width, int call_width)
    {
        std::stringstream namess, datass, tmpss;
        int tmp;

        for (int i = 0; i < name_width; i++)
            namess << "-";
        namess << "+";
        for (int i = 0; i < data_width; i++)
            datass << "-";
        datass << "+";

        for (int i = 0; i < name_width; i++)
            tmpss << " ";
        tmpss << "|";
        blank_name = tmpss.str();
        tmpss.clear();
        tmpss.str("");

        for (int i = 0; i < data_width; i++)
            tmpss << " ";
        tmpss << "|";
        blank_data = tmpss.str();
        tmpss.clear();
        tmpss.str("");

        for (int i = 0; i < call_width; i++)
            tmpss << " ";
        tmpss << "|";
        blank_matrix = tmpss.str();
        tmpss.clear();
        tmpss.str("");

        for (int i = 0; i < call_width; i++)
            tmpss << "-";
        tmpss << "+";
        separate_line_call_times = tmpss.str();
        tmpss.clear();
        tmpss.str("");

        tmpss << "+-------+" << namess.str() << datass.str();
        separate_line_top = tmpss.str();
        tmpss.clear();
        tmpss.str("");

        tmpss << "+-------+" << namess.str() << "-------+";
        for (int i = 0; i < PERF_COUNTER_COUNT; i++)
            tmpss << datass.str();
        separate_line_info = tmpss.str();
        tmpss.clear();
        tmpss.str("");

        tmpss << "+" << namess.str() << separate_line_call_times;
        separate_line_depmatrix = tmpss.str();
        tmpss.clear();
        tmpss.str("");

        tmpss << "+" << namess.str() << namess.str() << separate_line_call_times;
        separate_line_deplist = tmpss.str();
        tmpss.clear();
        tmpss.str("");

        tmpss << std::setw(data_width) << "none"
              << "|";
        none = tmpss.str();
        tmpss.clear();
        tmpss.str("");

        tmp = (call_width - 5) >> 1;
        tmpss << std::setw(call_width - tmp) << "TIMES";
        for (int i = 0; i < tmp; i++)
            tmpss << " ";
        tmpss << "|";
        view_call_times = tmpss.str();
        tmpss.clear();
        tmpss.str("");

        tmp = (name_width - 9) >> 1;
        tmpss << std::setw(name_width - tmp) << "TASK TYPE";
        for (int i = 0; i < tmp; i++)
            tmpss << " ";
        tmpss << "|";
        view_task_type = tmpss.str();
        tmpss.clear();
        tmpss.str("");

        tmp = (name_width - 11) >> 1;
        tmpss << std::setw(name_width - tmp) << "CALLER TYPE";
        for (int i = 0; i < tmp; i++)
            tmpss << " ";
        tmpss << "|";
        view_caller_type = tmpss.str();
        tmpss.clear();
        tmpss.str("");

        tmp = (name_width - 11) >> 1;
        tmpss << std::setw(name_width - tmp) << "CALLEE TYPE";
        for (int i = 0; i < tmp; i++)
            tmpss << " ";
        tmpss << "|";
        view_callee_type = tmpss.str();
    }
    std::string separate_line_call_times;
    std::string separate_line_info;
    std::string separate_line_top;
    std::string separate_line_deplist;
    std::string separate_line_depmatrix;
    std::string view_task_type;
    std::string view_caller_type;
    std::string view_callee_type;
    std::string view_call_times;
    std::string blank_matrix;
    std::string blank_name;
    std::string blank_data;
    std::string none;
};

struct task_spec_profiler
{
    perf_counter_wrapper ptr[PERF_COUNTER_COUNT];
    bool collect_call_count;
    bool is_profile;
    std::atomic<int64_t> *call_counts;

    task_spec_profiler()
    {
        collect_call_count = false;
        is_profile = false;
        call_counts = nullptr;
        memset((void *)ptr, 0, sizeof(ptr));
    }
};

std::string profiler_output_handler(const std::vector<std::string> &args);
std::string profiler_data_handler(const std::vector<std::string> &args);
std::string query_data_handler(const std::vector<std::string> &args);

void profiler_output_dependency_list_callee(std::stringstream &ss, const int task_id);
void profiler_output_dependency_list_caller(std::stringstream &ss, const int task_id);
void profiler_output_dependency_matrix(std::stringstream &ss);
void profiler_output_information_table(std::stringstream &ss, const int task_id);
void profiler_output_infomation_line(std::stringstream &ss,
                                     const int task_id,
                                     dsn_perf_counter_percentile_type_t percentile_type,
                                     const bool full_data);
void profiler_output_top(std::stringstream &ss,
                         const perf_counter_ptr_type counter_type,
                         const dsn_perf_counter_percentile_type_t percentile_type,
                         const int num);
void profiler_data_top(std::stringstream &ss,
                       const perf_counter_ptr_type counter_type,
                       const dsn_perf_counter_percentile_type_t percentile_type,
                       const int num);

} // namespace tools
} // namespace dsn
