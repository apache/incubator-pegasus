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
 *     Command handler of profiler
 *
 * Revision history:
 *     2015-06-01, zjc95, first version
 *     2015-06-01, zjc95, deleted pdh part
 *     2015-06-02, zjc95, revised License
 *     2015-06-02, zjc95, revised format of tab and brace
 *     2015-08-11, zjc95, revised format of variable name
 *     2015-08-11, zjc95, added function 'profiler_data_handler'
 *     2015-11-24, zjc95, revised the decription
 *
 */

#include <dsn/toollet/profiler.h>
#include "profiler_header.h"
#include <dsn/cpp/json_helper.h>
#include <dsn/utility/time_utils.h>

namespace dsn {
namespace tools {
extern task_spec_profiler *s_spec_profilers;
extern counter_info *counter_info_ptr[PERF_COUNTER_COUNT];

static int find_task_id(const std::string &name)
{
    return dsn::task_code::try_get(name, TASK_CODE_INVALID).code();
}

static dsn_perf_counter_percentile_type_t find_percentail_type(const std::string &name)
{
    int num = atoi(name.c_str());
    if (num == 0)
        return COUNTER_PERCENTILE_INVALID;
    switch (num) {
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
    if (it == counter_info::pointer_type.end()) {
        return PERF_COUNTER_INVALID;
    }
    return it->second;
}

std::string profiler_output_handler(const std::vector<std::string> &args)
{
    std::stringstream ss;

    if (args.size() < 1) {
        ss << "unenough arguments" << std::endl;
        return ss.str();
    }

    if ((args[0] == "dependency") || (args[0] == "dep")) {
        if (args.size() < 2) {
            ss << "unenough arguments" << std::endl;
            return ss.str();
        }

        if (args[1] == "matrix") {
            profiler_output_dependency_matrix(ss);
            return ss.str();
        }
        if (args[1] == "list") {
            if (args.size() < 3) {
                profiler_output_dependency_list_caller(ss, -1);
                return ss.str();
            }

            if (args[2] == "caller") {
                profiler_output_dependency_list_caller(ss, -1);
                return ss.str();
            }
            if (args[2] == "callee") {
                profiler_output_dependency_list_callee(ss, -1);
                return ss.str();
            }

            int task_id = find_task_id(args[2]);
            if (task_id == TASK_CODE_INVALID) {
                ss << "no such task" << std::endl;
                return ss.str();
            }

            if ((args.size() > 3) && (args[3] == "callee")) {
                profiler_output_dependency_list_callee(ss, task_id);
            } else {
                profiler_output_dependency_list_caller(ss, task_id);
            }

            return ss.str();
        }
        ss << "wrong arguments" << std::endl;
        return ss.str();
    } else if (args[0] == "info") {
        int task_id = -1;
        if ((args.size() > 1) && (args[1] != "all")) {
            task_id = find_task_id(args[1]);
        }
        if (task_id == TASK_CODE_INVALID) {
            ss << "no such task" << std::endl;
            return ss.str();
        }

        profiler_output_information_table(ss, task_id);
    } else if (args[0] == "top") {
        if (args.size() < 3) {
            ss << "unenough arguments" << std::endl;
            return ss.str();
        }

        int num = atoi(args[1].c_str());
        if (num == 0) {
            ss << "not a legal value" << std::endl;
            return ss.str();
        }
        perf_counter_ptr_type counter_type = find_counter_type(args[2]);
        if (counter_type == PERF_COUNTER_INVALID) {
            ss << "no such counter type" << std::endl;
            return ss.str();
        }

        dsn_perf_counter_percentile_type_t percentile_type = COUNTER_PERCENTILE_50;
        if ((args.size() > 3) && (find_percentail_type(args[3]) != COUNTER_PERCENTILE_INVALID)) {
            percentile_type = find_percentail_type(args[3]);
        }

        profiler_output_top(ss, counter_type, percentile_type, num);
    } else
        ss << "wrong arguments" << std::endl;
    return ss.str();
}
std::string profiler_data_handler(const std::vector<std::string> &args)
{
    int k = static_cast<int>(args.size());
    int task_id;
    perf_counter_ptr_type counter_type;
    dsn_perf_counter_percentile_type_t percentile_type;
    std::stringstream ss;
    std::vector<std::string> val;

    if ((args.size() > 0) && (args[0] == "top")) {
        if (k < 4) {
            return ss.str();
        }
        int num = atoi(args[1].c_str());
        counter_type = find_counter_type(args[2]);
        percentile_type = find_percentail_type(args[3]);

        if ((num == 0) || (counter_type == PERF_COUNTER_INVALID) ||
            (percentile_type == COUNTER_PERCENTILE_INVALID)) {
            return ss.str();
        }

        profiler_data_top(ss, counter_type, percentile_type, num);
        return ss.str();
    }

    for (int i = 0; i < k; i++) {
        utils::split_args(args[i].c_str(), val, ':');
        task_id = find_task_id(val[0]);
        counter_type = find_counter_type(val[1]);
        percentile_type = find_percentail_type(val[2]);

        if ((task_id != TASK_CODE_INVALID) && (counter_type != PERF_COUNTER_INVALID) &&
            (s_spec_profilers[task_id].ptr[counter_type].get() != nullptr) &&
            (s_spec_profilers[task_id].is_profile != false)) {
            ss << dsn::task_code(task_id).to_string() << ":"
               << counter_info_ptr[counter_type]->title << ":"
               << percentail_counter_string[percentile_type] << ":";
            if (counter_info_ptr[counter_type]->type != COUNTER_TYPE_NUMBER_PERCENTILES) {
                ss << s_spec_profilers[task_id].ptr[counter_type]->get_value() << " ";
            } else if (percentile_type != COUNTER_PERCENTILE_INVALID) {
                ss << s_spec_profilers[task_id].ptr[counter_type]->get_percentile(percentile_type)
                   << " ";
            }
        } else if ((task_id != TASK_CODE_INVALID) && (val[1] == "AllPercentile") &&
                   (s_spec_profilers[task_id].is_profile != false)) {
            for (int j = 0; j < PERF_COUNTER_COUNT; j++) {
                if ((s_spec_profilers[task_id].ptr[j].get() != nullptr) &&
                    (counter_info_ptr[j]->type == COUNTER_TYPE_NUMBER_PERCENTILES)) {
                    ss << dsn::task_code(i).to_string() << ":" << counter_info_ptr[j]->title << ":"
                       << percentail_counter_string[percentile_type] << ":"
                       << s_spec_profilers[task_id].ptr[j]->get_percentile(percentile_type) << " ";
                }
            }
        }
    }
    return ss.str();
}

struct call_link
{
    std::string name;
    uint64_t num;
    DEFINE_JSON_SERIALIZATION(name, num)
};

struct call_resp
{
    std::vector<std::string> task_list;
    std::vector<std::vector<uint64_t>> call_matrix;
    DEFINE_JSON_SERIALIZATION(task_list, call_matrix)
};

struct counter_sample_resp
{
    std::string name;
    std::vector<uint64_t> samples;
    DEFINE_JSON_SERIALIZATION(name, samples)
};

struct nv_pair
{
    std::string name;
    uint64_t value;
    DEFINE_JSON_SERIALIZATION(name, value)
};

struct counter_realtime_resp
{
    std::string time;
    std::vector<nv_pair> data;
    DEFINE_JSON_SERIALIZATION(time, data)
};
std::string query_data_handler(const std::vector<std::string> &args)
{
    std::stringstream ss;

    if (args.size() < 1) {
        ss << "incorrect arguments" << std::endl;
        return ss.str();
    }

    // return a matrix for all task codes contained with perf_counter percentile value
    else if (args[0] == "table") {
        int task_id;
        perf_counter_ptr_type counter_type;
        dsn_perf_counter_percentile_type_t percentile_type;

        bool first_flag = 0;
        ss << "[";
        for (int i = 0; i <= dsn::task_code::max(); ++i) {
            task_id = i;

            if ((i == TASK_CODE_INVALID) || (s_spec_profilers[task_id].is_profile == false))
                continue;

            double query_record[PERF_COUNTER_COUNT] = {0};
            for (int j = 0; j < COUNTER_PERCENTILE_COUNT; ++j) {
                if (first_flag)
                    ss << ",";
                else
                    first_flag = 1;

                percentile_type = static_cast<dsn_perf_counter_percentile_type_t>(j);
                ss << "[\"" << dsn::task_code(task_id).to_string() << "\",";
                ss << "\"" << percentail_counter_string[percentile_type] << "\"";

                for (int k = 0; k < PERF_COUNTER_COUNT; k++) {
                    counter_type = static_cast<perf_counter_ptr_type>(k);

                    if (s_spec_profilers[task_id].ptr[counter_type].get() == NULL) {
                        ss << ",\"\"";
                    } else {
                        if (counter_info_ptr[counter_type]->type ==
                            COUNTER_TYPE_NUMBER_PERCENTILES) {
                            ss << ","
                               << s_spec_profilers[task_id].ptr[counter_type]->get_percentile(
                                      percentile_type);
                        } else {
                            double res;
                            if (j == 0) {
                                res = s_spec_profilers[task_id].ptr[counter_type]->get_value();
                                query_record[k] = res;
                            } else
                                res = query_record[k];

                            if (std::isnan(res))
                                ss << ","
                                   << "\"NAN\"";
                            else
                                ss << "," << res << "";
                        }
                    }
                }
                ss << "]";
            }
        }
        ss << "]";
        return ss.str();
    }
    // return a list of names of all task codes
    else if (args[0] == "task_list") {
        int task_id;

        std::vector<std::string> task_list;
        for (int i = 0; i <= dsn::task_code::max(); ++i) {
            task_id = i;

            if ((i == TASK_CODE_INVALID) || (s_spec_profilers[task_id].is_profile == false))
                continue;
            task_list.push_back(dsn::task_code(task_id).to_string());
        }
        dsn::json::json_forwarder<decltype(task_list)>::encode(ss, task_list);
        return ss.str();
    }
    // return a list of 2 elements for a specific task:
    // 1. a list of all perf_counters
    // 2. 1000 samples for each perf_counter
    else if (args[0] == "counter_sample") {
        if (args.size() < 2) {
            ss << "unenough arguments" << std::endl;
            return ss.str();
        }

        int task_id = find_task_id(args[1]);

        if ((task_id == TASK_CODE_INVALID) || (s_spec_profilers[task_id].is_profile == false)) {
            ss << "no such task code or target task is not profiled" << std::endl;
            return ss.str();
        }

        if (task_spec::get(task_id)->type == TASK_TYPE_RPC_RESPONSE)
            task_id = task_spec::get(task_id)->rpc_paired_code;

        std::vector<counter_sample_resp> total_resp;
        do {
            for (int k = 0; k < PERF_COUNTER_COUNT; k++) {
                perf_counter_ptr_type counter_type = static_cast<perf_counter_ptr_type>(k);

                if (counter_info_ptr[counter_type]->type == COUNTER_TYPE_NUMBER_PERCENTILES) {
                    counter_sample_resp resp;

                    if (s_spec_profilers[task_id].ptr[counter_type].get() == NULL)
                        continue;

                    char name[20] = {0};
                    strcpy(name, counter_info_ptr[counter_type]->title);

                    char name_suffix[10] = {0};
                    switch (task_spec::get(task_id)->type) {
                    case TASK_TYPE_RPC_REQUEST:
                        strcat(name_suffix, "@server");
                        break;
                    case TASK_TYPE_RPC_RESPONSE:
                        strcat(name_suffix, "@client");
                        break;
                    default:
                        strcat(name_suffix, "");
                        break;
                    }

                    resp.name = std::string(name) + std::string(name_suffix);

                    // get samples
                    perf_counter::samples_t samples;
                    int sample_count = 1000;
                    sample_count = s_spec_profilers[task_id].ptr[counter_type]->get_latest_samples(
                        sample_count, samples);

                    // merge and sort
                    std::vector<uint64_t> sorted_samples;
                    sorted_samples.resize(sample_count);

                    int copy_index = 0;
                    for (auto &s : samples) {
                        if (s.second != 0) {

                            dassert(copy_index + s.second <= (int)sorted_samples.size(),
                                    "return of get_latest_samples() is inconsistent with what is "
                                    "get in samples");

                            memcpy((void *)&sorted_samples[copy_index],
                                   (const void *)s.first,
                                   s.second * sizeof(uint64_t));
                            copy_index += s.second;
                        }
                    }
                    dassert(copy_index == sample_count,
                            "return of get_latest_samples() is "
                            "inconsistent with what is get in samples");

                    std::sort(sorted_samples.begin(), sorted_samples.end());

                    for (unsigned int l = 0; l < sorted_samples.size(); l++)
                        resp.samples.push_back(sorted_samples[l]);

                    total_resp.push_back(resp);
                }
            }
            if (task_spec::get(task_id)->type == TASK_TYPE_RPC_RESPONSE ||
                task_spec::get(task_id)->type == TASK_TYPE_RPC_REQUEST)
                task_id = task_spec::get(task_id)->rpc_paired_code;
        } while (task_spec::get(task_id)->type == TASK_TYPE_RPC_RESPONSE);

        dsn::json::json_forwarder<decltype(total_resp)>::encode(ss, total_resp);
        return ss.str();
    }
    // return 6 types of latency times for a specific task
    else if (args[0] == "counter_breakdown") {
        if (args.size() < 2) {
            ss << "unenough arguments" << std::endl;
            return ss.str();
        }
        int task_id = find_task_id(args[1]);
        int query_percentile = (args.size() == 2) ? 50 : atoi(args[2].c_str());

        if ((task_id == TASK_CODE_INVALID) || (s_spec_profilers[task_id].is_profile == false)) {
            ss << "no such task code or target task is not profiled" << std::endl;
            return ss.str();
        }

        if (task_spec::get(task_id)->type == TASK_TYPE_RPC_RESPONSE)
            task_id = task_spec::get(task_id)->rpc_paired_code;

        std::vector<double> timeList{0, 0, 0, 0, 0, 0, 0};
        do {
            for (int k = 0; k < PERF_COUNTER_COUNT; k++) {
                perf_counter_ptr_type counter_type = static_cast<perf_counter_ptr_type>(k);

                if (counter_info_ptr[counter_type]->type == COUNTER_TYPE_NUMBER_PERCENTILES) {
                    if (s_spec_profilers[task_id].ptr[counter_type].get() == NULL)
                        continue;

                    double timeGet = 0;
                    switch (query_percentile) {
                    case 50:
                        timeGet = s_spec_profilers[task_id].ptr[counter_type]->get_percentile(
                            COUNTER_PERCENTILE_50);
                        break;
                    case 90:
                        timeGet = s_spec_profilers[task_id].ptr[counter_type]->get_percentile(
                            COUNTER_PERCENTILE_90);
                        break;
                    case 95:
                        timeGet = s_spec_profilers[task_id].ptr[counter_type]->get_percentile(
                            COUNTER_PERCENTILE_95);
                        break;
                    case 99:
                        timeGet = s_spec_profilers[task_id].ptr[counter_type]->get_percentile(
                            COUNTER_PERCENTILE_99);
                        break;
                    case 999:
                        timeGet = s_spec_profilers[task_id].ptr[counter_type]->get_percentile(
                            COUNTER_PERCENTILE_999);
                        break;
                    }

                    timeGet = ((timeGet < 0) ? 0 : timeGet);

                    if (strcmp(counter_info_ptr[counter_type]->title, "RPC.SERVER(ns)") == 0 &&
                        task_spec::get(task_id)->type == TASK_TYPE_RPC_REQUEST)
                        timeList[0] = timeGet;
                    else if (strcmp(counter_info_ptr[counter_type]->title, "QUEUE(ns)") == 0 &&
                             task_spec::get(task_id)->type == TASK_TYPE_RPC_REQUEST)
                        timeList[1] = timeGet;
                    else if (strcmp(counter_info_ptr[counter_type]->title, "EXEC(ns)") == 0 &&
                             task_spec::get(task_id)->type == TASK_TYPE_RPC_REQUEST)
                        timeList[2] = timeGet;
                    else if (strcmp(counter_info_ptr[counter_type]->title, "RPC.CLIENT(ns)") == 0 &&
                             task_spec::get(task_id)->type == TASK_TYPE_RPC_RESPONSE)
                        timeList[3] = timeGet;
                    else if (strcmp(counter_info_ptr[counter_type]->title, "QUEUE(ns)") == 0 &&
                             task_spec::get(task_id)->type == TASK_TYPE_RPC_RESPONSE)
                        timeList[4] = timeGet;
                    else if (strcmp(counter_info_ptr[counter_type]->title, "EXEC(ns)") == 0 &&
                             task_spec::get(task_id)->type == TASK_TYPE_RPC_RESPONSE)
                        timeList[5] = timeGet;
                    else if (strcmp(counter_info_ptr[counter_type]->title, "AIO.LATENCY(ns)") == 0)
                        timeList[6] = timeGet;
                }
            }
            if (task_spec::get(task_id)->type == TASK_TYPE_RPC_RESPONSE ||
                task_spec::get(task_id)->type == TASK_TYPE_RPC_REQUEST)
                task_id = task_spec::get(task_id)->rpc_paired_code;
        } while (task_spec::get(task_id)->type == TASK_TYPE_RPC_RESPONSE);

        // timeList[0] = (timeList[3] - timeList[0]) / 2;
        // timeList[3] = timeList[0];

        dsn::json::json_forwarder<decltype(timeList)>::encode(ss, timeList);
        return ss.str();
    }
    // return a list of current counter value for a specific task
    else if (args[0] == "counter_realtime") {
        if (args.size() < 2) {
            ss << "unenough arguments" << std::endl;
            return ss.str();
        }

        int task_id = find_task_id(args[1]);

        if ((task_id == TASK_CODE_INVALID) || (s_spec_profilers[task_id].is_profile == false)) {
            ss << "no such task code or target task is not profiled" << std::endl;
            return ss.str();
        }

        char str[24];
        ::dsn::utils::time_ms_to_string(dsn_now_ns() / 1000000, str);

        std::vector<nv_pair> data;

        if (task_spec::get(task_id)->type == TASK_TYPE_RPC_RESPONSE)
            task_id = task_spec::get(task_id)->rpc_paired_code;

        do {
            for (int k = 0; k < PERF_COUNTER_COUNT; k++) {
                perf_counter_ptr_type counter_type = static_cast<perf_counter_ptr_type>(k);

                if (counter_info_ptr[counter_type]->type == COUNTER_TYPE_NUMBER_PERCENTILES) {
                    if (s_spec_profilers[task_id].ptr[counter_type].get() == NULL)
                        continue;

                    char name[20] = {0};
                    strcpy(name, counter_info_ptr[counter_type]->title);

                    char name_suffix[10] = {0};
                    switch (task_spec::get(task_id)->type) {
                    case TASK_TYPE_RPC_REQUEST:
                        strcat(name_suffix, "@server");
                        break;
                    case TASK_TYPE_RPC_RESPONSE:
                        strcat(name_suffix, "@client");
                        break;
                    default:
                        strcat(name_suffix, "");
                        break;
                    }

                    uint64_t sample =
                        s_spec_profilers[task_id].ptr[counter_type]->get_latest_sample();

                    data.push_back(nv_pair{std::string(name) + std::string(name_suffix), sample});
                }
            }
            if (task_spec::get(task_id)->type == TASK_TYPE_RPC_RESPONSE ||
                task_spec::get(task_id)->type == TASK_TYPE_RPC_REQUEST)
                task_id = task_spec::get(task_id)->rpc_paired_code;
        } while (task_spec::get(task_id)->type == TASK_TYPE_RPC_RESPONSE);

        counter_realtime_resp{std::string(str), data}.encode_json_state(ss);
        return ss.str();
    }
    // return a list of 2 elements for a specific task
    // 1. a list of caller names and call times
    // 2. a list of callee names and call times
    else if (args[0] == "call") {
        if (args.size() < 1) {
            ss << "unenough arguments" << std::endl;
            return ss.str();
        } else if (args.size() == 1) {
            std::vector<std::string> task_list;
            std::vector<std::vector<uint64_t>> call_matrix;

            for (int j = 0; j <= dsn::task_code::max(); j++) {
                if ((j != TASK_CODE_INVALID) && (s_spec_profilers[j].is_profile)) {
                    task_list.push_back(std::string(dsn::task_code(j).to_string()));

                    std::vector<uint64_t> call_vector;
                    for (int k = 0; k <= dsn::task_code::max(); k++) {
                        if ((k != TASK_CODE_INVALID) && (s_spec_profilers[k].is_profile)) {
                            call_vector.push_back(s_spec_profilers[j].call_counts[k]);
                        }
                    }
                    call_matrix.push_back(call_vector);
                }
            }

            call_resp{task_list, call_matrix}.encode_json_state(ss);
            return ss.str();
        }

        int task_id = find_task_id(args[1]);

        if ((task_id == TASK_CODE_INVALID) || (s_spec_profilers[task_id].is_profile == false)) {
            ss << "no such task code or target task is not profiled" << std::endl;
            return ss.str();
        }
        std::vector<std::vector<call_link>> call_list;
        std::vector<call_link> caller_list;
        std::vector<call_link> callee_list;

        if (s_spec_profilers[task_id].collect_call_count) {
            for (int j = 0; j <= dsn::task_code::max(); j++) {
                if ((j != TASK_CODE_INVALID) && (s_spec_profilers[j].is_profile) &&
                    (s_spec_profilers[task_id].call_counts[j] > 0)) {
                    call_link tmp_call_link;
                    tmp_call_link.name = std::string(dsn::task_code(j).to_string());
                    tmp_call_link.num = s_spec_profilers[task_id].call_counts[j];
                    caller_list.push_back(tmp_call_link);
                }
            }
        }

        for (int j = 0; j <= dsn::task_code::max(); j++) {
            if ((j != TASK_CODE_INVALID) && (s_spec_profilers[j].is_profile) &&
                (s_spec_profilers[j].collect_call_count) &&
                (s_spec_profilers[j].call_counts[task_id] > 0)) {
                call_link tmp_call_link;
                tmp_call_link.name = std::string(dsn::task_code(j).to_string());
                tmp_call_link.num = s_spec_profilers[j].call_counts[task_id];
                callee_list.push_back(tmp_call_link);
            }
        }
        call_list.push_back(caller_list);
        call_list.push_back(callee_list);
        dsn::json::json_forwarder<decltype(call_list)>::encode(ss, call_list);
        return ss.str();
    }
    // return a list of all sharer using the same pool with a specific task
    else if (args[0] == "pool_sharer") {
        if (args.size() < 2) {
            ss << "unenough arguments" << std::endl;
            return ss.str();
        }

        int task_id = find_task_id(args[1]);

        if ((task_id == TASK_CODE_INVALID) || (s_spec_profilers[task_id].is_profile == false)) {
            ss << "no such task code or target task is not profiled" << std::endl;
            return ss.str();
        }

        auto pool = task_spec::get(task_id)->pool_code;

        std::vector<std::string> sharer_list;
        for (int j = 0; j <= dsn::task_code::max(); j++)
            if (j != TASK_CODE_INVALID && j != task_id && task_spec::get(j)->pool_code == pool &&
                task_spec::get(j)->type == TASK_TYPE_RPC_RESPONSE)
                sharer_list.push_back(std::string(dsn::task_code(j).to_string()));
        dsn::json::json_forwarder<decltype(sharer_list)>::encode(ss, sharer_list);
        return ss.str();
    }
    // query time
    else if (args[0] == "time") {
        char str[24];
        ::dsn::utils::time_ms_to_string(dsn_now_ns() / 1000000, str);
        ss << str;
        return ss.str();
    } else {
        ss << "wrong parameter";
        return ss.str();
    }
}
}
}
