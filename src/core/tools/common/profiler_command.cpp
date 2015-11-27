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
 *     What is this file about?
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */
#include <dsn/toollet/profiler.h>
#include "profiler_header.h"

namespace dsn {
    namespace tools {
        extern task_spec_profiler* s_spec_profilers;
        extern counter_info* counter_info_ptr[PREF_COUNTER_COUNT];

        static int find_task_id(const std::string &name)
        {
            auto code = dsn_task_code_from_string(name.c_str(), TASK_CODE_INVALID);
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

            if (args.size() < 1)
            {
                ss << "unenough arguments" << std::endl;
                return ss.str();
            }

            if ((args[0] == "dependency") || (args[0] == "dep"))
            {
                if (args.size() < 2)
                {
                    ss << "unenough arguments" << std::endl;
                    return ss.str();
                }

                if (args[1] == "matrix")
                {
                    profiler_output_dependency_matrix(ss);
                    return ss.str();
                }
                if (args[1] == "list")
                {
                    if (args.size() < 3)
                    {
                        profiler_output_dependency_list_caller(ss, -1);
                        return ss.str();
                    }

                    if (args[2] == "caller")
                    {
                        profiler_output_dependency_list_caller(ss, -1);
                        return ss.str();
                    }
                    if (args[2] == "callee")
                    {
                        profiler_output_dependency_list_callee(ss, -1);
                        return ss.str();
                    }

                    int task_id = find_task_id(args[2]);
                    if (task_id == TASK_CODE_INVALID)
                    {
                        ss << "no such task" << std::endl;
                        return ss.str();
                    }

                    if ((args.size() > 3) && (args[3] == "callee"))
                    {
                        profiler_output_dependency_list_callee(ss, task_id);
                    }
                    else
                    {
                        profiler_output_dependency_list_caller(ss, task_id);
                    }

                    return ss.str();
                }
                ss << "wrong arguments" << std::endl;
                return ss.str();
            }
            else if (args[0] == "info")
            {
                int task_id = -1;
                if ((args.size() > 1) && (args[1] != "all"))
                {
                    task_id = find_task_id(args[1]);
                }
                if (task_id == TASK_CODE_INVALID)
                {
                    ss << "no such task" << std::endl;
                    return ss.str();
                }

                profiler_output_information_table(ss, task_id);
            }
            else if (args[0] == "top")
            {
                if (args.size() < 3)
                {
                    ss << "unenough arguments" << std::endl;
                    return ss.str();
                }

                int num = atoi(args[1].c_str());
                if (num == 0)
                {
                    ss << "not a legal value" << std::endl;
                    return ss.str();
                }
                perf_counter_ptr_type counter_type = find_counter_type(args[2]);
                if (counter_type == PREF_COUNTER_INVALID)
                {
                    ss << "no such counter type" << std::endl;
                    return ss.str();
                }

                counter_percentile_type percentile_type = COUNTER_PERCENTILE_50;
                if ((args.size() > 3) && (find_percentail_type(args[3]) != COUNTER_PERCENTILE_INVALID))
                {
                    percentile_type = find_percentail_type(args[3]);
                }

                profiler_output_top(ss, counter_type, percentile_type, num);
            }
            else
                ss << "wrong arguments" << std::endl;
            return ss.str();
        }
        std::string profiler_data_handler(const std::vector<std::string>& args)
        {
            size_t k = args.size();
            int task_id;
            perf_counter_ptr_type counter_type;
            counter_percentile_type percentile_type;
            std::stringstream ss;
            std::vector<std::string> val;

            if ((args.size() > 0) && (args[0] == "top"))
            {
                if (k < 4)
                {
                    return ss.str();
                }
                int num = atoi(args[1].c_str());
                counter_type = find_counter_type(args[2]);
                percentile_type = find_percentail_type(args[3]);

                if ((num == 0) || (counter_type == PREF_COUNTER_INVALID) || (percentile_type == COUNTER_PERCENTILE_INVALID))
                {
                    return ss.str();
                }

                profiler_data_top(ss, counter_type, percentile_type, num);
                return ss.str();
            }

            for (int i = 0; i < k; i++)
            {
                utils::split_args(args[i].c_str(), val, ':');
                task_id = find_task_id(val[0]);
                counter_type = find_counter_type(val[1]);
                percentile_type = find_percentail_type(val[2]);

                if ((task_id != TASK_CODE_INVALID) && (counter_type != PREF_COUNTER_INVALID) && (s_spec_profilers[task_id].ptr[counter_type] != NULL) && (s_spec_profilers[task_id].is_profile != false))
                {
                    ss << dsn_task_code_to_string(task_id) << ":" << counter_info_ptr[counter_type]->title << ":" << percentail_counter_string[percentile_type] << ":";
                    if (counter_info_ptr[counter_type]->type != COUNTER_TYPE_NUMBER_PERCENTILES)
                    {
                        ss << s_spec_profilers[task_id].ptr[counter_type]->get_value() << " ";
                    }
                    else if (percentile_type != COUNTER_PERCENTILE_INVALID)
                    {
                        ss << s_spec_profilers[task_id].ptr[counter_type]->get_percentile(percentile_type) << " ";
                    }
                }
                else if ((task_id != TASK_CODE_INVALID) && (val[1] == "AllPercentile") && (s_spec_profilers[task_id].is_profile != false))
                {
                    for (int j = 0; j < PREF_COUNTER_COUNT; j++)
                    {
                        if ((s_spec_profilers[task_id].ptr[j] != NULL) && (counter_info_ptr[j]->type == COUNTER_TYPE_NUMBER_PERCENTILES))
                        {
                            ss << dsn_task_code_to_string(i) << ":" << counter_info_ptr[j]->title << ":" << percentail_counter_string[percentile_type] << ":" << s_spec_profilers[task_id].ptr[j]->get_percentile(percentile_type) << " ";
                        }
                    }
                }
            }
            return ss.str();
        }

        std::string query_data_handler(const std::vector<std::string>& args)
        {
            std::stringstream ss;

            if (args.size() < 1)
            {
                ss << "incorrect arguments" << std::endl;
                return ss.str();
            }

            //return a matrix for all task codes contained with perf_counter percentile value
            else if (args[0] == "table")
            {
                int task_id;
                perf_counter_ptr_type counter_type;
                counter_percentile_type percentile_type;

                ss << "[";
                for (int i = 0; i <= dsn_task_code_max(); ++i)
                {
                    task_id = i;

                    if ((i == TASK_CODE_INVALID) || (s_spec_profilers[task_id].is_profile == false))
                        continue;

                    double query_record[PREF_COUNTER_COUNT] = { 0 };
                    for (int j = 0; j < COUNTER_PERCENTILE_COUNT; ++j)
                    {
                        percentile_type = static_cast<counter_percentile_type>(j);
                        ss << "[\"" << dsn_task_code_to_string(task_id) << "\",";
                        ss << "\"" << percentail_counter_string[percentile_type] << "\"";

                        for (int k = 0; k < PREF_COUNTER_COUNT; k++)
                        {
                            counter_type = static_cast<perf_counter_ptr_type>(k);

                            if (s_spec_profilers[task_id].ptr[counter_type] == NULL)
                            {
                                ss << ",\"\"";
                            }
                            else
                            {
                                if (counter_info_ptr[counter_type]->type == COUNTER_TYPE_NUMBER_PERCENTILES)
                                {
                                    ss << "," << s_spec_profilers[task_id].ptr[counter_type]->get_percentile(percentile_type);
                                }
                                else
                                {
                                    double res;
                                    if (j == 0)
                                    {
                                        res = s_spec_profilers[task_id].ptr[counter_type]->get_value();
                                        query_record[k] = res;
                                    }
                                    else
                                        res = query_record[k];

                                    if (std::isnan(res))
                                        ss << "," << "\"NAN\"";
                                    else
                                        ss << "," << res << "";
                                }
                            }
                        }
                        ss << "],";
                    }
                }
                ss << "]";
                return ss.str();
            }
            //return a list of names of all task codes
            else if (args[0] == "task_list")
            {
                int task_id;

                ss << "[";
                for (int i = 0; i <= dsn_task_code_max(); ++i)
                {
                    task_id = i;

                    if ((i == TASK_CODE_INVALID) || (s_spec_profilers[task_id].is_profile == false))
                        continue;
                    ss << "\"" << dsn_task_code_to_string(task_id) << "\",";

                }
                ss << "]";
                return ss.str();
            }
            //return a list of 2 elements for a specific task:
            //1. a list of all perf_counters
            //2. 1000 samples for each perf_counter
            else if (args[0] == "counter_sample")
            {
                if (args.size() < 2)
                {
                    ss << "unenough arguments" << std::endl;
                    return ss.str();
                }

                int task_id = find_task_id(args[1]);

                if ((task_id == TASK_CODE_INVALID) || (s_spec_profilers[task_id].is_profile == false))
                {
                    ss << "no such task code or target task is not profiled" << std::endl;
                    return ss.str();
                }

                if (task_spec::get(task_id)->type == TASK_TYPE_RPC_RESPONSE)
                    task_id = task_spec::get(task_id)->rpc_paired_code;

                std::stringstream counterList;
                counterList << "[[";
                do{
                    for (int k = 0; k < PREF_COUNTER_COUNT; k++)
                    {
                        perf_counter_ptr_type counter_type = static_cast<perf_counter_ptr_type>(k);

                        if (counter_info_ptr[counter_type]->type == COUNTER_TYPE_NUMBER_PERCENTILES)
                        {
                            if (s_spec_profilers[task_id].ptr[counter_type] == NULL)
                                continue;

                            char name[20] = { 0 };
                            strcpy(name, counter_info_ptr[counter_type]->title);

                            char name_suffix[10] = { 0 };
                            switch (task_spec::get(task_id)->type)
                            {
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

                            ss << "[\"" << name << name_suffix << "\"";
                            counterList << "\"" << name << name_suffix << "\",";

                            int sample_count = 0;
                            uint64_t* samples = s_spec_profilers[task_id].ptr[counter_type]->get_samples(sample_count);

                            std::vector<uint64_t> sampleArr(samples, samples + sample_count);
                            std::sort(sampleArr.begin(), sampleArr.end());

                            for (int l = 0; l < sample_count; l += sample_count / 1000)
                                ss << ", " << sampleArr[l];

                            ss << "],\n";
                        }
                    }
                    if (task_spec::get(task_id)->type == TASK_TYPE_RPC_RESPONSE || task_spec::get(task_id)->type == TASK_TYPE_RPC_REQUEST)
                        task_id = task_spec::get(task_id)->rpc_paired_code;
                } while (task_spec::get(task_id)->type == TASK_TYPE_RPC_RESPONSE);
                counterList << "],[" << ss.str() << "]]";
                return counterList.str();
            }
            //return raw counter values for a specific task
            else if (args[0] == "counter_raw")
            {
                if (args.size() < 2)
                {
                    ss << "unenough arguments" << std::endl;
                    return ss.str();
                }

                int task_id = find_task_id(args[1]);

                if ((task_id == TASK_CODE_INVALID) || (s_spec_profilers[task_id].is_profile == false))
                {
                    ss << "no such task code or target task is not profiled" << std::endl;
                    return ss.str();
                }

                double timeList[8] = { 0 };
                for (int k = 0; k < PREF_COUNTER_COUNT; k++)
                {
                    perf_counter_ptr_type counter_type = static_cast<perf_counter_ptr_type>(k);

                    if (counter_info_ptr[counter_type]->type == COUNTER_TYPE_NUMBER_PERCENTILES)
                    {
                        if (s_spec_profilers[task_id].ptr[counter_type] == NULL)
                            continue;

                        timeList[k] = s_spec_profilers[task_id].ptr[counter_type]->get_percentile(COUNTER_PERCENTILE_50);
                    }
                }

                for (auto i : timeList)
                    ss << ((i < 0) ? 0 : i) << ",";
                return ss.str();
            }
            //return 6 types of latency times for a specific task
            else if (args[0] == "counter_calc")
            {
                if (args.size() < 2)
                {
                    ss << "unenough arguments" << std::endl;
                    return ss.str();
                }

                int task_id = find_task_id(args[1]);

                if ((task_id == TASK_CODE_INVALID) || (s_spec_profilers[task_id].is_profile == false))
                {
                    ss << "no such task code or target task is not profiled" << std::endl;
                    return ss.str();
                }

                if (task_spec::get(task_id)->type == TASK_TYPE_RPC_RESPONSE)
                    task_id = task_spec::get(task_id)->rpc_paired_code;

                double timeList[7] = { 0 };
                do{
                    for (int k = 0; k < PREF_COUNTER_COUNT; k++)
                    {
                        perf_counter_ptr_type counter_type = static_cast<perf_counter_ptr_type>(k);

                        if (counter_info_ptr[counter_type]->type == COUNTER_TYPE_NUMBER_PERCENTILES)
                        {
                            if (s_spec_profilers[task_id].ptr[counter_type] == NULL)
                                continue;

                            double timeGet = s_spec_profilers[task_id].ptr[counter_type]->get_percentile(COUNTER_PERCENTILE_50);

                            if (strcmp(counter_info_ptr[counter_type]->title, "RPC.SERVER(ns)") == 0 && task_spec::get(task_id)->type == TASK_TYPE_RPC_REQUEST)
                                timeList[0] = timeGet;
                            else if (strcmp(counter_info_ptr[counter_type]->title, "QUEUE(ns)") == 0 && task_spec::get(task_id)->type == TASK_TYPE_RPC_REQUEST)
                                timeList[1] = timeGet;
                            else if (strcmp(counter_info_ptr[counter_type]->title, "EXEC(ns)") == 0 && task_spec::get(task_id)->type == TASK_TYPE_RPC_REQUEST)
                                timeList[2] = timeGet;
                            else if (strcmp(counter_info_ptr[counter_type]->title, "RPC.CLIENT(ns)") == 0 && task_spec::get(task_id)->type == TASK_TYPE_RPC_RESPONSE)
                                timeList[3] = timeGet;
                            else if (strcmp(counter_info_ptr[counter_type]->title, "QUEUE(ns)") == 0 && task_spec::get(task_id)->type == TASK_TYPE_RPC_RESPONSE)
                                timeList[4] = timeGet;
                            else if (strcmp(counter_info_ptr[counter_type]->title, "EXEC(ns)") == 0 && task_spec::get(task_id)->type == TASK_TYPE_RPC_RESPONSE)
                                timeList[5] = timeGet;
                            else if (strcmp(counter_info_ptr[counter_type]->title, "AIO.LATENCY(ns)") == 0)
                                timeList[6] = timeGet;
                        }
                    }
                    if (task_spec::get(task_id)->type == TASK_TYPE_RPC_RESPONSE || task_spec::get(task_id)->type == TASK_TYPE_RPC_REQUEST)
                        task_id = task_spec::get(task_id)->rpc_paired_code;
                } while (task_spec::get(task_id)->type == TASK_TYPE_RPC_RESPONSE);

                timeList[0] = (timeList[3] - timeList[0]) / 2;
                timeList[3] = timeList[0];
                for (auto i : timeList)
                    ss << ((i < 0) ? 0 : i) << ",";
                return ss.str();
            }
            //return a list of current counter value for a specific task
            else if (args[0] == "counter_realtime")
            {
                if (args.size() < 2)
                {
                    ss << "unenough arguments" << std::endl;
                    return ss.str();
                }

                int task_id = find_task_id(args[1]);

                if ((task_id == TASK_CODE_INVALID) || (s_spec_profilers[task_id].is_profile == false))
                {
                    ss << "no such task code or target task is not profiled" << std::endl;
                    return ss.str();
                }

                ss << "[";
                if (task_spec::get(task_id)->type == TASK_TYPE_RPC_RESPONSE)
                    task_id = task_spec::get(task_id)->rpc_paired_code;

                bool fstFlag = 0;
                do{
                    for (int k = 0; k < PREF_COUNTER_COUNT; k++)
                    {
                        perf_counter_ptr_type counter_type = static_cast<perf_counter_ptr_type>(k);

                        if (counter_info_ptr[counter_type]->type == COUNTER_TYPE_NUMBER_PERCENTILES)
                        {
                            if (s_spec_profilers[task_id].ptr[counter_type] == NULL)
                                continue;

                            char name[20] = { 0 };
                            strcpy(name, counter_info_ptr[counter_type]->title);

                            char name_suffix[10] = { 0 };
                            switch (task_spec::get(task_id)->type)
                            {
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

                            if (!fstFlag)
                                fstFlag = 1;
                            else
                                ss << ",";

                            ss << "{\"name\":\"" << name << name_suffix << "\"";

                            uint64_t sample = s_spec_profilers[task_id].ptr[counter_type]->get_current_sample();
                            ss << ", \"value\":" << sample;

                            ss << "}\n";
                        }
                    }
                    if (task_spec::get(task_id)->type == TASK_TYPE_RPC_RESPONSE || task_spec::get(task_id)->type == TASK_TYPE_RPC_REQUEST)
                        task_id = task_spec::get(task_id)->rpc_paired_code;
                } while (task_spec::get(task_id)->type == TASK_TYPE_RPC_RESPONSE);

                ss << "]";
                return ss.str();
            }
            //return a list of 2 elements for a specific task
            //1. a list of caller names and call times 
            //2. a list of callee names and call times
            else if (args[0] == "call")
            {
                if (args.size() < 2)
                {
                    ss << "unenough arguments" << std::endl;
                    return ss.str();
                }

                int task_id = find_task_id(args[1]);

                if ((task_id == TASK_CODE_INVALID) || (s_spec_profilers[task_id].is_profile == false))
                {
                    ss << "no such task code or target task is not profiled" << std::endl;
                    return ss.str();
                }
                ss << "[[";
                if (s_spec_profilers[task_id].collect_call_count)
                {
                    for (int j = 0; j <= dsn_task_code_max(); j++)
                    {
                        if ((j != TASK_CODE_INVALID) && (s_spec_profilers[j].is_profile) && (s_spec_profilers[task_id].call_counts[j] > 0))
                        {
                            ss << "{\"name\":\"" << std::string(dsn_task_code_to_string(j)) << "\",\"num\":" << s_spec_profilers[task_id].call_counts[j] << "},";
                        }
                    }
                }
                ss << "],[";
                for (int j = 0; j <= dsn_task_code_max(); j++)
                {
                    if ((j != TASK_CODE_INVALID) && (s_spec_profilers[j].is_profile) && (s_spec_profilers[j].collect_call_count) && (s_spec_profilers[j].call_counts[task_id] > 0))
                    {
                        ss << "{\"name\":\"" << std::string(dsn_task_code_to_string(j)) << "\",\"num\":" << s_spec_profilers[j].call_counts[task_id] << "},";
                    }
                }
                ss << "]]";
                return ss.str();
            }
            //return a list of all sharer using the same pool with a specific task
            else if (args[0] == "pool_sharer")
            {
                if (args.size() < 2)
                {
                    ss << "unenough arguments" << std::endl;
                    return ss.str();
                }

                int task_id = find_task_id(args[1]);

                if ((task_id == TASK_CODE_INVALID) || (s_spec_profilers[task_id].is_profile == false))
                {
                    ss << "no such task code or target task is not profiled" << std::endl;
                    return ss.str();
                }

                auto pool = task_spec::get(task_id)->pool_code;

                ss << "[";
                for (int j = 0; j <= dsn_task_code_max(); j++)
                if (j != TASK_CODE_INVALID && j != task_id && task_spec::get(j)->pool_code == pool && std::string(dsn_task_code_to_string(j)).find("_ACK") == std::string::npos)
                    ss << "\"" << std::string(dsn_task_code_to_string(j)) << "\",";
                ss << "]";
                return ss.str();
            }
            else
            {
                ss << "wrong parameter";
                return ss.str();
            }
        }
    }
}
