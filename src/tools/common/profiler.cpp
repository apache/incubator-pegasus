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
#include <dsn/toollet/profiler.h>
#include <dsn/service_api.h>
#include "shared_io_service.h"
#include "profiler_header.h"
#include <dsn/internal/command.h>
#include <iostream>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "toollet.profiler"
using namespace dsn::service;

namespace dsn {
    namespace tools {

        typedef uint64_extension_helper<task> task_ext_for_profiler;
        typedef uint64_extension_helper<message> message_ext_for_profiler;

        task_spec_profiler* s_spec_profilers = nullptr;
        std::map<std::string, perf_counter_ptr_type> counter_info::pointer_type;
        counter_info* counter_info_ptr[] = {
            new counter_info({ "queue.time", "qt" },            TASK_QUEUEING_TIME_NS,              COUNTER_TYPE_NUMBER_PERCENTILES,    "QUEUE(ns)",       "ns"),
            new counter_info({ "exec.time", "et" },             TASK_EXEC_TIME_NS,                  COUNTER_TYPE_NUMBER_PERCENTILES,    "EXEC(ns)",        "ns"),
            new counter_info({ "throughput", "tp" },            TASK_THROUGHPUT,                    COUNTER_TYPE_RATE,                  "THP(#/s)",        "#/s"),
            new counter_info({ "cancelled", "cc" },             TASK_CANCELLED,                     COUNTER_TYPE_NUMBER,                "CANCEL(#)",       "#"),
            new counter_info({ "aio.latency",  "al" },          AIO_LATENCY_NS,                     COUNTER_TYPE_NUMBER_PERCENTILES,    "AIO.LATENCY(ns)", "ns"),
            new counter_info({ "rpc.server.latency", "rpcsl" }, RPC_SERVER_LATENCY_NS,              COUNTER_TYPE_NUMBER_PERCENTILES,    "RPC.SERVER(ns)",  "ns"),
            new counter_info({ "rpc.client.latency", "rpccl" }, RPC_CLIENT_NON_TIMEOUT_LATENCY_NS,  COUNTER_TYPE_NUMBER_PERCENTILES,    "RPC.CLIENT(ns)",  "ns"),
            new counter_info({ "rpc.client.timeout", "rpcto" }, RPC_CLIENT_TIMEOUT_THROUGHPUT,      COUNTER_TYPE_RATE,                  "TIMEOUT(#/s)",    "#/s")
        };

        // call normal task
        static void profiler_on_task_enqueue(task* caller, task* callee)
        {
            if (caller != nullptr)
            {
                auto& prof = s_spec_profilers[caller->spec().code];
                auto code = caller->spec().code;
                if (prof.collect_call_count)
                {
                    prof.call_counts[callee->spec().code]++;
                }
            }

            task_ext_for_profiler::get(callee) = ::dsn::service::env::now_ns();
        }

        static void profiler_on_task_begin(task* this_)
        {
            uint64_t& qts = task_ext_for_profiler::get(this_);
            uint64_t now = ::dsn::service::env::now_ns();
            s_spec_profilers[this_->spec().code].ptr[TASK_QUEUEING_TIME_NS]->set(now - qts);
            qts = now;

        }

        static void profiler_on_task_end(task* this_)
        {
            uint64_t qts = task_ext_for_profiler::get(this_);
            uint64_t now = ::dsn::service::env::now_ns();
            s_spec_profilers[this_->spec().code].ptr[TASK_EXEC_TIME_NS]->set(now - qts);
            s_spec_profilers[this_->spec().code].ptr[TASK_THROUGHPUT]->increment();
        }

        static void profiler_on_task_cancelled(task* this_)
        {
            s_spec_profilers[this_->spec().code].ptr[TASK_CANCELLED]->increment();
        }

        static void profiler_on_task_wait_pre(task* caller, task* callee, uint32_t timeout_ms)
        {

        }

        static void profiler_on_task_wait_post(task* caller, task* callee, bool succ)
        {

        }

        static void profiler_on_task_cancel_post(task* caller, task* callee, bool succ)
        {

        }

        // return true means continue, otherwise early terminate with task::set_error_code
        static void profiler_on_aio_call(task* caller, aio_task* callee)
        {
            auto& prof = s_spec_profilers[caller->spec().code];
            if (prof.collect_call_count)
            {
                prof.call_counts[callee->spec().code]++;
            }

            // time disk io starts
            task_ext_for_profiler::get(callee) = ::dsn::service::env::now_ns();
        }

        static void profiler_on_aio_enqueue(aio_task* this_)
        {
            uint64_t& ats = task_ext_for_profiler::get(this_);
            uint64_t now = ::dsn::service::env::now_ns();

            s_spec_profilers[this_->spec().code].ptr[AIO_LATENCY_NS]->set(now - ats);
            ats = now;
        }

        // return true means continue, otherwise early terminate with task::set_error_code
        static void profiler_on_rpc_call(task* caller, message* req, rpc_response_task* callee)
        {
            auto& prof = s_spec_profilers[caller->spec().code];
            if (prof.collect_call_count)
            {
                prof.call_counts[req->header().local_rpc_code]++;
            }

            // time rpc starts
            if (nullptr != callee)
            {
                task_ext_for_profiler::get(callee) = ::dsn::service::env::now_ns();
            }

        }

        static void profiler_on_rpc_request_enqueue(rpc_request_task* callee)
        {
            uint64_t now = ::dsn::service::env::now_ns();
            task_ext_for_profiler::get(callee) = now;
            message_ext_for_profiler::get(callee->get_request().get()) = now;
        }

        static void profiler_on_rpc_create_response(message* req, message* resp)
        {
            message_ext_for_profiler::get(resp) = message_ext_for_profiler::get(req);
        }

        // return true means continue, otherwise early terminate with task::set_error_code
        static void profiler_on_rpc_reply(task* caller, message* msg)
        {
            auto& prof = s_spec_profilers[caller->spec().code];
            if (prof.collect_call_count)
            {
                prof.call_counts[msg->header().local_rpc_code]++;
            }

            uint64_t qts = message_ext_for_profiler::get(msg);
            uint64_t now = ::dsn::service::env::now_ns();
            auto code = task_spec::get(msg->header().local_rpc_code)->rpc_paired_code;
            s_spec_profilers[code].ptr[RPC_SERVER_LATENCY_NS]->set(now - qts);
        }

        static void profiler_on_rpc_response_enqueue(rpc_response_task* resp)
        {
            uint64_t& cts = task_ext_for_profiler::get(resp);
            uint64_t now = ::dsn::service::env::now_ns();
            if (resp->get_response() != nullptr)
            {
                s_spec_profilers[resp->spec().code].ptr[RPC_CLIENT_NON_TIMEOUT_LATENCY_NS]->set(now - cts);
            }
            else
            {
                s_spec_profilers[resp->spec().code].ptr[RPC_CLIENT_TIMEOUT_THROUGHPUT]->increment();
            }
            cts = now;
        }

        void register_command_profiler()
        {
            std::stringstream tmpss;
            tmpss << "NAME:" << std::endl;
            tmpss << "    profiler - collect performance data" << std::endl;
            tmpss << "SYNOPSIS:" << std::endl;
            tmpss << "  show how tasks call each other with what frequency:" << std::endl;
            tmpss << "      p|P|profile|Profile task|t dependency|dep matrix" << std::endl;
            tmpss << "  show how tasks call each oether with list format sort by caller/callee:" << std::endl;
            tmpss << "      p|P|profile|Profile task|t dependency|dep list [$task] [caller(default)|callee]" << std::endl;
            tmpss << "  show performance data for specific tasks:" << std::endl;
            tmpss << "      p|P|profile|Profile task|t info [all|$task]:" << std::endl;
            tmpss << "  show the top N task kinds sort by counter_name:" << std::endl;
            tmpss << "      p|P|profile|Profile task|t top $N $counter_name [$percentile]:" << std::endl;
            tmpss << "ARGUMENTS:" << std::endl;            
            tmpss << "  $percentile : e.g, 50 for latency at 50 percentile, 50(default)|90|95|99|999:" << std::endl;
            tmpss << "  $counter_name :" << std::endl;
            for (int i = 0; i < PREF_COUNTER_COUNT; i++)
            {
                tmpss << "      " << std::setw(data_width) << counter_info_ptr[i]->title << " :";
                for (size_t j = 0; j < counter_info_ptr[i]->keys.size(); j++)
                {
                    tmpss << " " << counter_info_ptr[i]->keys[j];
                }
                tmpss << std::endl;
            }
            tmpss << "  $task : all task code, such as" << std::endl;
            for (int i = 1; i < task_code::max_value() && i <= 10; i++)
            {
                tmpss << "      " << task_code::to_string(i) << std::endl;
            }
                        
            register_command({ "p", "P", "profile", "Profile"}, "profile|Profile|p|P - performance profiling", tmpss.str().c_str(), profiler_output_handler);
        }

        void profiler::install(service_spec& spec)
        {
            s_spec_profilers = new task_spec_profiler[task_code::max_value() + 1];
            task_ext_for_profiler::register_ext();
            message_ext_for_profiler::register_ext();
            dassert(sizeof(counter_info_ptr) / sizeof(counter_info*) == PREF_COUNTER_COUNT, "PREF COUNTER ERROR");

            auto profile = config()->get_value<bool>("task.default", "is_profile", false);
            auto collect_call_count = config()->get_value<bool>("task.default", "collect_call_count", true);

            for (int i = 0; i <= task_code::max_value(); i++)
            {
                if (i == TASK_CODE_INVALID)
                    continue;

                std::string name = std::string("task.") + std::string(task_code::to_string(i));
                task_spec* spec = task_spec::get(i);
                dassert(spec != nullptr, "task_spec cannot be null");

                s_spec_profilers[i].collect_call_count = config()->get_value<bool>(name.c_str(), "collect_call_count", collect_call_count);
                s_spec_profilers[i].call_counts = new std::atomic<int64_t>[task_code::max_value() + 1];

                s_spec_profilers[i].ptr[TASK_QUEUEING_TIME_NS] = dsn::utils::perf_counters::instance().get_counter((name + std::string(".queue(ns)")).c_str(), COUNTER_TYPE_NUMBER_PERCENTILES, true);
                s_spec_profilers[i].ptr[TASK_EXEC_TIME_NS] = dsn::utils::perf_counters::instance().get_counter((name + std::string(".exec(ns)")).c_str(), COUNTER_TYPE_NUMBER_PERCENTILES, true);
                s_spec_profilers[i].ptr[TASK_THROUGHPUT] = dsn::utils::perf_counters::instance().get_counter((name + std::string(".qps")).c_str(), COUNTER_TYPE_RATE, true);
                s_spec_profilers[i].ptr[TASK_CANCELLED] = dsn::utils::perf_counters::instance().get_counter((name + std::string(".cancelled#")).c_str(), COUNTER_TYPE_NUMBER, true);

                if (spec->type == task_type::TASK_TYPE_RPC_REQUEST)
                {
                    s_spec_profilers[i].ptr[RPC_SERVER_LATENCY_NS] = dsn::utils::perf_counters::instance().get_counter((name + std::string(".latency.server")).c_str(), COUNTER_TYPE_NUMBER_PERCENTILES, true);
                }
                else if (spec->type == task_type::TASK_TYPE_RPC_RESPONSE)
                {
                    s_spec_profilers[i].ptr[RPC_CLIENT_NON_TIMEOUT_LATENCY_NS] = dsn::utils::perf_counters::instance().get_counter((name + std::string(".latency.client(ns)")).c_str(), COUNTER_TYPE_NUMBER_PERCENTILES, true);
                    s_spec_profilers[i].ptr[RPC_CLIENT_TIMEOUT_THROUGHPUT] = dsn::utils::perf_counters::instance().get_counter((name + std::string(".timeout.qps")).c_str(), COUNTER_TYPE_RATE, true);
                }
                else if (spec->type == task_type::TASK_TYPE_AIO)
                {
                    s_spec_profilers[i].ptr[AIO_LATENCY_NS] = dsn::utils::perf_counters::instance().get_counter((name + std::string(".latency(ns)")).c_str(), COUNTER_TYPE_NUMBER_PERCENTILES, true);
                }

                s_spec_profilers[i].is_profile = config()->get_value<bool>(name.c_str(), "is_profile", profile);

                if (!s_spec_profilers[i].is_profile)
                    continue;

                spec->on_task_enqueue.put_back(profiler_on_task_enqueue, "profiler");
                spec->on_task_begin.put_back(profiler_on_task_begin, "profiler");
                spec->on_task_end.put_back(profiler_on_task_end, "profiler");
                spec->on_task_cancelled.put_back(profiler_on_task_cancelled, "profiler");
                //spec->on_task_wait_pre.put_back(profiler_on_task_wait_pre, "profiler");
                //spec->on_task_wait_post.put_back(profiler_on_task_wait_post, "profiler");
                //spec->on_task_cancel_post.put_back(profiler_on_task_cancel_post, "profiler");
                spec->on_aio_call.put_back(profiler_on_aio_call, "profiler");
                spec->on_aio_enqueue.put_back(profiler_on_aio_enqueue, "profiler");
                spec->on_rpc_call.put_back(profiler_on_rpc_call, "profiler");
                spec->on_rpc_request_enqueue.put_back(profiler_on_rpc_request_enqueue, "profiler");
                spec->on_rpc_create_response.put_back(profiler_on_rpc_create_response, "profiler");
                spec->on_rpc_reply.put_back(profiler_on_rpc_reply, "profiler");
                spec->on_rpc_response_enqueue.put_back(profiler_on_rpc_response_enqueue, "profiler");
            }

            register_command_profiler();
        }

        profiler::profiler(const char* name)
            : toollet(name)
        {
        }
    }
}
