/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation

 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.

 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#include <dsn/toollet/profiler.h>
#include <dsn/service_api.h>

#define __TITLE__ "toollet.profiler"

namespace dsn {
    namespace tools {

        struct task_spec_profiler
        {
            perf_counter_ptr task_queueing_time_ns;
            perf_counter_ptr task_exec_time_ns;
            perf_counter_ptr task_throughput;
            perf_counter_ptr task_cancelled;

            perf_counter_ptr aio_latency_ns; // for AIO only, from aio call to aio callback task enqueued            
            perf_counter_ptr rpc_server_latency_ns; // for RPC_RESQUEST ONLY, from rpc request enqueue to rpc response
            perf_counter_ptr rpc_client_non_timeout_latency_ns; // for RPC_RESPONSE ONLY, from rpc call to rpc response enqueued 
            perf_counter_ptr rpc_client_timeout_throughput;
        };

        static task_spec_profiler* s_spec_profilers = nullptr;
        
        typedef uint64_extension_helper<task> task_ext_for_profiler;
        typedef uint64_extension_helper<message> message_ext_for_profiler;

        static void profiler_on_task_enqueue(task* caller, task* callee)
        {
            task_ext_for_profiler::get(callee) = ::dsn::service::env::now_ns();
        }

        static void profiler_on_task_begin(task* this_)
        {
            uint64_t& qts = task_ext_for_profiler::get(this_);
            uint64_t now = ::dsn::service::env::now_ns();
            s_spec_profilers[this_->spec().code].task_queueing_time_ns->set(now - qts);
            qts = now;
        }

        static void profiler_on_task_end(task* this_)
        {
            uint64_t qts = task_ext_for_profiler::get(this_);
            uint64_t now = ::dsn::service::env::now_ns();
            s_spec_profilers[this_->spec().code].task_exec_time_ns->set(now - qts);
            s_spec_profilers[this_->spec().code].task_throughput->increment();
        }

        static void profiler_on_task_cancelled(task* this_)
        {
            s_spec_profilers[this_->spec().code].task_cancelled->increment();
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
            // time disk io starts
            task_ext_for_profiler::get(callee) = ::dsn::service::env::now_ns();
        }

        static void profiler_on_aio_enqueue(aio_task* this_)
        {
            uint64_t& ats = task_ext_for_profiler::get(this_);
            uint64_t now = ::dsn::service::env::now_ns();

            s_spec_profilers[this_->spec().code].aio_latency_ns->set(now - ats);
            ats = now;
        }

        // return true means continue, otherwise early terminate with task::set_error_code
        static void profiler_on_rpc_call(task* caller, message* req, rpc_response_task* callee)
        {
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

        // return true means continue, otherwise early terminate with task::set_error_code
        static void profiler_on_rpc_reply(task* caller, message* msg)
        {
            uint64_t qts = message_ext_for_profiler::get(msg);
            uint64_t now = ::dsn::service::env::now_ns();
            s_spec_profilers[msg->header().local_rpc_code].rpc_server_latency_ns->set(now - qts);
        }

        static void profiler_on_rpc_response_enqueue(rpc_response_task* resp)
        {
            uint64_t& cts = task_ext_for_profiler::get(resp);
            uint64_t now = ::dsn::service::env::now_ns();
            if (resp->get_response() != nullptr)
            {
                s_spec_profilers[resp->spec().code].rpc_client_non_timeout_latency_ns->set(now - cts);
            }
            else
            {
                s_spec_profilers[resp->spec().code].rpc_client_timeout_throughput->increment();
            }
            cts = now;
        }

        void profiler::install(service_spec& spec)
        {
            s_spec_profilers = new task_spec_profiler[task_code::max_value()+1];
            task_ext_for_profiler::register_ext();
            message_ext_for_profiler::register_ext();

            auto profile = _configuration->get_value<bool>("task.default", "is_profile", false);
            
            for (int i = 0; i <= task_code::max_value(); i++)
            {
                if (i == TASK_CODE_INVALID)
                    continue;

                std::string name = std::string("task.") + std::string(task_code::to_string(i));
                task_spec* spec = task_spec::get(i);
                dassert (spec != nullptr, "task_spec cannot be null");

                s_spec_profilers[i].task_queueing_time_ns = dsn::utils::perf_counters::instance().get_counter((name + std::string(".queue(ns)")).c_str(), COUNTER_TYPE_NUMBER_PERCENTILES, true);
                s_spec_profilers[i].task_exec_time_ns = dsn::utils::perf_counters::instance().get_counter((name + std::string(".exec(ns)")).c_str(), COUNTER_TYPE_NUMBER_PERCENTILES, true);
                s_spec_profilers[i].task_throughput = dsn::utils::perf_counters::instance().get_counter((name + std::string(".qps")).c_str(), COUNTER_TYPE_RATE, true);
                s_spec_profilers[i].task_cancelled = dsn::utils::perf_counters::instance().get_counter((name + std::string(".cancelled#")).c_str(), COUNTER_TYPE_NUMBER, true);

                if (spec->type == task_type::TASK_TYPE_RPC_REQUEST)
                {
                    s_spec_profilers[i].rpc_server_latency_ns = dsn::utils::perf_counters::instance().get_counter((name + std::string(".latency.server")).c_str(), COUNTER_TYPE_NUMBER_PERCENTILES, true);
                }
                else if (spec->type == task_type::TASK_TYPE_RPC_RESPONSE)
                {
                    s_spec_profilers[i].rpc_client_non_timeout_latency_ns = dsn::utils::perf_counters::instance().get_counter((name + std::string(".latency.client(ns)")).c_str(), COUNTER_TYPE_NUMBER_PERCENTILES, true);
                    s_spec_profilers[i].rpc_client_timeout_throughput = dsn::utils::perf_counters::instance().get_counter((name + std::string(".timeout.qps")).c_str(), COUNTER_TYPE_RATE, true);                    
                }
                else if (spec->type == task_type::TASK_TYPE_AIO)
                {
                    s_spec_profilers[i].aio_latency_ns = dsn::utils::perf_counters::instance().get_counter((name + std::string(".latency(ns)")).c_str(), COUNTER_TYPE_NUMBER_PERCENTILES, true);
                }

                if (!_configuration->get_value<bool>(name.c_str(), "is_profile", profile))
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
                spec->on_rpc_reply.put_back(profiler_on_rpc_reply, "profiler");
                spec->on_rpc_response_enqueue.put_back(profiler_on_rpc_response_enqueue, "profiler");
            }

            // TODO: profiling on overall rpc/network/disk io. 
        }

        profiler::profiler(const char* name, configuration_ptr config)
            : toollet(name, config)
        {
        }
    }
}
