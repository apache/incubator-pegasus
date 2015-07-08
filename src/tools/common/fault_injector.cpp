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

#include <dsn/toollet/fault_injector.h>
#include <dsn/service_api.h>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "toollet.fault_injector"

namespace dsn {
    namespace tools {

        struct fj_opt
        {
            bool            fault_injection_enabled;

            // io failure 
            double          rpc_request_drop_ratio;
            double          rpc_response_drop_ratio;
            double          disk_read_fail_ratio;
            double          disk_write_fail_ratio;

            // delay
            uint32_t        rpc_message_delay_ms_min;
            uint32_t        rpc_message_delay_ms_max;
            uint32_t        disk_io_delay_ms_min;
            uint32_t        disk_io_delay_ms_max;
            uint32_t        execution_extra_delay_us_max;
            
            // node crash
            uint32_t        node_crash_minutes_min;
            uint32_t        node_crash_minutes_max;
            uint32_t        node_crash_minutes_recover_min;
            uint32_t        node_crash_minutes_recover_max;
            bool            node_crashed;
        };

        CONFIG_BEGIN(fj_opt)
            CONFIG_FLD(bool, fault_injection_enabled, true)

            CONFIG_FLD(double, rpc_request_drop_ratio, 0.0001)
            CONFIG_FLD(double, rpc_response_drop_ratio, 0.001)
            CONFIG_FLD(double, disk_read_fail_ratio, 0.000001)
            CONFIG_FLD(double, disk_write_fail_ratio, 0.000001)

            CONFIG_FLD(uint32_t, rpc_message_delay_ms_min, 0)
            CONFIG_FLD(uint32_t, rpc_message_delay_ms_max, 1000)
            CONFIG_FLD(uint32_t, disk_io_delay_ms_min, 1)
            CONFIG_FLD(uint32_t, disk_io_delay_ms_max, 12)
            CONFIG_FLD(uint32_t, execution_extra_delay_us_max, 0)

            CONFIG_FLD(uint32_t, node_crash_minutes_min, 40)
            CONFIG_FLD(uint32_t, node_crash_minutes_max, 60)
            CONFIG_FLD(uint32_t, node_crash_minutes_recover_min, 1)
            CONFIG_FLD(uint32_t, node_crash_minutes_recover_max, 4)
            CONFIG_FLD(bool, node_crashed, false)
        CONFIG_END
        
        static fj_opt* s_fj_opts = nullptr;

        static void fault_on_task_enqueue(task* caller, task* callee)
        {
        }

        static void fault_on_task_begin(task* this_)
        {
            
        }

        static void fault_on_task_end(task* this_)
        {
            fj_opt& opt = s_fj_opts[this_->spec().code];
            if (opt.execution_extra_delay_us_max > 0)
            {
                auto d = service::env::random32(0, opt.execution_extra_delay_us_max);
                std::this_thread::sleep_for(std::chrono::microseconds(d));
            }
        }

        static void fault_on_task_cancelled(task* this_)
        {
        }

        static void fault_on_task_wait_pre(task* caller, task* callee, uint32_t timeout_ms)
        {

        }

        static void fault_on_task_wait_post(task* caller, task* callee, bool succ)
        {

        }

        static void fault_on_task_cancel_post(task* caller, task* callee, bool succ)
        {

        }

        // return true means continue, otherwise early terminate with task::set_error_code
        static bool fault_on_aio_call(task* caller, aio_task* callee)
        {
            switch (callee->aio()->type)
            {
            case AIO_Read:
                if (service::env::probability() < s_fj_opts[callee->spec().code].disk_read_fail_ratio)
                {
                    callee->set_error_code(ERR_FILE_OPERATION_FAILED);
                    return false;
                }
                break;
            case AIO_Write:
                if (service::env::probability() < s_fj_opts[callee->spec().code].disk_write_fail_ratio)
                {
                    callee->set_error_code(ERR_FILE_OPERATION_FAILED);
                    return false;
                }
                break;
            }
            
            return true;
        }

        static void fault_on_aio_enqueue(aio_task* this_)
        {
            fj_opt& opt = s_fj_opts[this_->spec().code];
            this_->set_delay(service::env::random32(opt.disk_io_delay_ms_min, opt.disk_io_delay_ms_max));
        }

        // return true means continue, otherwise early terminate with task::set_error_code
        static bool fault_on_rpc_call(task* caller, message* req, rpc_response_task* callee)
        {
            fj_opt& opt = s_fj_opts[req->header().local_rpc_code];
            if (service::env::probability() < opt.rpc_request_drop_ratio)
            {
                return false;
            }
            else
            {
                return true;
            }
        }

        static void fault_on_rpc_request_enqueue(rpc_request_task* callee)
        {
            fj_opt& opt = s_fj_opts[callee->spec().code];
            callee->set_delay(service::env::random32(opt.rpc_message_delay_ms_min, opt.rpc_message_delay_ms_max));
        }

        // return true means continue, otherwise early terminate with task::set_error_code
        static bool fault_on_rpc_reply(task* caller, message* msg)
        {
            fj_opt& opt = s_fj_opts[msg->header().local_rpc_code];
            if (service::env::probability() < opt.rpc_response_drop_ratio)
            {
                return false;
            }
            else
            {
                return true;
            }
        }

        static void fault_on_rpc_response_enqueue(rpc_response_task* resp)
        {
            fj_opt& opt = s_fj_opts[resp->spec().code];
            resp->set_delay(service::env::random32(opt.rpc_message_delay_ms_min, opt.rpc_message_delay_ms_max));
        }

        void fault_injector::install(service_spec& spec)
        {
            s_fj_opts = new fj_opt[task_code::max_value() + 1];
            auto cf = config();
            fj_opt default_opt;
            read_config(cf, "task.default", default_opt);

            for (int i = 0; i <= task_code::max_value(); i++)
            {
                if (i == TASK_CODE_INVALID)
                    continue;

                std::string section_name = std::string("task.") + std::string(task_code::to_string(i));
                task_spec* spec = task_spec::get(i);
                dassert (spec != nullptr, "task_spec cannot be null");

                fj_opt& lopt = s_fj_opts[i];
                read_config(cf, section_name.c_str(), lopt, &default_opt);

                if (!lopt.fault_injection_enabled)
                    continue;
                
                //spec->on_task_enqueue.put_back(fault_on_task_enqueue, "fault_injector");
                //spec->on_task_begin.put_back(fault_on_task_begin, "fault_injector");
                spec->on_task_end.put_back(fault_on_task_end, "fault_injector");
                //spec->on_task_cancelled.put_back(fault_on_task_cancelled, "fault_injector");
                //spec->on_task_wait_pre.put_back(fault_on_task_wait_pre, "fault_injector");
                //spec->on_task_wait_post.put_back(fault_on_task_wait_post, "fault_injector");
                //spec->on_task_cancel_post.put_back(fault_on_task_cancel_post, "fault_injector");
                spec->on_aio_call.put_native(fault_on_aio_call);
                spec->on_aio_enqueue.put_back(fault_on_aio_enqueue, "fault_injector");
                spec->on_rpc_call.put_native(fault_on_rpc_call);
                spec->on_rpc_request_enqueue.put_back(fault_on_rpc_request_enqueue, "fault_injector");
                spec->on_rpc_reply.put_native(fault_on_rpc_reply);
                spec->on_rpc_response_enqueue.put_back(fault_on_rpc_response_enqueue, "fault_injector");
            }

            if (default_opt.node_crash_minutes_max > 0)
            {
                // TODO:
            }
        }

        fault_injector::fault_injector(const char* name)
            : toollet(name)
        {
        }
    }
}
