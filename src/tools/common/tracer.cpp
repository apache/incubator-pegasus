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

#include <dsn/toollet/tracer.h>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "toollet.tracer"

namespace dsn {
    namespace tools {

        static void tracer_on_task_enqueue(task* caller, task* callee)
        {
            ddebug("%s ENQUEUE, task_id = %016llx",
                callee->spec().name,
                callee->id()
                );
        }

        static void tracer_on_task_begin(task* this_)
        {
            switch (this_->spec().type)
            {
            case task_type::TASK_TYPE_COMPUTE:
            case task_type::TASK_TYPE_AIO:
                ddebug("%s EXEC BEGIN, task_id = %016llx",
                    this_->spec().name,
                    this_->id()
                    );
                break;
            case task_type::TASK_TYPE_RPC_REQUEST:
            {
                auto tsk = (rpc_request_task*)this_;
                ddebug("%s EXEC BEGIN, task_id = %016llx, %s:%d => %s:%d, rpc_id = %016llx",
                    this_->spec().name,
                    this_->id(),
                    tsk->get_request()->header().from_address.name.c_str(),
                    static_cast<int>(tsk->get_request()->header().from_address.port),
                    tsk->get_request()->header().to_address.name.c_str(),
                    static_cast<int>(tsk->get_request()->header().to_address.port),
                    tsk->get_request()->header().rpc_id
                    );
            }
                break;
            case task_type::TASK_TYPE_RPC_RESPONSE:
            {
                auto tsk = (rpc_response_task*)this_;
                ddebug("%s EXEC BEGIN, task_id = %016llx, %s:%d => %s:%d, rpc_id = %016llx",
                    this_->spec().name,
                    this_->id(),
                    tsk->get_request()->header().to_address.name.c_str(),
                    static_cast<int>(tsk->get_request()->header().to_address.port),
                    tsk->get_request()->header().from_address.name.c_str(),
                    static_cast<int>(tsk->get_request()->header().from_address.port),
                    tsk->get_request()->header().rpc_id
                    );
            }
                break;
            }
        }

        static void tracer_on_task_end(task* this_)
        {
            ddebug("%s EXEC END, task_id = %016llx, err = %s",
                this_->spec().name,
                this_->id(),
                this_->error().to_string()
                );
        }

        static void tracer_on_task_cancelled(task* this_)
        {
            ddebug("%s CANCELLED, task_id = %016llx",
                this_->spec().name,
                this_->id()
                );
        }

        static void tracer_on_task_wait_pre(task* caller, task* callee, uint32_t timeout_ms)
        {

        }

        static void tracer_on_task_wait_post(task* caller, task* callee, bool succ)
        {

        }

        static void tracer_on_task_cancel_post(task* caller, task* callee, bool succ)
        {

        }    

        // return true means continue, otherwise early terminate with task::set_error_code
        static void tracer_on_aio_call(task* caller, aio_task* callee)
        {
            ddebug("%s AIO.CALL, task_id = %016llx",
                callee->spec().name,
                callee->id()
                );
        }

        static void tracer_on_aio_enqueue(aio_task* this_)
        {
            ddebug("%s AIO.ENQUEUE, task_id = %016llx",
                this_->spec().name,
                this_->id()
                );
        }

        // return true means continue, otherwise early terminate with task::set_error_code
        static void tracer_on_rpc_call(task* caller, message* req, rpc_response_task* callee)
        {
            message_header& hdr = req->header();
            ddebug(
                "%s RPC.CALL: %s:%d => %s:%d, rpc_id = %016llx, callback_task = %016llx, timeout = %d ms",
                hdr.rpc_name,
                hdr.from_address.name.c_str(),
                static_cast<int>(hdr.from_address.port),
                hdr.to_address.name.c_str(),
                static_cast<int>(hdr.to_address.port),
                hdr.rpc_id,
                callee ? callee->id() : 0,
                hdr.client.timeout_ms
                );
        }

        static void tracer_on_rpc_request_enqueue(rpc_request_task* callee)
        {
            ddebug("%s RPC.REQUEST.ENQUEUE, task_id = %016llx, %s:%d => %s:%d, rpc_id = %016llx",
                callee->spec().name,
                callee->id(),
                callee->get_request()->header().from_address.name.c_str(),
                static_cast<int>(callee->get_request()->header().from_address.port),
                callee->get_request()->header().to_address.name.c_str(),
                static_cast<int>(callee->get_request()->header().to_address.port),
                callee->get_request()->header().rpc_id
                );
        }

        // return true means continue, otherwise early terminate with task::set_error_code
        static void tracer_on_rpc_reply(task* caller, message* msg)
        {
            message_header& hdr = msg->header();

            ddebug(
                "%s RPC.REPLY: %s:%d => %s:%d, rpc_id = %016llx",
                hdr.rpc_name,
                hdr.from_address.name.c_str(),
                static_cast<int>(hdr.from_address.port),
                hdr.to_address.name.c_str(),
                static_cast<int>(hdr.to_address.port),
                hdr.rpc_id
                );
        }

        static void tracer_on_rpc_response_enqueue(rpc_response_task* resp)
        {
            ddebug("%s RPC.RESPONSE.ENQUEUE, task_id = %016llx, %s:%d => %s:%d, rpc_id = %016llx",
                resp->spec().name,
                resp->id(),
                resp->get_request()->header().to_address.name.c_str(),
                static_cast<int>(resp->get_request()->header().to_address.port),
                resp->get_request()->header().from_address.name.c_str(),
                static_cast<int>(resp->get_request()->header().from_address.port),
                resp->get_request()->header().rpc_id
                );
        }

        void tracer::install(service_spec& spec)
        {
            auto trace = config()->get_value<bool>("task.default", "is_trace", false);

            for (int i = 0; i <= task_code::max_value(); i++)
            {
                if (i == TASK_CODE_INVALID)
                    continue;

                std::string section_name = std::string("task.") + std::string(task_code::to_string(i));
                task_spec* spec = task_spec::get(i);
                dassert (spec != nullptr, "task_spec cannot be null");

                if (!config()->get_value<bool>(section_name.c_str(), "is_trace", trace))
                    continue;

                if (config()->get_value<bool>(section_name.c_str(), "tracer::on_task_enqueue", true))
                    spec->on_task_enqueue.put_back(tracer_on_task_enqueue, "tracer");

                if (config()->get_value<bool>(section_name.c_str(), "tracer::on_task_begin", true))
                    spec->on_task_begin.put_back(tracer_on_task_begin, "tracer");

                if (config()->get_value<bool>(section_name.c_str(), "tracer::on_task_end", true))
                    spec->on_task_end.put_back(tracer_on_task_end, "tracer");

                //if (config()->get_value<bool>(section_name.c_str(), "tracer::on_task_cancelled", true))
                //    spec->on_task_cancelled.put_back(tracer_on_task_cancelled, "tracer");

                //if (config()->get_value<bool>(section_name.c_str(), "tracer::on_task_wait_pre", true))
                    //spec->on_task_wait_pre.put_back(tracer_on_task_wait_pre, "tracer");

                //if (config()->get_value<bool>(section_name.c_str(), "tracer::on_task_wait_post", true))
                    //spec->on_task_wait_post.put_back(tracer_on_task_wait_post, "tracer");

                //if (config()->get_value<bool>(section_name.c_str(), "tracer::on_task_cancel_post", true))
                    //spec->on_task_cancel_post.put_back(tracer_on_task_cancel_post, "tracer");

                if (config()->get_value<bool>(section_name.c_str(), "tracer::on_aio_call", true))
                    spec->on_aio_call.put_back(tracer_on_aio_call, "tracer");

                if (config()->get_value<bool>(section_name.c_str(), "tracer::on_aio_enqueue", true))
                    spec->on_aio_enqueue.put_back(tracer_on_aio_enqueue, "tracer");

                if (config()->get_value<bool>(section_name.c_str(), "tracer::on_rpc_call", true))
                    spec->on_rpc_call.put_back(tracer_on_rpc_call, "tracer");

                if (config()->get_value<bool>(section_name.c_str(), "tracer::on_rpc_request_enqueue", true))
                    spec->on_rpc_request_enqueue.put_back(tracer_on_rpc_request_enqueue, "tracer");

                if (config()->get_value<bool>(section_name.c_str(), "tracer::on_rpc_reply", true))
                    spec->on_rpc_reply.put_back(tracer_on_rpc_reply, "tracer");

                if (config()->get_value<bool>(section_name.c_str(), "tracer::on_rpc_response_enqueue", true))
                    spec->on_rpc_response_enqueue.put_back(tracer_on_rpc_response_enqueue, "tracer");
            }
        }

        tracer::tracer(const char* name)
            : toollet(name)
        {
        }
    }
}
