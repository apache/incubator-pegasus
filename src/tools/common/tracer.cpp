
#include <rdsn/toollet/tracer.h>

#define __TITLE__ "toollet.tracer"

namespace rdsn {
    namespace tools {

        static void tracer_on_task_enqueue(task* caller, task* callee)
        {
            rdsn_debug("ENQUEUE %s, task_id = %016llx",
                callee->spec().name,
                callee->id()
                );
        }

        static void tracer_on_task_begin(task* this_)
        {
            rdsn_debug("EXEC %s BEGIN, task_id = %016llx",
                this_->spec().name,
                this_->id()
                );
        }

        static void tracer_on_task_end(task* this_)
        {
            switch (this_->spec().type)
            {
            case task_type::TASK_TYPE_COMPUTE:
            case task_type::TASK_TYPE_AIO:
                rdsn_debug("EXEC %s END, task_id = %016llx, err = %s",
                    this_->spec().name,
                    this_->id(),
                    this_->error().to_string()
                    );
                break;
            case task_type::TASK_TYPE_RPC_REQUEST:
            {
                auto tsk = (rpc_request_task*)this_;
                rdsn_debug("EXEC %s, task_id = %016llx, err = %s, %s:%u => %s:%u, rpc_id = %016llx",
                    this_->spec().name,
                    this_->id(),
                    this_->error().to_string(),
                    tsk->get_request()->header().from_address.name.c_str(),
                    (int)tsk->get_request()->header().from_address.port,
                    tsk->get_request()->header().to_address.name.c_str(),
                    (int)tsk->get_request()->header().to_address.port,
                    tsk->get_request()->header().rpc_id
                    );
            }
                break;
            case task_type::TASK_TYPE_RPC_RESPONSE:
            {
                auto tsk = (rpc_response_task*)this_;
                rdsn_debug("EXEC %s, task_id = %016llx, err = %s, %s:%u => %s:%u, rpc_id = %016llx",
                    this_->spec().name,
                    this_->id(),
                    this_->error().to_string(),
                    tsk->get_request()->header().to_address.name.c_str(),
                    (int)tsk->get_request()->header().to_address.port,
                    tsk->get_request()->header().from_address.name.c_str(),
                    (int)tsk->get_request()->header().from_address.port,
                    tsk->get_request()->header().rpc_id
                    );
            }
                break;
            }
        }

        static void tracer_on_task_cancelled(task* this_)
        {
            rdsn_debug("CANCELLED %s, task_id = %016llx",
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
            rdsn_debug("AIO.CALL %s, task_id = %016llx",
                callee->spec().name,
                callee->id()
                );
        }

        static void tracer_on_aio_enqueue(aio_task* this_)
        {
            rdsn_debug("AIO.ENQUEUE %s, task_id = %016llx",
                this_->spec().name,
                this_->id()
                );
        }

        // return true means continue, otherwise early terminate with task::set_error_code
        static void tracer_on_rpc_call(task* caller, message* req, rpc_response_task* callee)
        {
            message_header& hdr = req->header();
            rdsn_debug(
                "RPC.CALL %s: %s:%u => %s:%u, rpc_id = %016llx, timeout_task = %016llx, timeout = %ums",
                hdr.rpc_name,
                hdr.from_address.name.c_str(),
                (int)hdr.from_address.port,
                hdr.to_address.name.c_str(),
                (int)hdr.to_address.port,
                hdr.rpc_id,
                callee ? callee->id() : 0,
                hdr.timeout_milliseconds
                );
        }

        static void tracer_on_rpc_request_enqueue(rpc_request_task* callee)
        {
            rdsn_debug("RPC.REQUEST.ENQUEUE %s, task_id = %016llx, %s:%u => %s:%u, rpc_id = %016llx",
                callee->spec().name,
                callee->id(),
                callee->get_request()->header().from_address.name.c_str(),
                (int)callee->get_request()->header().from_address.port,
                callee->get_request()->header().to_address.name.c_str(),
                (int)callee->get_request()->header().to_address.port,
                callee->get_request()->header().rpc_id
                );
        }

        // return true means continue, otherwise early terminate with task::set_error_code
        static void tracer_on_rpc_reply(task* caller, message* msg)
        {
            message_header& hdr = msg->header();

            rdsn_debug(
                "RPC.REPLY %s: %s:%u => %s:%u, rpc_id = %016llx",
                hdr.rpc_name,
                hdr.from_address.name.c_str(),
                (int)hdr.from_address.port,
                hdr.to_address.name.c_str(),
                (int)hdr.to_address.port,
                hdr.rpc_id
                );
        }

        static void tracer_on_rpc_response_enqueue(rpc_response_task* resp)
        {
            rdsn_debug("RPC.RESPONSE.ENQUEUE %s, task_id = %016llx, %s:%u => %s:%u, rpc_id = %016llx",
                resp->spec().name,
                resp->id(),
                resp->get_request()->header().to_address.name.c_str(),
                (int)resp->get_request()->header().to_address.port,
                resp->get_request()->header().from_address.name.c_str(),
                (int)resp->get_request()->header().from_address.port,
                resp->get_request()->header().rpc_id
                );
        }

        void tracer::install(service_spec& spec)
        {
            auto trace = _configuration->get_value<bool>("task.default", "is_trace", false);

            for (int i = 0; i <= task_code::max_value(); i++)
            {
                if (i == TASK_CODE_INVALID)
                    continue;

                std::string section_name = std::string("task.") + std::string(task_code::to_string(i));
                task_spec* spec = task_spec::get(i);
                rdsn_assert(spec != nullptr, "task_spec cannot be null");

                if (!_configuration->get_value<bool>(section_name.c_str(), "is_trace", trace))
                    continue;

                if (_configuration->get_value<bool>(section_name.c_str(), "tracer::on_task_enqueue", true))
                    spec->on_task_enqueue.put_back(tracer_on_task_enqueue, "tracer");

                if (_configuration->get_value<bool>(section_name.c_str(), "tracer::on_task_begin", true))
                    spec->on_task_begin.put_back(tracer_on_task_begin, "tracer");

                if (_configuration->get_value<bool>(section_name.c_str(), "tracer::on_task_end", true))
                    spec->on_task_end.put_back(tracer_on_task_end, "tracer");

                if (_configuration->get_value<bool>(section_name.c_str(), "tracer::on_task_cancelled", true))
                    spec->on_task_cancelled.put_back(tracer_on_task_cancelled, "tracer");

                //if (_configuration->get_value<bool>(section_name.c_str(), "tracer::on_task_wait_pre", true))
                    //spec->on_task_wait_pre.put_back(tracer_on_task_wait_pre, "tracer");

                //if (_configuration->get_value<bool>(section_name.c_str(), "tracer::on_task_wait_post", true))
                    //spec->on_task_wait_post.put_back(tracer_on_task_wait_post, "tracer");

                //if (_configuration->get_value<bool>(section_name.c_str(), "tracer::on_task_cancel_post", true))
                    //spec->on_task_cancel_post.put_back(tracer_on_task_cancel_post, "tracer");

                if (_configuration->get_value<bool>(section_name.c_str(), "tracer::on_aio_call", true))
                    spec->on_aio_call.put_back(tracer_on_aio_call, "tracer");

                if (_configuration->get_value<bool>(section_name.c_str(), "tracer::on_aio_enqueue", true))
                    spec->on_aio_enqueue.put_back(tracer_on_aio_enqueue, "tracer");

                if (_configuration->get_value<bool>(section_name.c_str(), "tracer::on_rpc_call", true))
                    spec->on_rpc_call.put_back(tracer_on_rpc_call, "tracer");

                if (_configuration->get_value<bool>(section_name.c_str(), "tracer::on_rpc_request_enqueue", true))
                    spec->on_rpc_request_enqueue.put_back(tracer_on_rpc_request_enqueue, "tracer");

                if (_configuration->get_value<bool>(section_name.c_str(), "tracer::on_rpc_reply", true))
                    spec->on_rpc_reply.put_back(tracer_on_rpc_reply, "tracer");

                if (_configuration->get_value<bool>(section_name.c_str(), "tracer::on_rpc_response_enqueue", true))
                    spec->on_rpc_response_enqueue.put_back(tracer_on_rpc_response_enqueue, "tracer");
            }
        }

        tracer::tracer(const char* name, configuration_ptr config)
            : toollet(name, config)
        {
        }
    }
}