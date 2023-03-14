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

#include "runtime/tracer.h"

// IWYU pragma: no_include <ext/alloc_traits.h>
#include <inttypes.h>
#include <stdio.h>
#include <mutex>
#include <string>
#include <vector>

#include "aio/aio_task.h"
#include "fmt/format.h"
#include "runtime/global_config.h"
#include "runtime/rpc/rpc_message.h"
#include "runtime/task/task.h"
#include "runtime/task/task_code.h"
#include "runtime/task/task_spec.h"
#include "utils/command_manager.h"
#include "utils/config_api.h"
#include "utils/enum_helper.h"
#include "utils/filesystem.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/join_point.h"

namespace dsn {
namespace tools {

DSN_DEFINE_bool(task..default, is_trace, false, "whether to trace tasks by default");

static void tracer_on_task_create(task *caller, task *callee)
{
    dsn_task_type_t type = callee->spec().type;
    if (TASK_TYPE_RPC_REQUEST == type) {
        rpc_request_task *tsk = (rpc_request_task *)callee;
        LOG_INFO("{} CREATE, task_id = {:#018x}, type = {}, rpc_name = {}, trace_id = {:#018x}",
                 callee->spec().name,
                 callee->id(),
                 enum_to_string(type),
                 tsk->get_request()->header->rpc_name,
                 tsk->get_request()->header->trace_id);
    } else if (TASK_TYPE_RPC_RESPONSE == type) {
        rpc_response_task *tsk = (rpc_response_task *)callee;
        LOG_INFO("{} CREATE, task_id = {:#018x}, type = {}, rpc_name = {}, trace_id = {:#018x}",
                 callee->spec().name,
                 callee->id(),
                 enum_to_string(type),
                 tsk->get_request()->header->rpc_name,
                 tsk->get_request()->header->trace_id);
    } else {
        LOG_INFO("{} CREATE, task_id = {:#018x}, type = {}",
                 callee->spec().name,
                 callee->id(),
                 enum_to_string(type));
    }
}

static void tracer_on_task_enqueue(task *caller, task *callee)
{
    LOG_INFO("{} ENQUEUE, task_id = {:#018x}, delay = {} ms, queue size = {}",
             callee->spec().name,
             callee->id(),
             callee->delay_milliseconds(),
             tls_dsn.last_worker_queue_size);
}

static void tracer_on_task_begin(task *this_)
{
    switch (this_->spec().type) {
    case dsn_task_type_t::TASK_TYPE_COMPUTE:
    case dsn_task_type_t::TASK_TYPE_AIO:
        LOG_INFO("{} EXEC BEGIN, task_id = {:#018x}", this_->spec().name, this_->id());
        break;
    case dsn_task_type_t::TASK_TYPE_RPC_REQUEST: {
        rpc_request_task *tsk = (rpc_request_task *)this_;
        LOG_INFO("{} EXEC BEGIN, task_id = {:#018x}, {} => {}, trace_id = {:#018x}",
                 this_->spec().name,
                 this_->id(),
                 tsk->get_request()->header->from_address,
                 tsk->get_request()->to_address,
                 tsk->get_request()->header->trace_id);
    } break;
    case dsn_task_type_t::TASK_TYPE_RPC_RESPONSE: {
        rpc_response_task *tsk = (rpc_response_task *)this_;
        LOG_INFO("{} EXEC BEGIN, task_id = {:#018x}, {} => {}, trace_id = {:#018x}",
                 this_->spec().name,
                 this_->id(),
                 tsk->get_request()->to_address,
                 tsk->get_request()->header->from_address,
                 tsk->get_request()->header->trace_id);
    } break;
    default:
        break;
    }
}

static void tracer_on_task_end(task *this_)
{
    LOG_INFO("{} EXEC END, task_id = {:#018x}, err = {}",
             this_->spec().name,
             this_->id(),
             this_->error());
}

static void tracer_on_task_cancelled(task *this_)
{
    LOG_INFO("{} CANCELLED, task_id = {:#018x}", this_->spec().name, this_->id());
}

static void tracer_on_task_wait_pre(task *caller, task *callee, uint32_t timeout_ms) {}

static void tracer_on_task_wait_post(task *caller, task *callee, bool succ) {}

static void tracer_on_task_cancel_post(task *caller, task *callee, bool succ) {}

// return true means continue, otherwise early terminate with task::set_error_code
static void tracer_on_aio_call(task *caller, aio_task *callee)
{
    LOG_INFO("{} AIO.CALL, task_id = {:#018x}, offset = {}, size = {}",
             callee->spec().name,
             callee->id(),
             callee->get_aio_context()->file_offset,
             callee->get_aio_context()->buffer_size);
}

static void tracer_on_aio_enqueue(aio_task *this_)
{
    LOG_INFO("{} AIO.ENQUEUE, task_id = {:#018x}, queue size = {}",
             this_->spec().name,
             this_->id(),
             tls_dsn.last_worker_queue_size);
}

// return true means continue, otherwise early terminate with task::set_error_code
static void tracer_on_rpc_call(task *caller, message_ex *req, rpc_response_task *callee)
{
    message_header &hdr = *req->header;
    LOG_INFO(
        "{} RPC.CALL: {} => {}, trace_id = {:#018x}, callback_task = {:#018x}, timeout = {} ms",
        hdr.rpc_name,
        req->header->from_address,
        req->to_address,
        hdr.trace_id,
        callee ? callee->id() : 0,
        hdr.client.timeout_ms);
}

static void tracer_on_rpc_request_enqueue(rpc_request_task *callee)
{
    LOG_INFO("{} RPC.REQUEST.ENQUEUE ({}), task_id = {:#018x}, {} => {}, trace_id = {:#018x}, "
             "queue size = {}",
             callee->spec().name,
             fmt::ptr(callee),
             callee->id(),
             callee->get_request()->header->from_address,
             callee->get_request()->to_address,
             callee->get_request()->header->trace_id,
             tls_dsn.last_worker_queue_size);
}

// return true means continue, otherwise early terminate with task::set_error_code
static void tracer_on_rpc_reply(task *caller, message_ex *msg)
{
    message_header &hdr = *msg->header;

    LOG_INFO("{} RPC.REPLY: {} => {}, trace_id = {:#018x}",
             hdr.rpc_name,
             msg->header->from_address,
             msg->to_address,
             hdr.trace_id);
}

static void tracer_on_rpc_response_enqueue(rpc_response_task *resp)
{
    LOG_INFO("{} RPC.RESPONSE.ENQUEUE, task_id = {:#018x}, {} => {}, trace_id = {:#018x}, queue "
             "size = {}",
             resp->spec().name,
             resp->id(),
             resp->get_request()->to_address,
             resp->get_request()->header->from_address,
             resp->get_request()->header->trace_id,
             tls_dsn.last_worker_queue_size);
}

static void tracer_on_rpc_create_response(message_ex *req, message_ex *resp)
{
    LOG_INFO("{} RPC.CREATE.RESPONSE, trace_id = {:#018x}",
             resp->header->rpc_name,
             resp->header->trace_id);
}

enum logged_event_t
{
    LET_TASK_BEGIN,
    LET_TASK_END,
    LET_LOG,
    LET_RPC_CALL,
    LET_RPC_REPLY,
    LET_AIO_CALL,
    LET_LPC_CALL,

    LET_INVALID
};

ENUM_BEGIN(logged_event_t, LET_INVALID)
ENUM_REG(LET_TASK_BEGIN)
ENUM_REG(LET_TASK_END)
ENUM_REG(LET_RPC_CALL)
ENUM_REG(LET_RPC_REPLY)
ENUM_REG(LET_AIO_CALL)
ENUM_REG(LET_LPC_CALL)
ENUM_END(logged_event_t)

struct logged_event
{
    uint64_t ts;
    logged_event_t event_type;
    uint64_t correlation_id; // task or rpc
    std::string context;     // log or rpc address
};

struct logged_task
{
    uint64_t task_id;
    uint64_t trace_id; // if present

    std::vector<logged_event> events;
};

static std::string tracer_log_flow_error(const char *msg)
{
    return std::string("invalid arguments for tracer.find: ") + msg;
}

static std::string tracer_log_flow(const std::vector<std::string> &args)
{
    // forward|f|backward|b rpc|r|task|t trace_id|task_id(e.g., 002a003920302390)
    // log_file_name(log.xx.txt)
    if (args.size() < 4) {
        return tracer_log_flow_error("not enough arguments");
    }

    // TODO: implement this
    if (args[0] == "forward" || args[0] == "f") {
    } else if (args[0] == "backward" || args[0] == "b") {
    } else {
        return tracer_log_flow_error("invalid direction argument - must be forward|f|backward|b");
    }

    // TODO: implement this
    if (args[1] == "rpc" || args[1] == "r") {
    } else if (args[1] == "task" || args[1] == "t") {
    } else {
        return tracer_log_flow_error("invalid id type argument - must be rpc|r|task|t");
    }

    uint64_t xid = 0;
    sscanf(args[2].c_str(), "%016" PRIx64, &xid);
    if (xid == 0) {
        return tracer_log_flow_error("invalid id value - must be with 016" PRIx64 " format");
    }

    std::string log_dir = utils::filesystem::path_combine(tools::spec().data_dir, "logs");

    std::string fpath = utils::filesystem::path_combine(log_dir, args[3]);

    if (!utils::filesystem::file_exists(fpath)) {
        return tracer_log_flow_error((fpath + " not exist").c_str());
    }

    return "Not implemented";
}

void tracer::install(service_spec &spec)
{
    for (int i = 0; i <= dsn::task_code::max(); i++) {
        if (i == TASK_CODE_INVALID)
            continue;

        std::string section_name =
            std::string("task.") + std::string(dsn::task_code(i).to_string());
        task_spec *spec = task_spec::get(i);
        CHECK_NOTNULL(spec, "");

        if (!dsn_config_get_value_bool(section_name.c_str(),
                                       "is_trace",
                                       FLAGS_is_trace,
                                       "whether to trace this kind of task"))
            continue;

        if (dsn_config_get_value_bool(section_name.c_str(),
                                      "tracer::on_task_create",
                                      true,
                                      "whether to trace when a task is created"))
            spec->on_task_create.put_back(tracer_on_task_create, "tracer");

        if (dsn_config_get_value_bool(section_name.c_str(),
                                      "tracer::on_task_enqueue",
                                      true,
                                      "whether to trace when a timer or async task is enqueued"))
            spec->on_task_enqueue.put_back(tracer_on_task_enqueue, "tracer");

        if (dsn_config_get_value_bool(section_name.c_str(),
                                      "tracer::on_task_begin",
                                      true,
                                      "whether to trace when a task begins"))
            spec->on_task_begin.put_back(tracer_on_task_begin, "tracer");

        if (dsn_config_get_value_bool(section_name.c_str(),
                                      "tracer::on_task_end",
                                      true,
                                      "whether to trace when a task ends"))
            spec->on_task_end.put_back(tracer_on_task_end, "tracer");

        if (dsn_config_get_value_bool(section_name.c_str(),
                                      "tracer::on_task_cancelled",
                                      true,
                                      "whether to trace when a task is cancelled"))
            spec->on_task_cancelled.put_back(tracer_on_task_cancelled, "tracer");

        if (dsn_config_get_value_bool(section_name.c_str(),
                                      "tracer::on_task_wait_pre",
                                      true,
                                      "whether to trace when a task is to be wait"))
            spec->on_task_wait_pre.put_back(tracer_on_task_wait_pre, "tracer");

        if (dsn_config_get_value_bool(section_name.c_str(),
                                      "tracer::on_task_wait_post",
                                      true,
                                      "whether to trace when a task is wait post"))
            spec->on_task_wait_post.put_back(tracer_on_task_wait_post, "tracer");

        if (dsn_config_get_value_bool(section_name.c_str(),
                                      "tracer::on_task_cancel_post",
                                      true,
                                      "whether to trace when a task is cancel post"))
            spec->on_task_cancel_post.put_back(tracer_on_task_cancel_post, "tracer");

        if (dsn_config_get_value_bool(section_name.c_str(),
                                      "tracer::on_aio_call",
                                      true,
                                      "whether to trace when an aio task is called"))
            spec->on_aio_call.put_back(tracer_on_aio_call, "tracer");

        if (dsn_config_get_value_bool(section_name.c_str(),
                                      "tracer::on_aio_enqueue",
                                      true,
                                      "whether to trace when an aio task is enqueued"))
            spec->on_aio_enqueue.put_back(tracer_on_aio_enqueue, "tracer");

        if (dsn_config_get_value_bool(section_name.c_str(),
                                      "tracer::on_rpc_call",
                                      true,
                                      "whether to trace when a rpc is made"))
            spec->on_rpc_call.put_back(tracer_on_rpc_call, "tracer");

        if (dsn_config_get_value_bool(section_name.c_str(),
                                      "tracer::on_rpc_request_enqueue",
                                      true,
                                      "whether to trace when a rpc request task is enqueued"))
            spec->on_rpc_request_enqueue.put_back(tracer_on_rpc_request_enqueue, "tracer");

        if (dsn_config_get_value_bool(section_name.c_str(),
                                      "tracer::on_rpc_reply",
                                      true,
                                      "whether to trace when reply a rpc request"))
            spec->on_rpc_reply.put_back(tracer_on_rpc_reply, "tracer");

        if (dsn_config_get_value_bool(section_name.c_str(),
                                      "tracer::on_rpc_response_enqueue",
                                      true,
                                      "whetehr to trace when a rpc response task is enqueued"))
            spec->on_rpc_response_enqueue.put_back(tracer_on_rpc_response_enqueue, "tracer");

        if (dsn_config_get_value_bool(section_name.c_str(),
                                      "tracer::on_rpc_create_response",
                                      true,
                                      "whetehr to trace when a rpc response task is created"))
            spec->on_rpc_create_response.put_back(tracer_on_rpc_create_response, "tracer");
    }

    static std::once_flag flag;
    std::call_once(flag, [&]() {
        _tracer_find_cmd = command_manager::instance().register_command(
            {"tracer.find"},
            "tracer.find - find related logs",
            "tracer.find forward|f|backward|b rpc|r|task|t trace_id|task_id(e.g., "
            "a023003920302390) log_file_name(log.xx.txt)",
            tracer_log_flow);
    });
}

tracer::tracer(const char *name) : toollet(name) {}

} // namespace tools
} // namespace dsn
