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
 *     inject failure through join points to mimic all network/disk/slow execution etc. failures
 *
 * Revision history:
 *     Mar., 2015, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include "runtime/fault_injector.h"
#include "runtime/api_task.h"
#include "runtime/api_layer1.h"
#include "runtime/app_model.h"
#include "utils/api_utilities.h"
#include "utils/rand.h"
#include "aio/aio_task.h"

namespace dsn {
namespace tools {

struct fj_opt
{
    bool fault_injection_enabled;

    // io failure
    double rpc_request_data_corrupted_ratio;
    double rpc_response_data_corrupted_ratio;
    std::string rpc_message_data_corrupted_type;

    double rpc_request_drop_ratio;
    double rpc_response_drop_ratio;
    double rpc_request_delay_ratio;
    double rpc_response_delay_ratio;
    double disk_read_fail_ratio;
    double disk_write_fail_ratio;

    // delay
    uint32_t rpc_message_delay_ms_min;
    uint32_t rpc_message_delay_ms_max;
    uint32_t disk_io_delay_ms_min;
    uint32_t disk_io_delay_ms_max;
    uint32_t execution_extra_delay_us_max;
    uint32_t execution_extra_delay_us_min;

    // node crash
    uint32_t node_crash_minutes_min;
    uint32_t node_crash_minutes_max;
    uint32_t node_crash_minutes_recover_min;
    uint32_t node_crash_minutes_recover_max;
    bool node_crashed;
};

CONFIG_BEGIN(fj_opt)
CONFIG_FLD(bool, bool, fault_injection_enabled, true, "whether enable fault injection")

CONFIG_FLD(double,
           double,
           rpc_request_data_corrupted_ratio,
           0,
           "data corrupted ratio for rpc request message")
CONFIG_FLD(double,
           double,
           rpc_response_data_corrupted_ratio,
           0,
           "data corrupted ratio for rpc response message")
CONFIG_FLD_STRING(rpc_message_data_corrupted_type,
                  "random",
                  "data corrupted type: random/header/body")

CONFIG_FLD(double, double, rpc_request_drop_ratio, 0, "drop ratio for rpc request messages")
CONFIG_FLD(double, double, rpc_response_drop_ratio, 0, "drop ratio for rpc response messages")
CONFIG_FLD(double, double, rpc_request_delay_ratio, 0, "delay ratio for rpc request messages")
CONFIG_FLD(double, double, rpc_response_delay_ratio, 0, "delay ratio for rpc response messages")
CONFIG_FLD(double, double, disk_read_fail_ratio, 0.000001, "failure ratio for disk read operations")
CONFIG_FLD(
    double, double, disk_write_fail_ratio, 0.000001, "failure ratio for disk write operations")

CONFIG_FLD(
    uint32_t, uint64, rpc_message_delay_ms_min, 0, "miminum message delay (ms) for rpc messages")
CONFIG_FLD(
    uint32_t, uint64, rpc_message_delay_ms_max, 1000, "maximum message delay (ms) for rpc messages")
CONFIG_FLD(uint32_t, uint64, disk_io_delay_ms_min, 1, "miminum disk operation delay (ms)")
CONFIG_FLD(uint32_t, uint64, disk_io_delay_ms_max, 12, "maximum disk operation delay (ms)")
CONFIG_FLD(uint32_t,
           uint64,
           execution_extra_delay_us_min,
           0,
           "extra execution time delay (us) for this task")
CONFIG_FLD(uint32_t,
           uint64,
           execution_extra_delay_us_max,
           0,
           "extra execution time delay (us) for this task")

CONFIG_FLD(uint32_t,
           uint64,
           node_crash_minutes_min,
           40,
           "every minimum period (mins) the node should crash")
CONFIG_FLD(uint32_t,
           uint64,
           node_crash_minutes_max,
           60,
           "every maximum period (mins) the node should crash")
CONFIG_FLD(
    uint32_t, uint64, node_crash_minutes_recover_min, 1, "minimum recovery time (ms) for the node")
CONFIG_FLD(
    uint32_t, uint64, node_crash_minutes_recover_max, 4, "minimum recovery time (ms) for the node")
CONFIG_FLD(bool, bool, node_crashed, false, "whether to enable node crash")
CONFIG_END

static fj_opt *s_fj_opts = nullptr;

typedef uint64_extension_helper<fj_opt, task> task_ext_for_fj;

static void fault_on_task_enqueue(task *caller, task *callee) {}

static void fault_on_task_begin(task *this_)
{
    fj_opt &opt = s_fj_opts[this_->spec().code];
    if (opt.execution_extra_delay_us_max > 0) {
        auto d = rand::next_u32(0, opt.execution_extra_delay_us_max);
        LOG_INFO("fault inject {} at {} with delay {} us", this_->spec().name, __FUNCTION__, d);
        std::this_thread::sleep_for(std::chrono::microseconds(d));
    }
}

static void fault_on_task_end(task *this_) {}

static void fault_on_task_cancelled(task *this_) {}

static void fault_on_task_wait_pre(task *caller, task *callee, uint32_t timeout_ms) {}

static void fault_on_task_wait_post(task *caller, task *callee, bool succ) {}

static void fault_on_task_cancel_post(task *caller, task *callee, bool succ) {}

// return true means continue, otherwise early terminate with task::set_error_code
static bool fault_on_aio_call(task *caller, aio_task *callee)
{
    switch (callee->get_aio_context()->type) {
    case AIO_Read:
        if (rand::next_double01() < s_fj_opts[callee->spec().code].disk_read_fail_ratio) {
            LOG_INFO("fault inject {} at {}", callee->spec().name, __FUNCTION__);
            callee->set_error_code(ERR_FILE_OPERATION_FAILED);
            return false;
        }
        break;
    case AIO_Write:
        if (rand::next_double01() < s_fj_opts[callee->spec().code].disk_write_fail_ratio) {
            LOG_INFO("fault inject {} at {}", callee->spec().name, __FUNCTION__);
            callee->set_error_code(ERR_FILE_OPERATION_FAILED);
            return false;
        }
        break;
    default:
        break;
    }

    return true;
}

static void fault_on_aio_enqueue(aio_task *this_)
{
    fj_opt &opt = s_fj_opts[this_->spec().code];
    if (this_->delay_milliseconds() == 0 && task_ext_for_fj::get(this_) == 0) {
        this_->set_delay(rand::next_u32(opt.disk_io_delay_ms_min, opt.disk_io_delay_ms_max));
        LOG_INFO("fault inject {} at {} with delay {} ms",
                 this_->spec().name,
                 __FUNCTION__,
                 this_->delay_milliseconds());
        task_ext_for_fj::get(this_) = 1; // ensure only fd once
    }
}

static void replace_value(std::vector<blob> &buffer_list, unsigned int offset)
{
    for (blob &bb : buffer_list) {
        if (offset < bb.length()) {
            (const_cast<char *>(bb.data()))[offset]++;
            break;
        } else
            offset -= bb.length();
    }
}

static void corrupt_data(message_ex *request, const std::string &corrupt_type)
{
    if (corrupt_type == "header")
        replace_value(request->buffers, rand::next_u32(0, sizeof(message_header) - 1));
    else if (corrupt_type == "body")
        replace_value(request->buffers,
                      rand::next_u32(0, request->body_size() - 1) + sizeof(message_header));
    else if (corrupt_type == "random")
        replace_value(request->buffers,
                      rand::next_u32(0, request->body_size() + sizeof(message_header) - 1));
    else {
        LOG_ERROR("try to inject an unknown data corrupt type: {}", corrupt_type);
    }
}

// return true means continue, otherwise early terminate with task::set_error_code
static bool fault_on_rpc_call(task *caller, message_ex *req, rpc_response_task *callee)
{
    fj_opt &opt = s_fj_opts[req->local_rpc_code];
    if (rand::next_double01() < opt.rpc_request_drop_ratio) {
        LOG_INFO("fault inject {} at {}: {} => {}",
                 req->header->rpc_name,
                 __FUNCTION__,
                 req->header->from_address,
                 req->to_address);
        return false;
    } else {
        if (rand::next_double01() < opt.rpc_request_data_corrupted_ratio) {
            LOG_INFO("corrupt the rpc call message from: {}, type: {}",
                     req->header->from_address,
                     opt.rpc_message_data_corrupted_type);
            corrupt_data(req, opt.rpc_message_data_corrupted_type);
        }
        return true;
    }
}

static void fault_on_rpc_request_enqueue(rpc_request_task *callee)
{
    fj_opt &opt = s_fj_opts[callee->spec().code];
    if (callee->delay_milliseconds() == 0 && task_ext_for_fj::get(callee) == 0) {
        if (rand::next_double01() < opt.rpc_request_delay_ratio) {
            callee->set_delay(
                rand::next_u32(opt.rpc_message_delay_ms_min, opt.rpc_message_delay_ms_max));
            LOG_INFO("fault inject {} at {} with delay {} ms",
                     callee->spec().name,
                     __FUNCTION__,
                     callee->delay_milliseconds());
            task_ext_for_fj::get(callee) = 1; // ensure only fd once
        }
    }
}

// return true means continue, otherwise early terminate with task::set_error_code
static bool fault_on_rpc_reply(task *caller, message_ex *msg)
{
    fj_opt &opt = s_fj_opts[msg->local_rpc_code];
    if (rand::next_double01() < opt.rpc_response_drop_ratio) {
        LOG_INFO("fault inject {} at {}: {} => {}",
                 msg->header->rpc_name,
                 __FUNCTION__,
                 msg->header->from_address,
                 msg->to_address);
        return false;
    } else {
        if (rand::next_double01() < opt.rpc_response_data_corrupted_ratio) {
            LOG_INFO("fault injector corrupt the rpc reply message from: {}, type: {}",
                     msg->header->from_address,
                     opt.rpc_message_data_corrupted_type);
            corrupt_data(msg, opt.rpc_message_data_corrupted_type);
        }
        return true;
    }
}

static void fault_on_rpc_response_enqueue(rpc_response_task *resp)
{
    fj_opt &opt = s_fj_opts[resp->spec().code];
    if (resp->delay_milliseconds() == 0 && task_ext_for_fj::get(resp) == 0) {
        if (rand::next_double01() < opt.rpc_response_delay_ratio) {
            resp->set_delay(
                rand::next_u32(opt.rpc_message_delay_ms_min, opt.rpc_message_delay_ms_max));
            LOG_INFO("fault inject {} at {} with delay {} ms",
                     resp->spec().name,
                     __FUNCTION__,
                     resp->delay_milliseconds());
            task_ext_for_fj::get(resp) = 1; // ensure only fd once
        }
    }
}

void fault_injector::install(service_spec &spec)
{
    task_ext_for_fj::register_ext();

    s_fj_opts = new fj_opt[dsn::task_code::max() + 1];
    fj_opt default_opt;
    read_config("task..default", default_opt);

    for (int i = 0; i <= dsn::task_code::max(); i++) {
        if (i == TASK_CODE_INVALID)
            continue;

        std::string section_name =
            std::string("task.") + std::string(dsn::task_code(i).to_string());
        task_spec *spec = task_spec::get(i);
        CHECK_NOTNULL(spec, "");

        fj_opt &lopt = s_fj_opts[i];
        read_config(section_name.c_str(), lopt, &default_opt);

        if (!lopt.fault_injection_enabled)
            continue;

        spec->on_task_enqueue.put_back(fault_on_task_enqueue, "fault_injector");
        spec->on_task_begin.put_back(fault_on_task_begin, "fault_injector");
        spec->on_task_end.put_back(fault_on_task_end, "fault_injector");
        spec->on_task_cancelled.put_back(fault_on_task_cancelled, "fault_injector");
        spec->on_task_wait_pre.put_back(fault_on_task_wait_pre, "fault_injector");
        spec->on_task_wait_post.put_back(fault_on_task_wait_post, "fault_injector");
        spec->on_task_cancel_post.put_back(fault_on_task_cancel_post, "fault_injector");
        spec->on_aio_call.put_native(fault_on_aio_call);
        spec->on_aio_enqueue.put_back(fault_on_aio_enqueue, "fault_injector");
        spec->on_rpc_call.put_native(fault_on_rpc_call);
        spec->on_rpc_request_enqueue.put_back(fault_on_rpc_request_enqueue, "fault_injector");
        spec->on_rpc_reply.put_native(fault_on_rpc_reply);
        spec->on_rpc_response_enqueue.put_back(fault_on_rpc_response_enqueue, "fault_injector");
    }

    if (default_opt.node_crash_minutes_max > 0) {
        // TODO:
    }
}

fault_injector::fault_injector(const char *name) : toollet(name) {}
}
}
