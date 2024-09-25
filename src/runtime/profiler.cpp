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
HELP GRAPH
                           CALL ===== net(call) ========> ENQUEUE ===== queue(server) ====> START
                            ^                               ^                                ||
                            |                               |                                ||
                            |                               |                                ||
                            |                               |                                ||
                            |                               |                                ||
                      Client Latency                Server Latency                     exec(server)
                            |                               |                                ||
                            |                               |                                ||
                            |                               |                                ||
                            |                               |                                ||
                            V                               V                                ||
START<== queue(server) == ENQUEUE <===== net(reply) ======= REPLY <=============================
  ||
  ||
 exec(client)
  ||
  ||
  \/
 END
*/
#include "runtime/profiler.h"

#include <atomic>
#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "aio/aio_task.h"
#include "fmt/core.h"
#include "profiler_header.h"
#include "rpc/rpc_message.h"
#include "runtime/api_layer1.h"
#include "task/task.h"
#include "task/task_code.h"
#include "task/task_spec.h"
#include "utils/config_api.h"
#include "utils/extensible_object.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/join_point.h"
#include "utils/metrics.h"

DSN_DEFINE_bool(task..default, is_profile, false, "Whether to profile task");
DSN_DEFINE_bool(task..default,
                collect_call_count,
                true,
                "Whether to collect the times of the task invoke each of other kinds tasks");

METRIC_DEFINE_entity(profiler);

METRIC_DEFINE_gauge_int64(profiler,
                          profiler_queued_tasks,
                          dsn::metric_unit::kTasks,
                          "The number of tasks in all queues");

METRIC_DEFINE_percentile_int64(profiler,
                               profiler_queue_latency_ns,
                               dsn::metric_unit::kNanoSeconds,
                               "The latency it takes for each task to wait in each queue "
                               "before beginning to be executed");

METRIC_DEFINE_percentile_int64(profiler,
                               profiler_execute_latency_ns,
                               dsn::metric_unit::kNanoSeconds,
                               "The latency it takes for each task to be executed");

METRIC_DEFINE_counter(profiler,
                      profiler_executed_tasks,
                      dsn::metric_unit::kTasks,
                      "The number of tasks that have been executed");

METRIC_DEFINE_counter(profiler,
                      profiler_cancelled_tasks,
                      dsn::metric_unit::kTasks,
                      "The number of cancelled tasks");

METRIC_DEFINE_percentile_int64(profiler,
                               profiler_server_rpc_latency_ns,
                               dsn::metric_unit::kNanoSeconds,
                               "The latency from enqueue point to reply point on the server side "
                               "for each RPC task");

METRIC_DEFINE_percentile_int64(profiler,
                               profiler_server_rpc_request_bytes,
                               dsn::metric_unit::kBytes,
                               "The body length of request received on the server side for each "
                               "RPC task");

METRIC_DEFINE_percentile_int64(profiler,
                               profiler_server_rpc_response_bytes,
                               dsn::metric_unit::kBytes,
                               "The body length of response replied on the server side for each "
                               "RPC task");

METRIC_DEFINE_counter(profiler,
                      profiler_dropped_timeout_rpcs,
                      dsn::metric_unit::kTasks,
                      "The accumulative number of dropped RPC tasks on the server side "
                      "due to timeout");

METRIC_DEFINE_percentile_int64(profiler,
                               profiler_client_rpc_latency_ns,
                               dsn::metric_unit::kNanoSeconds,
                               "The non-timeout latency from call point to enqueue point on "
                               "the client side for each RPC task");

METRIC_DEFINE_counter(profiler,
                      profiler_client_timeout_rpcs,
                      dsn::metric_unit::kTasks,
                      "The accumulative number of timeout RPC tasks on the client side");

METRIC_DEFINE_percentile_int64(profiler,
                               profiler_aio_latency_ns,
                               dsn::metric_unit::kNanoSeconds,
                               "The duration of the whole AIO operation (begin to aio -> "
                               "executing -> finished -> callback is put into queue)");

namespace dsn {
struct service_spec;

namespace tools {

typedef uint64_extension_helper<task_spec_profiler, task> task_ext_for_profiler;
typedef uint64_extension_helper<task_spec_profiler, message_ex> message_ext_for_profiler;

std::vector<task_spec_profiler> s_spec_profilers;

int s_task_code_max = 0;

// call normal task
static void profiler_on_task_create(task *caller, task *callee)
{
    task_ext_for_profiler::get(callee) = dsn_now_ns();
}

static void profiler_on_task_enqueue(task *caller, task *callee)
{
    auto callee_code = callee->spec().code;
    CHECK(callee_code >= 0 && callee_code <= s_task_code_max, "code = {}", callee_code.code());

    if (caller != nullptr) {
        auto caller_code = caller->spec().code;
        CHECK(caller_code >= 0 && caller_code <= s_task_code_max, "code = {}", caller_code.code());

        auto &prof = s_spec_profilers[caller_code];
        if (prof.collect_call_count) {
            prof.call_counts[callee_code]++;
        }
    }

    task_ext_for_profiler::get(callee) = dsn_now_ns();
    if (callee->delay_milliseconds() == 0) {
        METRIC_INCREMENT(s_spec_profilers[callee_code], profiler_queued_tasks);
    }
}

static void profiler_on_task_begin(task *this_)
{
    auto code = this_->spec().code;
    // TODO(yingchun): duplicate checks, should refactor later
    CHECK(code >= 0 && code <= s_task_code_max, "code = {}", code.code());

    uint64_t &qts = task_ext_for_profiler::get(this_);
    uint64_t now = dsn_now_ns();
    METRIC_SET(s_spec_profilers[code], profiler_queue_latency_ns, now - qts);
    qts = now;

    METRIC_DECREMENT(s_spec_profilers[code], profiler_queued_tasks);
}

static void profiler_on_task_end(task *this_)
{
    auto code = this_->spec().code;
    CHECK(code >= 0 && code <= s_task_code_max, "code = {}", code.code());

    uint64_t qts = task_ext_for_profiler::get(this_);
    uint64_t now = dsn_now_ns();
    METRIC_SET(s_spec_profilers[code], profiler_execute_latency_ns, now - qts);

    METRIC_INCREMENT(s_spec_profilers[code], profiler_executed_tasks);
}

static void profiler_on_task_cancelled(task *this_)
{
    auto code = this_->spec().code;
    CHECK(code >= 0 && code <= s_task_code_max, "code = {}", code.code());

    METRIC_INCREMENT(s_spec_profilers[code], profiler_cancelled_tasks);
}

static void profiler_on_task_wait_pre(task *caller, task *callee, uint32_t timeout_ms) {}

static void profiler_on_task_wait_post(task *caller, task *callee, bool succ) {}

static void profiler_on_task_cancel_post(task *caller, task *callee, bool succ) {}

// return true means continue, otherwise early terminate with task::set_error_code
static void profiler_on_aio_call(task *caller, aio_task *callee)
{
    if (nullptr != caller) {
        auto caller_code = caller->spec().code;
        CHECK(caller_code >= 0 && caller_code <= s_task_code_max, "code = {}", caller_code.code());

        auto &prof = s_spec_profilers[caller_code];
        if (prof.collect_call_count) {
            auto callee_code = callee->spec().code;
            CHECK(callee_code >= 0 && callee_code <= s_task_code_max,
                  "code = {}",
                  callee_code.code());
            prof.call_counts[callee_code]++;
        }
    }

    // time disk io starts
    task_ext_for_profiler::get(callee) = dsn_now_ns();
}

static void profiler_on_aio_enqueue(aio_task *this_)
{
    auto code = this_->spec().code;
    CHECK(code >= 0 && code <= s_task_code_max, "code = {}", code.code());

    uint64_t &ats = task_ext_for_profiler::get(this_);
    uint64_t now = dsn_now_ns();

    METRIC_SET(s_spec_profilers[code], profiler_aio_latency_ns, now - ats);
    ats = now;

    METRIC_INCREMENT(s_spec_profilers[code], profiler_queued_tasks);
}

// return true means continue, otherwise early terminate with task::set_error_code
static void profiler_on_rpc_call(task *caller, message_ex *req, rpc_response_task *callee)
{
    if (nullptr != caller) {
        auto caller_code = caller->spec().code;
        CHECK(caller_code >= 0 && caller_code <= s_task_code_max, "code = {}", caller_code.code());

        auto &prof = s_spec_profilers[caller_code];
        if (prof.collect_call_count) {
            CHECK(req->local_rpc_code >= 0 && req->local_rpc_code <= s_task_code_max,
                  "code = {}",
                  req->local_rpc_code.code());
            prof.call_counts[req->local_rpc_code]++;
        }
    }

    // time rpc starts
    if (nullptr != callee) {
        task_ext_for_profiler::get(callee) = dsn_now_ns();
    }
}

static void profiler_on_rpc_request_enqueue(rpc_request_task *callee)
{
    auto callee_code = callee->spec().code;
    CHECK(callee_code >= 0 && callee_code <= s_task_code_max, "code = {}", callee_code.code());

    uint64_t now = dsn_now_ns();
    task_ext_for_profiler::get(callee) = now;
    message_ext_for_profiler::get(callee->get_request()) = now;

    METRIC_INCREMENT(s_spec_profilers[callee_code], profiler_queued_tasks);

    METRIC_SET(s_spec_profilers[callee_code],
               profiler_server_rpc_request_bytes,
               callee->get_request()->header->body_length);
}

static void profile_on_rpc_task_dropped(rpc_request_task *callee)
{
    auto code = callee->spec().code;
    METRIC_INCREMENT(s_spec_profilers[code], profiler_dropped_timeout_rpcs);
}

static void profiler_on_rpc_create_response(message_ex *req, message_ex *resp)
{
    message_ext_for_profiler::get(resp) = message_ext_for_profiler::get(req);
}

// return true means continue, otherwise early terminate with task::set_error_code
static void profiler_on_rpc_reply(task *caller, message_ex *msg)
{
    auto caller_code = caller->spec().code;
    CHECK(caller_code >= 0 && caller_code <= s_task_code_max, "code = {}", caller_code.code());

    auto &prof = s_spec_profilers[caller_code];
    if (prof.collect_call_count) {
        CHECK(msg->local_rpc_code >= 0 && msg->local_rpc_code <= s_task_code_max,
              "code = {}",
              msg->local_rpc_code.code());
        prof.call_counts[msg->local_rpc_code]++;
    }

    uint64_t qts = message_ext_for_profiler::get(msg);
    uint64_t now = dsn_now_ns();
    task_spec *spec = task_spec::get(msg->local_rpc_code);
    CHECK_NOTNULL(spec, "task_spec cannot be null, code = {}", msg->local_rpc_code.code());
    auto code = spec->rpc_paired_code;
    CHECK(code >= 0 && code <= s_task_code_max, "code = {}", code.code());

    METRIC_SET(s_spec_profilers[code], profiler_server_rpc_latency_ns, now - qts);

    METRIC_SET(
        s_spec_profilers[code], profiler_server_rpc_response_bytes, msg->header->body_length);
}

static void profiler_on_rpc_response_enqueue(rpc_response_task *resp)
{
    auto resp_code = resp->spec().code;
    CHECK(resp_code >= 0 && resp_code <= s_task_code_max, "code = {}", resp_code.code());

    uint64_t &cts = task_ext_for_profiler::get(resp);
    uint64_t now = dsn_now_ns();

    if (resp->get_response() != nullptr) {
        METRIC_SET(s_spec_profilers[resp_code], profiler_client_rpc_latency_ns, now - cts);
    } else {
        METRIC_INCREMENT(s_spec_profilers[resp_code], profiler_client_timeout_rpcs);
    }
    cts = now;

    METRIC_INCREMENT(s_spec_profilers[resp_code], profiler_queued_tasks);
}

namespace {

metric_entity_ptr instantiate_profiler_metric_entity(const std::string &task_name)
{
    auto entity_id = fmt::format("task@{}", task_name);

    return METRIC_ENTITY_profiler.instantiate(entity_id, {{"task_name", task_name}});
}

} // anonymous namespace

task_spec_profiler::task_spec_profiler(int code)
    : collect_call_count(false),
      is_profile(false),
      call_counts(new std::atomic<int64_t>[s_task_code_max + 1]),
      _task_name(dsn::task_code(code).to_string()),
      _profiler_metric_entity(instantiate_profiler_metric_entity(_task_name))
{
    const auto &section_name = fmt::format("task.{}", _task_name);
    auto spec = task_spec::get(code);
    CHECK_NOTNULL(spec, "spec should be non-null: task_code={}, task_name={}", code, _task_name);

    collect_call_count = dsn_config_get_value_bool(
        section_name.c_str(), "collect_call_count", FLAGS_collect_call_count, "");

    for (int i = 0; i <= s_task_code_max; ++i) {
        call_counts[i].store(0);
    }

    is_profile =
        dsn_config_get_value_bool(section_name.c_str(), "is_profile", FLAGS_is_profile, "");

    if (!is_profile) {
        return;
    }

    LOG_INFO("register task into profiler: task_code={}, task_name={}, section_name={}, "
             "task_type={}",
             code,
             _task_name,
             section_name,
             enum_to_string(spec->type));

    if (dsn_config_get_value_bool(
            section_name.c_str(),
            "profiler::inqueue",
            true,
            "Whether to profile the count of this kind of tasks in all queues")) {
        METRIC_VAR_ASSIGN_profiler(profiler_queued_tasks);
    }

    if (dsn_config_get_value_bool(section_name.c_str(),
                                  "profiler::queue",
                                  true,
                                  "Whether to profile the queuing duration of a task")) {
        METRIC_VAR_ASSIGN_profiler(profiler_queue_latency_ns);
    }

    if (dsn_config_get_value_bool(section_name.c_str(),
                                  "profiler::exec",
                                  true,
                                  "Whether to profile the executing duration of a task")) {
        METRIC_VAR_ASSIGN_profiler(profiler_execute_latency_ns);
    }

    if (dsn_config_get_value_bool(
            section_name.c_str(), "profiler::qps", true, "Whether to profile the QPS of a task")) {
        METRIC_VAR_ASSIGN_profiler(profiler_executed_tasks);
    }

    if (dsn_config_get_value_bool(section_name.c_str(),
                                  "profiler::cancelled",
                                  true,
                                  "Whether to profile the cancelled times of a task")) {
        METRIC_VAR_ASSIGN_profiler(profiler_cancelled_tasks);
    }

    if (spec->type == dsn_task_type_t::TASK_TYPE_RPC_REQUEST) {
        if (dsn_config_get_value_bool(section_name.c_str(),
                                      "profiler::latency.server",
                                      true,
                                      "Whether to profile the server latency of a task")) {
            METRIC_VAR_ASSIGN_profiler(profiler_server_rpc_latency_ns);
        }
        if (dsn_config_get_value_bool(section_name.c_str(),
                                      "profiler::size.request.server",
                                      false,
                                      "Whether to profile the size of per request")) {
            METRIC_VAR_ASSIGN_profiler(profiler_server_rpc_request_bytes);
        }
        if (dsn_config_get_value_bool(section_name.c_str(),
                                      "profiler::size.response.server",
                                      false,
                                      "Whether to profile the size of per response")) {
            METRIC_VAR_ASSIGN_profiler(profiler_server_rpc_response_bytes);
        }
        if (dsn_config_get_value_bool(
                section_name.c_str(),
                "rpc_request_dropped_before_execution_when_timeout",
                false,
                "Whether to profile the count of RPC dropped for timeout reason")) {
            METRIC_VAR_ASSIGN_profiler(profiler_dropped_timeout_rpcs);
        }
    } else if (spec->type == dsn_task_type_t::TASK_TYPE_RPC_RESPONSE) {
        if (dsn_config_get_value_bool(section_name.c_str(),
                                      "profiler::latency.client",
                                      true,
                                      "Whether to profile the client latency of a task")) {
            METRIC_VAR_ASSIGN_profiler(profiler_client_rpc_latency_ns);
        }
        if (dsn_config_get_value_bool(section_name.c_str(),
                                      "profiler::timeout.qps",
                                      true,
                                      "Whether to profile the timeout QPS of a task")) {
            METRIC_VAR_ASSIGN_profiler(profiler_client_timeout_rpcs);
        }
    } else if (spec->type == dsn_task_type_t::TASK_TYPE_AIO) {
        if (dsn_config_get_value_bool(section_name.c_str(),
                                      "profiler::latency",
                                      true,
                                      "Whether to profile the latency of an AIO task")) {
            METRIC_VAR_ASSIGN_profiler(profiler_aio_latency_ns);
        }
    }

    spec->on_task_create.put_back(profiler_on_task_create, "profiler");
    spec->on_task_enqueue.put_back(profiler_on_task_enqueue, "profiler");
    spec->on_task_begin.put_back(profiler_on_task_begin, "profiler");
    spec->on_task_end.put_back(profiler_on_task_end, "profiler");
    spec->on_task_cancelled.put_back(profiler_on_task_cancelled, "profiler");
    spec->on_task_wait_pre.put_back(profiler_on_task_wait_pre, "profiler");
    spec->on_task_wait_post.put_back(profiler_on_task_wait_post, "profiler");
    spec->on_task_cancel_post.put_back(profiler_on_task_cancel_post, "profiler");
    spec->on_aio_call.put_back(profiler_on_aio_call, "profiler");
    spec->on_aio_enqueue.put_back(profiler_on_aio_enqueue, "profiler");
    spec->on_rpc_call.put_back(profiler_on_rpc_call, "profiler");
    spec->on_rpc_request_enqueue.put_back(profiler_on_rpc_request_enqueue, "profiler");
    spec->on_rpc_task_dropped.put_back(profile_on_rpc_task_dropped, "profiler");
    spec->on_rpc_create_response.put_back(profiler_on_rpc_create_response, "profiler");
    spec->on_rpc_reply.put_back(profiler_on_rpc_reply, "profiler");
    spec->on_rpc_response_enqueue.put_back(profiler_on_rpc_response_enqueue, "profiler");
}

const metric_entity_ptr &task_spec_profiler::profiler_metric_entity() const
{
    CHECK_NOTNULL(_profiler_metric_entity,
                  "profiler metric entity (task_name={}) should has been instantiated: "
                  "uninitialized entity cannot be used to instantiate metric",
                  _task_name);
    return _profiler_metric_entity;
}

void profiler::install(service_spec &)
{
    s_task_code_max = dsn::task_code::max();
    task_ext_for_profiler::register_ext();
    message_ext_for_profiler::register_ext();

    s_spec_profilers.clear();
    s_spec_profilers.reserve(s_task_code_max + 1);
    LOG_INFO("begin to choose the tasks that will be registered into profilers among "
             "all of the {} tasks",
             s_task_code_max + 1);
    for (int code = 0; code <= s_task_code_max; ++code) {
        if (code == TASK_CODE_INVALID) {
            // Though the task code `TASK_CODE_INVALID` is meaningless, it should still be pushed
            // into the `s_spec_profilers` by default constructor of `task_spec_profiler`, since
            // `task_spec_profiler` is indexed by the task code in `s_spec_profilers`.
            s_spec_profilers.emplace_back();
            continue;
        }

        s_spec_profilers.emplace_back(code);
    }
    CHECK_EQ(s_spec_profilers.size(), s_task_code_max + 1);
}

profiler::profiler(const char *name) : toollet(name) {}

} // namespace tools
} // namespace dsn
