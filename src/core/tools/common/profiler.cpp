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
#include <dsn/toollet/profiler.h>
#include <dsn/service_api_c.h>
#include "shared_io_service.h"
#include "profiler_header.h"
#include <dsn/tool-api/command_manager.h>
#include <dsn/perf_counter/perf_counter_wrapper.h>

namespace dsn {
namespace tools {

typedef uint64_extension_helper<task_spec_profiler, task> task_ext_for_profiler;
typedef uint64_extension_helper<task_spec_profiler, message_ex> message_ext_for_profiler;

std::unique_ptr<task_spec_profiler[]> s_spec_profilers;

int s_task_code_max = 0;

std::map<std::string, perf_counter_ptr_type> counter_info::pointer_type;

counter_info *counter_info_ptr[] = {
    new counter_info({"queue.time", "qt"},
                     TASK_QUEUEING_TIME_NS,
                     COUNTER_TYPE_NUMBER_PERCENTILES,
                     "QUEUE(ns)",
                     "ns"),
    new counter_info(
        {"exec.time", "et"}, TASK_EXEC_TIME_NS, COUNTER_TYPE_NUMBER_PERCENTILES, "EXEC(ns)", "ns"),
    new counter_info({"throughput", "tp"}, TASK_THROUGHPUT, COUNTER_TYPE_RATE, "THP(#/s)", "#/s"),
    new counter_info({"cancelled", "cc"}, TASK_CANCELLED, COUNTER_TYPE_NUMBER, "CANCEL(#)", "#"),
    new counter_info({"aio.latency", "al"},
                     AIO_LATENCY_NS,
                     COUNTER_TYPE_NUMBER_PERCENTILES,
                     "AIO.LATENCY(ns)",
                     "ns"),
    new counter_info({"rpc.server.latency", "rpcsl"},
                     RPC_SERVER_LATENCY_NS,
                     COUNTER_TYPE_NUMBER_PERCENTILES,
                     "RPC.SERVER(ns)",
                     "ns"),
    new counter_info({"rpc.server.size.request", "rpcssreq"},
                     RPC_SERVER_SIZE_PER_REQUEST_IN_BYTES,
                     COUNTER_TYPE_NUMBER_PERCENTILES,
                     "RPC.SERVER.SIZE.REQUEST(bytes)",
                     "bytes"),
    new counter_info({"rpc.server.size.response", "rpcssresp"},
                     RPC_SERVER_SIZE_PER_RESPONSE_IN_BYTES,
                     COUNTER_TYPE_NUMBER_PERCENTILES,
                     "RPC.SERVER.SIZE.RESPONSE(bytes)",
                     "bytes"),
    new counter_info({"rpc.client.latency", "rpccl"},
                     RPC_CLIENT_NON_TIMEOUT_LATENCY_NS,
                     COUNTER_TYPE_NUMBER_PERCENTILES,
                     "RPC.CLIENT(ns)",
                     "ns"),
    new counter_info({"rpc.client.timeout", "rpcto"},
                     RPC_CLIENT_TIMEOUT_THROUGHPUT,
                     COUNTER_TYPE_RATE,
                     "TIMEOUT(#/s)",
                     "#/s"),
    new counter_info(
        {"task.inqueue", "tiq"}, TASK_IN_QUEUE, COUNTER_TYPE_NUMBER, "InQueue(#)", "#")};

// call normal task
static void profiler_on_task_create(task *caller, task *callee)
{
    task_ext_for_profiler::get(callee) = dsn_now_ns();
}

static void profiler_on_task_enqueue(task *caller, task *callee)
{
    auto callee_code = callee->spec().code;
    dassert(callee_code >= 0 && callee_code <= s_task_code_max, "code = %d", callee_code.code());

    if (caller != nullptr) {
        auto caller_code = caller->spec().code;
        dassert(
            caller_code >= 0 && caller_code <= s_task_code_max, "code = %d", caller_code.code());

        auto &prof = s_spec_profilers[caller_code];
        if (prof.collect_call_count) {
            prof.call_counts[callee_code]++;
        }
    }

    task_ext_for_profiler::get(callee) = dsn_now_ns();
    if (callee->delay_milliseconds() == 0) {
        auto ptr = s_spec_profilers[callee_code].ptr[TASK_IN_QUEUE].get();
        if (ptr != nullptr)
            ptr->increment();
    }
}

static void profiler_on_task_begin(task *this_)
{
    auto code = this_->spec().code;
    dassert(code >= 0 && code <= s_task_code_max, "code = %d", code.code());

    uint64_t &qts = task_ext_for_profiler::get(this_);
    uint64_t now = dsn_now_ns();
    auto ptr = s_spec_profilers[code].ptr[TASK_QUEUEING_TIME_NS].get();
    if (ptr != nullptr)
        ptr->set(now - qts);
    qts = now;

    ptr = s_spec_profilers[code].ptr[TASK_IN_QUEUE].get();
    if (ptr != nullptr)
        ptr->decrement();
}

static void profiler_on_task_end(task *this_)
{
    auto code = this_->spec().code;
    dassert(code >= 0 && code <= s_task_code_max, "code = %d", code.code());

    uint64_t qts = task_ext_for_profiler::get(this_);
    uint64_t now = dsn_now_ns();
    auto ptr = s_spec_profilers[code].ptr[TASK_EXEC_TIME_NS].get();
    if (ptr != nullptr)
        ptr->set(now - qts);

    ptr = s_spec_profilers[code].ptr[TASK_THROUGHPUT].get();
    if (ptr != nullptr)
        ptr->increment();
}

static void profiler_on_task_cancelled(task *this_)
{
    auto code = this_->spec().code;
    dassert(code >= 0 && code <= s_task_code_max, "code = %d", code.code());

    auto ptr = s_spec_profilers[code].ptr[TASK_CANCELLED].get();
    if (ptr != nullptr)
        ptr->increment();
}

static void profiler_on_task_wait_pre(task *caller, task *callee, uint32_t timeout_ms) {}

static void profiler_on_task_wait_post(task *caller, task *callee, bool succ) {}

static void profiler_on_task_cancel_post(task *caller, task *callee, bool succ) {}

// return true means continue, otherwise early terminate with task::set_error_code
static void profiler_on_aio_call(task *caller, aio_task *callee)
{
    if (nullptr != caller) {
        auto caller_code = caller->spec().code;
        dassert(
            caller_code >= 0 && caller_code <= s_task_code_max, "code = %d", caller_code.code());

        auto &prof = s_spec_profilers[caller_code];
        if (prof.collect_call_count) {
            auto callee_code = callee->spec().code;
            dassert(callee_code >= 0 && callee_code <= s_task_code_max,
                    "code = %d",
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
    dassert(code >= 0 && code <= s_task_code_max, "code = %d", code.code());

    uint64_t &ats = task_ext_for_profiler::get(this_);
    uint64_t now = dsn_now_ns();

    auto ptr = s_spec_profilers[code].ptr[AIO_LATENCY_NS].get();
    if (ptr != nullptr)
        ptr->set(now - ats);
    ats = now;

    ptr = s_spec_profilers[code].ptr[TASK_IN_QUEUE].get();
    if (ptr != nullptr)
        ptr->increment();
}

// return true means continue, otherwise early terminate with task::set_error_code
static void profiler_on_rpc_call(task *caller, message_ex *req, rpc_response_task *callee)
{
    if (nullptr != caller) {
        auto caller_code = caller->spec().code;
        dassert(
            caller_code >= 0 && caller_code <= s_task_code_max, "code = %d", caller_code.code());

        auto &prof = s_spec_profilers[caller_code];
        if (prof.collect_call_count) {
            dassert(req->local_rpc_code >= 0 && req->local_rpc_code <= s_task_code_max,
                    "code = %d",
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
    dassert(callee_code >= 0 && callee_code <= s_task_code_max, "code = %d", callee_code.code());

    uint64_t now = dsn_now_ns();
    task_ext_for_profiler::get(callee) = now;
    message_ext_for_profiler::get(callee->get_request()) = now;

    auto ptr = s_spec_profilers[callee_code].ptr[TASK_IN_QUEUE].get();
    if (ptr != nullptr) {
        ptr->increment();
    }
    ptr = s_spec_profilers[callee_code].ptr[RPC_SERVER_SIZE_PER_REQUEST_IN_BYTES].get();
    if (ptr != nullptr) {
        ptr->set(callee->get_request()->header->body_length);
    }
}

static void profiler_on_rpc_create_response(message_ex *req, message_ex *resp)
{
    message_ext_for_profiler::get(resp) = message_ext_for_profiler::get(req);
}

// return true means continue, otherwise early terminate with task::set_error_code
static void profiler_on_rpc_reply(task *caller, message_ex *msg)
{
    auto caller_code = caller->spec().code;
    dassert(caller_code >= 0 && caller_code <= s_task_code_max, "code = %d", caller_code.code());

    auto &prof = s_spec_profilers[caller_code];
    if (prof.collect_call_count) {
        dassert(msg->local_rpc_code >= 0 && msg->local_rpc_code <= s_task_code_max,
                "code = %d",
                msg->local_rpc_code.code());
        prof.call_counts[msg->local_rpc_code]++;
    }

    uint64_t qts = message_ext_for_profiler::get(msg);
    uint64_t now = dsn_now_ns();
    task_spec *spec = task_spec::get(msg->local_rpc_code);
    dassert(spec != nullptr, "task_spec cannot be null, code = %d", msg->local_rpc_code.code());
    auto code = spec->rpc_paired_code;
    dassert(code >= 0 && code <= s_task_code_max, "code = %d", code.code());
    auto ptr = s_spec_profilers[code].ptr[RPC_SERVER_LATENCY_NS].get();
    if (ptr != nullptr) {
        ptr->set(now - qts);
    }
    ptr = s_spec_profilers[code].ptr[RPC_SERVER_SIZE_PER_RESPONSE_IN_BYTES].get();
    if (ptr != nullptr) {
        ptr->set(msg->header->body_length);
    }
}

static void profiler_on_rpc_response_enqueue(rpc_response_task *resp)
{
    auto resp_code = resp->spec().code;
    dassert(resp_code >= 0 && resp_code <= s_task_code_max, "code = %d", resp_code.code());

    uint64_t &cts = task_ext_for_profiler::get(resp);
    uint64_t now = dsn_now_ns();

    if (resp->get_response() != nullptr) {
        auto ptr = s_spec_profilers[resp_code].ptr[RPC_CLIENT_NON_TIMEOUT_LATENCY_NS].get();
        if (ptr != nullptr)
            ptr->set(now - cts);
    } else {
        auto ptr = s_spec_profilers[resp_code].ptr[RPC_CLIENT_TIMEOUT_THROUGHPUT].get();
        if (ptr != nullptr)
            ptr->increment();
    }
    cts = now;

    auto ptr = s_spec_profilers[resp_code].ptr[TASK_IN_QUEUE].get();
    if (ptr != nullptr)
        ptr->increment();
}

void register_command_profiler()
{
    std::stringstream textp, textpjs, textpd, textquery, textarg;
    textp << "NAME:" << std::endl;
    textp << "  profiler - collect performance data" << std::endl;
    textp << "SYNOPSIS:" << std::endl;
    textp << "  show how tasks call each other with what frequency:" << std::endl;
    textp << "      p|P|profile|Profile dependency|dep matrix" << std::endl;
    textp << "  show how tasks call each oether with list format sort by caller/callee:"
          << std::endl;
    textp << "      p|P|profile|Profile dependency|dep list [$task] [caller(default)|callee]"
          << std::endl;
    textp << "  show performance data for specific tasks:" << std::endl;
    textp << "      p|P|profile|Profile info [all|$task]" << std::endl;
    textp << "  show the top N task kinds sort by counter_name:" << std::endl;
    textp << "      p|P|profile|Profile top $N $counter_name [$percentile]" << std::endl;

    textpd << "NAME:" << std::endl;
    textpd << "  profiler data - get appointed data, using by pjs" << std::endl;
    textpd << "SYNOPSIS:" << std::endl;
    textpd << "  pd|PD|profiledata|ProfileData $task_name:$counter_name:$percentile ..."
           << std::endl;
    textpd << "  pd|PD|profiledata|ProfileData $task_name:AllPercentile:$percentile" << std::endl;

    textquery << "NAME:" << std::endl;
    textquery << "  query profiler data in batch" << std::endl;
    textquery << "SYNOPSIS:" << std::endl;
    textquery << "  show a matrix for all task codes contained with perf_counter percentile value"
              << std::endl;
    textquery << "    profiler.query|pq table" << std::endl;
    textquery << "  show a list of names of all task codes" << std::endl;
    textquery << "    profiler.query|pq task_list" << std::endl;
    textquery << "  show a list of all perf_counters and 1000 samples for each perf_counter"
              << std::endl;
    textquery << "    profiler.query|pq counter_sample $task" << std::endl;
    textquery << "  show raw counter values for a specific task" << std::endl;
    textquery << "    profiler.query|pq counter_raw $task" << std::endl;
    textquery << "  show 6 types of latency times for a specific task" << std::endl;
    textquery << "    profiler.query|pq counter_calc $task" << std::endl;
    textquery << "  show caller and callee list for a specific task" << std::endl;
    textquery << "    profiler.query|pq call $task" << std::endl;
    textquery << "  show a list of all sharer using the same pool with a specific task"
              << std::endl;
    textquery << "    profiler.query|pq pool_sharer $task" << std::endl;

    textarg << "ARGUMENTS:" << std::endl;
    textarg << "  $percentile : e.g, 50 for latency at 50 percentile, 50(default)|90|95|99|999"
            << std::endl;
    textarg << "  $counter_name :" << std::endl;
    for (int i = 0; i < PERF_COUNTER_COUNT; i++) {
        textarg << "      " << std::setw(data_width) << counter_info_ptr[i]->title << " :";
        for (size_t j = 0; j < counter_info_ptr[i]->keys.size(); j++) {
            textarg << " " << counter_info_ptr[i]->keys[j];
        }
        textarg << std::endl;
    }
    textarg << "  $task : all task code, such as" << std::endl;
    for (int i = 1; i <= dsn::task_code::max() && i <= 10; i++) {
        textarg << "      " << dsn::task_code(i).to_string() << std::endl;
    }

    textp << textarg.str();
    textpjs << textarg.str();
    textpd << textarg.str();
    textquery << textarg.str();

    command_manager::instance().register_command({"p", "P", "profile", "Profile"},
                                                 "profile|Profile|p|P - performance profiling",
                                                 textp.str().c_str(),
                                                 profiler_output_handler);

    command_manager::instance().register_command({"pd", "PD", "profiledata", "ProfileData"},
                                                 "profiler data - get appointed data, using by pjs",
                                                 textpd.str().c_str(),
                                                 profiler_data_handler);
    command_manager::instance().register_command(
        {"profiler.query", "pq"},
        "profiler.query|pq - query profiling data, output in json format",
        textquery.str().c_str(),
        query_data_handler);
}

void profiler::install(service_spec &)
{
    s_task_code_max = dsn::task_code::max();
    s_spec_profilers.reset(new task_spec_profiler[s_task_code_max + 1]);
    task_ext_for_profiler::register_ext();
    message_ext_for_profiler::register_ext();
    dassert(sizeof(counter_info_ptr) / sizeof(counter_info *) == PERF_COUNTER_COUNT,
            "PREF COUNTER ERROR");

    auto profile = dsn_config_get_value_bool(
        "task..default", "is_profile", false, "whether to profile this kind of task");
    auto collect_call_count = dsn_config_get_value_bool(
        "task..default",
        "collect_call_count",
        true,
        "whether to collect how many time this kind of tasks invoke each of other kinds tasks");

    for (int i = 0; i <= s_task_code_max; i++) {
        if (i == TASK_CODE_INVALID)
            continue;

        std::string name(dsn::task_code(i).to_string());
        std::string section_name = std::string("task.") + name;
        task_spec *spec = task_spec::get(i);
        dassert(spec != nullptr, "task_spec cannot be null");

        s_spec_profilers[i].collect_call_count = dsn_config_get_value_bool(
            section_name.c_str(),
            "collect_call_count",
            collect_call_count,
            "whether to collect how many time this kind of tasks invoke each of other kinds tasks");
        s_spec_profilers[i].call_counts = new std::atomic<int64_t>[ s_task_code_max + 1 ];
        std::fill(s_spec_profilers[i].call_counts,
                  s_spec_profilers[i].call_counts + s_task_code_max + 1,
                  0);

        s_spec_profilers[i].is_profile = dsn_config_get_value_bool(
            section_name.c_str(), "is_profile", profile, "whether to profile this kind of task");

        if (!s_spec_profilers[i].is_profile)
            continue;

        if (dsn_config_get_value_bool(
                section_name.c_str(),
                "profiler::inqueue",
                true,
                "whether to profile the number of this kind of tasks in all queues"))
            s_spec_profilers[i].ptr[TASK_IN_QUEUE].init_global_counter(
                "zion",
                "profiler",
                (name + std::string(".inqueue")).c_str(),
                COUNTER_TYPE_NUMBER,
                "task number in all queues");

        if (dsn_config_get_value_bool(section_name.c_str(),
                                      "profiler::queue",
                                      true,
                                      "whether to profile the queuing time of a task"))
            s_spec_profilers[i].ptr[TASK_QUEUEING_TIME_NS].init_global_counter(
                "zion",
                "profiler",
                (name + std::string(".queue(ns)")).c_str(),
                COUNTER_TYPE_NUMBER_PERCENTILES,
                "latency due to waiting in the queue");

        if (dsn_config_get_value_bool(section_name.c_str(),
                                      "profiler::exec",
                                      true,
                                      "whether to profile the executing time of a task"))
            s_spec_profilers[i].ptr[TASK_EXEC_TIME_NS].init_global_counter(
                "zion",
                "profiler",
                (name + std::string(".exec(ns)")).c_str(),
                COUNTER_TYPE_NUMBER_PERCENTILES,
                "latency due to executing tasks");

        if (dsn_config_get_value_bool(section_name.c_str(),
                                      "profiler::qps",
                                      true,
                                      "whether to profile the qps of a task"))
            s_spec_profilers[i].ptr[TASK_THROUGHPUT].init_global_counter(
                "zion",
                "profiler",
                (name + std::string(".qps")).c_str(),
                COUNTER_TYPE_RATE,
                "task numbers per second");

        if (dsn_config_get_value_bool(section_name.c_str(),
                                      "profiler::cancelled",
                                      true,
                                      "whether to profile the cancelled times of a task"))
            s_spec_profilers[i].ptr[TASK_CANCELLED].init_global_counter(
                "zion",
                "profiler",
                (name + std::string(".cancelled")).c_str(),
                COUNTER_TYPE_NUMBER,
                "cancelled times of a specific task type");

        if (spec->type == dsn_task_type_t::TASK_TYPE_RPC_REQUEST) {
            if (dsn_config_get_value_bool(section_name.c_str(),
                                          "profiler::latency.server",
                                          true,
                                          "whether to profile the server latency of a task")) {
                s_spec_profilers[i].ptr[RPC_SERVER_LATENCY_NS].init_global_counter(
                    "zion",
                    "profiler",
                    (name + std::string(".latency.server")).c_str(),
                    COUNTER_TYPE_NUMBER_PERCENTILES,
                    "latency from enqueue point to reply point on the server side for RPC "
                    "tasks");
            }
            if (dsn_config_get_value_bool(section_name.c_str(),
                                          "profiler::size.request.server",
                                          false,
                                          "whether to profile the size per request")) {
                s_spec_profilers[i].ptr[RPC_SERVER_SIZE_PER_REQUEST_IN_BYTES].init_global_counter(
                    "zion",
                    "profiler",
                    (name + std::string(".size.request.server")).c_str(),
                    COUNTER_TYPE_NUMBER_PERCENTILES,
                    "");
            }
            if (dsn_config_get_value_bool(section_name.c_str(),
                                          "profiler::size.response.server",
                                          false,
                                          "whether to profile the size per response")) {
                s_spec_profilers[i].ptr[RPC_SERVER_SIZE_PER_RESPONSE_IN_BYTES].init_global_counter(
                    "zion",
                    "profiler",
                    (name + std::string(".size.response.server")).c_str(),
                    COUNTER_TYPE_NUMBER_PERCENTILES,
                    "");
            }
        } else if (spec->type == dsn_task_type_t::TASK_TYPE_RPC_RESPONSE) {
            if (dsn_config_get_value_bool(section_name.c_str(),
                                          "profiler::latency.client",
                                          true,
                                          "whether to profile the client latency of a task"))
                s_spec_profilers[i].ptr[RPC_CLIENT_NON_TIMEOUT_LATENCY_NS].init_global_counter(
                    "zion",
                    "profiler",
                    (name + std::string(".latency.client(ns)")).c_str(),
                    COUNTER_TYPE_NUMBER_PERCENTILES,
                    "latency from call point to enqueue point on the client side for RPC "
                    "tasks");
            if (dsn_config_get_value_bool(section_name.c_str(),
                                          "profiler::timeout.qps",
                                          true,
                                          "whether to profile the timeout qps of a task"))
                s_spec_profilers[i].ptr[RPC_CLIENT_TIMEOUT_THROUGHPUT].init_global_counter(
                    "zion",
                    "profiler",
                    (name + std::string(".timeout.qps")).c_str(),
                    COUNTER_TYPE_RATE,
                    "time-out task numbers per second for RPC tasks");
        } else if (spec->type == dsn_task_type_t::TASK_TYPE_AIO) {
            if (dsn_config_get_value_bool(section_name.c_str(),
                                          "profiler::latency",
                                          true,
                                          "whether to profile the latency of an AIO task"))
                s_spec_profilers[i].ptr[AIO_LATENCY_NS].init_global_counter(
                    "zion",
                    "profiler",
                    (name + std::string(".latency(ns)")).c_str(),
                    COUNTER_TYPE_NUMBER_PERCENTILES,
                    "latency from call point to enqueue point for AIO tasks");
        }

        // we don't use perf_counter_ptr but perf_counter* in ptr[xxx] to avoid unnecessary memory
        // access cost
        // we need to add reference so that the counters won't go
        // release_ref should be done when the profiler exits (which never happens right now so we
        // omit that for the time being)
        for (size_t j = 0; j < sizeof(s_spec_profilers[i].ptr) / sizeof(perf_counter *); j++) {
            if (s_spec_profilers[i].ptr[j].get() != nullptr) {
                s_spec_profilers[i].ptr[j]->add_ref();
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
        spec->on_rpc_create_response.put_back(profiler_on_rpc_create_response, "profiler");
        spec->on_rpc_reply.put_back(profiler_on_rpc_reply, "profiler");
        spec->on_rpc_response_enqueue.put_back(profiler_on_rpc_response_enqueue, "profiler");
    }

    register_command_profiler();
}

profiler::profiler(const char *name) : toollet(name) {}

} // namespace tools
} // namespace dsn
