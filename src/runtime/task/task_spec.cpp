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

#include "task_spec.h"

#include <array>

#include "runtime/rpc/rpc_message.h"
#include "utils/fmt_logging.h"
#include "utils/command_manager.h"
#include "utils/threadpool_spec.h"
#include "utils/smart_pointers.h"

namespace dsn {

constexpr int TASK_SPEC_STORE_CAPACITY = 512;

std::set<dsn::task_code> &get_storage_rpc_req_codes()
{
    static std::set<dsn::task_code> s_storage_rpc_req_codes;
    return s_storage_rpc_req_codes;
}

// A sequential storage maps task_code to task_spec.
static std::array<std::unique_ptr<task_spec>, TASK_SPEC_STORE_CAPACITY> s_task_spec_store;

void task_spec::register_task_code(task_code code,
                                   dsn_task_type_t type,
                                   dsn_task_priority_t pri,
                                   dsn::threadpool_code pool)
{
    CHECK_GE(code, 0);
    CHECK_LT(code, TASK_SPEC_STORE_CAPACITY);
    if (!s_task_spec_store[code]) {
        s_task_spec_store[code] = make_unique<task_spec>(code, code.to_string(), type, pri, pool);
        auto &spec = s_task_spec_store[code];

        if (type == TASK_TYPE_RPC_REQUEST) {
            std::string ack_name = std::string(code.to_string()) + std::string("_ACK");
            // for a rpc request, we firstly register it's ack code to invalid threadpool,
            // then the response code's definition will reassign a proper valid threadpool code.
            // please refer to the DEFINE_TASK_CODE_RPC/DEFINE_STORAGE_RPC_CODE in task_code.h
            // for more details.
            dsn::task_code ack_code(
                ack_name.c_str(), TASK_TYPE_RPC_RESPONSE, pri, THREAD_POOL_INVALID);
            spec->rpc_paired_code = ack_code;
            task_spec::get(ack_code.code())->rpc_paired_code = code;
        }
    } else {
        auto spec = task_spec::get(code);
        CHECK_EQ_MSG(
            spec->type,
            type,
            "task code {} registerd for {}, which does not match with previously registered {}",
            code,
            enum_to_string(type),
            enum_to_string(spec->type));

        if (spec->priority != pri) {
            LOG_WARNING("overwrite priority for task %s from %s to %s",
                        code.to_string(),
                        enum_to_string(spec->priority),
                        enum_to_string(pri));
            spec->priority = pri;
        }

        if (spec->pool_code != pool) {
            if (spec->pool_code != THREAD_POOL_INVALID) {
                LOG_WARNING("overwrite default thread pool for task %s from %s to %s",
                            code.to_string(),
                            spec->pool_code.to_string(),
                            pool.to_string());
            }
            spec->pool_code = pool;
        }
    }
}

void task_spec::register_storage_task_code(task_code code,
                                           dsn_task_type_t type,
                                           dsn_task_priority_t pri,
                                           threadpool_code pool,
                                           bool is_write_operation,
                                           bool allow_batch,
                                           bool is_idempotent)
{
    register_task_code(code, type, pri, pool);
    task_spec *spec = task_spec::get(code);
    spec->rpc_request_for_storage = true;
    spec->rpc_request_is_write_operation = is_write_operation;
    spec->rpc_request_is_write_allow_batch = allow_batch;
    spec->rpc_request_is_write_idempotent = is_idempotent;
    if (TASK_TYPE_RPC_REQUEST == type) {
        get_storage_rpc_req_codes().insert(code);
    }
}

task_spec *task_spec::get(int code)
{
    CHECK_GE(code, 0);
    CHECK_LT(code, TASK_SPEC_STORE_CAPACITY);
    return s_task_spec_store[code].get();
}

task_spec::task_spec(int code,
                     const char *name,
                     dsn_task_type_t type,
                     dsn_task_priority_t pri,
                     dsn::threadpool_code pool)
    : code(code),
      type(type),
      name(name),
      rpc_paired_code(TASK_CODE_INVALID),
      rpc_request_for_storage(false),
      rpc_request_is_write_operation(false),
      rpc_request_is_write_allow_batch(false),
      rpc_request_is_write_idempotent(false),
      priority(pri),
      pool_code(pool),
      rpc_call_header_format(NET_HDR_DSN),
      rpc_call_channel(RPC_CHANNEL_TCP),
      rpc_message_crc_required(false),
      on_task_create((std::string(name) + std::string(".create")).c_str()),
      on_task_enqueue((std::string(name) + std::string(".enqueue")).c_str()),
      on_task_begin((std::string(name) + std::string(".begin")).c_str()),
      on_task_end((std::string(name) + std::string(".end")).c_str()),
      on_task_cancelled((std::string(name) + std::string(".cancelled")).c_str()),

      on_task_wait_pre((std::string(name) + std::string(".wait.pre")).c_str()),
      on_task_wait_notified((std::string(name) + std::string(".wait.notified")).c_str()),
      on_task_wait_post((std::string(name) + std::string(".wait.post")).c_str()),
      on_task_cancel_post((std::string(name) + std::string(".cancel.post")).c_str()),

      on_aio_call((std::string(name) + std::string(".aio.call")).c_str()),
      on_aio_enqueue((std::string(name) + std::string(".aio.enqueue")).c_str()),

      on_rpc_call((std::string(name) + std::string(".rpc.call")).c_str()),
      on_rpc_request_enqueue((std::string(name) + std::string(".rpc.request.enqueue")).c_str()),
      on_rpc_task_dropped((std::string(name) + std::string(".dropped")).c_str()),
      on_rpc_reply((std::string(name) + std::string(".rpc.reply")).c_str()),
      on_rpc_response_enqueue((std::string(name) + std::string(".rpc.response.enqueue")).c_str()),
      on_rpc_create_response((std::string(name) + std::string(".rpc.response.create")).c_str())
{
    CHECK_LT_MSG(strlen(name),
                 DSN_MAX_TASK_CODE_NAME_LENGTH,
                 "task code name '{}' is too long: length must be smaller than "
                 "DSN_MAX_TASK_CODE_NAME_LENGTH ({})",
                 name,
                 DSN_MAX_TASK_CODE_NAME_LENGTH);

    rpc_call_channel = RPC_CHANNEL_TCP;
    rpc_timeout_milliseconds = 5 * 1000; // 5 seconds
}

bool task_spec::init()
{
    /*
    [task..default]
    is_trace = false
    is_profile = false

    [task.RPC_PREPARE]
    pool_code = THREAD_POOL_REPLICATION
    priority = TASK_PRIORITY_HIGH
    is_trace = true
    is_profile = true
    */

    task_spec default_spec(
        0, "placeholder", TASK_TYPE_COMPUTE, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT);
    if (!read_config("task..default", default_spec))
        return false;

    for (int code = 0; code <= dsn::task_code::max(); code++) {
        if (code == TASK_CODE_INVALID)
            continue;

        std::string section_name =
            std::string("task.") + std::string(dsn::task_code(code).to_string());
        task_spec *spec = task_spec::get(code);
        CHECK_NOTNULL(spec, "");

        if (!read_config(section_name.c_str(), *spec, &default_spec))
            return false;

        if (code == TASK_CODE_EXEC_INLINED) {
            spec->allow_inline = true;
        }

        CHECK(spec->rpc_request_delays_milliseconds.size() == 0 ||
                  spec->rpc_request_delays_milliseconds.size() == 6,
              "invalid length of rpc_request_delays_milliseconds, must be of length 6");
        if (spec->rpc_request_delays_milliseconds.size() > 0) {
            spec->rpc_request_delayer.initialize(spec->rpc_request_delays_milliseconds);
        }

        if (spec->rpc_request_throttling_mode != TM_NONE) {
            if (spec->type != TASK_TYPE_RPC_REQUEST) {
                LOG_ERROR("%s: only rpc request type can have non TM_NONE throttling_mode",
                          spec->name.c_str());
                return false;
            }
        }
    }

    ::dsn::command_manager::instance().register_command(
        {"task-code"},
        "task-code - query task code containing any given keywords",
        "task-code keyword1 keyword2 ...",
        [](const std::vector<std::string> &args) {
            std::stringstream ss;

            for (int code = 0; code <= dsn::task_code::max(); code++) {
                if (code == TASK_CODE_INVALID)
                    continue;

                std::string codes = dsn::task_code(code).to_string();
                if (args.size() == 0) {
                    ss << "    " << codes << std::endl;
                } else {
                    for (auto &arg : args) {
                        if (codes.find(arg.c_str()) != std::string::npos) {
                            ss << "    " << codes << std::endl;
                        }
                    }
                }
            }
            return ss.str();
        });

    return true;
}

bool threadpool_spec::init(/*out*/ std::vector<threadpool_spec> &specs)
{
    /*
    [threadpool..default]
    worker_count = 4
    worker_priority = THREAD_xPRIORITY_NORMAL
    partitioned = false
    queue_aspects = xxx
    worker_aspects = xxx

    [threadpool.THREAD_POOL_REPLICATION]
    name = Thr.replication
    run = true
    worker_count = 4
    worker_priority = THREAD_xPRIORITY_NORMAL
    partitioned = false
    queue_aspects = xxx
    worker_aspects = xxx
    */

    threadpool_spec default_spec(THREAD_POOL_INVALID);
    if (false == read_config("threadpool..default", default_spec, nullptr))
        return false;

    default_spec.name = "";
    specs.clear();
    for (int code = 0; code <= threadpool_code::max(); code++) {
        std::string code_name = std::string(threadpool_code(code).to_string());
        std::string section_name = std::string("threadpool.") + code_name;
        threadpool_spec spec(default_spec);
        if (false == read_config(section_name.c_str(), spec, &default_spec))
            return false;

        spec.pool_code = threadpool_code(code);

        if ("" == spec.name)
            spec.name = code_name;

        if (false == spec.worker_share_core && 0 == spec.worker_affinity_mask) {
            spec.worker_affinity_mask = (1 << std::thread::hardware_concurrency()) - 1;
        }

        specs.push_back(spec);
    }

    return true;
}

} // end namespace
