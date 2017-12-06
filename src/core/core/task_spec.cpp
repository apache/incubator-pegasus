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
 *     What is this file about?
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include <dsn/tool-api/task_spec.h>
#include <dsn/utility/singleton.h>
#include <dsn/tool-api/perf_counter.h>
#include <dsn/tool-api/command_manager.h>
#include <sstream>
#include <vector>
#include <thread>

#ifdef __TITLE__
#undef __TITLE__
#endif
#define __TITLE__ "task_spec"

namespace dsn {

void task_spec::register_task_code(dsn_task_code_t code,
                                   dsn_task_type_t type,
                                   dsn_task_priority_t pri,
                                   dsn::threadpool_code pool)
{
    dassert(pool != THREAD_POOL_INVALID,
            "registered pool cannot be THREAD_POOL_INVALID for task %s, "
            "make sure it is registered AFTER the pool is registered",
            dsn_task_code_to_string(code));

    if (!dsn::utils::singleton_vector_store<task_spec *, nullptr>::instance().contains(code)) {
        task_spec *spec = new task_spec(code, dsn_task_code_to_string(code), type, pri, pool);
        dsn::utils::singleton_vector_store<task_spec *, nullptr>::instance().put(code, spec);

        if (type == TASK_TYPE_RPC_REQUEST) {
            std::string ack_name = std::string(dsn_task_code_to_string(code)) + std::string("_ACK");
            auto ack_code =
                dsn_task_code_register(ack_name.c_str(), TASK_TYPE_RPC_RESPONSE, pri, pool);
            spec->rpc_paired_code = ack_code;
            task_spec::get(ack_code)->rpc_paired_code = code;
        }
    } else {
        auto spec = task_spec::get(code);
        if (spec->type != type) {
            dassert(
                false,
                "task code %s registerd for %s, which does not match with previously registered %s",
                dsn_task_code_to_string(code),
                enum_to_string(type),
                enum_to_string(spec->type));
            return;
        }

        if (spec->priority != pri) {
            dwarn("overwrite priority for task %s from %s to %s",
                  dsn_task_code_to_string(code),
                  enum_to_string(spec->priority),
                  enum_to_string(pri));
            spec->priority = pri;
        }

        if (spec->pool_code != pool) {
            dwarn("overwrite default thread pool for task %s from %s to %s",
                  dsn_task_code_to_string(code),
                  spec->pool_code.to_string(),
                  pool.to_string());
            spec->pool_code = pool;
        }
    }
}

task_spec *task_spec::get(int code)
{
    return dsn::utils::singleton_vector_store<task_spec *, nullptr>::instance().get(code);
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
      on_rpc_reply((std::string(name) + std::string(".rpc.reply")).c_str()),
      on_rpc_response_enqueue((std::string(name) + std::string(".rpc.response.enqueue")).c_str()),
      on_rpc_create_response((std::string(name) + std::string("rpc.create.response")).c_str())
{
    dassert(strlen(name) < DSN_MAX_TASK_CODE_NAME_LENGTH,
            "task code name '%s' is too long: length must be smaller than "
            "DSN_MAX_TASK_CODE_NAME_LENGTH (%u)",
            name,
            DSN_MAX_TASK_CODE_NAME_LENGTH);

    rejection_handler = nullptr;
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

    for (int code = 0; code <= dsn_task_code_max(); code++) {
        if (code == TASK_CODE_INVALID)
            continue;

        std::string section_name =
            std::string("task.") + std::string(dsn_task_code_to_string(code));
        task_spec *spec = task_spec::get(code);
        dassert(spec != nullptr, "task_spec cannot be null");

        if (!read_config(section_name.c_str(), *spec, &default_spec))
            return false;

        if (code == TASK_CODE_EXEC_INLINED) {
            spec->allow_inline = true;
        }

        dassert(spec->rpc_request_delays_milliseconds.size() == 0 ||
                    spec->rpc_request_delays_milliseconds.size() == 6,
                "invalid length of rpc_request_delays_milliseconds, must be of length 6");
        if (spec->rpc_request_delays_milliseconds.size() > 0) {
            spec->rpc_request_delayer.initialize(spec->rpc_request_delays_milliseconds);
        }

        if (spec->rpc_request_throttling_mode != TM_NONE) {
            if (spec->type != TASK_TYPE_RPC_REQUEST) {
                derror("%s: only rpc request type can have non TM_NONE throttling_mode",
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

            for (int code = 0; code <= dsn_task_code_max(); code++) {
                if (code == TASK_CODE_INVALID)
                    continue;

                std::string codes = dsn_task_code_to_string(code);
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
    admission_controller_factory_name = xxx
    admission_controller_arguments = xxx

    [threadpool.THREAD_POOL_REPLICATION]
    name = Thr.replication
    run = true
    worker_count = 4
    worker_priority = THREAD_xPRIORITY_NORMAL
    partitioned = false
    queue_aspects = xxx
    worker_aspects = xxx
    admission_controller_factory_name = xxx
    admission_controller_arguments = xxx
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
