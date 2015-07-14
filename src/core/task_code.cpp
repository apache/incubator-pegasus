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
# include <dsn/internal/task_code.h>
# include <dsn/internal/singleton.h>
# include <dsn/internal/perf_counters.h>
# include <vector>
# include <dsn/internal/logging.h>
# include <dsn/internal/command.h>
# include <sstream>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "task_spec"

namespace dsn {

task_code::task_code(const char* xxx, task_type type, threadpool_code pool, task_priority pri, int rpcPairedCode) 
    : dsn::utils::customized_id<task_code>(xxx)
{
    if (!dsn::utils::singleton_vector_store<task_spec*, nullptr>::instance().contains(*this))
    {
        task_spec* spec = new task_spec(*this, xxx, type, pool, rpcPairedCode, pri);
        dsn::utils::singleton_vector_store<task_spec*, nullptr>::instance().put(*this, spec);
    }
}

task_spec* task_spec::get(int code)
{
    return dsn::utils::singleton_vector_store<task_spec*, nullptr>::instance().get(code);
}

task_spec::task_spec(int code, const char* name, task_type type, threadpool_code pool, int paired_code, task_priority pri)
    : code(code), name(name), type(type), pool_code(pool), rpc_paired_code(paired_code), priority(pri),
    on_task_enqueue((std::string(name) + std::string(".enqueue")).c_str()), 
    on_task_begin((std::string(name) + std::string(".begin")).c_str()), 
    on_task_end((std::string(name) + std::string(".end")).c_str()), 
    on_task_wait_pre((std::string(name) + std::string(".wait.pre")).c_str()), 
    on_task_wait_post((std::string(name) + std::string(".wait.post")).c_str()), 
    on_task_cancel_post((std::string(name) + std::string(".cancel.post")).c_str()), 
    on_task_cancelled((std::string(name) + std::string(".cancelled")).c_str()),
    on_aio_call((std::string(name) + std::string(".aio.call")).c_str()), 
    on_aio_enqueue((std::string(name) + std::string(".aio.enqueue")).c_str()), 
    on_rpc_call((std::string(name) + std::string(".rpc.call")).c_str()), 
    on_rpc_request_enqueue((std::string(name) + std::string(".rpc.request.enqueue")).c_str()),
    on_rpc_reply((std::string(name) + std::string(".rpc.reply")).c_str()), 
    on_rpc_response_enqueue((std::string(name) + std::string(".rpc.response.enqueue")).c_str()),
    on_rpc_create_response((std::string(name) + std::string("rpc.create.response")).c_str()),
    rpc_call_channel(RPC_CHANNEL_TCP),
    rpc_call_header_format(NET_HDR_DSN)
{
    if (paired_code != 0)
    {
        task_spec* pc_spec = task_spec::get(paired_code);
        pc_spec->rpc_paired_code.reset(task_code(code));
    }

    dassert (
        strlen(name) <= MAX_TASK_CODE_NAME_LENGTH, 
        "task code name '%s' is too long: length must not be larger than MAX_TASK_CODE_NAME_LENGTH (%u)", 
        name, MAX_TASK_CODE_NAME_LENGTH
        );

    rejection_handler = nullptr;

    // TODO: config for following values
    rpc_call_channel = RPC_CHANNEL_TCP;
    rpc_timeout_milliseconds = 5 * 1000; // 5 seconds
}

bool task_spec::init(configuration_ptr config)
{
    /*
    [task.default]
    is_trace = false
    is_profile = false

    [task.RPC_PREPARE]
    pool_code = THREAD_POOL_REPLICATION
    priority = TASK_PRIORITY_HIGH
    is_trace = true
    is_profile = true
    */

    task_spec default_spec(0, "placeholder", TASK_TYPE_COMPUTE, THREAD_POOL_DEFAULT, 0, TASK_PRIORITY_COMMON);
    if (!read_config(config, "task.default", default_spec))
        return false;
    
    for (int code = 0; code <= task_code::max_value(); code++)
    {
        if (code == TASK_CODE_INVALID)
            continue;

        std::string section_name = std::string("task.") + std::string(task_code::to_string(code));
        task_spec* spec = task_spec::get(code);
        dassert (spec != nullptr, "task_spec cannot be null");

        if (config->has_section(section_name.c_str()))
        {
            if (!read_config(config, section_name.c_str(), *spec, &default_spec))
                return false;

            auto pool = threadpool_code::from_string(config->get_string_value(section_name.c_str(), "pool_code", spec->pool_code.to_string()).c_str(), THREAD_POOL_INVALID);
            if (pool == THREAD_POOL_INVALID)
            {
                derror("invalid ThreadPool in [%s]", section_name.c_str());
                return false;
            }

            auto pri = enum_from_string(config->get_string_value(section_name.c_str(), "priority", enum_to_string(spec->priority)).c_str(), TASK_PRIORITY_INVALID);
            if (pri == TASK_PRIORITY_INVALID)
            {
                derror("invalid priority in [%s]", section_name.c_str());
                return false;
            }
            
            spec->pool_code.reset(pool);
            spec->priority = pri;                        
            spec->allow_inline = (spec->type != TASK_TYPE_RPC_RESPONSE
                && spec->type != TASK_TYPE_RPC_REQUEST
                && spec->allow_inline);
            spec->fast_execution_in_network_thread = 
                ((spec->type == TASK_TYPE_RPC_RESPONSE || spec->type == TASK_TYPE_RPC_REQUEST)
                && spec->fast_execution_in_network_thread);
        }
        else
        {
            spec->priority = default_spec.priority;
            spec->allow_inline = (spec->type != TASK_TYPE_RPC_RESPONSE 
                && spec->type != TASK_TYPE_RPC_REQUEST
                && default_spec.allow_inline
                );
            spec->fast_execution_in_network_thread =
                ((spec->type == TASK_TYPE_RPC_RESPONSE || spec->type == TASK_TYPE_RPC_REQUEST)
                && default_spec.fast_execution_in_network_thread);
            spec->rpc_call_channel = default_spec.rpc_call_channel;
            spec->rpc_call_header_format = default_spec.rpc_call_header_format;
            spec->rpc_timeout_milliseconds = default_spec.rpc_timeout_milliseconds;
        }
    }

    ::dsn::register_command("task-code", 
        "task-code - query task code containing any given keywords",        
        "task-code keyword1 keyword2 ...",
        [](const std::vector<std::string>& args)
        {
            std::stringstream ss;

            for (int code = 0; code <= task_code::max_value(); code++)
            {
                if (code == TASK_CODE_INVALID)
                    continue;

                std::string codes = task_code::to_string(code);
                if (args.size() == 0)
                {
                    ss << "    " << codes << std::endl;
                }
                else
                {
                    for (auto& arg : args)
                    {
                        if (codes.find(arg.c_str()) != std::string::npos)
                        {
                            ss << "    " << codes << std::endl;
                        }
                    }
                }
                
            }
            return ss.str();
        }
    );

    return true;
}

} // end namespace
