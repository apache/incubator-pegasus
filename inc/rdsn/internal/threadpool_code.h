/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation

 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.

 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
# pragma once

# include <string>
# include <rdsn/internal/enum_helper.h>
# include <rdsn/internal/customizable_id.h>
# include <rdsn/internal/configuration.h>

namespace rdsn {

enum worker_priority_t
{
    THREAD_xPRIORITY_LOWEST,
    THREAD_xPRIORITY_BELOW_NORMAL,
    THREAD_xPRIORITY_NORMAL,
    THREAD_xPRIORITY_ABOVE_NORMAL,
    THREAD_xPRIORITY_HIGHEST,
    THREAD_xPRIORITY_COUNT,
    THREAD_xPRIORITY_INVALID,
};

ENUM_BEGIN(worker_priority_t, THREAD_xPRIORITY_INVALID)
    ENUM_REG(THREAD_xPRIORITY_LOWEST)
    ENUM_REG(THREAD_xPRIORITY_BELOW_NORMAL)
    ENUM_REG(THREAD_xPRIORITY_NORMAL)
    ENUM_REG(THREAD_xPRIORITY_ABOVE_NORMAL)
    ENUM_REG(THREAD_xPRIORITY_HIGHEST)
ENUM_END(worker_priority_t)

DEFINE_CUSTOMIZED_ID_TYPE(threadpool_code)

#define DEFINE_THREAD_POOL_CODE(x) DEFINE_CUSTOMIZED_ID(rdsn::threadpool_code, x)

DEFINE_THREAD_POOL_CODE(THREAD_POOL_INVALID)
DEFINE_THREAD_POOL_CODE(THREAD_POOL_DEFAULT)

struct threadpool_spec
{
    std::string             name;
    threadpool_code          pool_code;
    bool                    run;
    int                     worker_count;
    worker_priority_t       worker_priority;
    bool                    worker_share_core;
    uint64_t                worker_affinity_mask;
    unsigned int            max_input_queue_length; // INFINITE by default
    bool                    partitioned;         // false by default
    std::string             queue_factory_name;
    std::string             worker_factory_name;
    std::list<std::string>  queue_aspects;
    std::list<std::string>  worker_aspects;
    std::string             admission_controller_factory_name;
    std::string             admission_controller_arguments;

    threadpool_spec(const threadpool_code& code) : pool_code(code), name(code.to_string()) {}
    threadpool_spec(const char* name) : pool_code(name), name(name) {}
    threadpool_spec(const threadpool_spec& source);
    threadpool_spec& operator=(const threadpool_spec& source);

    static bool init(configuration_ptr& config, __out_param std::vector<threadpool_spec>& specs);
};

} // end namespace
