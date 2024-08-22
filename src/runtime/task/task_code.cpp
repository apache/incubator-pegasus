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

#include "task_code.h"

#include <memory>
#include <vector>
#include <sstream> // IWYU pragma: keep

#include "task_spec.h"
#include "utils/command_manager.h"
#include "utils/customizable_id.h"

namespace dsn {

typedef dsn::utils::customized_id_mgr<dsn::task_code> task_code_mgr;

namespace utils {
template <>
void task_code_mgr::register_commands()
{
    _cmds.emplace_back(command_manager::instance().register_single_command(
        "task-code",
        "Query task code containing any given keywords",
        "[keyword1] [keyword2] ...",
        [](const std::vector<std::string> &args) {
            std::stringstream ss;
            for (int code = 0; code <= dsn::task_code::max(); code++) {
                if (code == TASK_CODE_INVALID) {
                    continue;
                }

                const std::string code_str = dsn::task_code(code).to_string();
                if (args.empty()) {
                    ss << "    " << code_str << std::endl;
                } else {
                    for (const auto &arg : args) {
                        if (code_str.find(arg) != std::string::npos) {
                            ss << "    " << code_str << std::endl;
                        }
                    }
                }
            }
            return ss.str();
        }));
}
} // namespace utils

/*static*/
int task_code::max() { return task_code_mgr::instance().max_value(); }

/*static*/
bool task_code::is_exist(const char *name) { return task_code_mgr::instance().get_id(name) != -1; }

/*static*/
task_code task_code::try_get(const char *name, task_code default_value)
{
    int code = task_code_mgr::instance().get_id(name);
    if (code == -1)
        return default_value;
    return task_code(code);
}

/*static*/
task_code task_code::try_get(const std::string &name, task_code default_value)
{
    int code = task_code_mgr::instance().get_id(name);
    if (code == -1)
        return default_value;
    return task_code(code);
}

task_code::task_code(const char *name) : _internal_code(task_code_mgr::instance().register_id(name))
{
}

task_code::task_code(const char *name,
                     dsn_task_type_t tt,
                     dsn_task_priority_t pri,
                     dsn::threadpool_code pool)
    : task_code(name)
{
    task_spec::register_task_code(*this, tt, pri, pool);
}

task_code::task_code(const char *name,
                     dsn_task_type_t tt,
                     dsn_task_priority_t pri,
                     dsn::threadpool_code pool,
                     bool is_storage_write,
                     bool allow_batch,
                     bool is_idempotent)
    : task_code(name)
{
    task_spec::register_storage_task_code(
        *this, tt, pri, pool, is_storage_write, allow_batch, is_idempotent);
}

const char *task_code::to_string() const
{
    return task_code_mgr::instance().get_name(_internal_code);
}
} // namespace dsn
