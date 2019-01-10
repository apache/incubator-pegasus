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

#pragma once

#include <dsn/utility/synchronize.h>
#include <dsn/utility/singleton.h>
#include <dsn/c/api_utilities.h>
#include <map>

namespace dsn {

class command_manager : public ::dsn::utils::singleton<command_manager>
{
public:
    typedef std::function<std::string(const std::vector<std::string> &)> command_handler;
    command_manager();

    ~command_manager();

    dsn_handle_t register_command(const std::vector<std::string> &commands,
                                  const std::string &help_one_line,
                                  const std::string &help_long,
                                  command_handler handler);
    dsn_handle_t register_app_command(const std::vector<std::string> &commands,
                                      const std::string &help_one_line,
                                      const std::string &help_long,
                                      command_handler handler);
    void deregister_command(dsn_handle_t handle);
    bool run_command(const std::string &cmd,
                     const std::vector<std::string> &args,
                     /*out*/ std::string &output);

private:
    struct command_instance
    {
        std::vector<std::string> commands;
        std::string help_short;
        std::string help_long;
        command_handler handler;
    };

    ::dsn::utils::rw_lock_nr _lock;
    std::map<std::string, command_instance *> _handlers;
    std::vector<command_instance *> _commands;
};
}

#define UNREGISTER_VALID_HANDLER(ptr)                                                              \
    do {                                                                                           \
        if (ptr != nullptr) {                                                                      \
            dsn::command_manager::instance().deregister_command(ptr);                              \
            ptr = nullptr;                                                                         \
        }                                                                                          \
    } while (0)

// handle bool-flag "flag" with "args": if args are empty, then return the old flag; otherwise
// set the proper "flag" according to args
#define HANDLE_CLI_FLAGS(flag, args)                                                               \
    do {                                                                                           \
        std::string ret_msg("OK");                                                                 \
        if (args.empty()) {                                                                        \
            ret_msg = flag ? "true" : "false";                                                     \
        } else {                                                                                   \
            if (strcmp(args[0].c_str(), "true") == 0) {                                            \
                flag = true;                                                                       \
                ddebug("set " #flag " to true by remote command");                                 \
            } else if (strcmp(args[0].c_str(), "false") == 0) {                                    \
                flag = false;                                                                      \
                ddebug("set " #flag " to false by remote command");                                \
            } else {                                                                               \
                ret_msg = "ERR: invalid arguments";                                                \
            }                                                                                      \
        }                                                                                          \
        return ret_msg;                                                                            \
    } while (0)
