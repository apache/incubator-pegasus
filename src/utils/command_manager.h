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

#include <map>

#include "utils/api_utilities.h"
#include "utils/autoref_ptr.h"
#include "utils/fmt_logging.h"
#include "utils/ports.h"
#include "utils/singleton.h"
#include "utils/synchronize.h"

namespace dsn {

class command_deregister;

class command_manager : public ::dsn::utils::singleton<command_manager>
{
public:
    typedef std::function<std::string(const std::vector<std::string> &)> command_handler;

    std::unique_ptr<command_deregister>
    register_command(const std::vector<std::string> &commands,
                     const std::string &help_one_line,
                     const std::string &help_long,
                     command_handler handler) WARN_UNUSED_RESULT;

    bool run_command(const std::string &cmd,
                     const std::vector<std::string> &args,
                     /*out*/ std::string &output);

private:
    friend class command_deregister;
    friend class utils::singleton<command_manager>;

    command_manager();
    ~command_manager();

    struct command_instance : public ref_counter
    {
        std::vector<std::string> commands;
        std::string help_short;
        std::string help_long;
        command_handler handler;
    };

    void deregister_command(uintptr_t handle);

    typedef ref_ptr<command_instance> command_instance_ptr;
    utils::rw_lock_nr _lock;
    std::map<std::string, command_instance_ptr> _handlers;

    std::vector<std::unique_ptr<command_deregister>> _cmds;
};

class command_deregister
{
public:
    command_deregister(uintptr_t id) : cmd_id_(id) {}
    ~command_deregister()
    {
        if (cmd_id_ != 0) {
            dsn::command_manager::instance().deregister_command(cmd_id_);
            cmd_id_ = 0;
        }
    }

private:
    uintptr_t cmd_id_ = 0;
};

} // namespace dsn

// if args are empty, then return the old flag;
// otherwise set the proper "flag" according to args
inline std::string remote_command_set_bool_flag(bool &flag,
                                                const char *flag_name,
                                                const std::vector<std::string> &args)
{
    std::string ret_msg("OK");
    if (args.empty()) {
        ret_msg = flag ? "true" : "false";
    } else {
        if (args[0] == "true") {
            flag = true;
            LOG_INFO("set {} to true by remote command", flag_name);
        } else if (args[0] == "false") {
            flag = false;
            LOG_INFO("set {} to false by remote command", flag_name);
        } else {
            ret_msg = "ERR: invalid arguments";
        }
    }
    return ret_msg;
}
