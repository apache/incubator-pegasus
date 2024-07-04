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

#include <fmt/core.h>
// IWYU pragma: no_include <ext/alloc_traits.h>
#include <stdint.h>
#include <functional>
#include <map>
#include <memory>
#include <nlohmann/json.hpp>
#include <nlohmann/json_fwd.hpp>
#include <string>
#include <vector>

#include "utils/autoref_ptr.h"
#include "utils/fmt_logging.h"
#include "utils/ports.h"
#include "utils/singleton.h"
#include "utils/string_conv.h"
#include "utils/strings.h"
#include "utils/synchronize.h"

namespace dsn {

class command_deregister;

class command_manager : public ::dsn::utils::singleton<command_manager>
{
public:
    using command_handler = std::function<std::string(const std::vector<std::string> &)>;

    // Register command which query or update a boolean configuration.
    // The 'value' will be queried or updated by the command named 'command' with the 'help'
    // description.
    std::unique_ptr<command_deregister> register_bool_command(
        bool &value, const std::string &command, const std::string &help) WARN_UNUSED_RESULT;

    // Register command which query or update an integer configuration.
    // The 'value' will be queried or updated by the command named 'command' with the 'help'
    // description.
    // 'validator' is used to validate the new value.
    // The value is reset to 'default_value' if passing "DEFAULT" argument.
    template <typename T>
    WARN_UNUSED_RESULT std::unique_ptr<command_deregister>
    register_int_command(T &value,
                         T default_value,
                         const std::string &command,
                         const std::string &help,
                         std::function<bool(int64_t new_value)> validator =
                             [](int64_t new_value) -> bool { return new_value >= 0; })
    {
        return register_single_command(
            command,
            help,
            fmt::format("[num | DEFAULT]"),
            [&value, default_value, command, validator](const std::vector<std::string> &args) {
                return set_int(value, default_value, command, args, validator);
            });
    }

    // Register a single 'command' with the 'help' description, its arguments are describe in
    // 'args'.
    std::unique_ptr<command_deregister>
    register_single_command(const std::string &command,
                            const std::string &help,
                            const std::string &args,
                            command_handler handler) WARN_UNUSED_RESULT;

    // Register multiple 'commands' with the 'help' description, their arguments are describe in
    // 'args'.
    std::unique_ptr<command_deregister>
    register_multiple_commands(const std::vector<std::string> &commands,
                               const std::string &help,
                               const std::string &args,
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
        std::string help;
        std::string args;
        command_handler handler;
    };

    std::unique_ptr<command_deregister>
    register_command(const std::vector<std::string> &commands,
                     const std::string &help,
                     const std::string &args,
                     command_handler handler) WARN_UNUSED_RESULT;

    void deregister_command(uintptr_t handle);

    static std::string
    set_bool(bool &value, const std::string &name, const std::vector<std::string> &args);

    template <typename T>
    static std::string set_int(T &value,
                               T default_value,
                               const std::string &name,
                               const std::vector<std::string> &args,
                               const std::function<bool(int64_t value)> &validator)
    {
        nlohmann::json msg;
        msg["error"] = "ok";
        // Query.
        if (args.empty()) {
            msg[name] = fmt::format("{}", std::to_string(value));
            return msg.dump(2);
        }

        // Invalid arguments size.
        if (args.size() > 1) {
            msg["error"] = "ERR: invalid arguments, only one integer argument is acceptable";
            return msg.dump(2);
        }

        // Reset to the default value.
        if (dsn::utils::iequals(args[0], "DEFAULT")) {
            value = default_value;
            msg[name] = default_value;
            return msg.dump(2);
        }

        // Invalid argument.
        T new_value = 0;
        if (!internal::buf2signed(args[0], new_value) ||
            !validator(static_cast<int64_t>(new_value))) {
            msg["error"] = "ERR: invalid arguments";
            return msg.dump(2);
        }

        // Set to a new value.
        value = new_value;
        LOG_INFO("set {} to {} by remote command", name, new_value);

        return msg.dump(2);
    }

    typedef ref_ptr<command_instance> command_instance_ptr;
    utils::rw_lock_nr _lock;
    std::map<std::string, command_instance_ptr> _handlers;

    std::vector<std::unique_ptr<command_deregister>> _cmds;
};

class command_deregister
{
public:
    explicit command_deregister(uintptr_t id) : cmd_id_(id) {}
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
