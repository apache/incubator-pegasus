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

#include "utils/command_manager.h"

#include <fmt/format.h>
// IWYU pragma: no_include <ext/alloc_traits.h>
#include <stdlib.h>
#include <algorithm>
#include <chrono>
#include <limits>
#include <sstream> // IWYU pragma: keep
#include <thread>
#include <type_traits>
#include <unordered_set>
#include <utility>

#include "gutil/map_util.h"

namespace dsn {

std::unique_ptr<command_deregister>
command_manager::register_command(const std::vector<std::string> &commands,
                                  const std::string &help,
                                  const std::string &args,
                                  command_handler handler)
{
    auto ch = std::make_shared<commands_handler>();
    ch->commands = commands;
    ch->help = help;
    ch->args = args;
    ch->handler = std::move(handler);

    utils::auto_write_lock l(_lock);
    for (const auto &cmd : commands) {
        CHECK(!cmd.empty(), "should not register empty command");
        gutil::InsertOrDie(&_handler_by_cmd, cmd, ch);
    }

    return std::make_unique<command_deregister>(reinterpret_cast<uintptr_t>(ch.get()));
}

std::unique_ptr<command_deregister> command_manager::register_bool_command(
    bool &value, const std::string &command, const std::string &help)
{
    return register_single_command(command,
                                   help,
                                   fmt::format("<true|false>"),
                                   [&value, command](const std::vector<std::string> &args) {
                                       return set_bool(value, command, args);
                                   });
}

std::unique_ptr<command_deregister>
command_manager::register_single_command(const std::string &command,
                                         const std::string &help,
                                         const std::string &args,
                                         command_handler handler)
{
    return register_command({command},
                            fmt::format("{} - {}", command, help),
                            fmt::format("{} {}", command, args),
                            handler);
}

std::unique_ptr<command_deregister>
command_manager::register_multiple_commands(const std::vector<std::string> &commands,
                                            const std::string &help,
                                            const std::string &args,
                                            command_handler handler)
{
    return register_command(commands,
                            fmt::format("{} - {}", fmt::join(commands, "|"), help),
                            fmt::format("{} {}", fmt::join(commands, "|"), args),
                            handler);
}

void command_manager::deregister_command(uintptr_t cmd_id)
{
    const auto ch = reinterpret_cast<commands_handler *>(cmd_id);
    CHECK_NOTNULL(ch, "cannot deregister a null command id");
    utils::auto_write_lock l(_lock);
    for (const auto &cmd : ch->commands) {
        _handler_by_cmd.erase(cmd);
    }
}

void command_manager::add_global_cmd(std::unique_ptr<command_deregister> cmd)
{
    utils::auto_write_lock l(_lock);
    _cmds.push_back(std::move(cmd));
}

bool command_manager::run_command(const std::string &cmd,
                                  const std::vector<std::string> &args,
                                  /*out*/ std::string &output)
{
    std::shared_ptr<commands_handler> ch;
    {
        utils::auto_read_lock l(_lock);
        ch = gutil::FindPtrOrNull(_handler_by_cmd, cmd);
    }

    if (!ch) {
        output = fmt::format("unknown command '{}'", cmd);
        return false;
    }

    output = ch->handler(args);
    return true;
}

std::string command_manager::set_bool(bool &value,
                                      const std::string &name,
                                      const std::vector<std::string> &args)
{
    nlohmann::json msg;
    msg["error"] = "ok";
    // Query.
    if (args.empty()) {
        msg[name] = value ? "true" : "false";
        return msg.dump(2);
    }

    // Invalid arguments size.
    if (args.size() > 1) {
        msg["error"] = "ERR: invalid arguments, only one boolean argument is acceptable";
        return msg.dump(2);
    }

    // Invalid argument.
    bool new_value;
    if (!dsn::buf2bool(args[0], new_value, /* ignore_case */ true)) {
        msg["error"] = fmt::format("ERR: invalid arguments, '{}' is not a boolean", args[0]);
        return msg.dump(2);
    }

    // Set to a new value.
    value = new_value;
    LOG_INFO("set {} to {} by remote command", name, new_value);

    return msg.dump(2);
}

command_manager::command_manager()
{
    _cmds.emplace_back(register_multiple_commands(
        {"help", "h", "H", "Help"},
        "Display help information",
        "[command]",
        [this](const std::vector<std::string> &args) {
            std::stringstream ss;
            if (args.empty()) {
                std::unordered_set<commands_handler *> chs;
                utils::auto_read_lock l(_lock);
                for (const auto &[_, ch] : _handler_by_cmd) {
                    // Multiple commands with the same handler are print only once.
                    if (gutil::InsertIfNotPresent(&chs, ch.get())) {
                        ss << ch->help << std::endl;
                    }
                }
            } else {
                utils::auto_read_lock l(_lock);
                const auto ch = gutil::FindPtrOrNull(_handler_by_cmd, args[0]);
                if (!ch) {
                    ss << "cannot find command '" << args[0] << "'";
                } else {
                    ss.width(6);
                    ss << std::left << ch->help << std::endl << ch->args << std::endl;
                }
            }

            return ss.str();
        }));

    _cmds.emplace_back(register_multiple_commands(
        {"repeat", "r", "R", "Repeat"},
        "Execute a command periodically in every interval seconds for the max count time (0 for "
        "infinite)",
        "<interval_seconds> <max_count> <command>",
        [this](const std::vector<std::string> &args) {
            std::stringstream ss;
            if (args.size() < 3) {
                return "insufficient arguments";
            }

            int interval_seconds = atoi(args[0].c_str());
            if (interval_seconds <= 0) {
                return "invalid interval argument";
            }

            uint32_t max_count;
            if (!dsn::buf2uint32(args[1], max_count)) {
                return "invalid max count";
            }

            if (max_count == 0) {
                max_count = std::numeric_limits<uint32_t>::max();
            }

            const auto &command = args[2];
            std::vector<std::string> command_args;
            for (size_t i = 3; i < args.size(); i++) {
                command_args.push_back(args[i]);
            }

            // TODO(yingchun): the 'repeat' command may last long time (or even infinity), it's
            //  easy to timeout, the remote_command timeout is a fixed value of 5 seconds (see
            //  call_remote_command()), and it also consumes thread resource on server side.
            for (uint32_t i = 0; i < max_count; i++) {
                std::string output;
                auto r = this->run_command(command, command_args, output);
                if (!r) {
                    break;
                }

                std::this_thread::sleep_for(std::chrono::seconds(interval_seconds));
            }

            return "repeat command completed";
        }));
}

command_manager::~command_manager()
{
    _cmds.clear();
    CHECK(_handler_by_cmd.empty(),
          "All commands must be deregistered before command_manager is destroyed, however '{}' is "
          "still registered",
          _handler_by_cmd.begin()->first);
}

} // namespace dsn
