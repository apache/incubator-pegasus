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

#include <iostream>
#include <sstream>
#include <thread>

#include "utils/fmt_logging.h"
#include "utils/logging_provider.h"
#include "utils/smart_pointers.h"
#include "utils/utils.h"

namespace dsn {

std::unique_ptr<command_deregister>
command_manager::register_command(const std::vector<std::string> &commands,
                                  const std::string &help_one_line,
                                  const std::string &help_long,
                                  command_handler handler)
{
    utils::auto_write_lock l(_lock);
    bool is_valid_cmd = false;
    for (const std::string &cmd : commands) {
        if (!cmd.empty()) {
            is_valid_cmd = true;
            CHECK(_handlers.find(cmd) == _handlers.end(), "command '{}' already regisered", cmd);
        }
    }
    CHECK(is_valid_cmd, "should not register empty command");

    command_instance *c = new command_instance();
    c->commands = commands;
    c->help_short = help_one_line;
    c->help_long = help_long;
    c->handler = handler;

    for (const std::string &cmd : commands) {
        if (!cmd.empty()) {
            _handlers[cmd] = c;
        }
    }

    return dsn::make_unique<command_deregister>(reinterpret_cast<uintptr_t>(c));
}

void command_manager::deregister_command(uintptr_t handle)
{
    auto c = reinterpret_cast<command_instance *>(handle);
    CHECK_NOTNULL(c, "cannot deregister a null handle");
    utils::auto_write_lock l(_lock);
    for (const std::string &cmd : c->commands) {
        _handlers.erase(cmd);
    }
}

bool command_manager::run_command(const std::string &cmd,
                                  const std::vector<std::string> &args,
                                  /*out*/ std::string &output)
{
    command_instance *h = nullptr;
    {
        utils::auto_read_lock l(_lock);
        auto it = _handlers.find(cmd);
        if (it != _handlers.end())
            h = it->second;
    }

    if (h == nullptr) {
        output = std::string("unknown command '") + cmd + "'";
        return false;
    } else {
        output = h->handler(args);
        return true;
    }
}

command_manager::command_manager()
{
    _cmds.emplace_back(register_command({"help", "h", "H", "Help"},
                                        "help|Help|h|H [command] - display help information",
                                        "",
                                        [this](const std::vector<std::string> &args) {
                                            std::stringstream ss;

                                            if (args.size() == 0) {
                                                utils::auto_read_lock l(_lock);
                                                for (const auto &c : this->_handlers) {
                                                    ss << c.second->help_short << std::endl;
                                                }
                                            } else {
                                                utils::auto_read_lock l(_lock);
                                                auto it = _handlers.find(args[0]);
                                                if (it == _handlers.end())
                                                    ss << "cannot find command '" << args[0] << "'";
                                                else {
                                                    ss.width(6);
                                                    ss << std::left << it->first << ": "
                                                       << it->second->help_short << std::endl
                                                       << it->second->help_long << std::endl;
                                                }
                                            }

                                            return ss.str();
                                        }));

    _cmds.emplace_back(register_command(
        {"repeat", "r", "R", "Repeat"},
        "repeat|Repeat|r|R interval_seconds max_count command - execute command periodically",
        "repeat|Repeat|r|R interval_seconds max_count command - execute command every interval "
        "seconds, to the max count as max_count (0 for infinite)",
        [this](const std::vector<std::string> &args) {
            std::stringstream ss;

            if (args.size() < 3) {
                return "insufficient arguments";
            }

            int interval_seconds = atoi(args[0].c_str());
            if (interval_seconds <= 0) {
                return "invalid interval argument";
            }

            int max_count = atoi(args[1].c_str());
            if (max_count < 0) {
                return "invalid max count";
            }

            if (max_count == 0) {
                max_count = std::numeric_limits<int>::max();
            }

            std::string cmd = args[2];
            std::vector<std::string> largs;
            for (int i = 3; i < (int)args.size(); i++) {
                largs.push_back(args[i]);
            }

            for (int i = 0; i < max_count; i++) {
                std::string output;
                auto r = this->run_command(cmd, largs, output);

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
    CHECK(_handlers.empty(),
          "All commands must be deregistered before command_manager is destroyed, however {} is "
          "still registered",
          _handlers.begin()->first);
}

} // namespace dsn
