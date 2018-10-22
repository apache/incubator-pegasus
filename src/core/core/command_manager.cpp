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

#include <iostream>
#include <thread>
#include <sstream>

#include <dsn/utility/utils.h>
#include <dsn/cpp/service_app.h>
#include <dsn/tool-api/command_manager.h>

namespace dsn {

dsn_handle_t command_manager::register_app_command(const std::vector<std::string> &commands,
                                                   const std::string &help_one_line,
                                                   const std::string &help_long,
                                                   command_handler handler)
{
    std::string app_tag = std::string(service_app::current_service_app_info().full_name) + ".";
    std::vector<std::string> commands_with_app_tag;
    commands_with_app_tag.reserve(commands.size());
    for (const std::string &c : commands) {
        commands_with_app_tag.push_back(app_tag + c);
    }
    return register_command(
        commands_with_app_tag, app_tag + help_one_line, app_tag + help_long, handler);
}

dsn_handle_t command_manager::register_command(const std::vector<std::string> &commands,
                                               const std::string &help_one_line,
                                               const std::string &help_long,
                                               command_handler handler)
{
    utils::auto_write_lock l(_lock);

    for (const std::string &cmd : commands) {
        if (!cmd.empty()) {
            auto it = _handlers.find(cmd);
            dassert(it == _handlers.end(), "command '%s' already regisered", cmd.c_str());
        }
    }

    command_instance *c = new command_instance();
    c->commands = commands;
    c->help_long = help_long;
    c->help_short = help_one_line;
    c->handler = handler;
    _commands.push_back(c);

    for (const std::string &cmd : commands) {
        if (!cmd.empty()) {
            _handlers[cmd] = c;
        }
    }
    return c;
}

void command_manager::deregister_command(dsn_handle_t handle)
{
    auto c = reinterpret_cast<command_instance *>(handle);
    dassert(c != nullptr, "cannot deregister a null handle");
    utils::auto_write_lock l(_lock);
    for (const std::string &cmd : c->commands) {
        ddebug("unregister command: %s", cmd.c_str());
        _handlers.erase(cmd);
    }
    std::remove(_commands.begin(), _commands.end(), c);
    delete c;
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
    register_command({"help", "h", "H", "Help"},
                     "help|Help|h|H [command] - display help information",
                     "",
                     [this](const std::vector<std::string> &args) {
                         std::stringstream ss;

                         if (args.size() == 0) {
                             utils::auto_read_lock l(_lock);
                             for (auto c : this->_commands) {
                                 ss << c->help_short << std::endl;
                             }
                         } else {
                             utils::auto_read_lock l(_lock);
                             auto it = _handlers.find(args[0]);
                             if (it == _handlers.end())
                                 ss << "cannot find command '" << args[0] << "'";
                             else {
                                 ss.width(6);
                                 ss << std::left << it->first << ": " << it->second->help_short
                                    << std::endl
                                    << it->second->help_long << std::endl;
                             }
                         }

                         return ss.str();
                     });

    register_command(
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
        });
}

command_manager::~command_manager()
{
    for (command_instance *c : _commands) {
        delete c;
    }
}

} // namespace dsn
