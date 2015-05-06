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
# include "command_manager.h"
# include <iostream>
# include <thread>
# include <dsn/internal/utils.h>
# include <dsn/internal/logging.h>

# define __TITLE__ "command_manager"

namespace dsn {


    void register_command(
        const std::vector<const char*>& commands, // commands, e.g., {"help", "Help", "HELP", "h", "H"}
        const char* help, // help info for users
        command_handler handler
        )
    {
        command_manager::instance().register_command(commands, help, handler);
    }

    void register_command(
        const char* command, // commands, e.g., "help"
        const char* help, // help info for users
        command_handler handler
        )
    {
        std::vector<const char*> cmds;
        cmds.push_back(command);
        register_command(cmds, help, handler);
    }

    void register_command(
        const char** commands, // commands, e.g., {"help", "Help", nullptr}
        const char* help, // help info for users
        command_handler handler
        )
    {
        std::vector<const char*> cmds;
        while (*commands != nullptr)
        {
            cmds.push_back(*commands);
            commands++;
        }

        register_command(cmds, help, handler);
    }

    void command_manager::register_command(const std::vector<const char*>& commands, const char* help, command_handler handler)
    {
        utils::auto_write_lock l(_lock);

        for (auto cmd : commands)
        {
            if (cmd != nullptr)
            {
                auto it = _handlers.find(std::string(cmd));
                dassert(it == _handlers.end(), "command '%s' already regisered", cmd);
            }
        }

        command* c = new command;
        c->help = help;
        c->handler = handler;
        
        for (auto cmd : commands)
        {
            if (cmd != nullptr)
            {
                _handlers[std::string(cmd)] = c;
            }
        }
    }

    bool command_manager::run_command(const std::string& cmd, __out_param std::string& output)
    {
        std::string scmd = cmd;
        std::vector<std::string> args;
        
        utils::split_args(scmd.c_str(), args, ' ');

        if (args.size() < 1)
            return false;

        command* h = nullptr;
        {
            utils::auto_read_lock l(_lock);
            auto it = _handlers.find(args[0]);
            if (it != _handlers.end())
                h = it->second;
        }

        if (h == nullptr)
        {
            output = std::string("unknown command '") + args[0] + "'";
            return false;
        }
        else
        {
            std::vector<std::string> args2;
            for (size_t i = 1; i < args.size(); i++)
            {
                args2.push_back(args[i]);
            }

            output = h->handler(args2);
            return true;
        }
    }

    void command_manager::run_console()
    {
        std::cout << "dsn cli begin ... (type 'help' + Enter to learn more)" << std::endl;

        while (true)
        {
            std::string cmdline;
            std::cout << ">";
            std::getline(std::cin, cmdline);

            std::string result;
            run_command(cmdline, result);
            std::cout << result << std::endl;
        }
    }

    void command_manager::start_local_cli()
    {
        new std::thread(std::bind(&command_manager::run_console, this));
    }

    void command_manager::start_remote_cli()
    {
        // TODO:
    }

    command_manager::command_manager()
    {
        register_command({ "help", "h", "H", "Help", nullptr }, "help", 
            [this](const std::vector<std::string>& args)
            {
                std::stringstream ss;

                utils::auto_read_lock l(_lock);
                for (auto c : this->_handlers)
                {
                    ss << c.first << ": " << c.second->help << std::endl;
                }

                return ss.str();
            }
        );
    }
}
