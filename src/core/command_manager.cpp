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
# include <sstream>
# include <dsn/internal/utils.h>
# include <dsn/internal/logging.h>
# include <dsn/service_api.h>
# include <dsn/internal/serialization.h>
# include "service_engine.h"

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "command_manager"

namespace dsn {


    void register_command(
        const std::vector<const char*>& commands, // commands, e.g., {"help", "Help", "HELP", "h", "H"}
        const char* help_one_line,
        const char* help_long, // help info for users
        command_handler handler
        )
    {
        command_manager::instance().register_command(commands, help_one_line, help_long, handler);
    }

    void register_command(
        const char* command, // commands, e.g., "help"
        const char* help_one_line,
        const char* help_long,
        command_handler handler
        )
    {
        std::vector<const char*> cmds;
        cmds.push_back(command);
        register_command(cmds, help_one_line, help_long, handler);
    }

    void command_manager::register_command(const std::vector<const char*>& commands, const char* help_one_line, const char* help_long, command_handler handler)
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
        c->commands = commands;
        c->help_long = help_long;
        c->help_short = help_one_line;
        c->handler = handler;
        _commands.push_back(c);
        
        for (auto cmd : commands)
        {
            if (cmd != nullptr)
            {
                _handlers[std::string(cmd)] = c;
            }
        }
    }

    bool command_manager::run_command(const std::string& cmdline, __out_param std::string& output)
    {
        std::string scmd = cmdline;
        std::vector<std::string> args;
        
        utils::split_args(scmd.c_str(), args, ' ');

        if (args.size() < 1)
            return false;

        std::vector<std::string> args2;
        for (size_t i = 1; i < args.size(); i++)
        {
            args2.push_back(args[i]);
        }

        return run_command(args[0], args2, output);
    }

    bool command_manager::run_command(const std::string& cmd, const std::vector<std::string>& args, __out_param std::string& output)
    {
        command* h = nullptr;
        {
            utils::auto_read_lock l(_lock);
            auto it = _handlers.find(cmd);
            if (it != _handlers.end())
                h = it->second;
        }

        if (h == nullptr)
        {
            output = std::string("unknown command '") + cmd + "'";
            return false;
        }
        else
        {
            output = h->handler(args);
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

    DEFINE_TASK_CODE_RPC(RPC_DSN_CLI_CALL, TASK_PRIORITY_HIGH, THREAD_POOL_DEFAULT);

    class cli_rpc_request_task : public rpc_request_task
    {
    public:
        cli_rpc_request_task(message_ptr& request, service_node* node)
            : rpc_request_task(request, node)
        {
        }

        virtual void  exec()
        {
            command_manager::instance().on_remote_cli(get_request());
        }
    };

    class cli_rpc_server_handler : public rpc_server_handler
    {
    public:
        virtual rpc_request_task* new_request_task(message_ptr& request, service_node* node)
        {
            return (new cli_rpc_request_task(request, node));
        }
    };

    void command_manager::start_remote_cli()
    {
        ::dsn::service_engine::instance().register_system_rpc_handler(RPC_DSN_CLI_CALL, "dsn.cli", new cli_rpc_server_handler());
    }

    void command_manager::on_remote_cli(message_ptr& request)
    {
        std::string cmd;
        unmarshall(request->reader(), cmd);

        std::vector<std::string> args;
        unmarshall(request->reader(), args);

        std::string result;
        run_command(cmd, args, result);

        auto resp = request->create_response();
        marshall(resp->writer(), result);

        ::dsn::service::rpc::reply(resp);
    }

    command_manager::command_manager()
    {
        register_command(
            {"help", "h", "H", "Help"}, 
            "help|Help|h|H [command] - display help information", 
            "",
            [this](const std::vector<std::string>& args)
            {
                std::stringstream ss;

                if (args.size() == 0)
                {
                    utils::auto_read_lock l(_lock);
                    for (auto c : this->_commands)
                    {
                        ss << c->help_short << std::endl;
                    }
                }
                else
                {
                    utils::auto_read_lock l(_lock);
                    auto it = _handlers.find(args[0]);
                    if (it == _handlers.end())
                        ss << "cannot find command '" << args[0] << "'" << std::endl;
                    else
                    {
                        ss.width(6);
                        ss << std::left << it->first << ": " << it->second->help_short << std::endl << it->second->help_long << std::endl;
                    }
                }

                return ss.str();
            }
        );
    }
}
