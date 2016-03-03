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

/*
 * Description:
 *     What is this file about?
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */


# include "daemon.server.h"
# include "daemon.h"
# include <dsn/cpp/utils.h>
# include <dsn/internal/module_init.cpp.h>
 
using namespace ::dsn::replication;

MODULE_INIT_BEGIN
    dsn::register_app< ::dsn::dist::daemon>("daemon");
MODULE_INIT_END

namespace dsn
{
    namespace dist
    {
        daemon_s_service::daemon_s_service() 
            : ::dsn::serverlet<daemon_s_service>("daemon_s"), _online(false)
        {
            _working_dir = utils::filesystem::path_combine(dsn_get_current_app_data_dir(), "apps");
            _package_server = rpc_address(
                dsn_config_get_value_string("apps.daemon", "package_server_host", "", "the host name of the app store where to download package"),
                (uint16_t)dsn_config_get_value_uint64("apps.daemon", "package_server_port", 26788, "the port of the app store where to download package")
                );
            _package_dir_on_package_server = dsn_config_get_value_string("apps.daemon", "package_dir", "", "the dir on the app store where to download package");
            _app_port_min = (uint32_t)dsn_config_get_value_uint64("apps.daemon", "app_port_min", 59001, "the minimum port that can be assigned to app");
            _app_port_max = (uint32_t)dsn_config_get_value_uint64("apps.daemon", "app_port_max", 60001, "the maximum port that can be assigned to app");

            if (!utils::filesystem::directory_exists(_working_dir))
            {
                utils::filesystem::create_directory(_working_dir);
            }

# ifdef _WIN32
            _job = CreateJobObjectA(NULL, NULL);
            dassert(_job != NULL, "create windows job failed, err = %d", ::GetLastError());

            JOBOBJECT_EXTENDED_LIMIT_INFORMATION jeli = { 0 };

            // Configure all child processes associated with the job to terminate when the
            jeli.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;

            if (0 == SetInformationJobObject(_job, JobObjectExtendedLimitInformation, &jeli, sizeof(jeli)))
            {
                dassert(false, "Could not SetInformationJobObject, err = %d", ::GetLastError());
            }
# else
# endif
        }

        daemon_s_service::~daemon_s_service()
        {
        }

        void daemon_s_service::on_config_proposal(const ::dsn::replication::configuration_update_request& proposal)
        {
            dassert(proposal.is_stateful == false, 
                "stateful replication not supported by daemon, please using a different layer2 handler");

            switch (proposal.type)
            {
            case CT_ADD_SECONDARY:
            case CT_ADD_SECONDARY_FOR_LB:
                on_add_app(proposal);
                break;

            case CT_REMOVE:          
                on_remove_app(proposal);
                break;

            default:
                dwarn("not supported configuration type %s received", enum_to_string(proposal.type));
                break;
            }
        }

        static std::vector<rpc_address> read_meta_servers()
        {
            std::vector<rpc_address> meta_servers;

            const char* server_ss[10];
            int capacity = 10, need_count;
            need_count = dsn_config_get_all_keys("meta_servers", server_ss, &capacity);
            dassert(need_count <= capacity, "too many meta servers specified");

            std::ostringstream oss;
            for (int i = 0; i < capacity; i++)
            {
                std::string s(server_ss[i]);
                // name:port
                auto pos1 = s.find_first_of(':');
                if (pos1 != std::string::npos)
                {
                    ::dsn::rpc_address ep(s.substr(0, pos1).c_str(), atoi(s.substr(pos1 + 1).c_str()));
                    meta_servers.push_back(ep);
                    oss << "[" << ep.to_string() << "] ";
                }
            }
            ddebug("read meta servers from config: %s", oss.str().c_str());

            return std::move(meta_servers);
        }


        DEFINE_TASK_CODE(LPC_DAEMON_APPS_CHECK_TIMER, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)

        void daemon_s_service::open_service()
        {
            auto meta_servers = std::move(read_meta_servers());
            _fd.reset(new slave_failure_detector_with_multimaster(meta_servers, 
                [=]() { this->on_master_disconnected(); },
                [=]() { this->on_master_connected(); }
                ));

            // TOOD: handle daemon fail over

            this->register_rpc_handler(RPC_CONFIG_PROPOSAL, "config_proposal", &daemon_s_service::on_config_proposal);

            auto err = _fd->start(
                5,
                3,
                14,
                15
                );

            dassert(ERR_OK == err, "failure detector start failed, err = %s", err.to_string());

            _fd->register_master(_fd->current_server_contact());

            _app_check_timer = tasking::enqueue_timer(
                LPC_DAEMON_APPS_CHECK_TIMER,
                this,
                [this]{ this->check_apps(); },
                std::chrono::milliseconds(30)
                );
        }

        void daemon_s_service::close_service()
        {
            _app_check_timer->cancel(true);
            _fd->stop();
            this->unregister_rpc_handler(RPC_CONFIG_PROPOSAL);
        }

        void daemon_s_service::on_master_connected()
        {
            _online = true;
            dinfo("master is connected");
        }
        
        void daemon_s_service::on_master_disconnected()
        {       
            _online = false;

            // TODO: fail-over
           /* {
                ::dsn::service::zauto_read_lock l(_lock);
                for (auto& app : _apps)
                {
                    kill_app(std::move(app.second));
                }

                _apps.clear();
            }*/

            dinfo("master is disconnected");
        }

        DEFINE_TASK_CODE_AIO(LPC_DAEMON_DOWNLOAD_PACKAGE, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)

        void daemon_s_service::on_add_app(const ::dsn::replication::configuration_update_request & proposal)
        {
            // check app exists or not
            std::shared_ptr<layer1_app_info> old_app, app;

            {
                ::dsn::service::zauto_write_lock l(_lock);
                auto it = _apps.find(proposal.config.gpid);

                // app is running with the same package
                if (it != _apps.end())
                {
                    // proposal's package is older or the same
                    // ballot is the package version for stateless applications
                    if (proposal.config.ballot <= it->second->configuration.ballot)
                        return;
                    else
                    {
                        old_app = std::move(it->second);
                        _apps.erase(it);
                    }
                }

                app.reset(new layer1_app_info(proposal));
                app->package_dir = utils::filesystem::path_combine(_working_dir, proposal.config.package_id);


# ifdef _WIN32
                const char* runner = "run.cmd";
# elif defined(__linux__)
                const char* runner = "run.sh";
# else
//# error "not supported yet"
# endif

                std::string runner_path = utils::filesystem::path_combine(app->package_dir, app->configuration.package_id);
                app->runner_script = utils::filesystem::path_combine(runner_path, runner);

                _apps.emplace(app->configuration.gpid, app);
            }
            
            // TODO: add confliction with the same package
            
            // kill old app if necessary
            if (nullptr != old_app)
                kill_app(std::move(old_app));
            
            // check and start
            std::string package_dir = app->package_dir;
            if (app->resource_ready)
            {                
                app->package_dir = utils::filesystem::path_combine(app->package_dir, app->configuration.package_id);
                start_app(std::move(app));
            }

            // download package first if necesary
            else
            {
                utils::filesystem::create_directory(package_dir);

                // TODO: better way to download package from app store 
                std::vector<std::string> files{ app->configuration.package_id + ".7z" };

                dinfo("start downloading package %s from %s to %s",
                    app->configuration.package_id.c_str(),
                    _package_server.to_string(),
                    app->package_dir.c_str()
                    );

                file::copy_remote_files(
                    _package_server,
                    _package_dir_on_package_server,
                    files,
                    package_dir,
                    true,
                    LPC_DAEMON_DOWNLOAD_PACKAGE,
                    this,
                    [this, cap_app = std::move(app)](error_code err, size_t sz) mutable
                    {
                        if (err == ERR_OK)
                        {

                            // TODO: using zip lib instead
                            // 
                            std::string command = "7z x " + cap_app->package_dir + '/' + cap_app->configuration.package_id + ".7z -y -o" + cap_app->package_dir;
                            // decompress when completed
                            system(command.c_str());

                            if (utils::filesystem::file_exists(cap_app->runner_script))
                            {
                                cap_app->resource_ready = true;
                                start_app(std::move(cap_app));
                                return;
                            }
                            else
                            {
                                derror("package %s does not contain runner '%s' in it",
                                    cap_app->package_dir.c_str(),
                                    cap_app->runner_script.c_str()
                                    );

                                err = ERR_OBJECT_NOT_FOUND;
                            }
                        }
                        

                        {
                            ::dsn::service::zauto_write_lock l(_lock);
                            _apps.erase(cap_app->configuration.gpid);
                        }

                        derror("add app %s (%s) failed, err = %s ...",
                            cap_app->configuration.app_type.c_str(),
                            cap_app->configuration.package_id.c_str(),
                            err.to_string()
                            );

                        utils::filesystem::remove_path(cap_app->package_dir);
                        return;
                    }
                    );
            }
        }

        void daemon_s_service::on_remove_app(const ::dsn::replication::configuration_update_request & proposal)
        {
            // check app exists or not
            std::shared_ptr<layer1_app_info> app;

            {
                ::dsn::service::zauto_read_lock l(_lock);
                auto it = _apps.find(proposal.config.gpid);

                // app is running with the same package
                if (it != _apps.end())
                {
                    // proposal's package is older or the same
                    // ballot is the package version for stateless applications
                    if (proposal.config.ballot <= it->second->configuration.ballot)
                        return;
                    else
                    {
                        app = std::move(it->second);
                        _apps.erase(it);
                    }
                }
            }

            if (nullptr != app)
            {
                auto cap_app = app;
                kill_app(std::move(cap_app));

                update_configuration_on_meta_server(CT_REMOVE, std::move(app));
            }
        }

        void daemon_s_service::start_app(std::shared_ptr<layer1_app_info> && app)
        {
            // set port and run
            for (int i = 0; i < 10; i++)
            {
                uint32_t port = (uint32_t)dsn_random32(_app_port_min, _app_port_max);

                // set up working dir
                {
                    std::stringstream ss;
                    ss << app->configuration.gpid.app_id << "." << app->configuration.gpid.pidx << "." << port;
                    app->working_dir = utils::filesystem::path_combine(app->package_dir, ss.str());

                    if (utils::filesystem::directory_exists(app->working_dir))
                        continue;

                    utils::filesystem::create_directory(app->working_dir);
                }

                std::stringstream ss;
# ifdef _WIN32
                ss << "cmd.exe /k SET port=" << port;
                ss << " && CALL " << app->runner_script;
# else
//# error not implemented
# endif
                std::string command = ss.str();

                app->working_port = port;

                dinfo("try start app %s (%s) with command %s at working dir %s ...",
                    app->configuration.app_type.c_str(),
                    app->configuration.package_id.c_str(),
                    command.c_str(),
                    app->working_dir.c_str()
                    );

# ifdef _WIN32
                STARTUPINFOA si;
                PROCESS_INFORMATION pi;

                ZeroMemory(&si, sizeof(si));
                si.cb = sizeof(si);
                ZeroMemory(&pi, sizeof(pi));
                
                if (::CreateProcessA(NULL, (LPSTR)command.c_str(), NULL, NULL, TRUE, CREATE_NEW_CONSOLE, NULL, 
                    (LPSTR)app->working_dir.c_str(), &si, &pi))
                {
                    if (0 == ::AssignProcessToJobObject(_job, pi.hProcess))
                    {
                        // dassert(false, "cannot attach process to job, err = %d", ::GetLastError());
                    }

                    CloseHandle(pi.hThread);
                    app->process_handle = pi.hProcess;
                }
                else
                {
                    derror("create process failed, err = %d", ::GetLastError());
                    kill_app(std::move(app));
                    return;
                }

                // sleep a while to see whether the port is usable
                if (WAIT_TIMEOUT == ::WaitForSingleObject(app->process_handle, 6000))
                {
                    break;
                }
                else
                {
                    ::CloseHandle(pi.hProcess);
                }
# else
//# error not implemented
# endif
            }

            // register to meta server if successful
            if (!app->exited)
            {
                update_configuration_on_meta_server(CT_ADD_SECONDARY, std::move(app));
            }

            // remove for failure too many times
            else
            {
                kill_app(std::move(app));
            }
        }
        
        void daemon_s_service::kill_app(std::shared_ptr<layer1_app_info> && app)
        {
            dinfo("kill app %s (%s) at working dir %s, port %d",
                app->configuration.app_type.c_str(),
                app->configuration.package_id.c_str(),
                app->working_dir.c_str(),
                (int)app->working_port
                );

            {
                ::dsn::service::zauto_write_lock l(_lock);
                _apps.erase(app->configuration.gpid);
            }

            if (!app->exited && app->process_handle)
            {
# ifdef _WIN32
                ::TerminateProcess(app->process_handle, 0);
                ::CloseHandle(app->process_handle);
                app->process_handle = nullptr;
# else
//# error not implemented
# endif
                app->exited = true;
            }

            utils::filesystem::remove_path(app->working_dir);
        }
        
        void daemon_s_service::check_apps()
        {
            _lock.lock_read();
            auto apps = _apps;
            _lock.unlock_read();

            for (auto& app : apps)
            {
                if (app.second->process_handle)
                {
# ifdef _WIN32
                    if (WAIT_OBJECT_0 == ::WaitForSingleObject(app.second->process_handle, 0))
                    {
                        app.second->exited = true;
                        DWORD exit_code = 0xdeadbeef;
                        ::GetExitCodeProcess(app.second->process_handle, &exit_code);
                        ::CloseHandle(app.second->process_handle);
                        app.second->process_handle = nullptr;

                        dinfo("app %s (%s) exits (code = %x), with working dir = %s, port = %d",
                            app.second->configuration.app_type.c_str(),
                            app.second->configuration.package_id.c_str(),
                            exit_code,
                            app.second->working_dir.c_str(),
                            (int)app.second->working_port
                            );
                    }
# else

# endif
                }
                if (app.second->exited)
                {
                    auto cap_app = app.second;
                    kill_app(std::move(cap_app));
                    update_configuration_on_meta_server(CT_REMOVE, std::move(app.second));
                }
            }
            apps.clear();
        }
        
        void daemon_s_service::update_configuration_on_meta_server(::dsn::replication::config_type type, std::shared_ptr<layer1_app_info>&& app)
        {
            rpc_address node = primary_address();
            node.assign_ipv4(node.ip(), app->working_port);
            
            dsn_message_t msg = dsn_msg_create_request(RPC_CM_UPDATE_PARTITION_CONFIGURATION, 0, 0);

            std::shared_ptr<configuration_update_request> request(new configuration_update_request);
            request->config = app->configuration;
            request->type = type;
            request->node = node;
            request->host_node = primary_address();

            if (type == CT_REMOVE)
            {
                std::remove(
                    app->configuration.secondaries.begin(),
                    app->configuration.secondaries.end(),
                    node
                    );
            }
            else
            {
                app->configuration.secondaries.emplace_back(node);
            }

            ::marshall(msg, *request);

            rpc::call(
                _fd->get_servers(),
                msg,
                this,
                [=, cap_app = std::move(app)](error_code err, dsn_message_t reqmsg, dsn_message_t response) mutable
                {
                    on_update_configuration_on_meta_server_reply(type, std::move(cap_app), err, reqmsg, response);
                }
                );
        }

        void daemon_s_service::on_update_configuration_on_meta_server_reply(
            ::dsn::replication::config_type type, std::shared_ptr<layer1_app_info> &&  app,
            error_code err, dsn_message_t request, dsn_message_t response
            )
        {
            if (false == _online)
            {
                err.end_tracking();
                return;
            }

            configuration_update_response resp;
            if (err == ERR_OK)
            {
                ::unmarshall(response, resp);
                err = resp.err;
            }
            else if (err == ERR_TIMEOUT)
            {
                // TODO: suicide after trying several times ...
                rpc::call(
                    _fd->get_servers(),
                    request,
                    this,
                    [=, cap_app = std::move(app)](error_code err, dsn_message_t reqmsg, dsn_message_t response) mutable
                    {
                        on_update_configuration_on_meta_server_reply(type, std::move(cap_app), err, reqmsg, response);
                    }
                    );
            }
            else
            {
                if (type == CT_ADD_SECONDARY)
                    kill_app(std::move(app));
            }
        }


        ::dsn::error_code daemon::start(int argc, char** argv)
        {
            _daemon_s_svc.reset(new daemon_s_service());
            _daemon_s_svc->open_service();
            return ::dsn::ERR_OK;
        }

        void daemon::stop(bool cleanup)
        {
            _daemon_s_svc->close_service();
            _daemon_s_svc = nullptr;
        }

        daemon::daemon()
        {

        }

        daemon::~daemon()
        {

        }
    }
}