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
*     This scheduler is responsible to deploy services upon windows cluster.
*
* Revision history:
*     2/1/2016, ykwd, first version
*/

#include "windows_cluster_scheduler.h"
#include "windows_cluster_error.h"
#include <fstream>

namespace dsn
{
    namespace dist
    {
        windows_cluster_scheduler::windows_cluster_scheduler()
            :_mgr("windows")
        {
            _default_remote_package_directory = dsn_config_get_value_string(
                "windows", 
                "remote_package_directory", 
                "C:\\rdsn\\packages", 
                "the location of deployed packages"
            );
        }

        error_code windows_cluster_scheduler::initialize()
        {
            error_code err = ERR_OK;
            return err;
        }

        void windows_cluster_scheduler::schedule(
            std::shared_ptr<deployment_unit>& unit
            )
        {
            bool found = false;
            error_code err = ERR_WIN_DEPLOY_FAILED;
            {
                zauto_lock l(_lock);
                found = (_deploy_map.find(unit->name) != _deploy_map.end());
                if (!found)
                {
                    std::vector<std::string> assign_list;
                    std::string service_url;
                    err = allocate_machine(unit->name, unit->local_package_directory, assign_list, service_url);
                    if (err == ERR_OK)
                    {
                        _deploy_map[unit->name] = unit;
                        _machine_map[unit->name] = assign_list;
                        unit->service_url = service_url;

                        std::stringstream ss;
                        ss << unit->package_id << "." << unit->name;
                        unit->remote_package_directory = dsn::utils::filesystem::path_combine(_default_remote_package_directory, ss.str());
                    }
                }
            }
            if (err != ERR_OK)
            {
                unit->deployment_callback(err, rpc_address());
            }
            else
            { 
                dsn::tasking::enqueue(LPC_WIN_CREATE, this, [this, unit]() {
                    run_apps(unit->name, unit->deployment_callback, unit->local_package_directory, unit->remote_package_directory);
                });
            }
        }

        void windows_cluster_scheduler::unschedule(
            std::shared_ptr<deployment_unit>& unit
            )
        {
            bool found = false;

            {
                zauto_lock l(_lock);
                found = (_deploy_map.find(unit->name) != _deploy_map.end());
            }

            if (found)
            {
                dsn::tasking::enqueue(LPC_WIN_DELETE, this, [this, unit]() {
                    stop_apps(unit->name, unit->undeployment_callback, unit->local_package_directory, unit->remote_package_directory);
                });
            }
            else
            {
                unit->undeployment_callback(ERR_WIN_UNDEPLOY_FAILED, std::string());
            }
        }

        void windows_cluster_scheduler::run_apps(std::string& name, std::function<void(error_code, rpc_address)>& deployment_callback, std::string& local_package_directory, std::string& remote_package_directory)
        {
            int ret;
            std::ostringstream command;
            command << "..\\run_windows.cmd deploy_and_start ";
            command << local_package_directory;
            if (remote_package_directory == "")
                command << " " << dsn::utils::filesystem::path_combine(_default_remote_package_directory, name);
            else
                command << " " << remote_package_directory;
            printf("%s\n", command.str().c_str());
            ret = system(command.str().c_str());

            if (ret == 0)
            {
                deployment_callback(ERR_OK, rpc_address());
            }
            else
            {
                deployment_callback(::dsn::dist::ERR_WIN_DEPLOY_FAILED, rpc_address());
            }
        }

        void windows_cluster_scheduler::stop_apps(std::string& name, std::function<void(error_code, const std::string&)>& deployment_callback, std::string& local_package_directory, std::string& remote_package_directory)
        {
            int ret;
            std::ostringstream command;
            command << "..\\run_windows.cmd stop_and_cleanup ";
            command << local_package_directory;
            if (remote_package_directory == "")
                command << " " << dsn::utils::filesystem::path_combine(_default_remote_package_directory, name);
            else
                command << " " << remote_package_directory;

            ret = system(command.str().c_str());

            if (ret == 0)
            {
                {
                    zauto_lock l(_lock);
                    if (_deploy_map.find(name) != _deploy_map.end())
                    {
                        _mgr.return_machine(_machine_map[name]);
                        _machine_map.erase(name);
                        _deploy_map.erase(name);
                    }
                }
                deployment_callback(ERR_OK, std::string());
            }
            else
            {
                deployment_callback(::dsn::dist::ERR_WIN_DEPLOY_FAILED, std::string());
            }
        }

        error_code windows_cluster_scheduler::allocate_machine(std::string& name, std::string& ldir, /* out */ std::vector<std::string>& assign_list, /* out */ std::string& service_url)
        {
            assign_list.clear();
            service_url.clear();

            /* read apps from config file */
            std:: string app_list_file = dsn::utils::filesystem::path_combine(ldir, std::string("apps.txt"));

            FILE* fd = fopen(app_list_file.c_str(), "r");
            if (fd == nullptr)
            {
                dinfo("cannot open apps.txt");
                return ERR_FILE_OPERATION_FAILED;
            }

            std::vector<std::string> app_list;
            char str[128];
            str[127] = '\0';
            while (1 == fscanf(fd, "%127s", str))
            {
                app_list.push_back(std::string(str));
            }
            fclose(fd);

            /* allocate machine */
            error_code err = ERR_OK;
            machine_pool_mgr::alloca_options opt;
            opt.allow_partial_allocation = false;
            opt.allow_same_machine_slots = true;
            opt.slot_count = (int)app_list.size();
            err = _mgr.get_machine(opt, assign_list);
            if (err != ERR_OK)
            {
                dinfo("not enough machines");
                return err;
            }

            /* write machine allocation to file;
             * construct assign_list;
             * construct service_url.
             */
            std::ostringstream svc;
            for (size_t i = 0; i < app_list.size(); i++)
            {
                std::string machine_file_dir = dsn::utils::filesystem::path_combine(ldir, app_list[i]);
                std::string machine_file = dsn::utils::filesystem::path_combine(machine_file_dir, std::string("machines.txt"));

                std::ofstream fd;
                fd.open(machine_file.c_str(), std::ios_base::out);
                if (!fd.is_open())
                {
                    err = ERR_FILE_OPERATION_FAILED;
                    dinfo("cannot open machines.txt");
                    break;
                }
                fd << assign_list[i] << std::endl;
                svc << app_list[i] << " : " << assign_list[i] << std::endl;
                fd.close();
            }

            if (err != ERR_OK)
            {
                _mgr.return_machine(assign_list);
                assign_list.clear();
                service_url.clear();
            }
            service_url = svc.str();
            return err;
        }
    }
}
