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

# pragma once

# include <dsn/dist/cluster_scheduler.h>
# include <unordered_map>
# include "machine_pool_mgr.h"
using namespace ::dsn::service;

namespace dsn
{
    namespace dist
    {
        DEFINE_TASK_CODE(LPC_DOCKER_CREATE,TASK_PRIORITY_COMMON,THREAD_POOL_SCHEDULER_LONG)
        DEFINE_TASK_CODE(LPC_DOCKER_DELETE,TASK_PRIORITY_COMMON,THREAD_POOL_SCHEDULER_LONG)
        class docker_scheduler 
            : public cluster_scheduler, public clientlet
        {
        public:
            docker_scheduler();
            virtual error_code initialize() override;

            virtual void schedule(
                std::shared_ptr<deployment_unit>& unit
                ) override;

            void unschedule(
                    std::shared_ptr<deployment_unit>& unit
                    );
            virtual cluster_type type() const override
            {
                return cluster_type::cstype_docker;
            }

            static void deploy_docker_unit(void* context, int argc, const char** argv, dsn_cli_reply* reply);
            static void deploy_docker_unit_cleanup(dsn_cli_reply reply);
            static void undeploy_docker_unit(void* context, int argc, const char** argv, dsn_cli_reply* reply);
            static void undeploy_docker_unit_cleanup(dsn_cli_reply reply);
        private:
            void create_containers(std::string& name,std::function<void(error_code, rpc_address)>& deployment_callback, std::string& local_package_directory, std::string& remote_package_directory);
            void delete_containers(std::string& name,std::function<void(error_code, const std::string&)>& undeployment_callback, std::string& local_package_directory, std::string& remote_package_directory);

            void get_app_list(std::string& ldir, /*out*/std::vector<std::string>& app_list);
            void write_machine_list(std::string& name, std::string& ldir);
            void return_machines(std::string& name);
            using deploy_map = std::unordered_map<std::string, std::shared_ptr<deployment_unit> >;
            using machine_map = std::unordered_map<std::string, std::vector<std::string> >;
            std::string                 _run_path;
            dsn_handle_t                _docker_state_handle;
            dsn_handle_t                _docker_deploy_handle;
            dsn_handle_t                _docker_undeploy_handle;
            deploy_map                  _deploy_map;
            zlock                       _lock;
            machine_pool_mgr            _mgr;
            machine_map                 _machine_map;

        };

    }
}
