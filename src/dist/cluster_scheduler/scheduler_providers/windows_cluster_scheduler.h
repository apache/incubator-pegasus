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

# pragma once

# include <dsn/dist/cluster_scheduler.h>
# include <unordered_map>
# include "machine_pool_mgr.h"

using namespace ::dsn::service;

namespace dsn
{
    namespace dist
    {
        DEFINE_TASK_CODE(LPC_WIN_CREATE, TASK_PRIORITY_COMMON, THREAD_POOL_SCHEDULER_LONG)
        DEFINE_TASK_CODE(LPC_WIN_DELETE, TASK_PRIORITY_COMMON, THREAD_POOL_SCHEDULER_LONG)

        class windows_cluster_scheduler
            : public cluster_scheduler, public clientlet
        {
        public:
            windows_cluster_scheduler();
            virtual error_code initialize() override;

            virtual void schedule(
                std::shared_ptr<deployment_unit>& unit
                ) override;

            void unschedule(
                std::shared_ptr<deployment_unit>& unit
                )override;
            virtual cluster_type type() const override
            {
                return cluster_type::cstype_bare_medal_windows;
            }

        private:
            zlock                       _lock;
            std::unordered_map<std::string, std::shared_ptr<deployment_unit> > _deploy_map;
            std::unordered_map<std::string, std::vector<std::string> > _machine_map;
            machine_pool_mgr            _mgr;
            std::string                 _default_remote_package_directory;

            void run_apps(
                std::string& name, 
                std::function<void(error_code, rpc_address)>& deployment_callback, 
                std::string& local_package_directory, std::string& remote_package_directory
                );

            void stop_apps(
                std::string& name, 
                std::function<void(error_code, const std::string&)>& deployment_callback, 
                std::string& local_package_directory, std::string& remote_package_directory
                );
            
            error_code allocate_machine(
                std::string& name, 
                std::string& ldir, 
                /* out */ std::vector<std::string>& assign_list,
                /* out */ std::string& service_url
                );
        };

    }
}
