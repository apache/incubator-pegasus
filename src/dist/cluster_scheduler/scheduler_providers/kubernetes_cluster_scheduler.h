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
using namespace ::dsn::service;

namespace dsn
{
    namespace dist
    {
        DEFINE_TASK_CODE(LPC_K8S_CREATE,TASK_PRIORITY_COMMON, THREAD_POOL_SCHEDULER_LONG)
        DEFINE_TASK_CODE(LPC_K8S_DELETE,TASK_PRIORITY_COMMON, THREAD_POOL_SCHEDULER_LONG)

        class kubernetes_cluster_scheduler 
            : public cluster_scheduler, public clientlet
        {
        public:
            kubernetes_cluster_scheduler();
            virtual error_code initialize() override;

            virtual void schedule(
                std::shared_ptr<deployment_unit>& unit
                ) override;

            void unschedule(
                    std::shared_ptr<deployment_unit>& unit
                    )override;
            virtual cluster_type type() const override
            {
                return cluster_type::cstype_kubernetes;
            }

            static void deploy_k8s_unit(void* context, int argc, const char** argv, dsn_cli_reply* reply);
            static void deploy_k8s_unit_cleanup(dsn_cli_reply reply);
            static void undeploy_k8s_unit(void* context, int argc, const char** argv, dsn_cli_reply* reply);
            static void undeploy_k8s_unit_cleanup(dsn_cli_reply reply);
        private:
            void create_pod(std::string& name,std::function<void(error_code, rpc_address)>& deployment_callback, std::string& local_package_directory);
            void delete_pod(std::string& name,std::function<void(error_code, const std::string&)>& undeployment_callback, std::string& local_package_directory);
            using deploy_map = std::unordered_map<std::string, std::shared_ptr<deployment_unit> >;
            std::string                 _run_path;
            dsn_handle_t                _k8s_state_handle;
            dsn_handle_t                _k8s_deploy_handle;
            dsn_handle_t                _k8s_undeploy_handle;
            deploy_map                  _deploy_map;
            zlock                       _lock;
        };

    }
}
