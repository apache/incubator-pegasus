# pragma once

# include "deploy_svc.server.h"
# include <unordered_map>
# include <dsn/dist/cluster_scheduler.h>

namespace dsn 
{
    namespace dist
    {

        class deploy_svc_service_impl
            : public deploy_svc_service
        {
        public:
            error_code start();
            
            virtual void on_deploy(const deploy_request& req, ::dsn::rpc_replier<deploy_info>& reply) override;

            virtual void on_undeploy(const std::string& service_name, ::dsn::rpc_replier<error_code>& reply) override;

            virtual void on_get_service_list(const std::string& package_id, ::dsn::rpc_replier<deploy_info_list>& reply) override;

            virtual void on_get_service_info(const std::string& service_url, ::dsn::rpc_replier<deploy_info>& reply) override;

            virtual void on_get_cluster_list(const std::string& format, ::dsn::rpc_replier<cluster_list>& reply) override;

        private:
            mutable ::dsn::service::zrwlock_nr _service_lock;
            std::unordered_map<std::string, std::shared_ptr<::dsn::dist::deployment_unit> > _services;

            struct cluster_ex
            {
                cluster_info cluster;
                std::unique_ptr<::dsn::dist::cluster_scheduler> scheduler;
            };

            mutable ::dsn::service::zrwlock_nr _cluster_lock;
            std::unordered_map<std::string, std::shared_ptr<cluster_ex> > _clusters;

            std::string _service_dir;

        private:
            void download_service_resource_completed(error_code err, std::shared_ptr<::dsn::dist::deployment_unit> svc);
            std::shared_ptr<::dsn::dist::deployment_unit> get_service(const std::string& name);
            std::shared_ptr<cluster_ex> get_cluster(const std::string& name);

            void on_service_deployed(
                std::shared_ptr<::dsn::dist::deployment_unit> unit,
                ::dsn::error_code err,
                ::dsn::rpc_address addr
                );

            void on_service_failure(
                std::shared_ptr<::dsn::dist::deployment_unit> unit,
                ::dsn::error_code err,
                const std::string& err_msg
                );
        };
    }
}