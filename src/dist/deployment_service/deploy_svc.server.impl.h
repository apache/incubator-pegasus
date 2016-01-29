# pragma once

# include "deploy_svc.server.h"
# include "deploy_svc.types.h"
# include <dsn/dist/cluster_scheduler.h>

# include <unordered_map>

namespace dsn
{
    namespace dist
    {

        class deploy_svc_service_impl
            : public deploy_svc_service
        {
        public:
            deploy_svc_service_impl();
            ~deploy_svc_service_impl();

            error_code start();

            void stop();

            virtual void on_deploy(const deploy_request& req, /*out*/ ::dsn::rpc_replier<deploy_info>& reply) override;

            virtual void on_undeploy(const std::string& service_name, /*out*/ ::dsn::rpc_replier<error_code>& reply) override;

            virtual void on_get_service_list(const std::string& package_id, /*out*/ ::dsn::rpc_replier<deploy_info_list>& reply) override;

            virtual void on_get_service_info(const std::string& service_url, /*out*/ ::dsn::rpc_replier<deploy_info>& reply) override;

            virtual void on_get_cluster_list(const std::string& format, /*out*/ ::dsn::rpc_replier<cluster_list>& reply) override;

        private:
            mutable ::dsn::service::zrwlock_nr _service_lock;
            std::unordered_map<std::string, std::shared_ptr< ::dsn::dist::deployment_unit> > _services;

            struct cluster_ex
            {
                cluster_info cluster;
                std::unique_ptr< ::dsn::dist::cluster_scheduler> scheduler;
            };

            mutable ::dsn::service::zrwlock_nr _cluster_lock;
            std::unordered_map<std::string, std::shared_ptr<cluster_ex> > _clusters;

            std::string _service_dir;

            dsn_handle_t _cli_deploy;
            dsn_handle_t _cli_undeploy;
            dsn_handle_t _cli_get_service_list;
            dsn_handle_t _cli_get_service_info;
            dsn_handle_t _cli_get_cluster_list;

        private:
            void download_service_resource_completed(error_code err, std::shared_ptr< ::dsn::dist::deployment_unit> svc);
            std::shared_ptr< ::dsn::dist::deployment_unit> get_service(const std::string& name);
            std::shared_ptr<cluster_ex> get_cluster(const std::string& name);

            void on_service_deployed(
                std::shared_ptr< ::dsn::dist::deployment_unit> unit,
                ::dsn::error_code err,
                ::dsn::rpc_address addr
                );

            void on_service_failure(
                std::shared_ptr< ::dsn::dist::deployment_unit> unit,
                ::dsn::error_code err,
                const std::string& err_msg
                );
            void on_service_undeployed(
                std::string service_name,
                ::dsn::error_code err,
                const std::string& err_msg
                );

            void on_deploy_internal(const deploy_request& req, /*out*/ deploy_info& di);

            void on_undeploy_internal(const std::string& service_name, /*out*/ error_code& err);

            void on_get_service_list_internal(const std::string& package_id, /*out*/ deploy_info_list& dlist);

            void on_get_service_info_internal(const std::string& service_url, /*out*/ deploy_info& di);

            void on_get_cluster_list_internal(const std::string& format, /*out*/ cluster_list& clist);

            void on_deploy_cli(void *context, int argc, const char **argv, /*out*/ dsn_cli_reply *reply);

            void on_undeploy_cli(void *context, int argc, const char **argv, /*out*/ dsn_cli_reply *reply);

            void on_get_service_list_cli(void *context, int argc, const char **argv, /*out*/ dsn_cli_reply *reply);

            void on_get_service_info_cli(void *context, int argc, const char **argv, /*out*/ dsn_cli_reply *reply);

            void on_get_cluster_list_cli(void *context, int argc, const char **argv, /*out*/ dsn_cli_reply *reply);
        };
    }
}
