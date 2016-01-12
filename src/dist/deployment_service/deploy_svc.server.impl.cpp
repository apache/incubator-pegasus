
# include "deploy_svc.server.impl.h"
# include <dsn/internal/factory_store.h>

namespace dsn
{
    namespace dist
    {


        DEFINE_THREAD_POOL_CODE(THREAD_POOL_DEPLOY_LONG)

        DEFINE_TASK_CODE(LPC_DEPLOY_DOWNLOAD_RESOURCE, TASK_PRIORITY_COMMON, THREAD_POOL_DEPLOY_LONG)

        error_code deploy_svc_service_impl::start()
        {
            std::string pdir = utils::filesystem::path_combine(dsn_get_current_app_data_dir(), "services");
            _service_dir = dsn_config_get_value_string("deploy.service",
                "deploy_dir",
                pdir.c_str(),
                "where to put temporal deployment resources"
                );

            // load clusters
            const char* clusters[100];
            int sz = 100;
            int count = dsn_config_get_all_keys("deploy.service.clusters", clusters, &sz);
            dassert(count <= 100, "too many clusters");

            for (int i = 0; i < count; i++)
            {
                std::string cluster_name = dsn_config_get_value_string(
                    clusters[i],
                    "name",
                    "",
                    "cluster name"
                    );

                if (nullptr != get_cluster(cluster_name))
                {
                    derror("cluster %s already defined", cluster_name.c_str());
                    return ERR_CLUSTER_ALREADY_EXIST;
                }

                std::string cluster_factory_type = dsn_config_get_value_string(
                    clusters[i],
                    "factory",
                    "",
                    "factory name to create the target cluster scheduler"
                    );

                auto cluster = ::dsn::utils::factory_store<cluster_scheduler>::create(
                    cluster_factory_type.c_str(),
                    PROVIDER_TYPE_MAIN
                    );

                if (nullptr == cluster)
                {
                    derror("cluster type %s is not defined", cluster_factory_type.c_str());
                    return ERR_OBJECT_NOT_FOUND;
                }

                std::shared_ptr<cluster_ex> ce(new cluster_ex);
                ce->scheduler.reset(cluster);
                ce->cluster.name = cluster_name;
                ce->cluster.type = cluster->type();

                _clusters[cluster_name] = ce;
            }

            return ERR_OK;
        }

        void deploy_svc_service_impl::on_deploy(const deploy_request& req, ::dsn::rpc_replier<deploy_info>& reply)
        {
            deploy_info di;
            di.name = req.name;
            di.package_id = req.package_id;
            di.cluster = req.cluster_name;
            di.status = service_status::SS_FAILED;

            auto svc = get_service(req.name);

            // service with the same name is already deployed
            if (svc != nullptr)
            {
                di.error = ::dsn::ERR_SERVICE_ALREADY_RUNNING;
                di.cluster = svc->cluster;

                reply(di);
                return;
            }

            auto cluster = get_cluster(req.cluster_name);

            // cluster is missing
            if (cluster == nullptr)
            {
                di.error = ::dsn::ERR_CLUSTER_NOT_FOUND;
                di.cluster = req.cluster_name;

                reply(di);
                return;
            }

            // prepare for svc starting
            svc.reset(new ::dsn::dist::deployment_unit());
            svc->cluster = req.cluster_name;
            svc->package_id = req.package_id;
            svc->name = req.name;
            svc->deployment_callback = [this, svc](::dsn::error_code err, ::dsn::rpc_address addr)
            {
                this->on_service_deployed(svc, err, addr);
            };
            svc->failure_notification = [this, svc](::dsn::error_code err, const std::string& err_msg)
            {
                this->on_service_failure(svc, err, err_msg);
            };

            // add to service collections
            {
                ::dsn::service::zauto_write_lock l(_service_lock);
                auto it = _services.find(req.name);
                if (it != _services.end())
                {
                    di.error = ::dsn::ERR_SERVICE_ALREADY_RUNNING;
                }
                else
                {
                    di.error = ::dsn::ERR_OK.to_string();
                    di.status = service_status::SS_PREPARE_RESOURCE;
                    _services.insert(
                        std::unordered_map<std::string, std::shared_ptr<::dsn::dist::deployment_unit> >::value_type(
                        req.name, svc
                        ));
                }
            }

            // start resource downloading ...
            if (di.error == ::dsn::ERR_IO_PENDING)
            {
                std::stringstream ss;
                ss << req.package_id << "." << req.name;

                std::string ldir = utils::filesystem::path_combine(_service_dir, ss.str());
                std::vector<std::string> files;

                file::copy_remote_files(
                    req.package_server,
                    req.package_full_path,
                    files,
                    ldir,
                    true,
                    LPC_DEPLOY_DOWNLOAD_RESOURCE,
                    this,
                    [this, svc](error_code err, size_t sz)
                    {
                        this->download_service_resource_completed(err, svc);
                    }
                );
            }

            // reply to client
            reply(di);
            return;
        }

        void deploy_svc_service_impl::download_service_resource_completed(error_code err, std::shared_ptr<::dsn::dist::deployment_unit> svc)
        {
            if (err != ::dsn::ERR_OK)
            {
                svc->status = ::dsn::dist::service_status::SS_FAILED;
                return;
            }   

            svc->status = ::dsn::dist::service_status::SS_DEPLOYING;
            auto cluster = get_cluster(svc->cluster);
            dassert(nullptr != cluster, "cluster %s is missing", svc->cluster.c_str());

            cluster->scheduler->schedule(svc);
        }

        void deploy_svc_service_impl::on_service_deployed(
            std::shared_ptr<::dsn::dist::deployment_unit> unit,
            ::dsn::error_code err,
            ::dsn::rpc_address addr
            )
        {
            if (err != ::dsn::ERR_OK)
                unit->status = ::dsn::dist::service_status::SS_FAILED;
            else
                unit->status = ::dsn::dist::service_status::SS_RUNNING;
        }

        void deploy_svc_service_impl::on_service_failure(
            std::shared_ptr<::dsn::dist::deployment_unit> unit,
            ::dsn::error_code err,
            const std::string& err_msg
            )
        {
            // TODO: fail-over?
            unit->status = ::dsn::dist::service_status::SS_FAILED;
        }

        void deploy_svc_service_impl::on_undeploy(const std::string& service_name, ::dsn::rpc_replier<error_code>& reply)
        {
            error_code err;

            {
                ::dsn::service::zauto_write_lock l(_service_lock);
                auto it = _services.find(service_name);
                if (it != _services.end())
                {
                    _services.erase(it);
                    err = ::dsn::ERR_OK;
                }
                else
                {
                    err = ::dsn::ERR_SERVICE_NOT_FOUND;
                }
            }

            reply(err);
        }

        void deploy_svc_service_impl::on_get_service_list(const std::string& package_id, ::dsn::rpc_replier<deploy_info_list>& reply)
        {
            deploy_info_list clist;

            {
                ::dsn::service::zauto_read_lock l(_service_lock);
                for (auto& c : _services)
                {
                    if (c.second->package_id == package_id)
                    {
                        deploy_info di;
                        di.cluster = c.second->cluster;
                        di.package_id = c.second->package_id;
                        di.error = ::dsn::ERR_OK;
                        di.status = c.second->status;
                        clist.services.push_back(di);
                    }
                }
            }

            reply(clist);
        }

        void deploy_svc_service_impl::on_get_service_info(const std::string& service_name, ::dsn::rpc_replier<deploy_info>& reply)
        {
            deploy_info di;
            di.name = service_name;

            {
                ::dsn::service::zauto_read_lock l(_service_lock);
                auto it = _services.find(service_name);
                if (it == _services.end())
                {
                    di.error = ::dsn::ERR_SERVICE_NOT_FOUND;
                }
                else
                {
                    di.cluster = it->second->cluster;
                    di.package_id = it->second->package_id;
                    di.error = ::dsn::ERR_OK;
                    di.status = it->second->status;
                }
            }

            reply(di);
        }

        void deploy_svc_service_impl::on_get_cluster_list(const std::string&, ::dsn::rpc_replier<cluster_list>& reply)
        {
            cluster_list clist;

            {
                ::dsn::service::zauto_read_lock l(_cluster_lock);
                for (auto& c : _clusters)
                {
                    clist.clusters.push_back(c.second->cluster);
                }
            }

            reply(clist);
        }

        std::shared_ptr<::dsn::dist::deployment_unit> deploy_svc_service_impl::get_service(const std::string& name)
        {
            ::dsn::service::zauto_read_lock l(_service_lock);
            auto it = _services.find(name);
            if (it != _services.end())
            {
                return it->second;
            }
            else
            {
                return nullptr;
            }
        }

        std::shared_ptr<deploy_svc_service_impl::cluster_ex> deploy_svc_service_impl::get_cluster(const std::string& name)
        {
            ::dsn::service::zauto_read_lock l(_cluster_lock);
            auto it = _clusters.find(name);
            if (it != _clusters.end())
            {
                return it->second;
            }
            else
            {
                return nullptr;
            }
        }

    }
}
