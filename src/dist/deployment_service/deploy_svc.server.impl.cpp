
# include "deploy_svc.server.impl.h"
# include <dsn/utility/factory_store.h>

# include <rapidjson/document.h> 
# include <rapidjson/writer.h>
# include <rapidjson/stringbuffer.h>


namespace dsn
{
    namespace dist
    {

        #define TEST_PARAM(x) {if(!(x)){return ERR_INVALID_PARAMETERS;}}

        inline const char* rm_type_prefix(const char* s)
        {
            //the monitor need not know the exact class name of the status code
            //for example, instead of "cluster_type::cstype_docker",
            //just "docker" would be enough
            const char* postfix = strchr(s, ':');
            if (postfix != nullptr)
            {
                return postfix + 2;
            }
            else
            {
                //s might be something like "unknown"
                return s;
            }
        }
        
        DEFINE_THREAD_POOL_CODE(THREAD_POOL_DEPLOY_LONG)

        DEFINE_TASK_CODE_AIO(LPC_DEPLOY_DOWNLOAD_RESOURCE, TASK_PRIORITY_COMMON, THREAD_POOL_DEPLOY_LONG)

        deploy_svc_service_impl::deploy_svc_service_impl()
        {
        }

        deploy_svc_service_impl::~deploy_svc_service_impl()
        {
        }

        void deploy_svc_service_impl::stop(dsn_gpid gd)
        {
            close_service(gd);
        }

        error_code deploy_svc_service_impl::start(dsn_gpid gd)
        {
            std::string pdir = utils::filesystem::path_combine(dsn_get_app_data_dir(), "services");
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

            open_service(gd);
            return ERR_OK;
        }

        void deploy_svc_service_impl::on_service_failure(
            std::shared_ptr< ::dsn::dist::deployment_unit> unit,
            ::dsn::error_code err,
            const std::string& err_msg
            )
        {
            // TODO: fail-over?
            unit->status = ::dsn::dist::service_status::SS_FAILED;
        }


        void deploy_svc_service_impl::download_service_resource_completed(error_code err, std::shared_ptr< ::dsn::dist::deployment_unit> svc)
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
            std::shared_ptr< ::dsn::dist::deployment_unit> unit,
            ::dsn::error_code err,
            ::dsn::rpc_address addr
            )
        {
            if (err != ::dsn::ERR_OK)
                unit->status = ::dsn::dist::service_status::SS_FAILED;
            else
                unit->status = ::dsn::dist::service_status::SS_RUNNING;
        }

        void deploy_svc_service_impl::on_service_undeployed(
            std::string service_name,
            ::dsn::error_code err,
            const std::string& err_msg
            )
        {
            ::dsn::service::zauto_write_lock l(_service_lock);
            auto it = _services.find(service_name);
            if (it != _services.end())
            {
                if (err == ::dsn::ERR_OK)
                {
                    _services.erase(service_name);
                }
                else
                {
                    //TODO: undeploy failded doesn't means that service failed,
                    //      this status code need to be explicitly discussed.
                    //      for example: if service failed on deployment,
                    //      the scheduler will simply erase it, and report FAILED when
                    //      called to undeploy it, which makes it impossible for user to delete it.
                    auto svc = it->second;
                    svc->status = ::dsn::dist::service_status::SS_FAILED;
                }
            }
        }


        void deploy_svc_service_impl::on_deploy_internal(const deploy_request& req, /*out*/ deploy_info& di)
        {
            di.name = req.name;
            di.info = req.info;
            di.cluster = req.cluster_name;
            di.status = service_status::SS_FAILED;

            auto svc = get_service(req.name);

            // service with the same name is already deployed
            if (svc != nullptr)
            {
                di.error = ::dsn::ERR_SERVICE_ALREADY_RUNNING;
                di.cluster = svc->cluster;
                return;
            }

            auto cluster = get_cluster(req.cluster_name);

            // cluster is missing
            if (cluster == nullptr)
            {
                di.error = ::dsn::ERR_CLUSTER_NOT_FOUND;
                di.cluster = req.cluster_name;
                return;
            }

            // prepare for svc starting
            svc.reset(new ::dsn::dist::deployment_unit());
            svc->cluster = req.cluster_name;
            svc->info = req.info;
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
                        std::unordered_map<std::string, std::shared_ptr< ::dsn::dist::deployment_unit> >::value_type(
                        req.name, svc
                        ));
                }
            }

            // start resource downloading ...
            if (di.error == ::dsn::ERR_OK)
            {
                std::stringstream ss;
                ss << req.info.app_name << "." << req.name;

                std::string ldir = utils::filesystem::path_combine(_service_dir, ss.str());
                auto pos = req.package_full_path.rfind("/");
                std::string source_dir = req.package_full_path.substr(0,pos);
                std::string file = req.package_full_path.substr( pos + 1, std::string::npos);
                std::vector<std::string> files{file};
                svc->local_package_directory = ldir;

                dinfo("source dir is %s and file is %s",source_dir.c_str(),file.c_str());

                file::copy_remote_files(
                    req.package_server,
                    source_dir,
                    files,
                    ldir,
                    true,
                    LPC_DEPLOY_DOWNLOAD_RESOURCE,
                    this,
                    [this, svc, ldir, file](error_code err, size_t sz)
                {
                    std::string command = "7z x " + ldir + '/' + file + " -y -o" + ldir;
                // decompress when completed
                    system(command.c_str());
                    this->download_service_resource_completed(err, svc);
                }
                );
            }
        }
        
        void deploy_svc_service_impl::on_deploy(const deploy_request& req, ::dsn::rpc_replier<deploy_info>& reply)
        {
            deploy_info di;

            on_deploy_internal(req, di);

            reply(di);
            return;
        }

        void deploy_svc_service_impl::on_undeploy_internal(const std::string& service_name, error_code& err)
        {
            ::dsn::service::zauto_write_lock l(_service_lock);
            auto it = _services.find(service_name);
            if (it != _services.end())
            {
                auto svc = it->second;
                auto cluster_name = svc->cluster;
                auto cluster = get_cluster(cluster_name);

                if (cluster == nullptr)
                {
                    err = ::dsn::ERR_CLUSTER_NOT_FOUND;
                    return;
                }

                svc->undeployment_callback = [service_name, this](::dsn::error_code err, const std::string& err_msg)
                {
                    this->on_service_undeployed(service_name, err, err_msg);
                };
                svc->status = service_status::SS_UNDEPLOYING;
                cluster->scheduler->unschedule(svc);
                err = ::dsn::ERR_IO_PENDING;
            }
            else
            {
                err = ::dsn::ERR_SERVICE_NOT_FOUND;
            }
        }
        
        void deploy_svc_service_impl::on_undeploy(const std::string& service_name, ::dsn::rpc_replier<error_code>& reply)
        {
            error_code err;

            on_undeploy_internal(service_name, err);

            reply(err);
        }

        void deploy_svc_service_impl::on_get_service_list_internal(const std::string& app_type, deploy_info_list& dlist)
        {
            ::dsn::service::zauto_read_lock l(_service_lock);
            for (auto& c : _services)
            {
                if (c.second->info.app_type == app_type || app_type.size() == 0)
                {
                    deploy_info di;
                    di.cluster = c.second->cluster;
                    di.name = c.second->name;
                    di.service_url = c.second->service_url;
                    di.info = c.second->info;
                    di.error = ::dsn::ERR_OK;
                    di.status = c.second->status;
                    dlist.services.push_back(di);
                }
            }
        }

        void deploy_svc_service_impl::on_get_service_list(const std::string& package_id, ::dsn::rpc_replier<deploy_info_list>& reply)
        {
            deploy_info_list dlist;

            on_get_service_list_internal(package_id, dlist);

            reply(dlist);
        }

        void deploy_svc_service_impl::on_get_service_info_internal(const std::string& service_name, deploy_info& di)
        {
            di.name = service_name;

            ::dsn::service::zauto_read_lock l(_service_lock);
            auto it = _services.find(service_name);
            if (it == _services.end())
            {
                di.error = ::dsn::ERR_SERVICE_NOT_FOUND;
            }
            else
            {
                di.cluster = it->second->cluster;
                di.info = it->second->info;
                di.error = ::dsn::ERR_OK;
                di.status = it->second->status;
            }
        }

        void deploy_svc_service_impl::on_get_service_info(const std::string& service_name, ::dsn::rpc_replier<deploy_info>& reply)
        {
            deploy_info di;

            on_get_service_info_internal(service_name, di);

            reply(di);
        }

        void deploy_svc_service_impl::on_get_cluster_list_internal(const std::string& format, cluster_list& clist)
        {
            ::dsn::service::zauto_read_lock l(_cluster_lock);
            for (auto& c : _clusters)
            {
                clist.clusters.push_back(c.second->cluster);
            }
        }

        void deploy_svc_service_impl::on_get_cluster_list(const std::string& format, ::dsn::rpc_replier<cluster_list>& reply)
        {
            cluster_list clist;

            on_get_cluster_list_internal(format, clist);

            reply(clist);
        }

        std::shared_ptr< ::dsn::dist::deployment_unit> deploy_svc_service_impl::get_service(const std::string& name)
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
