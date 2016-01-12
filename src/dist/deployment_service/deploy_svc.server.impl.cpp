
# include "deploy_svc.server.impl.h"

DEFINE_THREAD_POOL_CODE(THREAD_POOL_DEPLOY_LONG)

DEFINE_TASK_CODE(LPC_DEPLOY_DOWNLOAD_RESOURCE, TASK_PRIORITY_COMMON, THREAD_POOL_DEPLOY_LONG)

void deploy_svc_service_impl::on_deploy(const deploy_request& req, ::dsn::rpc_replier<deploy_info>& reply)
{
    deploy_info di;
    di.name = req.name;
    di.package_id = req.package_id;
    di.cluster = req.cluster_name;
    di.status = enum_to_string(::dsn::dist::service_status::SS_FAILED);

    auto svc = get_service(req.name);

    // service with the same name is already deployed
    if (svc != nullptr)
    {
        di.error = ::dsn::ERR_SERVICE_ALREADY_RUNNING.to_string();
        di.cluster = svc->cluster;
    
        reply(di);
        return;
    }

    auto cluster = get_cluster(req.cluster_name);

    // cluster is missing
    if (cluster == nullptr)
    {
        di.error = ::dsn::ERR_CLUSTER_NOT_FOUND.to_string();
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
            di.error = ::dsn::ERR_SERVICE_ALREADY_RUNNING.to_string();
        }
        else
        {
            di.error = ::dsn::ERR_OK.to_string();
            di.status = enum_to_string(::dsn::dist::service_status::SS_PREPARE_RESOURCE);
            _services.insert(
                std::unordered_map<std::string, std::shared_ptr<::dsn::dist::deployment_unit> >::value_type(
                req.name, svc
                ));
        }
    }   
    
    // start resource downloading ...
    if (di.error == std::string(::dsn::ERR_IO_PENDING.to_string()))
    {
     /*::dsn::tasking
            LPC_DEPLOY_DOWNLOAD_RESOURCE,
            this,
            [this, svc, ru]() {
                this->download_service_resource_completed(ru, svc);
            }
        );*/

        std::string ru = req.package_local_path;

        // use embedded nfs to download the resource to local machine
        // TODO:

        /*utils::filesystem::remove_path(_app->learn_dir());
        utils::filesystem::create_directory(_app->learn_dir());

        _potential_secondary_states.learn_remote_files_task =
            file::copy_remote_files(resp->config.primary,
            resp->base_local_dir,
            resp->state.files,
            _app->learn_dir(),
            true,
            LPC_REPLICATION_COPY_REMOTE_FILES,
            this,
            std::bind(&replica::on_copy_remote_state_completed, this,
            std::placeholders::_1,
            std::placeholders::_2,
            req,
            resp)
            );*/
    }

    // reply to client
    reply(di);
    return;
}

void deploy_svc_service_impl::download_service_resource_completed(std::shared_ptr<::dsn::dist::deployment_unit> svc)
{
    // TODO: 
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

void deploy_svc_service_impl::on_undeploy(const std::string& service_name, ::dsn::rpc_replier<std::string>& reply)
{
    std::string err;
    
    {
        ::dsn::service::zauto_write_lock l(_service_lock);
        auto it = _services.find(service_name);
        if (it != _services.end())
        {
            _services.erase(it);
            err = ::dsn::ERR_OK.to_string();
        }
        else
        {
            err = ::dsn::ERR_SERVICE_NOT_FOUND.to_string();
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
                di.error = ::dsn::ERR_OK.to_string();
                di.status = enum_to_string(c.second->status);
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
            di.error = ::dsn::ERR_SERVICE_NOT_FOUND.to_string();
        }
        else
        {    
            di.cluster = it->second->cluster;
            di.package_id = it->second->package_id;
            di.error = ::dsn::ERR_OK.to_string();
            di.status = enum_to_string(it->second->status);
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
