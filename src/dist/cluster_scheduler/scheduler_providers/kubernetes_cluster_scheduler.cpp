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

# include "kubernetes_cluster_scheduler.h"
# include "kubernetes_cluster_error.h"
# include <stdlib.h>
# include <future>
namespace dsn
{
namespace dist
{
kubernetes_cluster_scheduler::kubernetes_cluster_scheduler()
    :_run_path(""),
     _k8s_state_handle(nullptr),
     _k8s_deploy_handle(nullptr),
     _k8s_undeploy_handle(nullptr)
{

}

void kubernetes_cluster_scheduler::deploy_k8s_unit(void* context, int argc, const char** argv, dsn_cli_reply* reply)
{
    auto k8s = reinterpret_cast<kubernetes_cluster_scheduler*>(context);
    if( argc == 2 )
    {
        std::string name = argv[0];
        std::string directory = argv[1];
        std::function<void(error_code,rpc_address)> cb = [](error_code err,rpc_address addr){
            dinfo("deploy err %s",err.to_string());
        };
        k8s->create_pod(name,cb,directory);
    }
    reply->message = "";
    reply->size = 0;
    reply->context = nullptr;
}
void kubernetes_cluster_scheduler::deploy_k8s_unit_cleanup(dsn_cli_reply reply)
{

}

void kubernetes_cluster_scheduler::undeploy_k8s_unit(void* context, int argc, const char** argv, dsn_cli_reply* reply)
{
    auto k8s = reinterpret_cast<kubernetes_cluster_scheduler*>(context);
    if( argc == 2 )
    {
        std::string name = argv[0];
        std::string directory = argv[1];
        std::function<void(error_code,rpc_address)> cb = [](error_code err,rpc_address addr){
            dinfo("deploy err %s",err.to_string());
        };
        k8s->delete_pod(name,cb,directory);
    }
    reply->message = "";
    reply->size = 0;
    reply->context = nullptr;
}
void kubernetes_cluster_scheduler::undeploy_k8s_unit_cleanup(dsn_cli_reply reply)
{

}
error_code kubernetes_cluster_scheduler::initialize()
{ 
#ifndef _WIN32    
    int ret;
    ret = system("kubectl version");
    if (ret != 0)
    {
        dinfo("kubectl is not in the PATH");
        return ::dsn::dist::ERR_K8S_KUBECTL_NOT_FOUND;
    }
    ret = system("kubectl cluster-info");
    if (ret != 0)
    {
        dinfo("kubernetes cluster is not on");
        return ::dsn::dist::ERR_K8S_CLUSTER_NOT_FOUND;
    }
#endif
    
    dassert(_k8s_deploy_handle == nullptr, "k8s deploy is initialized twice");
    _k8s_deploy_handle = dsn_cli_app_register("deploy","deploy onto k8s scheduler","",this,&deploy_k8s_unit,&deploy_k8s_unit_cleanup);
    dassert(_k8s_deploy_handle != nullptr, "register cli handler failed");

    dassert(_k8s_undeploy_handle == nullptr, "k8s undeploy is initialized twice");
    _k8s_undeploy_handle = dsn_cli_app_register("undeploy","undeploy from k8s scheduler","",this,&undeploy_k8s_unit,&undeploy_k8s_unit_cleanup);
    dassert(_k8s_undeploy_handle != nullptr, "register cli handler failed");
    return ::dsn::ERR_OK;
}

void kubernetes_cluster_scheduler::schedule(
                std::shared_ptr<deployment_unit>& unit
                )
{
    int ret = -1;
    bool found = false;
    {
        zauto_lock l(_lock);
        auto it = _deploy_map.find(unit->name);
        found = (it != _deploy_map.end());
    }
    if ( found )
    {
        unit->deployment_callback(::dsn::dist::ERR_K8S_DEPLOY_FAILED,rpc_address());
    }
    else
    {
        {
            zauto_lock l(_lock);
            _deploy_map.insert(std::make_pair(unit->name,unit));
        }
        dsn::tasking::enqueue(LPC_K8S_CREATE,this, [this, unit]() {
            create_pod(unit->name, unit->deployment_callback, unit->local_package_directory);
        });
    }
    
}

void kubernetes_cluster_scheduler::create_pod(std::string& name,std::function<void(error_code, rpc_address)>& deployment_callback, std::string& local_package_directory)
{
    int ret;
    std::ostringstream command;
    command << "./run.sh k8s_deploy ";
    command << "--image " << name << " -s " << local_package_directory;
    ret = system(command.str().c_str());
    if( ret == 0 )
    {
        deployment_callback(ERR_OK,rpc_address());
    }
    else
    {
        {
            zauto_lock l(_lock);
            _deploy_map.erase(name);
        }
        deployment_callback(::dsn::dist::ERR_K8S_DEPLOY_FAILED,rpc_address());
    }
}

void kubernetes_cluster_scheduler::unschedule(
        std::shared_ptr<deployment_unit>& unit
        )
{
    bool found = false;
    
    _lock.lock();
    auto it = _deploy_map.find(unit->name);
    found = (it != _deploy_map.end());
    
    if( found )
    {
        _deploy_map.erase(it);
        _lock.unlock();

        dsn::tasking::enqueue(LPC_K8S_DELETE,this, [this, unit]() {
            delete_pod(unit->name, unit->deployment_callback, unit->local_package_directory);
        });
    }
    else
    {
        _lock.unlock();
        unit->deployment_callback(ERR_K8S_UNDEPLOY_FAILED,rpc_address());
    }
}

void kubernetes_cluster_scheduler::delete_pod(std::string& name,std::function<void(error_code, rpc_address)>& deployment_callback, std::string& local_package_directory)
{
    int ret;
    std::ostringstream command;
    command << "./run.sh k8s_undeploy ";
    command << "--image " << name << " -s " << local_package_directory;
    ret = system(command.str().c_str());
    dassert( ret == 0, "k8s can't delete pods");

    // ret == 0
    deployment_callback(::dsn::ERR_OK,rpc_address()); 
}



}
}

