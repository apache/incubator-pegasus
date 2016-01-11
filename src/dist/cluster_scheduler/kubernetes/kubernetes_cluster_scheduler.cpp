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
     _k8s_deploy_handle(nullptr)
{

}
void kubernetes_cluster_scheduler::get_k8s_state(void* context, int argc, const char** argv, dsn_cli_reply* reply)
{
    auto k8s = reinterpret_cast<kubernetes_cluster_scheduler*>(context);
    std::string message = std::string("client info is ") + k8s->_run_path;
    auto danglingstring = new std::string(message);

    reply->message = danglingstring->c_str();
    reply->size = danglingstring->size();
    reply->context = danglingstring;
}
void kubernetes_cluster_scheduler::get_k8s_state_cleanup(dsn_cli_reply reply)
{
    dassert(reply.context != nullptr, "corrupted cli reply context");
    auto danglingstring = reinterpret_cast<std::string*>(reply.context);
    dassert(reply.message == danglingstring->c_str(),"corrupted cli reply message");
    delete danglingstring;
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
error_code kubernetes_cluster_scheduler::initialize()
{ 
    int ret;
    _run_path = dsn_config_get_value_string("apps.client","run_path","","");
    dassert( _run_path != "", "run path is empty");
    dinfo("run path is %s",_run_path.c_str());
    
    ret = system("kubectl version");
    if (ret != 0)
    {
        dinfo("kubectl is not in the PATH");
        return ::dsn::dist::ERR_K8S;
    }
    ret = system("kubectl cluster-info");
    if (ret != 0)
    {
        dinfo("kubernetes cluster is not on");
        return ::dsn::dist::ERR_K8S;
    }
    
    dassert(_k8s_state_handle == nullptr, "k8s state is initialized twice");
    _k8s_state_handle = dsn_cli_app_register("info","get info from k8s scheduler","",this,&get_k8s_state,&get_k8s_state_cleanup);
    dassert(_k8s_state_handle != nullptr, "register cli handler failed");
    
    dassert(_k8s_deploy_handle == nullptr, "k8s state is initialized twice");
    _k8s_deploy_handle = dsn_cli_app_register("deploy","deploy onto k8s scheduler","",this,&deploy_k8s_unit,&deploy_k8s_unit_cleanup);
    dassert(_k8s_deploy_handle != nullptr, "register cli handler failed");

    return ::dsn::ERR_OK;
}

void kubernetes_cluster_scheduler::schedule(
                std::shared_ptr<deployment_unit>& unit
                )
{
    int ret = -1;
    auto it = _deploy_map.find(unit->name);
    if (it != _deploy_map.end())
    {
        unit->deployment_callback(::dsn::dist::ERR_K8S,rpc_address());
    }
    else
    {
        _deploy_map.insert(std::make_pair(unit->name,unit));
        std::async(std::launch::async,&kubernetes_cluster_scheduler::create_pod,this,std::ref(unit->name),std::ref(unit->deployment_callback),std::ref(unit->local_package_directory));
    }
    
}

void kubernetes_cluster_scheduler::create_pod(std::string& name,std::function<void(error_code, rpc_address)>& deployment_callback, std::string& local_package_directory)
{
    int ret;
    std::ostringstream full_path;
    full_path << "./run.sh k8s_deploy ";
    full_path << "--image " << name << " -s " << local_package_directory;
    ret = system(full_path.str().c_str());
    if( ret == 0 )
    {
        deployment_callback(::dsn::ERR_OK,rpc_address());
    }
    else
    {
        _deploy_map.erase(name);
        deployment_callback(::dsn::dist::ERR_K8S,rpc_address());
    }
}



}
}

