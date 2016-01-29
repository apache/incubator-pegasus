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

# include "docker_scheduler.h"
# include "docker_error.h"
# include <stdlib.h>
# include <fstream>

namespace dsn
{
namespace dist
{
docker_scheduler::docker_scheduler()
    :_run_path(""),
     _docker_state_handle(nullptr),
     _docker_deploy_handle(nullptr),
     _docker_undeploy_handle(nullptr),
     _mgr("docker")
{

}

void docker_scheduler::deploy_docker_unit(void* context, int argc, const char** argv, dsn_cli_reply* reply)
{
    auto docker = reinterpret_cast<docker_scheduler*>(context);
    if( argc == 3 )
    {
        std::string name = argv[0];
        std::string ldir = argv[1];
        std::string rdir = argv[2];
        std::function<void(error_code,rpc_address)> cb = [](error_code err,rpc_address addr){
            dinfo("deploy err %s",err.to_string());
        };
        docker->create_containers(name,cb,ldir,rdir);
    }
    reply->message = "";
    reply->size = 0;
    reply->context = nullptr;
}
void docker_scheduler::deploy_docker_unit_cleanup(dsn_cli_reply reply)
{

}

void docker_scheduler::undeploy_docker_unit(void* context, int argc, const char** argv, dsn_cli_reply* reply)
{
    auto docker = reinterpret_cast<docker_scheduler*>(context);
    if( argc == 3 )
    {
        std::string name = argv[0];
        std::string ldir = argv[1];
        std::string rdir = argv[2];
        std::function<void(error_code,const std::string&)> cb = [](error_code err,const std::string& err_msg){
            dinfo("deploy err %s",err.to_string());
        };
        docker->delete_containers(name,cb,ldir,rdir);
    }
    reply->message = "";
    reply->size = 0;
    reply->context = nullptr;
}
void docker_scheduler::undeploy_docker_unit_cleanup(dsn_cli_reply reply)
{

}
error_code docker_scheduler::initialize()
{ 
    _run_path = dsn_config_get_value_string("apps.client","run_path","","");
    dassert( _run_path != "", "run path is empty");
    dinfo("run path is %s",_run_path.c_str());
#ifndef _WIN32    
    int ret;
    FILE *in;
    ret = system("docker version");
    if (ret != 0)
    {
        dinfo("docker is not in the PATH");
        return ::dsn::dist::ERR_DOCKER_BINARY_NOT_FOUND;
    }
    in = popen("service docker status","r");
    if (in == nullptr)
    {
        dinfo("docker daemon is not running");
        return ::dsn::dist::ERR_DOCKER_DAEMON_NOT_FOUND;
    }
    else
    {
        char buff[512];
        while( fgets(buff,sizeof(buff),in) != nullptr)
        {
            constexpr const char * substr = "docker start/running";
            constexpr size_t length = strlen(substr);
            if( strncmp(substr,buff,length) != 0 )
            {
                dinfo("docker daemon is not running");
                return ::dsn::dist::ERR_DOCKER_DAEMON_NOT_FOUND;
            }
        }
    }
    pclose(in);
#endif
    
    dassert(_docker_deploy_handle == nullptr, "docker deploy is initialized twice");
    _docker_deploy_handle = dsn_cli_app_register("deploy","deploy onto docker scheduler","",this,&deploy_docker_unit,&deploy_docker_unit_cleanup);
    dassert(_docker_deploy_handle != nullptr, "register cli handler failed");

    dassert(_docker_undeploy_handle == nullptr, "docker undeploy is initialized twice");
    _docker_undeploy_handle = dsn_cli_app_register("undeploy","undeploy from docker scheduler","",this,&undeploy_docker_unit,&undeploy_docker_unit_cleanup);
    dassert(_docker_undeploy_handle != nullptr, "register cli handler failed");
    return ::dsn::ERR_OK;
}

void docker_scheduler::get_app_list(std::string& ldir,/*out*/std::vector<std::string>& app_list )
{
#ifndef _WIN32
    std::string popen_command = "cat " + ldir + "/applist";
    FILE *f = popen(popen_command.c_str(),"r");
    char buffer[128];
    fgets(buffer,128,f);
    ::dsn::utils::split_args(buffer, app_list, ' ');
#endif
}


void docker_scheduler::write_machine_list(std::string& name, std::string& ldir)
{
    std::vector<std::string> app_list;

    get_app_list(ldir,app_list);

    for( auto& app : app_list)
    {
        std::string machine_file = ldir + "/" + app + "list";
        std::vector<std::string> machine_list;
        std::vector<std::string> f_list;
        std::vector<std::string> a_list;
        int count = 1;
        //TODO: handle error if machine not enough
        _mgr.get_machine( count, f_list, a_list);
        _machine_map[name].insert(_machine_map[name].begin(),a_list.begin(),a_list.end());
        std::ofstream fd;
        fd.open(machine_file.c_str(), std::ios_base::app);
        //TODO: handle error if file open failed
        for( auto& machine: a_list )
        {
            fd << machine << std::endl;
        }
        fd.close();
    }
}

void docker_scheduler::return_machines(std::string& name)
{
    _mgr.return_machine(_machine_map[name]);
}


void docker_scheduler::schedule(
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
        unit->deployment_callback(::dsn::dist::ERR_DOCKER_DEPLOY_FAILED,rpc_address());
    }
    else
    {
        write_machine_list(unit->name, unit->local_package_directory);
        {
            zauto_lock l(_lock);
            _deploy_map.insert(std::make_pair(unit->name,unit));
        }

        dsn::tasking::enqueue(LPC_DOCKER_CREATE,this, [this, unit]() {
            create_containers(unit->name, unit->deployment_callback, unit->local_package_directory, unit->remote_package_directory);
        });
    }
    
}

void docker_scheduler::create_containers(std::string& name,std::function<void(error_code, rpc_address)>& deployment_callback, std::string& local_package_directory, std::string& remote_package_directory)
{
    int ret;
    std::ostringstream command;
    command << "./run_docker.sh deploy_and_start ";
    command << " -d " << name << " -s " << local_package_directory;
    if( remote_package_directory == "" )
        command << " -t " << local_package_directory;
    else
        command << " -t " << remote_package_directory;
    ret = system(command.str().c_str());
    if( ret == 0 )
    {
#ifndef _WIN32
        std::string popen_command = "IP=`cat "+ local_package_directory +"/metalist`;echo ${IP#*@}";
        FILE *f = popen(popen_command.c_str(),"r");
        char buffer[30];
        fgets(buffer,30,f);
        {
            zauto_lock l(_lock);
            auto unit = _deploy_map[name];
            unit->service_url = buffer;
        }
#endif
        deployment_callback(ERR_OK,rpc_address());
    }
    else
    {
        return_machines(name);
        {
            zauto_lock l(_lock);
            _deploy_map.erase(name);
        }
        deployment_callback(::dsn::dist::ERR_DOCKER_DEPLOY_FAILED,rpc_address());
    }
}

void docker_scheduler::unschedule(
        std::shared_ptr<deployment_unit>& unit
        )
{
    bool found = false;
    
    _lock.lock();
    auto it = _deploy_map.find(unit->name);
    found = (it != _deploy_map.end());
    
    if( found )
    {
        return_machines(unit->name);
        _deploy_map.erase(it);
        _lock.unlock();

        dsn::tasking::enqueue(LPC_DOCKER_DELETE,this, [this, unit]() {
            delete_containers(unit->name, unit->undeployment_callback, unit->local_package_directory, unit->remote_package_directory);
        });
    }
    else
    {
        _lock.unlock();
        unit->undeployment_callback(ERR_DOCKER_UNDEPLOY_FAILED,std::string());
    }
}

void docker_scheduler::delete_containers(std::string& name,std::function<void(error_code, const std::string&)>& undeployment_callback, std::string& local_package_directory, std::string& remote_package_directory)
{
    int ret;
    std::ostringstream command;
    command << "./run_docker.sh stop_and_clean ";
    command << " -d " << name << " -s " << local_package_directory;
    if (remote_package_directory == "")
    {
        command << " -t " << local_package_directory;
    }
    else
    {
        command << " -t " << remote_package_directory;
    }
    puts("docker_scheduler delete_containers before system call");
    ret = system(command.str().c_str());
    puts("docker_scheduler delete_containers after system call");

    // TODO: deal with this error or notice the dev server
    dassert( ret == 0, "docker can't delete pods");

    // ret == 0
    undeployment_callback(::dsn::ERR_OK,std::string()); 
}



}
}

