// apps
# include "deploy_svc.app.h"
# include "../cluster_scheduler/scheduler_providers/scheduler_providers.h"


void dsn_app_registration_deploy_svc()
{
    dsn::dist::register_cluster_scheduler_providers();
    // register all possible service apps
    dsn::register_app< ::dsn::dist::deploy_svc_server_app>("server");
    dsn::register_app< ::dsn::dist::deploy_svc_client_app>("client");
    dsn::register_app< ::dsn::dist::deploy_svc_perf_test_client_app>("client.perf.deploy_svc");
}

# ifndef DSN_RUN_USE_SVCHOST

int main(int argc, char** argv)
{
    dsn_app_registration_deploy_svc();
    
    // specify what services and tools will run in config file, then run
    dsn_run(argc, argv, true);
    return 0;
}

# else

# include <dsn/tool-api/module_int.cpp.h>

MODULE_INIT_BEGIN(deploy_svc)
    dsn_app_registration_deploy_svc();
MODULE_INIT_END

# endif
