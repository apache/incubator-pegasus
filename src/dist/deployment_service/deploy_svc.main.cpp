// apps
# include "deploy_svc.app.h"
# include <dsn/internal/factory_store.h>
# include "../cluster_scheduler/scheduler_providers/kubernetes_cluster_scheduler.h"
# include "../cluster_scheduler/scheduler_providers/docker_scheduler.h"
static bool register_component_provider(
        const char * name,
        ::dsn::dist::cluster_scheduler::factory f)
{
    return dsn::utils::factory_store<::dsn::dist::cluster_scheduler>::register_factory(
            name,
            f,
            PROVIDER_TYPE_MAIN);
}



void dsn_app_registration()
{
    // register all cluster provider
    register_component_provider(
            "dsn::dist::kubernetes_cluster_scheduler",
            ::dsn::dist::cluster_scheduler::create<::dsn::dist::kubernetes_cluster_scheduler>
            );
    register_component_provider(
            "dsn::dist::docker_scheduler",
            ::dsn::dist::cluster_scheduler::create<::dsn::dist::docker_scheduler>
            );
    // register all possible service apps
    dsn::register_app< ::dsn::dist::deploy_svc_server_app>("server");
    dsn::register_app< ::dsn::dist::deploy_svc_client_app>("client");
    dsn::register_app< ::dsn::dist::deploy_svc_perf_test_client_app>("client.perf.deploy_svc");
}

# ifndef DSN_RUN_USE_SVCHOST

int main(int argc, char** argv)
{
    dsn_app_registration();
    
    // specify what services and tools will run in config file, then run
    dsn_run(argc, argv, true);
    return 0;
}

# else

# include <dsn/internal/module_int.cpp.h>

MODULE_INIT_BEGIN
    dsn_app_registration();
MODULE_INIT_END

# endif
