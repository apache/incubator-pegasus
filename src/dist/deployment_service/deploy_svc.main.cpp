// apps
# include "deploy_svc.app.example.h"

void dsn_app_registration()
{
    // register all possible service apps
    dsn::register_app< deploy_svc_server_app>("server");
    dsn::register_app< deploy_svc_client_app>("client");
    dsn::register_app< deploy_svc_perf_test_client_app>("client.perf.deploy_svc");
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
