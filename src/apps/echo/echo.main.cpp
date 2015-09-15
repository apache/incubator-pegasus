// apps
# include "echo.app.example.h"

void module_init()
{
    // register all possible service apps
    dsn::register_app<::dsn::example::echo_server_app>("server");
    dsn::register_app<::dsn::example::echo_client_app>("client");
    dsn::register_app<::dsn::example::echo_perf_test_client_app>("client.perf.echo");
}

# ifndef DSN_RUN_USE_SVCHOST

int main(int argc, char** argv)
{
    module_init();
    
    // specify what services and tools will run in config file, then run
    dsn_run(argc, argv, true);

    // TODO: external echo lib test
    dsn_mimic_app("client", 1);
    dsn::rpc_address server_addr("localhost", 27001);
    dsn::example::echo_client client(server_addr);
    std::string resp;

    client.ping("hihihihihihii", resp);

    return 0;
}

# else

# include <dsn/internal/module_int.cpp.h>

# endif
