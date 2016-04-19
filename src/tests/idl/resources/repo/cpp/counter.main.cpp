// apps
# include "counter.app.example.h"
# include "counter.server.impl.h"
# include "test.common.h"

void dsn_app_registration()
{
    // register all possible service apps
    dsn::register_app< ::dsn::example::counter_server_app>("server");
    dsn::register_app< ::dsn::example::counter_client_app>("client");
    dsn::register_app< ::dsn::example::counter_perf_test_client_app>("client.perf.counter");
}

# ifndef DSN_RUN_USE_SVCHOST

int main(int argc, char** argv)
{
    dsn_app_registration();

    // specify what services and tools will run in config file, then run
    dsn_run_config(argv[1], false);
    int counter = 0;
    while (witness == NULL || !witness->finished())
    {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        if (++counter > 10)
        {
            std::cout << "it takes too long to complete..." << std::endl;
            dsn_exit(1);
        }
    }
    bool passed = true;
    std::map<std::string, bool> result = witness->result();
    for (auto i : result)
    {
        std::string name = i.first;
        bool ok = i.second;
        passed = passed && ok;
        std::cout << name << " " << (ok ? "passed" : "failed") << std::endl;
    }
    int ret = (int)(!passed);
    dsn_exit(ret);
}

# else

# include <dsn/internal/module_int.cpp.h>

MODULE_INIT_BEGIN
dsn_app_registration();
MODULE_INIT_END

# endif
