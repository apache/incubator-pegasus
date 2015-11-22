// apps
# include "repli.app.h"
# include <dsn/cpp/utils.h>
# include <iostream>
# include <thread>
# include "unistd.h"

bool g_done = false;

int main(int argc, char** argv)
{
    if (argc < 2)
    {
        std::cerr << "USAGE: " << argv[0] << " <command> <params...>" << std::endl;
        dsn::service::repli_app::usage();
        return -1;
    }

    std::string conf;
    char buf[4096];
    int slen = readlink("/proc/self/exe", buf, sizeof(buf));
    if (slen != -1)
    {
        std::string dir = dsn::utils::filesystem::remove_file_name(buf);
        conf = dir + "/config.ini";
        if (!dsn::utils::filesystem::file_exists(conf))
        {
            std::cerr << "ERROR: config file not found: " << conf << std::endl;
            dsn::service::repli_app::usage();
            return -1;
        }
    }

    // register all possible service apps
    dsn::register_app<::dsn::service::repli_app>("repli");

    dsn::service::repli_app::set_args(argc - 1, argv + 1);

    // specify what services and tools will run in config file, then run
    dsn_run_config(conf.c_str(), false);

    while (!g_done)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    return 0;
}
