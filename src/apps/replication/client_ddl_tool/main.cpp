#include <dsn/service_api_c.h>
#include <dsn/ports.h>
#include <dsn/dist/replication/empty_app.h>
#include <dsn/dist/replication/client_ddl.h>
#include <iostream>

using namespace dsn::client;

void usage(char* exe)
{
    std::cout << "Usage:" << std::endl;
    std::cout << "\t" << exe << " <config.ini> create_app -name <app_name> -type <app_type> [-pc partition_count] [-rc replication_count]" << std::endl;
    std::cout << "\t" << exe << " <config.ini> drop_app -name <app_name>" << std::endl;
    std::cout << "\t\tpartition count must be a power of 2" << std::endl;
    std::cout << "\t\tapp_name and app_type shoud be composed of a-z, 0-9 and underscore" << std::endl;
    exit(-1);
}

extern void dsn_core_init();

int init_environment(char* exe, char* config_file)
{
    // register all possible services
    dsn::register_app<dsn::client::empty_app>("empty_app");

    dsn_core_init();

    //use config file to run
    char arg1[] = "-app_list";
    char arg2[] = "empty_app@1`";
    char* argv[] = { exe, config_file, arg1, arg2};

    dsn_run(4, argv, false);
    dsn_mimic_app("empty_app", 1);
    return 0;
}

int main(int argc, char** argv)
{
    if(argc < 5)
    {
        usage(argv[0]);
    }
    std::cout << "running ddl tool... " << std::endl;

    std::string app_name;
    std::string app_type;
    int partition_count = 4;
    int replica_count = 3;

    for(int index = 3; index < argc; index++)
    {
        if(strcmp(argv[index], "-name") == 0 && argc > index)
        {
            app_name.assign(argv[++index]);
            std::cout << "app_name:" << app_name <<std::endl;
        }
        if(strcmp(argv[index], "-type") == 0 && argc > index)
        {
            app_type.assign(argv[++index]);
            std::cout << "app_type:" << app_type <<std::endl;
        }
        if(strcmp(argv[index], "-pc") == 0 && argc > index)
        {
            partition_count = atol(argv[++index]);
        }
        if(strcmp(argv[index], "-rc") == 0 && argc > index)
        {
            replica_count = atol(argv[++index]);
        }
    }

    if(init_environment(argv[0], argv[1]) < 0)
    {
        std::cerr << "Init failed" << std::endl;
    }
    std::cout << "Init succeed" << std::endl;

    std::vector<dsn::rpc_address> meta_servers;
    dsn::replication::replication_app_client_base::load_meta_servers(meta_servers);
    client_ddl client(meta_servers);
    std::string command = argv[2];

    if (command == "create_app") {
        if(app_name.empty() || app_type.empty())
            usage(argv[0]);
        dsn::error_code err = client.create_app(app_name, app_type, partition_count, replica_count);
        if(err == dsn::ERR_OK)
            std::cout << "create app:" << app_name << " succeed" << std::endl;
        else
            std::cout << "create app:" << app_name << " failed, error=" << dsn_error_to_string(err) << std::endl;
    }
    else if(command == "drop_app") {
        if(app_name.empty())
            usage(argv[0]);
        dsn::error_code err = client.drop_app(app_name);
        if(err == dsn::ERR_OK)
            std::cout << "drop app:" << app_name << " succeed" << std::endl;
        else
            std::cout << "drop app:" << app_name << " failed, error=" << dsn_error_to_string(err) << std::endl;
    }
}
