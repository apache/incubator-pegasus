#include "client_ddl.h"
#include <iostream>

using namespace dsn::replication;

void usage(char* exe)
{
    std::cout << "Usage:" << std::endl;
    std::cout << "\t" << exe << " <config.ini> create_app -name <app_name> -type <app_type> [-pc partition_count] [-rc replication_count]" << std::endl;
    std::cout << "\t" << exe << " <config.ini> drop_app -name <app_name>" << std::endl;
    std::cout << "\t" << exe << " <config.ini> list_apps [-status <all|available|creating|creating_failed|dropping|dropping_failed|dropped>] [-o <out_file>]" << std::endl;
    std::cout << "\t" << exe << " <config.ini> list_nodes [-status <all|alive|unalive>] [-o <out_file>]" << std::endl;
    std::cout << "\t" << exe << " <config.ini> list_app -name <app_name> [-detailed] [-o <out_file>]" << std::endl;
    std::cout << "\t" << exe << " <config.ini> stop_migration" << std::endl;
    std::cout << "\t" << exe << " <config.ini> start_migration" << std::endl;
    std::cout << "\t" << exe << " <config.ini> balancer -gpid <appid.pidx> -type <move_pri|copy_pri|copy_sec> -from <from_address> -to <to_address>" << std::endl;
    std::cout << "\t\tpartition count must be a power of 2" << std::endl;
    std::cout << "\t\tapp_name and app_type shoud be composed of a-z, 0-9 and underscore" << std::endl;
    std::cout << "\t\twithout -o option, program will print status on screen" << std::endl;
    std::cout << "\t\twith -detailed option, program will also print partition state" << std::endl;
    exit(-1);
}

int init_environment(char* exe, char* config_file)
{
    //use config file to run
    char* argv[] = {exe, config_file};

    dsn_run(2, argv, false);
    return 0;
}

int main(int argc, char** argv)
{
    if(argc < 3)
    {
        usage(argv[0]);
    }
    std::cout << "running ddl tool... " << std::endl;

    std::string app_name;
    std::string app_type;
    int partition_count = 4;
    int replica_count = 3;
    std::string status;
    bool detailed = false;
    std::string out_file;

    for(int index = 3; index < argc; index++)
    {
        if(strcmp(argv[index], "-name") == 0 && argc > index)
        {
            app_name.assign(argv[++index]);
            std::cout << "app_name:" << app_name <<std::endl;
        }
        else if(strcmp(argv[index], "-type") == 0 && argc > index)
        {
            app_type.assign(argv[++index]);
            std::cout << "app_type:" << app_type <<std::endl;
        }
        else if(strcmp(argv[index], "-pc") == 0 && argc > index)
        {
            partition_count = atol(argv[++index]);
            std::cout << "partition_count:" << partition_count <<std::endl;
        }
        else if(strcmp(argv[index], "-rc") == 0 && argc > index)
        {
            replica_count = atol(argv[++index]);
            std::cout << "replica_count:" << replica_count <<std::endl;
        }
        else if(strcmp(argv[index], "-status") == 0 && argc > index)
        {
            status.assign(argv[++index]);
            std::cout << "status:" << status <<std::endl;
        }
        else if(strcmp(argv[index], "-detailed") == 0)
        {
            detailed = true;
            std::cout << "show details." <<std::endl;
        }
        else if(strcmp(argv[index], "-o") == 0 && argc > index)
        {
            out_file = argv[++index];
            std::cout << "out to file:" << out_file <<std::endl;
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
    else if(command == "list_apps") {
        dsn::replication::app_status s = dsn::replication::AS_INVALID;
        if (!status.empty() && status != "all") {
            std::transform(status.begin(), status.end(), status.begin(), ::toupper);
            status = "AS_" + status;
            s = enum_from_string(status.c_str(), dsn::replication::AS_INVALID);
            if(s == dsn::replication::AS_INVALID)
                usage(argv[0]);
        }
        dsn::error_code err = client.list_apps(s, out_file);
        if(err != dsn::ERR_OK)
            std::cout << "list apps failed, error=" << dsn_error_to_string(err) << std::endl;
    }
    else if(command == "list_nodes") {
        dsn::replication::node_status s = dsn::replication::NS_INVALID;
        if (!status.empty() && status != "all") {
            std::transform(status.begin(), status.end(), status.begin(), ::toupper);
            status = "NS_" + status;
            s = enum_from_string(status.c_str(), dsn::replication::NS_INVALID);
            if(s == dsn::replication::NS_INVALID)
                usage(argv[0]);
        }
        dsn::error_code err = client.list_nodes(s, out_file);
        if(err != dsn::ERR_OK)
            std::cout << "list nodes failed, error=" << dsn_error_to_string(err) << std::endl;
    }
    else if(command == "list_app") {
        if(app_name.empty())
            usage(argv[0]);
        dsn::error_code err = client.list_app(app_name, detailed, out_file);
        if(err == dsn::ERR_OK)
            std::cout << "list app:" << app_name << " succeed" << std::endl;
        else
            std::cout << "list app:" << app_name << " failed, error=" << dsn_error_to_string(err) << std::endl;
    }
    else if (command == "stop_migration") {
        dsn::error_code err = client.control_meta_balancer_migration(false);
        std::cout << "stop migration result: " << dsn_error_to_string(err) << std::endl;
    }
    else if (command == "start_migration") {
        dsn::error_code err = client.control_meta_balancer_migration(true);
        std::cout << "start migration result: " << err.to_string() << std::endl;
    }
    else if (command == "balancer") {
        dsn::replication::balancer_proposal_request request;
        for (int i=3; i<argc-1; i+=2) {
            if (strcmp(argv[i], "-gpid") == 0){
                sscanf(argv[i+1], "%d.%d", &request.gpid.app_id, &request.gpid.pidx);
            }
            else if (strcmp(argv[i], "-type") == 0){
                std::map<std::string, dsn::replication::balancer_type> mapper = {
                    {"move_pri", BT_MOVE_PRIMARY},
                    {"copy_pri", BT_COPY_PRIMARY},
                    {"copy_sec", BT_COPY_SECONDARY}
                };
                if (mapper.find(argv[i+1]) == mapper.end()) {
                    usage(argv[0]);
                }
                request.type = mapper[argv[i+1]];
            }
            else if (strcmp(argv[i], "-from") == 0) {
                request.from.from_string_ipv4(argv[i+1]);
            }
            else if (strcmp(argv[i], "-to") == 0) {
                request.to.from_string_ipv4(argv[i+1]);
            }
        }
        dsn::error_code err = client.send_balancer_proposal(request);
        std::cout << "send balancer proposal result: " << err.to_string() << std::endl;
    }
    else {
        std::cout << "invalid command:" << command << std::endl;
        usage(argv[0]);
    }
}
