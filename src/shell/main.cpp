// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <pegasus/version.h>
#include <setjmp.h>
#include <signal.h>
#include <algorithm>
#include "args.h"
#include "command_executor.h"
#include "commands.h"

std::string s_last_history;
const int max_params_count = 10000;
std::map<std::string, command_executor *> commands_map;
shell_context global_context;
size_t max_length = 0;

void print_help();
bool help_info(command_executor *e, shell_context *sc, arguments args)
{
    print_help();
    return true;
}

command_executor commands[] = {
    {
        "help", "print help info", "", help_info,
    },
    {
        "version", "get the shell version", "", version,
    },
    {
        "cluster_info", "get the informations for the cluster", "", query_cluster_info,
    },
    {
        "app",
        "get the partition information for some specific app",
        "<app_name> [-d|--detailed] [-o|--output <out_file>]",
        query_app,
    },
    {
        "app_disk",
        "get the disk usage information for some specific app",
        "<app_name> [-d|--detailed] [-o|--output <out_file>]",
        app_disk,
    },
    {
        "ls",
        "list all apps",
        "[-a|-all] [-d|--detailed] [-s|--status "
        "<all|available|creating|dropping|dropped>] "
        "[-o|--output FILE_PATH]",
        ls_apps,
    },
    {
        "nodes",
        "get the node status for this cluster",
        "[-d|--detailed] [-s|--status <all|alive|unalive>] [-o|--output "
        "FILE_PATH]",
        ls_nodes,
    },
    {
        "create",
        "create an app",
        "app_name [--partition_count|-p NUMBER] [--replica_count|-r NUMBER]",
        create_app,
    },
    {
        "drop", "drop an app", "app_name [--reserve_seconds|-r NUMBER]", drop_app,
    },
    {
        "recall", "recall an app", "<app_id> [new_app_name]", recall_app,
    },
    {
        "set_meta_level",
        "set the meta function level: stopped, blind, freezed, steady, lively",
        "<stopped | blind | freezed | steady | lively>",
        set_meta_level,
    },
    {
        "get_meta_level", "get the meta function level", "", get_meta_level,
    },
    {
        "balance",
        "send explicit balancer request for the cluster",
        "-g|--gpid <appid.pidx> -p|--type <move_pri|copy_pri|copy_sec> -f|--from "
        "<from_address> "
        "-t|--to <to_address>",
        balance,
    },
    {
        "propose",
        "send configuration proposals to cluster",
        "[-f|--force] "
        "-g|--gpid <appid.pidx> -p|--type <ASSIGN_PRIMARY|ADD_SECONDARY|DOWNGRADE_TO_INACTIVE...> "
        "-t|--target <node_exec_command> -n|--node <node_affected> ",
        propose,
    },
    {
        "use",
        "set the current app used for the data access commands",
        "[app_name]",
        use_app_as_current,
    },
    {
        "escape_all",
        "if escape all characters when printing key/value bytes",
        "[true|false]",
        process_escape_all,
    },
    {
        "timeout",
        "default timeout in milliseconds for read/write operations",
        "[time_in_ms]",
        process_timeout,
    },
    {
        "hash",
        "calculate the hash result for some hash key",
        "<hash_key> <sort_key>",
        calculate_hash_value,
    },
    {
        "set", "set value", "<hash_key> <sort_key> <value> [ttl_in_seconds]", data_operations,
    },
    {
        "multi_set",
        "set multiple values for a single hash key",
        "<hash_key> <sort_key> <value> [sort_key value...]",
        data_operations,
    },
    {
        "get", "get value", "<hash_key> <sort_key>", data_operations,
    },
    {
        "multi_get",
        "get multiple values for a single hash key",
        "<hash_key> [sort_key...]",
        data_operations,
    },
    {
        "multi_get_range",
        "get multiple values under sort key range for a single hash key",
        "<hash_key> <start_sort_key> <stop_sort_key> "
        "[--start_inclusive|-a <true|false>] [--stop_inclusive|-b <true|false>] "
        "[--sort_key_filter_type|-s <anywhere|prefix|postfix>] "
        "[--sort_key_filter_pattern|-y <str>] "
        "[--max_count|-n <num>] [--no_value|-i] [--reverse|-r]",
        data_operations,
    },
    {
        "multi_get_sortkeys",
        "get multiple sort keys for a single hash key",
        "<hash_key>",
        data_operations,
    },
    {
        "del", "del a key", "<hash_key> <sort_key>", data_operations,
    },
    {
        "multi_del",
        "delete multiple values for a single hash key",
        "<hash_key> <sort_key> [sort_key...]",
        data_operations,
    },
    {
        "multi_del_range",
        "delete multiple values under sort key range for a single hash key",
        "<hash_key> <start_sort_key> <stop_sort_key> "
        "[--start_inclusive|-a <true|false>] [--stop_inclusive|-b <true|false>] "
        "[--sort_key_filter_type|-s <anywhere|prefix|postfix>] "
        "[--sort_key_filter_pattern|-y <str>] "
        "[--output|-o <file_name>] [--silent|-i]",
        data_operations,
    },
    {
        "exist", "check value exist", "<hash_key> <sort_key>", data_operations,
    },
    {
        "count", "get sort key count for a single hash key", "<hash_key>", data_operations,
    },
    {
        "ttl", "query ttl for a specific key", "<hash_key> <sort_key>", data_operations,
    },
    {
        "hash_scan",
        "scan all sorted keys for a single hash key",
        "<hash_key> <start_sort_key> <stop_sort_key> [-d|--detailed] "
        "[-o|--output <file_name>] [-n|--max_count <num>] [-t|--timeout_ms <num>] "
        "[--start_inclusive|-a <true|false>] [--stop_inclusive|-b <true|false>] "
        "[--sort_key_filter_type|-s <anywhere|prefix|postfix>] "
        "[--sort_key_filter_pattern|-y <str>] "
        "[--no_value|-i]",
        data_operations,
    },
    {
        "full_scan",
        "scan all hash keys",
        "[-d|--detailed] [-p|--partition <num>] [-o|--output <file_name>] "
        "[-n|--max_count <num>] [-t|--timeout_ms <num>] "
        "[--hash_key_filter_type|-h <anywhere|prefix|postfix>] "
        "[--hash_key_filter_pattern|-x <str>] "
        "[--sort_key_filter_type|-s <anywhere|prefix|postfix>] "
        "[--sort_key_filter_pattern|-y <str>] "
        "[--no_value|-i]",
        data_operations,
    },
    {
        "copy_data",
        "copy app data",
        "-c|--target_cluster_name <str> -a|--target_app_name <str> "
        "[-s|--max_split_count <num>] "
        "[-b|--max_batch_count <num>] [-t|--timeout_ms <num>]",
        data_operations,
    },
    {
        "clear_data",
        "clear app data",
        "[-f|--force] [-s|--max_split_count <num>] [-b|--max_batch_count <num>] "
        "[-t|--timeout_ms "
        "<num>]",
        data_operations,
    },
    {
        "count_data",
        "get app row count",
        "[-s|--max_split_count <num>] [-b|--max_batch_count <num>] "
        "[-t|--timeout_ms <num>] "
        "[-z|--stat_size] [-c|--top_count <num>]",
        data_operations,
    },
    {
        "remote_command",
        "send remote command to servers",
        "[-t all|meta-server|replica-server] [-l ip:port,ip:port,...,ip:port] "
        "command [arguments...]",
        remote_command,
    },
    {
        "server_info",
        "get info of servers",
        "[-t all|meta-server|replica-server] [-l ip:port,ip:port,...,ip:port]",
        server_info,
    },
    {
        "server_stat",
        "get stat of servers",
        "[-t all|meta-server|replica-server] [-l ip:port,ip:port,...,ip:port]",
        server_stat,
    },
    {
        "app_stat", "get stat of apps", "[-a|--app_name <str>] [-o|--output <out_file>]", app_stat,
    },
    {
        "flush_log",
        "flush log of servers",
        "[-t all|meta-server|replica-server] [-l ip:port,ip:port,...,ip:port]",
        flush_log,
    },
    {
        "local_get", "get value from local db", "<db_path> <hash_key> <sort_key>", local_get,
    },
    {
        "sst_dump",
        "dump sstable dir or files",
        "[--command=check|scan|none|raw] --file=data_dir_OR_sst_file "
        "[--from=<user_key>] "
        "[--to=<user_key>] [--read_num=NUM] [--show_properties]",
        sst_dump,
    },
    {
        "mlog_dump",
        "dump mutation log dir",
        "-i|--input log_dir [-o|--output file_name] [-d|--detailed]",
        mlog_dump,
    },
    {
        "recover",
        "control the meta to recover the system from given nodes",
        "[-f|--node_list_file file_name] [-s|--node_list_str node_str] "
        "[-w|--wait_seconds seconds] "
        "[-b|--skip_bad_nodes] [-l|--skip_lost_partitions] [-o|--output "
        "FILE_NAME]",
        recover,
    },
    {
        "add_backup_policy",
        "add new cold backup policy",
        "<-p|--policy_name p1> <-b|--backup_provider_type provider> <-a|--app_ids 1,2,3..> "
        "<-i|--backup_interval_seconds sec> <-s|--start_time hour:minute> "
        "<-c|--backup_history_cnt count>",
        add_backup_policy,
    },
    {"ls_backup_policy", "list the names of the subsistent backup policies", "", ls_backup_policy},
    {
        "query_backup_policy",
        "query subsistent backup policy and last backup infos",
        "<-p|--policy_name p1,p2> [-b|--backup_info_cnt cnt]",
        query_backup_policy,
    },
    {
        "modify_backup_policy",
        "modify the backup policy",
        "<-p|--policy_name p1> [-a|--add_app 1,2...] [-r|--remove_app 1,2...] "
        "[-i|--backup_interval_seconds sec] [-c|--backup_history_count cnt] "
        "[-s|--start_time hour:minute]",
        modify_backup_policy,
    },
    {
        "disable_backup_policy",
        "stop policy continue backup",
        "<-p|--policy_name p1>",
        disable_backup_policy,
    },
    {
        "enable_backup_policy",
        "start backup policy to backup again",
        "<-p|--policy_name p1>",
        enable_backup_policy,
    },
    {
        "restore_app",
        "restore app from backup media",
        "<-c|--old_cluster_name name> <-p|--old_policy_name name> <-a|--old_app_name name> "
        "<-i|--old_app_id id> <-t|--timestamp/backup_id timestamp> "
        "<-b|--backup_provider_type provider> [-n|--new_app_name name] [-s|--skip_bad_partition] ",
        restore,
    },
    {
        "query_restore_status",
        "query restore status",
        "<restore_app_id> [-d|--detailed]",
        query_restore_status,
    },
    {
        "exit", "exit shell", "", exit_shell,
    },
    {
        nullptr, nullptr, nullptr, nullptr,
    }};

void print_help(command_executor *e, size_t length)
{
    int padding = length - strlen(e->name);
    std::cout << "\t" << e->name << ": ";
    for (int i = 0; i < padding; ++i)
        std::cout << " ";
    std::cout << e->name << " " << e->option_usage << std::endl;
}

void print_help()
{
    std::cout << "Usage:" << std::endl;
    for (int i = 0; commands[i].name != nullptr; ++i) {
        print_help(&commands[i], max_length);
    }
}

void register_all_commands()
{
    for (int i = 0; commands[i].name != nullptr; ++i) {
        auto pr = commands_map.emplace(commands[i].name, &commands[i]);
        dassert(pr.second, "the command '%s' is already registered!!!", commands[i].name);
        max_length = std::max(max_length, strlen(commands[i].name));
    }
}

void execute_command(command_executor *e, int argc, std::string str_args[])
{
    static char buffer[max_params_count][512]; // 512*32
    static char *argv[max_params_count];
    for (int i = 0; i < max_params_count; ++i) {
        argv[i] = buffer[i];
    }

    for (int i = 0; i < argc && i < max_params_count; ++i) {
        if (!str_args[i].empty()) {
            strcpy(argv[i], str_args[i].c_str());
        } else {
            memset(argv[i], 0, sizeof(512));
        }
    }

    if (!e->exec(e, &global_context, {argc, argv})) {
        printf("USAGE: ");
        print_help(e, max_length);
    }
}

static sigjmp_buf s_ctrlc_buf;
void handle_signals(int signo)
{
    if (signo == SIGINT) {
        std::cout << std::endl << "Type \"Ctrl-D\" to exit the shell." << std::endl;
        siglongjmp(s_ctrlc_buf, 1);
    }
}

void initialize(int argc, char **argv)
{
    if (signal(SIGINT, handle_signals) == SIG_ERR) {
        std::cout << "ERROR: register signal handler failed" << std::endl;
        dsn_exit(-1);
    }

    std::cout << "Pegasus Shell " << PEGASUS_VERSION << std::endl;
    std::cout << "Type \"help\" for more information." << std::endl;
    std::cout << "Type \"Ctrl-D\" to exit the shell." << std::endl;
    std::cout << std::endl;

    std::string config_file = argc > 1 ? argv[1] : "config.ini";
    if (!pegasus::pegasus_client_factory::initialize(config_file.c_str())) {
        std::cout << "ERROR: init pegasus failed: " << config_file << std::endl;
        dsn_exit(-1);
    } else {
        std::cout << "The config file is: " << config_file << std::endl;
    }

    std::string cluster_name = argc > 2 ? argv[2] : "mycluster";
    std::cout << "The cluster name is: " << cluster_name << std::endl;

    global_context.current_cluster_name = cluster_name;
    std::string section = "uri-resolver.dsn://" + global_context.current_cluster_name;
    std::string key = "arguments";
    std::string server_list = dsn_config_get_value_string(section.c_str(), key.c_str(), "", "");
    std::cout << "The cluster meta list is: " << server_list << std::endl;

    dsn::replication::replica_helper::load_meta_servers(
        global_context.meta_list, section.c_str(), key.c_str());
    global_context.ddl_client =
        new dsn::replication::replication_ddl_client(global_context.meta_list);

    register_all_commands();
}

void run()
{
    while (sigsetjmp(s_ctrlc_buf, 1) != 0)
        ;

    while (true) {
        int arg_count;
        std::string args[max_params_count];
        scanfCommand(arg_count, args, max_params_count);
        if (arg_count > 0) {
            int i = 0;
            for (; i < arg_count; ++i) {
                std::string &s = args[i];
                int j = 0;
                for (; j < s.size(); ++j) {
                    if (!isprint(s.at(j))) {
                        std::cout << "ERROR: found unprintable character in '"
                                  << pegasus::utils::c_escape_string(s) << "'" << std::endl;
                        break;
                    }
                }
                if (j < s.size())
                    break;
            }
            if (i < arg_count)
                continue;
            auto iter = commands_map.find(args[0]);
            if (iter != commands_map.end()) {
                execute_command(iter->second, arg_count, args);
            } else {
                std::cout << "ERROR: invalid subcommand '" << args[0] << "'" << std::endl;
                print_help();
            }
        }
    }
}

int main(int argc, char **argv)
{
    initialize(argc, argv);
    run();
    return 0;
}

#if defined(__linux__)
#include <dsn/git_commit.h>
#include <dsn/version.h>
#include <pegasus/git_commit.h>
#include <pegasus/version.h>
static char const rcsid[] =
    "$Version: Pegasus Shell " PEGASUS_VERSION " (" PEGASUS_GIT_COMMIT ")"
#if defined(DSN_BUILD_TYPE)
    " " STR(DSN_BUILD_TYPE)
#endif
        ", built with rDSN " DSN_CORE_VERSION " (" DSN_GIT_COMMIT ")"
        ", built by gcc " STR(__GNUC__) "." STR(__GNUC_MINOR__) "." STR(__GNUC_PATCHLEVEL__)
#if defined(DSN_BUILD_HOSTNAME)
            ", built on " STR(DSN_BUILD_HOSTNAME)
#endif
                ", built at " __DATE__ " " __TIME__ " $";
const char *pegasus_shell_rcsid() { return rcsid; }
#endif
