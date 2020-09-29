#include "command_utils.h"

bool validate_ip(shell_context *sc,
                 const std::string &ip_str,
                 /*out*/ dsn::rpc_address &target_address,
                 /*out*/ std::string &err_info)
{
    std::map<dsn::rpc_address, dsn::replication::node_status::type> nodes;
    auto error = sc->ddl_client->list_nodes(::dsn::replication::node_status::NS_INVALID, nodes);
    if (error != dsn::ERR_OK) {
        err_info = fmt::format("list nodes failed, error={} \n", error.to_string());
        return false;
    }

    if (!target_address.from_string_ipv4(ip_str.c_str())) {
        err_info = fmt::format("invalid ip:port:{}, can't transform it into rpc_address\n", ip_str);
        return false;
    }

    bool not_find_ip = true;
    for (const auto &node : nodes) {
        if (target_address == node.first) {
            not_find_ip = false;
        }
    }

    if (not_find_ip) {
        err_info = fmt::format("invalid ip:port:{}, can't find it in metas \n", ip_str);
        return false;
    }

    return true;
}
