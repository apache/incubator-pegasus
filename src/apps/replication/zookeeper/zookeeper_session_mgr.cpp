#include "zookeeper_session_mgr.h"
#include "zookeeper_session.h"
#include <stdio.h>
#include <zookeeper.h>
#include <stdexcept>

namespace dsn { namespace dist {

zookeeper_session_mgr::zookeeper_session_mgr()
{
    _zoo_hosts = dsn_config_get_value_string("zookeeper", "hosts_list", "", "zookeeper_hosts");
    _timeout_ms = dsn_config_get_value_uint64("zookeeper", "timeout_ms", 30000, "zookeeper_timeout_milliseconds");
    _zoo_logfile = dsn_config_get_value_string("zookeeper", "logfile", "", "zookeeper logfile");

    FILE* fp = fopen(_zoo_logfile.c_str(), "a");
    if (fp != nullptr)
        zoo_set_log_stream(fp);
    else
        throw std::runtime_error("can't open zoologfile: " + _zoo_logfile);
}

zookeeper_session* zookeeper_session_mgr::get_session(void* service_node)
{
    auto& store = utils::singleton_store<void*, zookeeper_session*>::instance();
    zookeeper_session* ans = nullptr;
    utils::auto_lock<utils::ex_lock_nr> l(_store_lock);
    if ( !store.get(service_node, ans) )
    {
        ans = new zookeeper_session(service_node);
        store.put(service_node, ans);
    }
    return ans;
}

}}
