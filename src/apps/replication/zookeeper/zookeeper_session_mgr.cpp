#include "zookeeper_session_mgr.h"
#include "zookeeper_session.h"

namespace dsn { namespace dist {

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
