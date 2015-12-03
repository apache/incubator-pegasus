#pragma once

#include <dsn/dist/distributed_lock_service.h>
#include <unordered_map>
#include "lock_ptrs.h"

namespace dsn { namespace dist {

class zookeeper_session;
class distributed_lock_service_zookeeper: public distributed_lock_service, public clientlet, public ref_counter
{
public:
    explicit distributed_lock_service_zookeeper();

    virtual ~distributed_lock_service_zookeeper();
    virtual error_code initialize() override;
    virtual std::pair<task_ptr, task_ptr> lock(
        const std::string& lock_id,
        const std::string& myself_id,
        bool create_if_not_exist,
        task_code lock_cb_code,
        const lock_callback& lock_cb,
        task_code lease_expire_code,
        const lock_callback& lease_expire_callback
        ) override;
    virtual task_ptr cancel_pending_lock(
        const std::string& lock_id,
        const std::string& myself_id,
        task_code cb_code,
        const lock_callback& cb) override;
    virtual task_ptr unlock(
        const std::string& lock_id,
        const std::string& myself_id,
        bool destroy,
        task_code cb_code,
        const err_callback& cb) override;
    virtual task_ptr query_lock(
        const std::string& lock_id,
        task_code cb_code,
        const lock_callback& cb) override;

private:
    static std::string LOCK_ROOT;
    static std::string LOCK_NODE;

    typedef std::pair<std::string, std::string> lock_key;
    struct pair_hash 
    {
        template<typename T, typename U>
        std::size_t operator ()(const std::pair<T, U>& key) const
        {
            return std::hash<T>()(key.first)+std::hash<U>()(key.second);
        }
    };
    
    typedef std::unordered_map<lock_key, lock_struct_ptr, pair_hash> lock_map;
    utils::rw_lock_nr _service_lock;
    lock_map _zookeeper_locks;

    zookeeper_session* _session;
    int _zoo_state;
    bool _first_call;
    utils::notify_event _waiting_attach;

    void erase(const lock_key& key);
    void dispatch_zookeeper_session_expire();
    zookeeper_session* session() { return _session; }

    static void on_zoo_session_evt(lock_srv_ptr _this, int zoo_state);

    friend class lock_struct;
};

}}
