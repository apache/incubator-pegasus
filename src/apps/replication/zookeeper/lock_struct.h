#pragma once

#include <dsn/dist/distributed_lock_service.h>
#include <string>

#include "lock_ptrs.h"

namespace dsn { namespace dist {

enum lock_state
{
    uninitialized,
    pending,
    locked,
    expired,
    cancelled,
    unlocking, 
    state_count
};

struct zoolock_pair
{
    std::string _node_value;
    std::string _node_seq_name;
    int64_t _sequence_id;
};

class lock_struct: public clientlet, public ref_counter
{
public:
    lock_struct(lock_srv_ptr srv);
    void initialize(std::string lock_id, std::string myself_id);
    const int hash() const { return _hash; }
    
    static void try_lock(lock_struct_ptr _this, lock_task_t lock_callback, lock_task_t expire_callback);
    static void cancel_pending_lock(lock_struct_ptr _this, lock_task_t cancel_callback);
    static void unlock(lock_struct_ptr _this, unlock_task_t unlock_callback);
    static void query(lock_struct_ptr _this, lock_task_t query_callback);
    
    static void lock_expired(lock_struct_ptr _this);
    
private:
    void create_locknode();
    void get_lockdir_nodes();
    void get_lock_owner(bool watch_myself);
    void remove_duplicated_locknode(std::string&& znode_path);

    void remove_my_locknode(std::string&& znode_path, bool ignore_callback,  bool remove_for_unlock);
    
    void clear();
    void remove_lock();
    void on_operation_timeout();
    void on_expire();
    
    static int64_t parse_seq_path(const std::string& path);
    static void after_create_lockdir(lock_struct_ptr _this, int ec);
    static void after_get_lockdir_nodes(lock_struct_ptr _this, int ec, std::shared_ptr<std::vector<std::string>> children);
    static void after_create_locknode(lock_struct_ptr _this, int ec, std::shared_ptr<std::string> path);
    static void after_get_lock_owner(lock_struct_ptr _this, int ec, std::shared_ptr<std::string> value);
    static void after_self_check(lock_struct_ptr _this, int ec, std::shared_ptr<std::string> value);
    static void after_remove_duplicated_locknode(lock_struct_ptr _this,
                                                 int ec, std::shared_ptr<std::string> value);
    static void after_remove_my_locknode(lock_struct_ptr _this, int ec, bool need_to_notify);
    
    /*lock owner watch callback*/
    static void owner_change(lock_struct_ptr _this, int zoo_event);
    static void my_lock_removed(lock_struct_ptr _this, int zoo_event);
    
private:
    lock_task_t _lock_callback;
    lock_task_t _lease_expire_callback;
    lock_task_t _cancel_callback;
    unlock_task_t _unlock_callback;
    
    std::string _lock_id;
    std::string _lock_dir;
    zoolock_pair _myself, _owner;
    lock_state _state;
    int _hash;
    
    lock_srv_ptr _dist_lock_service;
};

}}
