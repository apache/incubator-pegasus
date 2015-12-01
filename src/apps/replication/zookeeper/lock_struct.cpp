#include <functional>
#include <string>
#include <memory>
#include <vector>

#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>

#include "distributed_lock_service_zookeeper.h"
#include "lock_struct.h"
#include "lock_ptrs.h"
#include "zookeeper_session.h"
#include "zookeeper_error.h"

#ifdef __TITLE__
#undef __TITLE__
#define __TITLE__ "dlock.lock_struct"
#endif

namespace dsn { namespace dist {
        
static const char* states[] = {
    "uninitialized",
    "pending",
    "locked",
    "expired",
    "cancelled",
    "unlocking"
};

static inline const char* string_state(lock_state state)
{
    dassert(state<lock_state::state_count, "");
    return states[state];
}

static inline const char* string_zooevt(int zoo_event)
{
    if (ZOO_SESSION_EVENT==zoo_event) 
        return "session event";
    if (ZOO_CREATED_EVENT==zoo_event) 
        return "created event";
    if (ZOO_DELETED_EVENT==zoo_event) 
        return "deleted event";
    if (ZOO_CHANGED_EVENT==zoo_event) 
        return "changed event";
    if (ZOO_CHILD_EVENT==zoo_event) 
        return "child event";
    if (ZOO_NOTWATCHING_EVENT==zoo_event)
        return "notwatching event";
    return "invalid event";
}

static inline void __lock_task_bind_and_enqueue(lock_task_t lock_task, error_code ec, const std::string& id, int version)
{
    lock_task->bind_and_enqueue([=](distributed_lock_service::lock_callback& cb){
        return std::bind(cb, ec, id, version);
    });
}

static inline void __unlock_task_bind_and_enqueue(unlock_task_t unlock_task, error_code ec)
{
    unlock_task->bind_and_enqueue([=](distributed_lock_service::err_callback& cb){
        return std::bind(cb, ec);
    });
}

#define __check_code(code, allow_list, allow_list_size, code_str) do {\
    int i=0;\
    for (; i!=allow_list_size; ++i){\
        if (code==allow_list[i])\
            break;\
    }\
    dassert(i<allow_list_size, "invalid code(%s)", code_str);\
}while(0)

#define __execute( cb, _this )\
    tasking::enqueue(TASK_CODE_DLOCK, nullptr, cb, _this->hash())
    
#define IGNORE_CALLBACK true
#define DONT_IGNORE_CALLBACK false
#define REMOVE_FOR_UNLOCK true
#define REMOVE_FOR_CANCEL false

lock_struct::lock_struct(lock_srv_ptr srv): clientlet()
{
    _dist_lock_service = srv;    
    clear();
    _state = lock_state::uninitialized;
    
    _hash = 0;
}

void lock_struct::initialize(std::string lock_id, std::string myself_id)
{
    _lock_id = lock_id;
    _myself._node_value = myself_id;

    _hash = std::hash<std::string>()(lock_id) + std::hash<std::string>()(myself_id);
}

void lock_struct::clear()
{
    _lock_callback = nullptr; 
    _lease_expire_callback = nullptr;
    _cancel_callback = nullptr;
    _unlock_callback = nullptr;
    
    _lock_id = _lock_dir = "";
    _myself._node_value = _owner._node_value = "";
    _myself._node_seq_name = _owner._node_seq_name = "";
    _myself._sequence_id = _owner._sequence_id = -1;
}

void lock_struct::remove_lock()
{
    check_hashed_access();
    _dist_lock_service->erase( std::make_pair(_lock_id, _myself._node_value) );
    _dist_lock_service->session()->detach(this);
    _dist_lock_service = nullptr;
}

void lock_struct::on_operation_timeout()
{
    _state = lock_state::uninitialized;
    __lock_task_bind_and_enqueue(_lock_callback, 
                                 ERR_TIMEOUT, 
                                 _owner._node_value, 
                                 _owner._sequence_id);
}

void lock_struct::on_expire()
{
    _state = lock_state::expired;
    remove_lock();
    __lock_task_bind_and_enqueue(_lease_expire_callback, ERR_EXPIRED, _owner._node_value, _owner._sequence_id);
    clear();
}

int64_t lock_struct::parse_seq_path(const std::string& path)
{
    int64_t ans = -1;
    bool check_ok = true;
    std::string znode_name = path;
    boost::iterator_range<std::string::iterator> it = boost::algorithm::find_last(
        znode_name, distributed_lock_service_zookeeper::LOCK_NODE);
    if ( it.empty() )
        check_ok = false;
    else {
        try {//As boost lecial_cast use exception, we must catch it
            ans = boost::lexical_cast<int64_t>( znode_name.substr(it.end()-znode_name.begin()) );
        }
        catch (const boost::bad_lexical_cast &) {
            check_ok = false;
        }
    }
    if ( !check_ok )
        dwarn("got an invalid znode name(%s)", znode_name.c_str());
    return ans;    
}

/*static*/
void lock_struct::my_lock_removed(lock_struct_ptr _this, int zoo_event) 
{
    static const lock_state allow_state[] = {
        lock_state::locked, lock_state::unlocking, lock_state::expired
    };
    _this->check_hashed_access();
    __check_code(_this->_state, allow_state, 3, string_state(_this->_state));
    
    if(_this->_state==lock_state::unlocking || _this->_state==lock_state::expired)
    {
        return;
    }
    else {
        _this->on_expire();
    }
}
/*static*/
void lock_struct::owner_change(lock_struct_ptr _this, int zoo_event)
{
    static const lock_state allow_state[] = {
        lock_state::pending, lock_state::cancelled, lock_state::expired
    };
    _this->check_hashed_access();
    __check_code(_this->_state, allow_state, 3, string_state(_this->_state));
    
    if (_this->_state==lock_state::cancelled || _this->_state==lock_state::expired) {
        return;
    }
    if (ZOO_DELETED_EVENT == zoo_event) {
        _this->_owner._sequence_id = -1;
        _this->_owner._node_seq_name.clear();
        _this->_owner._node_value.clear();
        _this->get_lockdir_nodes();
    }
    else if (ZOO_NOTWATCHING_EVENT == zoo_event) 
        _this->get_lock_owner(false);
    else 
        dassert(false, "unexpected event");
}
/*static*/
void lock_struct::after_remove_duplicated_locknode(lock_struct_ptr _this, int ec, std::shared_ptr<std::string> path)
{
    static const int allow_ec[] = {
        ZOK, ZNONODE, //ok
        ZCONNECTIONLOSS, ZOPERATIONTIMEOUT, ZINVALIDSTATE //operation timeout
    };
    static const int allow_state[] = {
        lock_state::pending, lock_state::cancelled, lock_state::expired
    };
    _this->check_hashed_access();
    __check_code(ec, allow_ec, 5, zerror(ec));
    __check_code(_this->_state, allow_state, 3, string_state(_this->_state));

    if (_this->_state==lock_state::cancelled || _this->_state==lock_state::expired) {
        return;
    }
    if (ZOK==ec || ZNONODE==ec) {
        ddebug("lock(%s) remove duplicated node(%s), rely on delete watcher to be actived", 
               _this->_lock_id.c_str(), 
               path->c_str()
        );
    }
    else {
        /* when remove duplicated locknode timeout, we can choose notify the api user
         * or retry. If notify, we need to clean the _owner's temporary state for user
         * to retry, as a preceding node-delete watch may be actived. Here we just retry
         * and rely on the session expired to stop it*/
        _this->remove_duplicated_locknode(std::move(*path));
    }
}

void lock_struct::remove_duplicated_locknode(std::string&& znode_path)
{
    lock_struct_ptr _this = this;
    dwarn("duplicated value(%s) ephe/seq node(%s and %s) create on zookeeper, remove the smaller one",
          _myself._node_value.c_str(), 
          _owner._node_seq_name.c_str(), _myself._node_seq_name.c_str());
    
    auto delete_callback_wrapper = [_this](zookeeper_session::zoo_opcontext* op) {
        __execute(std::bind(&lock_struct::after_remove_duplicated_locknode, 
                            _this, op->_output.error, 
                            std::shared_ptr<std::string>(new std::string(std::move(op->_input._path)))
                           ), _this);
    };
    zookeeper_session::zoo_opcontext* op = zookeeper_session::create_context();
    op->_optype = zookeeper_session::ZOO_DELETE;
    op->_callback_function = delete_callback_wrapper;
    op->_input._path = std::move(znode_path);
    _dist_lock_service->session()->visit(op);
}
/*static*/
void lock_struct::after_get_lock_owner(lock_struct_ptr _this, int ec, std::shared_ptr<std::string> value)
{
    static const int allow_ec[] = {
        ZOK, //OK
        ZNONODE, //owner session removed
        ZCONNECTIONLOSS, ZOPERATIONTIMEOUT, ZINVALIDSTATE //operation timeout
    };
    static const int allow_state[] = {
        lock_state::pending, lock_state::cancelled, lock_state::expired
    };
    _this->check_hashed_access();
    __check_code(ec, allow_ec, 5, zerror(ec));
    __check_code(_this->_state, allow_state, 3, string_state(_this->_state));
    
    if (_this->_state==lock_state::cancelled || _this->_state==lock_state::expired) 
    {
        return;
    }
    if (ZNONODE == ec) 
    {
        //lock owner removed
        ddebug("the lock(%s) old owner(%s:%s) has removed, myself(%s:%s) try to get the lock for another turn", 
               _this->_lock_id.c_str(), 
               _this->_owner._node_seq_name.c_str(), _this->_owner._node_value.c_str(), 
               _this->_myself._node_seq_name.c_str(), _this->_myself._node_value.c_str()
              );
        _this->_owner._sequence_id = -1;
        _this->_owner._node_seq_name.clear();
        _this->get_lockdir_nodes();
        return;
    }
    if (ZOK == ec)
    {
        _this->_owner._node_value = std::move(*value);
        if (_this->_myself._node_value == _this->_owner._node_value)
            _this->remove_duplicated_locknode(_this->_lock_dir + "/" + _this->_owner._node_seq_name);
        else {
            ddebug("wait the lock(%s) owner(%s:%s) to remove, myself(%s:%s)", value->c_str(), 
                  _this->_owner._node_seq_name.c_str(), _this->_myself._node_seq_name.c_str());
        }
    }
    else {
        _this->on_operation_timeout();
    }
}
/*static*/
void lock_struct::after_self_check(lock_struct_ptr _this, int ec, std::shared_ptr<std::string> value)
{
    static const int allow_ec[] = {
        ZOK, //OK
        ZNONODE, //removed by unlock, or session expired
        ZCONNECTIONLOSS, ZOPERATIONTIMEOUT, ZINVALIDSTATE //operation timeout
    };
    static const lock_state allow_state[] = {
        lock_state::locked, lock_state::unlocking, lock_state::expired
    };
    _this->check_hashed_access();
    __check_code(ec, allow_ec, 5, zerror(ec));
    __check_code(_this->_state, allow_state, 3, string_state(_this->_state));

    if (_this->_state==lock_state::unlocking || _this->_state==lock_state::expired) 
    {
        ddebug("skip lock(%s) owner self check, do nothing, myself(%s:%s)", 
               _this->_lock_id.c_str(), 
               _this->_myself._node_seq_name.c_str(), _this->_myself._node_value.c_str()
        );
        return;
    }
    if (ZNONODE == ec)
    {
        ddebug("lock(%s) owner can't get the node, " 
               "treat as session expired , myself(%s:%s), owner(%s:%s)", 
               _this->_lock_id.c_str(), 
               _this->_myself._node_seq_name.c_str(), _this->_myself._node_value.c_str());
        _this->on_expire();
        return;
    }
    if (ZOK != ec) 
    {
        //operation timeout, we just ignore it
        ddebug("lock(%s) get value of myself(%s:%s) failed, ignore", 
               _this->_lock_id.c_str(), 
               _this->_myself._node_seq_name.c_str(), _this->_myself._node_value.c_str());
        return;
    }
    dassert(*value==_this->_myself._node_value, 
            "lock(%s) get wrong value, local myself(%s), from zookeeper(%s)", 
            _this->_lock_id.c_str(), _this->_myself._node_value.c_str(), 
            value->c_str()
           );
}

void lock_struct::get_lock_owner(bool watch_myself)
{    
    lock_struct_ptr _this = this;
    auto watcher_callback_wrapper = [_this, watch_myself](int event){
        ddebug("get watcher callback, event type(%s), path(%s)", string_zooevt(event));
        if (watch_myself)
            __execute( std::bind(&lock_struct::my_lock_removed, _this, event), _this );
        else
            __execute( std::bind(&lock_struct::owner_change, _this, event), _this );
    };
    
    auto after_get_owner_wrapper = [_this, watch_myself](zookeeper_session::zoo_opcontext* op) {
        zookeeper_session::zoo_output& output = op->_output;
        std::function<void (int, std::shared_ptr<std::string>)> cb;
        if (!watch_myself)
            cb = std::bind(&lock_struct::after_get_lock_owner, _this, std::placeholders::_1, std::placeholders::_2);
        else
            cb = std::bind(&lock_struct::after_self_check, _this, std::placeholders::_1, std::placeholders::_2);
        
        if (output.error != ZOK)
            __execute( std::bind(cb, output.error, nullptr), _this );
        else {
            std::shared_ptr<std::string> buf(new std::string(output.get_op.value, output.get_op.value_length));
            __execute( std::bind(cb, ZOK, buf), _this );
        }
    };
    
    zookeeper_session::zoo_opcontext* op = zookeeper_session::create_context();
    op->_optype = zookeeper_session::ZOO_GET;
    op->_callback_function = after_get_owner_wrapper;
    op->_input._path = _lock_dir + "/" + _owner._node_seq_name;

    op->_input._is_set_watch = 1;
    op->_input._owner = this;
    op->_input._watcher_callback = watcher_callback_wrapper;
    
    _dist_lock_service->session()->visit(op);    
}
/*static*/
void lock_struct::after_get_lockdir_nodes(lock_struct_ptr _this, int ec, std::shared_ptr< std::vector<std::string> > children)
{
    static const int allow_ec[] = {
        ZOK, //succeed
        ZCONNECTIONLOSS, ZOPERATIONTIMEOUT, ZINVALIDSTATE//operation timeout
    };
    static const int allow_state[] = {
        lock_state::pending, lock_state::cancelled, lock_state::expired
    };
    
    _this->check_hashed_access();
    __check_code(ec, allow_ec, 4, zerror(ec));
    __check_code(_this->_state, allow_state, 3, string_state(_this->_state));

    if (_this->_state==lock_state::cancelled || _this->_state==lock_state::expired) {
        return;
    }
    if (ZOK != ec) {
        ddebug("get lockdir(%s) children failed, ec(%s), treat as timeout", 
               _this->_lock_dir.c_str(), zerror(ec));
        _this->on_operation_timeout();
        return;
    }

    int64_t min_seq = -1, min_pos = -1, my_pos = -1;
    int64_t myself_seq = _this->_myself._sequence_id;
    
    for (int i=0; i!=(int)children->size(); ++i)
    {
        std::string& child = (*children)[i];
        int64_t seq = parse_seq_path(child);
        if (seq == -1) {
            dwarn("an invalid node(%s) in lockdir(%s), ignore", child.c_str(), _this->_lock_dir.c_str());
            continue;
        }
        if (min_pos==-1 || min_seq>seq)
            min_seq = seq, min_pos = i;
        if (myself_seq == seq)
            my_pos = i;
    }

    ddebug("min sequece number(%lld) in lockdir(%s)", min_seq, _this->_lock_dir.c_str());
    if (my_pos == -1)
    {
        //znode removed on zookeeper, may timeout or removed by other procedure
        dwarn("sequence and ephemeral node(%s/%s) removed when get_children, this is abnormal, "
              "try to reaquire the lock", _this->_lock_dir.c_str(), 
              _this->_myself._node_seq_name.c_str());
        _this->_myself._node_seq_name.clear();
        _this->_myself._sequence_id = -1;
        _this->create_locknode();
        return;
    }
    else
    {
        _this->_owner._sequence_id = min_seq;
        _this->_owner._node_seq_name = std::move((*children)[min_pos]);
        bool watch_myself = false;
        if (min_seq == myself_seq)
        {
            //i am the smallest one, so i get the lock :-)
            dassert(min_pos==my_pos, "same sequence node number on zookeeper, dir(%s), number(%d)",
                    _this->_lock_dir.c_str(), myself_seq);
            _this->_state = lock_state::locked;
            _this->_owner._node_value = _this->_myself._node_value;
            watch_myself = true;
            __lock_task_bind_and_enqueue(_this->_lock_callback, 
                                        ERR_OK, 
                                        _this->_myself._node_value, 
                                        _this->_myself._sequence_id);
        }
        _this->get_lock_owner(watch_myself);
    }
}

void lock_struct::get_lockdir_nodes()
{
    lock_struct_ptr _this = this;
    auto result_wrapper = [_this](zookeeper_session::zoo_opcontext* op)
    {
        if (ZOK != op->_output.error)
            __execute(std::bind(&lock_struct::after_get_lockdir_nodes, _this, op->_output.error, nullptr), _this);
        else {
            const String_vector* vec = op->_output.getchildren_op.strings;
            std::shared_ptr< std::vector<std::string> > children(new std::vector<std::string>(vec->count) );
            for (int i=0; i!=vec->count; ++i)
                (*children)[i].assign(vec->data[i]);
            __execute(std::bind(&lock_struct::after_get_lockdir_nodes, _this, op->_output.error, children), _this);
        }
    };
    
    zookeeper_session::zoo_opcontext* op = zookeeper_session::create_context();
    op->_optype = zookeeper_session::ZOO_GETCHILDREN;
    op->_callback_function = result_wrapper;
    op->_input._path = _lock_dir;
    op->_input._is_set_watch = 0;
    _dist_lock_service->session()->visit(op);    
}
/*static*/
void lock_struct::after_create_locknode(lock_struct_ptr _this, int ec, std::shared_ptr<std::string> path)
{
    static const int allow_ec[] = {
        ZOK, //succeed
        ZCONNECTIONLOSS, ZOPERATIONTIMEOUT, ZINVALIDSTATE//operation timeout
    };
    static const int allow_state[] = {
        lock_state::pending, lock_state::cancelled, lock_state::expired
    };
    
    _this->check_hashed_access();
    __check_code(ec, allow_ec, 4, zerror(ec));
    __check_code(_this->_state, allow_state, 3, string_state(_this->_state));
    
    ddebug("after create seq and ephe node, error(%s), path(%s)", zerror(ec), path.get());    

    if (_this->_state==lock_state::cancelled || _this->_state==lock_state::expired)
    {
        ddebug("current state(%s), ignore event create lockdir(%s)", _this->_lock_dir.c_str());
        if (ZOK == ec && _this->_state==lock_state::cancelled) {
            _this->remove_my_locknode(std::move(*path), IGNORE_CALLBACK, REMOVE_FOR_CANCEL);
        }
        return;
    }
    if (ZOK != ec)
    {
        ddebug("creating seq and ephe node timeout");
        _this->on_operation_timeout();
        return;
    }
    
    char splitter[] = {'/'};
    _this->_myself._node_seq_name = utils::get_last_component(*path, splitter);
    _this->_myself._sequence_id = parse_seq_path(_this->_myself._node_seq_name);
    dassert(_this->_myself._sequence_id!=-1, "invalid seq path created");
    _this->get_lockdir_nodes();
}

void lock_struct::create_locknode()
{
    //create an ZOO_EPHEMERAL|ZOO_SEQUENCE node
    zookeeper_session::zoo_opcontext* op = zookeeper_session::create_context();
    op->_optype = zookeeper_session::ZOO_OPERATION::ZOO_CREATE;

    lock_struct_ptr _this = this;
    auto result_wrapper = [ _this ](zookeeper_session::zoo_opcontext* op) {
        if (op->_output.error!=ZOK)
            __execute(std::bind(&lock_struct::after_create_locknode, _this, op->_output.error, nullptr), _this);
        else {
            std::shared_ptr<std::string> path(new std::string(op->_output.create_op._created_path));
            __execute(std::bind(&lock_struct::after_create_locknode, _this, ZOK, path), _this);
        }
    };
    
    zookeeper_session::zoo_input& input = op->_input;
    input._path = _lock_dir + "/" + distributed_lock_service_zookeeper::LOCK_NODE;
    input._value.assign(_myself._node_value.c_str(), 0, _myself._node_value.length());
    input._flags = ZOO_EPHEMERAL|ZOO_SEQUENCE;
    op->_callback_function = result_wrapper;
    _dist_lock_service->session()->visit(op);    
}
/*static*/
void lock_struct::after_create_lockdir(lock_struct_ptr _this, int ec)
{
    _this->check_hashed_access();
    static const int allow_ec[] = {
        ZOK, ZNODEEXISTS, //succeed state
        ZCONNECTIONLOSS, ZOPERATIONTIMEOUT, //operation timeout
        ZINVALIDSTATE//session expire
    };
    static const lock_state allow_state[] = {
        lock_state::pending, 
        lock_state::cancelled, 
        lock_state::expired
    };
    __check_code(ec, allow_ec, 5, zerror(ec));
    __check_code(_this->_state, allow_state, 3, string_state(_this->_state));

    if (_this->_state==lock_state::cancelled || _this->_state==lock_state::expired)
    {
        ddebug("current state(%s), ignore event create lockdir(%s)", 
               string_state(_this->_state), _this->_lock_dir.c_str());
        return;
    }
    if (ZOK!=ec && ZNODEEXISTS!=ec) {
        ddebug("create lock dir failed, error(%s), _path(%s), treat as timeout", 
               zerror(ec), _this->_lock_dir.c_str());
        _this->_lock_dir.clear();
        _this->on_operation_timeout();
        return;
    }
    _this->create_locknode();
}
/*static*/
void lock_struct::try_lock(lock_struct_ptr _this, lock_task_t lock_callback, lock_task_t expire_callback)
{
    _this->check_hashed_access();
    
    if ( _this->_state!=lock_state::uninitialized ) 
    {
        __lock_task_bind_and_enqueue(lock_callback, ERR_RECURSIVE_LOCK, "", -1);
        return;
    }
    
    _this->_lock_callback = lock_callback;
    _this->_lease_expire_callback = expire_callback;
    _this->_state = lock_state::pending;
    
    if (_this->_lock_dir.empty()) {
        _this->_lock_dir = distributed_lock_service_zookeeper::LOCK_ROOT + "/" + _this->_lock_id;
        auto result_wrapper = [_this](zookeeper_session::zoo_opcontext* op) {
            __execute(std::bind(&lock_struct::after_create_lockdir, _this, op->_output.error), _this);
        };
        zookeeper_session::zoo_opcontext* op = zookeeper_session::create_context();
        op->_optype = zookeeper_session::ZOO_CREATE;
        op->_input._path = _this->_lock_dir;
        op->_callback_function = result_wrapper;
        _this->_dist_lock_service->session()->visit(op);
    }
    else if (_this->_myself._sequence_id==-1) 
        _this->create_locknode();
    else if(_this->_owner._sequence_id==-1)
        _this->get_lockdir_nodes();
    else
        _this->get_lock_owner(false);
}

void lock_struct::after_remove_my_locknode(lock_struct_ptr _this, int ec, bool remove_for_unlock)
{
    static const int allow_ec[] = {
        ZOK, ZNONODE, //ok
        ZCONNECTIONLOSS, ZOPERATIONTIMEOUT, ZINVALIDSTATE //operation timeout
    };
    static const int allow_state[] = {
        lock_state::cancelled, lock_state::unlocking, lock_state::expired
    };
    _this->check_hashed_access();
    __check_code(ec, allow_ec, 5, zerror(ec));
    __check_code(_this->_state, allow_state, 2, string_state(_this->_state));

    error_code dsn_ec;
    if (lock_state::expired == _this->_state) {
        ddebug("during unlock/cancel lock(%s), encountered expire, owner(%s:%s), myself(%s:%s)", 
            _this->_lock_id.c_str(), _this->_owner._node_seq_name.c_str(), 
            _this->_owner._node_value.c_str(), _this->_myself._node_seq_name.c_str(), 
            _this->_myself._node_value.c_str()
        );
        dsn_ec = ERR_INVALID_STATE;
    }
    else {
        if (ZOK==ec || ZNONODE==ec) //remove ok
            dsn_ec = ERR_OK;
        else
            dsn_ec = ERR_TIMEOUT;
    }
    
    if (dsn_ec==ERR_OK)
        _this->remove_lock();
    
    if (REMOVE_FOR_UNLOCK == remove_for_unlock)
        __unlock_task_bind_and_enqueue(_this->_unlock_callback, dsn_ec);
    else {
        __lock_task_bind_and_enqueue(_this->_cancel_callback, dsn_ec, _this->_owner._node_value, _this->_owner._sequence_id);
    }
    
    if (dsn_ec==ERR_OK)
        _this->clear();
}

void lock_struct::remove_my_locknode(std::string&& znode_path, bool ignore_callback, bool remove_for_unlock)
{
    lock_struct_ptr _this = this;
    auto result_wrapper = [ _this, ignore_callback, remove_for_unlock ](zookeeper_session::zoo_opcontext* op){
        ddebug("delete node %s, result(%s)", op->_input._path.c_str(), zerror(op->_output.error));
        if (IGNORE_CALLBACK == ignore_callback)
            return;
        __execute( std::bind(&lock_struct::after_remove_my_locknode, 
                             _this, 
                             op->_output.error, 
                             remove_for_unlock
                            ), _this );
    };
    zookeeper_session::zoo_opcontext* op = zookeeper_session::create_context();
    op->_optype = zookeeper_session::ZOO_DELETE;
    op->_input._path = std::move(znode_path);
    op->_callback_function = result_wrapper;
    _dist_lock_service->session()->visit(op);
}

/*static*/
void lock_struct::cancel_pending_lock(lock_struct_ptr _this, lock_task_t cancel_callback)
{
    _this->check_hashed_access();
    if ( _this->_state!=lock_state::uninitialized || 
         _this->_state!=lock_state::pending || 
         _this->_state!=lock_state::cancelled) {
        __lock_task_bind_and_enqueue(cancel_callback, ERR_INVALID_PARAMETERS, "", _this->_owner._sequence_id);
        return;
    }
    
    _this->_state = lock_state::cancelled;
    _this->_cancel_callback = cancel_callback;
    if ( !_this->_myself._node_seq_name.empty() )
        _this->remove_my_locknode( _this->_lock_dir + "/" + _this->_myself._node_seq_name, DONT_IGNORE_CALLBACK, REMOVE_FOR_CANCEL);
    else {
        _this->remove_lock();
        __lock_task_bind_and_enqueue(cancel_callback, ERR_OK, _this->_owner._node_value, _this->_owner._sequence_id);
        _this->clear();
    }
}

/*static*/
void lock_struct::unlock(lock_struct_ptr _this, unlock_task_t unlock_callback)
{
    _this->check_hashed_access();
    if (_this->_state != lock_state::locked || 
        _this->_state != lock_state::unlocking
    ) {
        __unlock_task_bind_and_enqueue(unlock_callback, ERR_INVALID_PARAMETERS);
        return;
    }
    
    _this->_state = lock_state::unlocking;
    _this->_unlock_callback = unlock_callback;
    _this->remove_my_locknode( _this->_lock_dir+"/"+_this->_myself._node_seq_name, DONT_IGNORE_CALLBACK, REMOVE_FOR_UNLOCK);
}

/*static*/
void lock_struct::query(lock_struct_ptr _this, lock_task_t query_callback)
{
    _this->check_hashed_access();
    __lock_task_bind_and_enqueue(query_callback, ERR_OK, _this->_owner._node_value, _this->_owner._sequence_id);
}

/*static*/
void lock_struct::lock_expired(lock_struct_ptr _this)
{
    _this->check_hashed_access();
    _this->on_expire();
}

}}
