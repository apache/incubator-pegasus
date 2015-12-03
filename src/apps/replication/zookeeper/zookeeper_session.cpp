#include <dsn/internal/task.h>
#include <zookeeper.h>

#include "zookeeper_session.h"
#include "zookeeper_session_mgr.h"

#ifdef __TITLE__
#undef __TITLE__
#endif

#define __TITLE__ "zookeeper_session"

namespace dsn { namespace dist{

zookeeper_session::zookeeper_session(void *srv_node):
    _handle(nullptr)
{
    _srv_node = (service_node*)srv_node;
}

int zookeeper_session::attach(
    void *callback_owner,
    const state_callback &cb)
{
    utils::auto_write_lock l(_watcher_lock);
    if (nullptr == _handle)
    {
        _handle = zookeeper_init(
            zookeeper_session_mgr::instance().zoo_hosts(),
            global_watcher,
            zookeeper_session_mgr::instance().timeout(),
            nullptr,
            this,
            0);
        dassert(_handle!=nullptr, "zookeeper session init failed");
    }

    _watchers.push_back(watcher_object());
    _watchers.back().watcher_path = "";
    _watchers.back().callback_owner = callback_owner;
    _watchers.back().watcher_callback = cb;

    return zoo_state(_handle);
}

void zookeeper_session::detach(void *callback_owner)
{
    utils::auto_write_lock l(_watcher_lock);
    _watchers.remove_if( [callback_owner](const watcher_object& obj) { return obj.callback_owner==callback_owner; } );
}

void zookeeper_session::dispatch_event(int type, int zstate, const char* path)
{
    {
        utils::auto_read_lock l(_watcher_lock);
        int ret_code = type;
        if (ZOO_SESSION_EVENT == ret_code)
            ret_code = zstate;

        std::for_each(_watchers.begin(), _watchers.end(), [path, ret_code](const watcher_object& obj){
            if (obj.watcher_path == path)
                obj.watcher_callback(ret_code);
        });
    }
    {
        if (ZOO_SESSION_EVENT!=type) {
            utils::auto_write_lock l(_watcher_lock);
            _watchers.remove_if([path](const watcher_object& obj) {
                return obj.watcher_path == path;
            } );
        }
    }
}

void zookeeper_session::visit(zoo_opcontext *ctx)
{
    ctx->_priv_session_ref = this;
    int &ec = ctx->_output.error;

    if ( zoo_state(_handle)!=ZOO_CONNECTED_STATE ) {
        ec = ZCONNECTIONLOSS;
        ctx->_callback_function(ctx);
        free_context(ctx);
        return;
    }

    auto add_watch_object = [this, ctx]() {
        utils::auto_write_lock l(_watcher_lock);
        _watchers.push_back(watcher_object());
        _watchers.back().watcher_path =ctx->_input._path;
        _watchers.back().callback_owner =ctx->_input._owner;
        _watchers.back().watcher_callback = std::move(ctx->_input._watcher_callback);
    };

    //TODO: the read ops from zookeeper might get the staled data, need to fix
    zoo_input& input = ctx->_input;
    const char* path = input._path.c_str();
    switch (ctx->_optype)
    {
    case ZOO_CREATE:
        ec = zoo_acreate(
            _handle, 
            path, 
            input._value.data(), 
            input._value.length(),
            &ZOO_OPEN_ACL_UNSAFE, 
            ctx->_input._flags, 
            global_string_completion,
            (const void*)ctx);
        break;
    case ZOO_DELETE:
        ec = zoo_adelete(
            _handle, 
            path, 
            -1,
            global_void_completion, 
            (const void*)ctx);
        break;
    case ZOO_EXISTS:
        if (1 == input._is_set_watch)
            add_watch_object();
        ec = zoo_aexists(
            _handle,
            path,
            input._is_set_watch,
            global_state_completion,
            (const void*)ctx);
        break;
    case ZOO_GET:
        if (1 == input._is_set_watch)
            add_watch_object();
        ec = zoo_aget(
            _handle, 
            path, 
            input._is_set_watch,
            global_data_completion,
            (const void*)ctx);
        break;
    case ZOO_SET:
        ec = zoo_aset(
            _handle, 
            path, 
            input._value.data(),
            input._value.length(),
            -1,
            global_state_completion,
            (const void*)ctx);
        break;
    case ZOO_GETCHILDREN:
        if (1 == input._is_set_watch)
            add_watch_object();
        ec = zoo_aget_children(
            _handle,
            path,
            input._is_set_watch,
            global_strings_completion,
            (const void*)ctx);
        break;
    default:
        break;
    }

    if (ZOK != ec) {
        ctx->_callback_function(ctx);
        free_context(ctx);
    }
}

void zookeeper_session::init_non_dsn_thread()
{
    static __thread int dsn_context_init = 0;
    if ( dsn_context_init == 0) {
        task::set_tls_dsn_context(_srv_node, nullptr, nullptr);
        dsn_context_init = 1;
    }
}

/*
 * the following static functions are in zookeeper threads,
 */
/* static */
void zookeeper_session::global_watcher(zhandle_t *handle, int type, int state, const char *path, void *ctx)
{
    zookeeper_session* zoo_session = (zookeeper_session*)ctx;
    zoo_session->init_non_dsn_thread();
    ddebug("global watcher, type(%d), state(%d), path(%s)", type, state, path);
    dassert(zoo_session->_handle == handle, "");
    zoo_session->dispatch_event(type, state, type==ZOO_SESSION_EVENT?"":path);
}

#define COMPLETION_INIT(rc, data) \
    zoo_opcontext* op_ctx = (zoo_opcontext*)data;\
    op_ctx->_priv_session_ref->init_non_dsn_thread();\
    zoo_output& output = op_ctx->_output;\
    output.error = rc
/* static */
void zookeeper_session::global_string_completion(int rc, const char *name, const void *data)
{
    COMPLETION_INIT(rc, data);
    dinfo("%s, rc(%s), name(%s), input path(%s)", __PRETTY_FUNCTION__, zerror(rc), name, op_ctx->_input._path.c_str());
    output.create_op._created_path = name;
    op_ctx->_callback_function(op_ctx);
    free_context(op_ctx);
}
/* static */
void zookeeper_session::global_data_completion(int rc, const char *value, int value_length, const Stat*, const void *data)
{
    COMPLETION_INIT(rc, data);
    dinfo("%s, rc(%s), input path(%s)", __PRETTY_FUNCTION__, zerror(rc), op_ctx->_input._path.c_str());
    output.get_op.value_length = value_length;
    output.get_op.value = value;
    op_ctx->_callback_function(op_ctx);
    free_context(op_ctx);
}
/* static */
void zookeeper_session::global_state_completion(int rc, const Stat *stat, const void *data)
{
    COMPLETION_INIT(rc, data);
    dinfo("%s, rc(%s), input path(%s)", __PRETTY_FUNCTION__, zerror(rc), op_ctx->_input._path.c_str());
    if (op_ctx->_optype == ZOO_EXISTS) {
        output.exists_op._node_stat = stat;
        op_ctx->_callback_function(op_ctx);
    }
    else {
        output.set_op._node_stat = stat;
        op_ctx->_callback_function(op_ctx);
    }
    free_context(op_ctx);
}
/* static */
void zookeeper_session::global_strings_completion(int rc, const String_vector *strings, const void *data)
{
    COMPLETION_INIT(rc, data);
    dinfo("%s, rc(%s), input path(%s), child count(%d)", __PRETTY_FUNCTION__, zerror(rc), op_ctx->_input._path.c_str(), strings->count);
    output.getchildren_op.strings = strings;
    op_ctx->_callback_function(op_ctx);
    free_context(op_ctx);
}
/* static */
void zookeeper_session::global_void_completion(int rc, const void *data)
{
    COMPLETION_INIT(rc, data);
    dinfo("%s, rc(%s), input path(%s)", __PRETTY_FUNCTION__, zerror(rc), op_ctx->_input._path.c_str());
    op_ctx->_callback_function(op_ctx);
    free_context(op_ctx);
}

}}
