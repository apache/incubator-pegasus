/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

/*
 * Description:
 *     a C++ wrapper of zookeeper c async api, implementation
 *
 * Revision history:
 *     2015-12-04, @shengofsun (sunweijie@xiaomi.com)
 */

#include <zookeeper/zookeeper.h>
#include <sasl/sasl.h>

#include "zookeeper_session.h"
#include "zookeeper_session_mgr.h"

#include "utils/flags.h"

namespace dsn {
namespace security {
DSN_DECLARE_bool(enable_zookeeper_kerberos);
DSN_DEFINE_string(security,
                  zookeeper_kerberos_service_name,
                  "zookeeper",
                  "zookeeper kerberos service name");
} // namespace security
} // namespace dsn

namespace dsn {
namespace dist {
// TODO(yingchun): to keep compatibility, the global name is FLAGS_timeout_ms. The name is not very
//  suitable, maybe improve the macro to us another global name.
DSN_DEFINE_int32(zookeeper,
                 timeout_ms,
                 30000,
                 "The timeout of accessing ZooKeeper, in milliseconds");
DSN_DEFINE_string(zookeeper, hosts_list, "", "Zookeeper hosts list");

zookeeper_session::zoo_atomic_packet::zoo_atomic_packet(unsigned int size)
{
    _capacity = size;
    _count = 0;

    _ops = (zoo_op_t *)malloc(sizeof(zoo_op_t) * size);
    _results = (zoo_op_result_t *)malloc(sizeof(zoo_op_result_t) * size);

    _paths.resize(size);
    _datas.resize(size);
}

zookeeper_session::zoo_atomic_packet::~zoo_atomic_packet()
{
    for (int i = 0; i < _count; ++i) {
        if (_ops[i].type == ZOO_CREATE_OP)
            free(_ops[i].create_op.buf);
        else if (_ops[i].type == ZOO_SETDATA_OP)
            free(_ops[i].set_op.stat);
    }
    free(_ops);
    free(_results);
}

char *zookeeper_session::zoo_atomic_packet::alloc_buffer(int buffer_length)
{
    return (char *)malloc(buffer_length);
}

/*static*/
const char *zookeeper_session::string_zoo_operation(ZOO_OPERATION op)
{
    switch (op) {
    case ZOO_CREATE:
        return "zoo_create";
    case ZOO_DELETE:
        return "zoo_delete";
    case ZOO_EXISTS:
        return "zoo_exists";
    case ZOO_GET:
        return "zoo_get";
    case ZOO_GETCHILDREN:
        return "zoo_getchildren";
    case ZOO_SET:
        return "zoo_set";
    case ZOO_ASYNC:
        return "zoo_async";
    case ZOO_TRANSACTION:
        return "zoo_transaction";
    default:
        return "invalid";
    }
}

/*static*/
const char *zookeeper_session::string_zoo_event(int zoo_event)
{
    if (ZOO_SESSION_EVENT == zoo_event)
        return "session event";
    if (ZOO_CREATED_EVENT == zoo_event)
        return "created event";
    if (ZOO_DELETED_EVENT == zoo_event)
        return "deleted event";
    if (ZOO_CHANGED_EVENT == zoo_event)
        return "changed event";
    if (ZOO_CHILD_EVENT == zoo_event)
        return "child event";
    if (ZOO_NOTWATCHING_EVENT == zoo_event)
        return "notwatching event";
    return "invalid event";
}

/*static*/
const char *zookeeper_session::string_zoo_state(int zoo_state)
{
    if (ZOO_CONNECTED_STATE == zoo_state)
        return "connected_state";
    if (ZOO_EXPIRED_SESSION_STATE == zoo_state)
        return "expired_session_state";
    if (ZOO_AUTH_FAILED_STATE == zoo_state)
        return "auth_failed_state";
    if (ZOO_CONNECTING_STATE == zoo_state)
        return "connecting_state";
    if (ZOO_ASSOCIATING_STATE == zoo_state)
        return "associating_state";
    if (ZOO_CONNECTED_STATE == zoo_state)
        return "connected_state";
    return "invalid_state";
}

zookeeper_session::~zookeeper_session() {}

zookeeper_session::zookeeper_session(const service_app_info &node) : _handle(nullptr)
{
    _srv_node = node;
}

int zookeeper_session::attach(void *callback_owner, const state_callback &cb)
{
    utils::auto_write_lock l(_watcher_lock);
    if (nullptr == _handle) {
        if (dsn::security::FLAGS_enable_zookeeper_kerberos) {
            zoo_sasl_params_t sasl_params = {0};
            sasl_params.service = dsn::security::FLAGS_zookeeper_kerberos_service_name;
            sasl_params.mechlist = "GSSAPI";
            _handle = zookeeper_init_sasl(FLAGS_hosts_list,
                                          global_watcher,
                                          FLAGS_timeout_ms,
                                          nullptr,
                                          this,
                                          0,
                                          NULL,
                                          &sasl_params);
        } else {
            _handle = zookeeper_init(
                FLAGS_hosts_list, global_watcher, FLAGS_timeout_ms, nullptr, this, 0);
        }
        CHECK_NOTNULL(_handle, "zookeeper session init failed");
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
    _watchers.remove_if([callback_owner](const watcher_object &obj) {
        return obj.callback_owner == callback_owner;
    });
}

void zookeeper_session::dispatch_event(int type, int zstate, const char *path)
{
    {
        utils::auto_read_lock l(_watcher_lock);
        int ret_code = type;
        if (ZOO_SESSION_EVENT == ret_code)
            ret_code = zstate;

        std::for_each(
            _watchers.begin(), _watchers.end(), [path, ret_code](const watcher_object &obj) {
                if (obj.watcher_path == path)
                    obj.watcher_callback(ret_code);
            });
    }
    {
        if (ZOO_SESSION_EVENT != type) {
            utils::auto_write_lock l(_watcher_lock);
            _watchers.remove_if(
                [path](const watcher_object &obj) { return obj.watcher_path == path; });
        }
    }
}

void zookeeper_session::visit(zoo_opcontext *ctx)
{
    ctx->_priv_session_ref = this;

    if (zoo_state(_handle) != ZOO_CONNECTED_STATE) {
        ctx->_output.error = ZINVALIDSTATE;
        ctx->_callback_function(ctx);
        release_ref(ctx);
        return;
    }

    auto add_watch_object = [this, ctx]() {
        utils::auto_write_lock l(_watcher_lock);
        _watchers.push_back(watcher_object());
        _watchers.back().watcher_path = ctx->_input._path;
        _watchers.back().callback_owner = ctx->_input._owner;
        _watchers.back().watcher_callback = std::move(ctx->_input._watcher_callback);
    };

    // TODO: the read ops from zookeeper might get the staled data, need to fix
    int ec = ZOK;
    zoo_input &input = ctx->_input;
    const char *path = input._path.c_str();
    switch (ctx->_optype) {
    case ZOO_CREATE:
        ec = zoo_acreate(_handle,
                         path,
                         input._value.data(),
                         input._value.length(),
                         &ZOO_OPEN_ACL_UNSAFE,
                         ctx->_input._flags,
                         global_string_completion,
                         (const void *)ctx);
        break;
    case ZOO_DELETE:
        ec = zoo_adelete(_handle, path, -1, global_void_completion, (const void *)ctx);
        break;
    case ZOO_EXISTS:
        if (1 == input._is_set_watch)
            add_watch_object();
        ec = zoo_aexists(
            _handle, path, input._is_set_watch, global_state_completion, (const void *)ctx);
        break;
    case ZOO_GET:
        if (1 == input._is_set_watch)
            add_watch_object();
        ec =
            zoo_aget(_handle, path, input._is_set_watch, global_data_completion, (const void *)ctx);
        break;
    case ZOO_SET:
        ec = zoo_aset(_handle,
                      path,
                      input._value.data(),
                      input._value.length(),
                      -1,
                      global_state_completion,
                      (const void *)ctx);
        break;
    case ZOO_GETCHILDREN:
        if (1 == input._is_set_watch)
            add_watch_object();
        ec = zoo_aget_children(
            _handle, path, input._is_set_watch, global_strings_completion, (const void *)ctx);
        break;
    case ZOO_TRANSACTION:
        ec = zoo_amulti(_handle,
                        input._pkt->_count,
                        input._pkt->_ops,
                        input._pkt->_results,
                        global_void_completion,
                        (const void *)ctx);
        break;
    default:
        break;
    }

    if (ZOK != ec) {
        ctx->_output.error = ec;
        ctx->_callback_function(ctx);
        release_ref(ctx);
    }
}

void zookeeper_session::init_non_dsn_thread()
{
    static __thread int dsn_context_init = 0;
    if (dsn_context_init == 0) {
        dsn_mimic_app(_srv_node.role_name.c_str(), _srv_node.index);
        dsn_context_init = 1;
    }
}

/*
 * the following static functions are in zookeeper threads,
 */
/* static */
void zookeeper_session::global_watcher(
    zhandle_t *handle, int type, int state, const char *path, void *ctx)
{
    zookeeper_session *zoo_session = (zookeeper_session *)ctx;
    zoo_session->init_non_dsn_thread();
    LOG_INFO(
        "global watcher, type({}), state({})", string_zoo_event(type), string_zoo_state(state));
    if (type != ZOO_SESSION_EVENT && path != nullptr)
        LOG_INFO("watcher path: {}", path);

    CHECK(zoo_session->_handle == handle, "");
    zoo_session->dispatch_event(type, state, type == ZOO_SESSION_EVENT ? "" : path);
}

#define COMPLETION_INIT(rc, data)                                                                  \
    zoo_opcontext *op_ctx = (zoo_opcontext *)data;                                                 \
    op_ctx->_priv_session_ref->init_non_dsn_thread();                                              \
    zoo_output &output = op_ctx->_output;                                                          \
    output.error = rc
/* static */
void zookeeper_session::global_string_completion(int rc, const char *name, const void *data)
{
    COMPLETION_INIT(rc, data);
    LOG_DEBUG("rc({}), input path({})", zerror(rc), op_ctx->_input._path);
    if (ZOK == rc && name != nullptr)
        LOG_DEBUG("created path: {}", name);
    output.create_op._created_path = name;
    op_ctx->_callback_function(op_ctx);
    release_ref(op_ctx);
}
/* static */
void zookeeper_session::global_data_completion(
    int rc, const char *value, int value_length, const Stat *, const void *data)
{
    COMPLETION_INIT(rc, data);
    LOG_DEBUG("rc({}), input path({})", zerror(rc), op_ctx->_input._path);
    output.get_op.value_length = value_length;
    output.get_op.value = value;
    op_ctx->_callback_function(op_ctx);
    release_ref(op_ctx);
}
/* static */
void zookeeper_session::global_state_completion(int rc, const Stat *stat, const void *data)
{
    COMPLETION_INIT(rc, data);
    LOG_DEBUG("rc({}), input path({})", zerror(rc), op_ctx->_input._path);
    if (op_ctx->_optype == ZOO_EXISTS) {
        output.exists_op._node_stat = stat;
        op_ctx->_callback_function(op_ctx);
    } else {
        output.set_op._node_stat = stat;
        op_ctx->_callback_function(op_ctx);
    }
    release_ref(op_ctx);
}
/* static */
void zookeeper_session::global_strings_completion(int rc,
                                                  const String_vector *strings,
                                                  const void *data)
{
    COMPLETION_INIT(rc, data);
    LOG_DEBUG("rc({}), input path({})", zerror(rc), op_ctx->_input._path);
    if (rc == ZOK && strings != nullptr)
        LOG_DEBUG("child count: {}", strings->count);
    output.getchildren_op.strings = strings;
    op_ctx->_callback_function(op_ctx);
    release_ref(op_ctx);
}
/* static */
void zookeeper_session::global_void_completion(int rc, const void *data)
{
    COMPLETION_INIT(rc, data);
    if (op_ctx->_optype == ZOO_DELETE)
        LOG_DEBUG("rc({}), input path({})", zerror(rc), op_ctx->_input._path);
    else
        LOG_DEBUG("rc({})", zerror(rc));
    op_ctx->_callback_function(op_ctx);
    release_ref(op_ctx);
}
}
}
