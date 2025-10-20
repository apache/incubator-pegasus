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

#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/types.h>
#include <sasl/sasl.h>
#include <zookeeper/zookeeper.h>
#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <utility>

#include "rpc/rpc_address.h"
#include "runtime/app_model.h"
#include "utils/defer.h"
#include "utils/enum_helper.h"
#include "utils/filesystem.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/safe_strerror_posix.h"
#include "utils/strings.h"
#include "zookeeper/proto.h"
#include "zookeeper/zookeeper.jute.h"
#include "zookeeper_session.h"

#define INVALID_ZOO_LOG_LEVEL static_cast<ZooLogLevel>(0)
ENUM_BEGIN(ZooLogLevel, INVALID_ZOO_LOG_LEVEL)
ENUM_REG(ZOO_LOG_LEVEL_ERROR)
ENUM_REG(ZOO_LOG_LEVEL_WARN)
ENUM_REG(ZOO_LOG_LEVEL_INFO)
ENUM_REG(ZOO_LOG_LEVEL_DEBUG)
ENUM_END(ZooLogLevel)

DSN_DECLARE_bool(enable_zookeeper_kerberos);
DSN_DEFINE_string(security,
                  zookeeper_kerberos_service_name,
                  "",
                  "[Deprecated] zookeeper kerberos service name");
DSN_DEFINE_string(security,
                  zookeeper_sasl_service_fqdn,
                  "",
                  "[Deprecated] The FQDN of a Zookeeper server, used in Kerberos Principal");
// TODO(yingchun): to keep compatibility, the global name is FLAGS_timeout_ms. The name is not very
//  suitable, maybe improve the macro to us another global name.
DSN_DEFINE_int32(zookeeper,
                 timeout_ms,
                 30000,
                 "The timeout of accessing ZooKeeper, in milliseconds");
DSN_DEFINE_string(zookeeper, zoo_log_level, "ZOO_LOG_LEVEL_INFO", "ZooKeeper log level");
DSN_DEFINE_string(zookeeper, hosts_list, "", "ZooKeeper hosts list");
DSN_DEFINE_string(zookeeper, sasl_service_name, "zookeeper", "");
DSN_DEFINE_string(zookeeper,
                  sasl_service_fqdn,
                  "",
                  "SASL server name ('zk-sasl-md5' for DIGEST-MD5; default: reverse DNS lookup)");
DSN_DEFINE_string(zookeeper,
                  sasl_mechanisms_type,
                  "",
                  "SASL mechanisms (GSSAPI and/or DIGEST-MD5)");
DSN_DEFINE_string(zookeeper, sasl_user_name, "", "");
DSN_DEFINE_string(zookeeper, sasl_realm, "", "Realm (for SASL/GSSAPI)");
DSN_DEFINE_string(zookeeper,
                  sasl_password_file,
                  "",
                  "File containing the password (recommended for SASL/DIGEST-MD5)");
DSN_DEFINE_string(zookeeper,
                  sasl_password_encryption_scheme,
                  "",
                  "If non-empty, specify the scheme in which the password is encrypted; "
                  "otherwise, the password is unencrypted plaintext");
DSN_DEFINE_group_validator(enable_zookeeper_kerberos, [](std::string &message) -> bool {
    if (FLAGS_enable_zookeeper_kerberos &&
        !dsn::utils::equals(FLAGS_sasl_mechanisms_type, "GSSAPI")) {
        message = "Please set [zookeeper] sasl_mechanisms_type to GSSAPI if [security] "
                  "enable_zookeeper_kerberos is true.";
        return false;
    }

    return true;
});
DSN_DEFINE_group_validator(consistency_between_configurations, [](std::string &message) -> bool {
    if (!dsn::utils::is_empty(FLAGS_zookeeper_kerberos_service_name) &&
        !dsn::utils::equals(FLAGS_zookeeper_kerberos_service_name, FLAGS_sasl_service_name)) {
        message = "zookeeper_kerberos_service_name deprecated, if set should be same as "
                  "sasl_service_name.";
        return false;
    }
    if (!dsn::utils::is_empty(FLAGS_zookeeper_sasl_service_fqdn) &&
        !dsn::utils::equals(FLAGS_zookeeper_sasl_service_fqdn, FLAGS_sasl_service_fqdn)) {
        message =
            "zookeeper_sasl_service_fqdn deprecated, if set should be same as sasl_service_fqdn.";
        return false;
    }

    return true;
});

namespace dsn {
namespace dist {

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

zookeeper_session::zookeeper_session(service_app_info info)
    : _info(std::move(info)), _handle(nullptr)
{
}

namespace {

int decode_base64(
    const char *content, size_t content_len, char *buf, size_t buf_len, size_t *passwd_len)
{
    if (content_len == 0) {
        return SASL_BADPARAM;
    }

    BIO *bio = BIO_new_mem_buf(content, static_cast<int>(content_len));
    if (bio == nullptr) {
        return SASL_FAIL;
    }

    BIO *b64 = BIO_new(BIO_f_base64());
    if (b64 == nullptr) {
        BIO_free(bio);
        return SASL_FAIL;
    }

    BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);

    bio = BIO_push(b64, bio);
    const auto cleanup = dsn::defer([bio]() { BIO_free_all(bio); });

    const int read_len = BIO_read(bio, buf, static_cast<int>(buf_len));
    if (read_len <= 0) {
        return SASL_FAIL;
    }

    *passwd_len = read_len;

    return SASL_OK;
}

int zookeeper_password_decoder(const char *content,
                               size_t content_len,
                               void *context,
                               char *buf,
                               size_t buf_len,
                               size_t *passwd_len)
{
    if (dsn::utils::iequals(FLAGS_sasl_password_encryption_scheme, "base64")) {
        return decode_base64(content, content_len, buf, buf_len, passwd_len);
    }

    return SASL_BADPARAM;
}

bool is_password_file_plaintext()
{
    return dsn::utils::is_empty(FLAGS_sasl_password_encryption_scheme) ||
           dsn::utils::iequals(FLAGS_sasl_password_encryption_scheme, "plaintext");
}

zhandle_t *create_zookeeper_handle(watcher_fn watcher, void *context)
{
    const auto zoo_log_level = enum_from_string(FLAGS_zoo_log_level, INVALID_ZOO_LOG_LEVEL);
    CHECK(zoo_log_level != INVALID_ZOO_LOG_LEVEL, "Invalid zoo log level: {}", FLAGS_zoo_log_level);
    zoo_set_debug_level(zoo_log_level);

    // SASL auth is enabled iff FLAGS_sasl_mechanisms_type is non-empty.
    if (dsn::utils::is_empty(FLAGS_sasl_mechanisms_type)) {
        return zookeeper_init(FLAGS_hosts_list, watcher, FLAGS_timeout_ms, nullptr, context, 0);
    }

    const int err = sasl_client_init(nullptr);
    CHECK_EQ_MSG(err,
                 SASL_OK,
                 "Unable to initialize SASL library {}",
                 sasl_errstring(err, nullptr, nullptr));

    CHECK(!dsn::utils::is_empty(FLAGS_sasl_password_file),
          "sasl_password_file must be specified when SASL auth is enabled");
    CHECK(utils::filesystem::file_exists(FLAGS_sasl_password_file),
          "sasl_password_file {} not exist!",
          FLAGS_sasl_password_file);

    const char *host = "";
    if (!dsn::utils::is_empty(FLAGS_sasl_service_fqdn)) {
        CHECK(dsn::rpc_address::from_host_port(FLAGS_sasl_service_fqdn),
              "sasl_service_fqdn '{}' is invalid",
              FLAGS_sasl_service_fqdn);
        host = FLAGS_sasl_service_fqdn;
    }

    // DIGEST-MD5 requires '--server-fqdn zk-sasl-md5' for historical reasons on ZooKeeper
    // C client.
    if (dsn::utils::equals(FLAGS_sasl_mechanisms_type, "DIGEST-MD5")) {
        host = "zk-sasl-md5";
    }

    // Only encrypted passwords need their decoders.
    zoo_sasl_password_t passwd = {FLAGS_sasl_password_file,
                                  nullptr,
                                  is_password_file_plaintext() ? nullptr
                                                               : zookeeper_password_decoder};

    zoo_sasl_params_t sasl_params = {};
    sasl_params.service = FLAGS_sasl_service_name;
    sasl_params.host = host;
    sasl_params.mechlist = FLAGS_sasl_mechanisms_type;
    sasl_params.callbacks =
        zoo_sasl_make_password_callbacks(FLAGS_sasl_user_name, FLAGS_sasl_realm, &passwd);

    return zookeeper_init_sasl(
        FLAGS_hosts_list, watcher, FLAGS_timeout_ms, nullptr, context, 0, nullptr, &sasl_params);
}

} // anonymous namespace

int zookeeper_session::attach(void *callback_owner, const state_callback &cb)
{
    utils::auto_write_lock l(_watcher_lock);

    if (_handle == nullptr) {
        // zookeeper_init* functions will set `errno` while initialization failed due to some
        // error.
        errno = 0;
        _handle = create_zookeeper_handle(global_watcher, this);
        CHECK_NOTNULL(_handle, "zookeeper session init failed: {}", utils::safe_strerror(errno));
    }

    _watchers.emplace_back();
    _watchers.back().watcher_path = std::make_shared<std::string>();
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
                if (*obj.watcher_path == path) {
                    obj.watcher_callback(ret_code);
                }
            });
    }
    {
        if (ZOO_SESSION_EVENT != type) {
            utils::auto_write_lock l(_watcher_lock);
            _watchers.remove_if(
                [path](const watcher_object &obj) { return *obj.watcher_path == path; });
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

    // TODO(clang-tidy): the read ops from zookeeper might get the staled data, need to fix
    int ec = ZOK;
    zoo_input &input = ctx->_input;

    // A local variable is needed here to hold a reference to `_path` to prevent it from
    // being freed in the thread executing the completion callback, which could otherwise
    // lead to a dangling pointer issue.
    const std::shared_ptr<std::string> path(input._path);

    switch (ctx->_optype) {
    case ZOO_CREATE:
        ec = zoo_acreate(_handle,
                         path->c_str(),
                         input._value.data(),
                         static_cast<int>(input._value.length()),
                         &ZOO_OPEN_ACL_UNSAFE,
                         ctx->_input._flags,
                         global_string_completion,
                         ctx);
        break;
    case ZOO_DELETE:
        ec = zoo_adelete(_handle, path->c_str(), -1, global_void_completion, ctx);
        break;
    case ZOO_EXISTS:
        if (1 == input._is_set_watch) {
            add_watch_object();
        }
        ec = zoo_aexists(_handle, path->c_str(), input._is_set_watch, global_state_completion, ctx);
        break;
    case ZOO_GET:
        if (1 == input._is_set_watch) {
            add_watch_object();
        }
        ec = zoo_aget(_handle, path->c_str(), input._is_set_watch, global_data_completion, ctx);
        break;
    case ZOO_SET:
        ec = zoo_aset(_handle,
                      path->c_str(),
                      input._value.data(),
                      static_cast<int>(input._value.length()),
                      -1,
                      global_state_completion,
                      ctx);
        break;
    case ZOO_GETCHILDREN:
        if (1 == input._is_set_watch) {
            add_watch_object();
        }
        ec = zoo_aget_children(
            _handle, path->c_str(), input._is_set_watch, global_strings_completion, ctx);
        break;
    case ZOO_TRANSACTION:
        ec = zoo_amulti(_handle,
                        input._pkt->_count,
                        input._pkt->_ops,
                        input._pkt->_results,
                        global_void_completion,
                        ctx);
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
        dsn_mimic_app(_info.role_name.c_str(), _info.index);
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
    auto *op_ctx = static_cast<zoo_opcontext *>(const_cast<void *>(data));                         \
    op_ctx->_priv_session_ref->init_non_dsn_thread();                                              \
    zoo_output &output = op_ctx->_output;                                                          \
    output.error = rc
/* static */
void zookeeper_session::global_string_completion(int rc, const char *name, const void *data)
{
    COMPLETION_INIT(rc, data);
    LOG_DEBUG("rc({}), input path({})", zerror(rc), *op_ctx->_input._path);
    if (ZOK == rc && name != nullptr) {
        LOG_DEBUG("created path: {}", name);
    }
    output.create_op._created_path = name;
    op_ctx->_callback_function(op_ctx);
    release_ref(op_ctx);
}
/* static */
void zookeeper_session::global_data_completion(
    int rc, const char *value, int value_length, const Stat *, const void *data)
{
    COMPLETION_INIT(rc, data);
    LOG_DEBUG("rc({}), input path({})", zerror(rc), *op_ctx->_input._path);
    output.get_op.value_length = value_length;
    output.get_op.value = value;
    op_ctx->_callback_function(op_ctx);
    release_ref(op_ctx);
}
/* static */
void zookeeper_session::global_state_completion(int rc, const Stat *stat, const void *data)
{
    COMPLETION_INIT(rc, data);
    LOG_DEBUG("rc({}), input path({})", zerror(rc), *op_ctx->_input._path);
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
    LOG_DEBUG("rc({}), input path({})", zerror(rc), *op_ctx->_input._path);
    if (rc == ZOK && strings != nullptr) {
        LOG_DEBUG("child count: {}", strings->count);
    }
    output.getchildren_op.strings = strings;
    op_ctx->_callback_function(op_ctx);
    release_ref(op_ctx);
}
/* static */
void zookeeper_session::global_void_completion(int rc, const void *data)
{
    COMPLETION_INIT(rc, data);
    if (op_ctx->_optype == ZOO_DELETE) {
        LOG_DEBUG("rc({}), input path({})", zerror(rc), *op_ctx->_input._path);
    } else {
        LOG_DEBUG("rc({})", zerror(rc));
    }
    op_ctx->_callback_function(op_ctx);
    release_ref(op_ctx);
}

} // namespace dist
} // namespace dsn
