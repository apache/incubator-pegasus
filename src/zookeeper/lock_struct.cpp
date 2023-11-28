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

#include <ctype.h>
#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "distributed_lock_service_zookeeper.h"
#include "lock_struct.h"
#include "lock_types.h"
#include "runtime/task/async_calls.h"
#include "runtime/task/task.h"
#include "utils/blob.h"
#include "utils/error_code.h"
#include "utils/fmt_logging.h"
#include "utils/strings.h"
#include "utils/thread_access_checker.h"
#include "zookeeper/zookeeper.h"
#include "zookeeper/zookeeper.jute.h"
#include "zookeeper_session.h"

namespace dsn {
namespace dist {

static const char *states[] = {
    "uninitialized", "pending", "locked", "expired", "cancelled", "unlocking"};

static inline const char *string_state(lock_state state)
{
    CHECK_LT(state, lock_state::state_count);
    return states[state];
}

static bool is_zookeeper_timeout(int zookeeper_error)
{
    return zookeeper_error == ZCONNECTIONLOSS || zookeeper_error == ZOPERATIONTIMEOUT;
}

#define __check_code(code, allow_list, allow_list_size, code_str)                                  \
    do {                                                                                           \
        int i = 0;                                                                                 \
        for (; i != allow_list_size; ++i) {                                                        \
            if (code == allow_list[i])                                                             \
                break;                                                                             \
        }                                                                                          \
        CHECK_LT_MSG(i, allow_list_size, "invalid code({})", code_str);                            \
    } while (0)

#define __execute(cb, _this) tasking::enqueue(TASK_CODE_DLOCK, nullptr, cb, _this->hash())

#define __add_ref_and_delay_call(op, _this)                                                        \
    LOG_WARNING("operation {} on {} encounter error, retry later",                                 \
                zookeeper_session::string_zoo_operation(op->_optype),                              \
                op->_input._path);                                                                 \
    zookeeper_session::add_ref(op);                                                                \
    tasking::enqueue(TASK_CODE_DLOCK,                                                              \
                     nullptr,                                                                      \
                     [_this, op]() { _this->_dist_lock_service->session()->visit(op); },           \
                     _this->hash(),                                                                \
                     std::chrono::seconds(1));

#define IGNORE_CALLBACK true
#define DONT_IGNORE_CALLBACK false
#define REMOVE_FOR_UNLOCK true
#define REMOVE_FOR_CANCEL false

lock_struct::lock_struct(lock_srv_ptr srv) : ref_counter()
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
    _checker.only_one_thread_access();

    if (_dist_lock_service != nullptr) {
        _dist_lock_service->erase(std::make_pair(_lock_id, _myself._node_value));
        _dist_lock_service->session()->detach(this);
        _dist_lock_service = nullptr;
    }
}

void lock_struct::on_operation_timeout()
{
    LOG_INFO("zookeeper operation times out, removing the current watching");
    _state = lock_state::uninitialized;
    _dist_lock_service->session()->detach(this);
    _lock_callback->enqueue_with(ERR_TIMEOUT, _owner._node_value, _owner._sequence_id);
}

void lock_struct::on_expire()
{
    if (_state == lock_state::expired)
        return;
    _state = lock_state::expired;
    remove_lock();
    _lease_expire_callback->enqueue_with(ERR_EXPIRED, _owner._node_value, _owner._sequence_id);
    clear();
}

int64_t lock_struct::parse_seq_path(const std::string &path)
{
    int64_t ans = 0;
    int64_t power = 1;
    int i = ((int)path.size()) - 1;
    for (; i >= 0 && isdigit(path[i]); --i) {
        ans = ans + (path[i] - '0') * power;
        power *= 10;
    }
    const std::string &match = distributed_lock_service_zookeeper::LOCK_NODE_PREFIX;
    int j = ((int)match.size()) - 1;
    for (; i >= 0 && j >= 0 && path[i] == match[j]; --i, --j)
        ;
    if (power == 1 || j >= 0) {
        LOG_WARNING("invalid path: {}", path);
        return -1;
    }
    return ans;
}

/*static*/
void lock_struct::my_lock_removed(lock_struct_ptr _this, int zoo_event)
{
    static const lock_state allow_state[] = {
        lock_state::locked, lock_state::unlocking, lock_state::expired};
    _this->_checker.only_one_thread_access();
    __check_code(_this->_state, allow_state, 3, string_state(_this->_state));

    if (_this->_state == lock_state::unlocking || _this->_state == lock_state::expired) {
        return;
    } else {
        _this->on_expire();
    }
}
/*static*/
void lock_struct::owner_change(lock_struct_ptr _this, int zoo_event)
{
    static const lock_state allow_state[] = {
        lock_state::uninitialized, lock_state::pending, lock_state::cancelled, lock_state::expired};
    _this->_checker.only_one_thread_access();
    __check_code(_this->_state, allow_state, 3, string_state(_this->_state));

    if (_this->_state == lock_state::uninitialized) {
        LOG_WARNING("this is mainly due to a timeout happens before, just ignore the event {}",
                    zookeeper_session::string_zoo_event(zoo_event));
        return;
    }
    if (_this->_state == lock_state::cancelled || _this->_state == lock_state::expired) {
        return;
    }
    if (ZOO_DELETED_EVENT == zoo_event) {
        _this->_owner._sequence_id = -1;
        _this->_owner._node_seq_name.clear();
        _this->_owner._node_value.clear();
        _this->get_lockdir_nodes();
    } else if (ZOO_NOTWATCHING_EVENT == zoo_event)
        _this->get_lock_owner(false);
    else
        CHECK(false, "unexpected event");
}
/*static*/
void lock_struct::after_remove_duplicated_locknode(lock_struct_ptr _this,
                                                   int ec,
                                                   std::shared_ptr<std::string> path)
{
    static const int allow_ec[] = {
        ZOK,
        ZNONODE,      // ok
        ZINVALIDSTATE // operation timeout
    };
    static const int allow_state[] = {
        lock_state::pending, lock_state::cancelled, lock_state::expired, lock_state::locked};
    _this->_checker.only_one_thread_access();
    __check_code(ec, allow_ec, 3, zerror(ec));
    __check_code(_this->_state, allow_state, 4, string_state(_this->_state));

    if (_this->_state == lock_state::cancelled || _this->_state == lock_state::expired) {
        return;
    }

    if (_this->_state == lock_state::locked) {
        // why the state may be locked:
        // 1. send request to remove duplicated node
        // 2. the node_delete_watcher is notified
        // 3. check the children and found myself get the lock
        // 4. requst in 1 resonsed
        LOG_INFO("the state is locked mainly because owner changed watcher is triggered first");
    }

    if (ZOK == ec || ZNONODE == ec) {
        LOG_INFO("lock({}) remove duplicated node({}), rely on delete watcher to be actived",
                 _this->_lock_id,
                 *path);
    } else {
        LOG_ERROR("lock struct({}), myself({}) got session expire",
                  _this->_lock_dir,
                  _this->_myself._node_seq_name);
        _this->on_expire();
    }
}

void lock_struct::remove_duplicated_locknode(std::string &&znode_path)
{
    lock_struct_ptr _this = this;
    LOG_WARNING(
        "duplicated value({}) ephe/seq node({} and {}) create on zookeeper, remove the smaller one",
        _myself._node_value,
        _owner._node_seq_name,
        _myself._node_seq_name);

    auto delete_callback_wrapper = [_this](zookeeper_session::zoo_opcontext *op) {
        if (is_zookeeper_timeout(op->_output.error)) {
            __add_ref_and_delay_call(op, _this);
        } else {
            __execute(std::bind(&lock_struct::after_remove_duplicated_locknode,
                                _this,
                                op->_output.error,
                                std::shared_ptr<std::string>(
                                    new std::string(std::move(op->_input._path)))),
                      _this);
        }
    };
    zookeeper_session::zoo_opcontext *op = zookeeper_session::create_context();
    op->_optype = zookeeper_session::ZOO_DELETE;
    op->_callback_function = delete_callback_wrapper;
    op->_input._path = std::move(znode_path);
    _dist_lock_service->session()->visit(op);
}
/*static*/
void lock_struct::after_get_lock_owner(lock_struct_ptr _this,
                                       int ec,
                                       std::shared_ptr<std::string> value)
{
    static const int allow_ec[] = {
        ZOK,          // OK
        ZNONODE,      // owner session removed
        ZINVALIDSTATE // operation timeout
    };
    static const int allow_state[] = {
        lock_state::pending, lock_state::cancelled, lock_state::expired};
    _this->_checker.only_one_thread_access();
    __check_code(ec, allow_ec, 3, zerror(ec));
    __check_code(_this->_state, allow_state, 3, string_state(_this->_state));

    if (_this->_state == lock_state::cancelled || _this->_state == lock_state::expired) {
        return;
    }
    if (ZNONODE == ec) {
        // lock owner removed
        LOG_INFO("the lock({}) old owner({}:{}) has removed, myself({}:{}) try to get the lock "
                 "for another turn",
                 _this->_lock_id,
                 _this->_owner._node_seq_name,
                 _this->_owner._node_value,
                 _this->_myself._node_seq_name,
                 _this->_myself._node_value);
        _this->_owner._sequence_id = -1;
        _this->_owner._node_seq_name.clear();
        _this->get_lockdir_nodes();
        return;
    }
    if (ZOK == ec) {
        _this->_owner._node_value = std::move(*value);
        if (_this->_myself._node_value == _this->_owner._node_value)
            _this->remove_duplicated_locknode(_this->_lock_dir + "/" +
                                              _this->_owner._node_seq_name);
        else {
            _this->_dist_lock_service->refresh_lock_cache(
                _this->_lock_id, _this->_owner._node_value, _this->_owner._sequence_id);
            LOG_INFO("wait the lock({}) owner({}:{}) to remove, myself({}:{})",
                     _this->_lock_id,
                     _this->_owner._node_seq_name,
                     _this->_owner._node_value,
                     _this->_myself._node_seq_name,
                     _this->_myself._node_value);
        }
    } else {
        LOG_ERROR("lock_dir({}), myself({}), sessin expired",
                  _this->_lock_dir,
                  _this->_myself._node_seq_name);
        _this->on_expire();
    }
}
/*static*/
void lock_struct::after_self_check(lock_struct_ptr _this,
                                   int ec,
                                   std::shared_ptr<std::string> value)
{
    static const int allow_ec[] = {
        ZOK,          // OK
        ZNONODE,      // removed by unlock, or session expired
        ZINVALIDSTATE // session expired
    };
    static const lock_state allow_state[] = {
        lock_state::locked, lock_state::unlocking, lock_state::expired};
    _this->_checker.only_one_thread_access();
    __check_code(ec, allow_ec, 3, zerror(ec));
    __check_code(_this->_state, allow_state, 3, string_state(_this->_state));

    if (_this->_state == lock_state::unlocking || _this->_state == lock_state::expired) {
        LOG_INFO("skip lock({}) owner self check, do nothing, myself({}:{})",
                 _this->_lock_id,
                 _this->_myself._node_seq_name,
                 _this->_myself._node_value);
        return;
    }
    if (ZNONODE == ec || ZINVALIDSTATE == ec) {
        LOG_INFO("lock({}) session expired, error reason({}), myself({}:{})",
                 _this->_lock_id,
                 zerror(ec),
                 _this->_myself._node_seq_name,
                 _this->_myself._node_value);
        _this->on_expire();
        return;
    }
    CHECK_EQ_MSG(*value,
                 _this->_myself._node_value,
                 "lock({}) get wrong value, local myself({}), from zookeeper({})",
                 _this->_lock_id,
                 _this->_myself._node_value,
                 *value);
}

void lock_struct::get_lock_owner(bool watch_myself)
{
    lock_struct_ptr _this = this;
    auto watcher_callback_wrapper = [_this, watch_myself](int event) {
        LOG_INFO("get watcher callback, event type({})",
                 zookeeper_session::string_zoo_event(event));
        if (watch_myself)
            __execute(std::bind(&lock_struct::my_lock_removed, _this, event), _this);
        else
            __execute(std::bind(&lock_struct::owner_change, _this, event), _this);
    };

    auto after_get_owner_wrapper = [_this, watch_myself](zookeeper_session::zoo_opcontext *op) {
        zookeeper_session::zoo_output &output = op->_output;
        std::function<void(int, std::shared_ptr<std::string>)> cb;
        if (!watch_myself)
            cb = std::bind(&lock_struct::after_get_lock_owner,
                           _this,
                           std::placeholders::_1,
                           std::placeholders::_2);
        else
            cb = std::bind(&lock_struct::after_self_check,
                           _this,
                           std::placeholders::_1,
                           std::placeholders::_2);

        if (is_zookeeper_timeout(output.error)) {
            _this->_dist_lock_service->session()->detach(
                _this.get()); // before retry, first we need to remove the watcher
            __add_ref_and_delay_call(op, _this);
        } else if (output.error != ZOK)
            __execute(std::bind(cb, output.error, nullptr), _this);
        else {
            std::shared_ptr<std::string> buf(
                new std::string(output.get_op.value, output.get_op.value_length));
            __execute(std::bind(cb, ZOK, buf), _this);
        }
    };

    zookeeper_session::zoo_opcontext *op = zookeeper_session::create_context();
    op->_optype = zookeeper_session::ZOO_GET;
    op->_callback_function = after_get_owner_wrapper;
    op->_input._path = _lock_dir + "/" + _owner._node_seq_name;

    op->_input._is_set_watch = 1;
    op->_input._owner = this;
    op->_input._watcher_callback = watcher_callback_wrapper;

    _dist_lock_service->session()->visit(op);
}
/*static*/
void lock_struct::after_get_lockdir_nodes(lock_struct_ptr _this,
                                          int ec,
                                          std::shared_ptr<std::vector<std::string>> children)
{
    static const int allow_ec[] = {
        ZOK,          // succeed
        ZINVALIDSTATE // session expired
    };
    static const int allow_state[] = {
        lock_state::pending, lock_state::cancelled, lock_state::expired};

    _this->_checker.only_one_thread_access();
    __check_code(ec, allow_ec, 2, zerror(ec));
    __check_code(_this->_state, allow_state, 3, string_state(_this->_state));

    if (_this->_state == lock_state::cancelled || _this->_state == lock_state::expired) {
        return;
    }
    if (ZINVALIDSTATE == ec) {
        LOG_ERROR("get lockdir({}) children got session expired", _this->_lock_dir);
        _this->on_expire();
        return;
    }

    int64_t min_seq = -1, min_pos = -1, my_pos = -1;
    int64_t myself_seq = _this->_myself._sequence_id;

    for (int i = 0; i != (int)children->size(); ++i) {
        std::string &child = (*children)[i];
        int64_t seq = parse_seq_path(child);
        if (seq == -1) {
            LOG_WARNING("an invalid node({}) in lockdir({}), ignore", child, _this->_lock_dir);
            continue;
        }
        if (min_pos == -1 || min_seq > seq)
            min_seq = seq, min_pos = i;
        if (myself_seq == seq)
            my_pos = i;
    }

    LOG_INFO("min sequece number({}) in lockdir({})", min_seq, _this->_lock_dir);
    if (my_pos == -1) {
        // znode removed on zookeeper, may timeout or removed by other procedure
        LOG_WARNING("sequence and ephemeral node({}/{}) removed when get_children, this is "
                    "abnormal, try to reaquire the lock",
                    _this->_lock_dir,
                    _this->_myself._node_seq_name);
        _this->_myself._node_seq_name.clear();
        _this->_myself._sequence_id = -1;
        _this->create_locknode();
        return;
    } else {
        _this->_owner._sequence_id = min_seq;
        _this->_owner._node_seq_name = std::move((*children)[min_pos]);
        bool watch_myself = false;
        if (min_seq == myself_seq) {
            // i am the smallest one, so i get the lock :-)
            CHECK_EQ_MSG(min_pos,
                         my_pos,
                         "same sequence node number on zookeeper, dir({}), number({})",
                         _this->_lock_dir,
                         myself_seq);
            _this->_state = lock_state::locked;
            _this->_owner._node_value = _this->_myself._node_value;
            _this->_dist_lock_service->refresh_lock_cache(
                _this->_lock_id, _this->_owner._node_value, _this->_owner._sequence_id);

            watch_myself = true;
            LOG_INFO("got the lock({}), myself({}:{})",
                     _this->_lock_id,
                     _this->_myself._node_seq_name,
                     _this->_myself._node_value);
            _this->_lock_callback->enqueue_with(
                ERR_OK, _this->_myself._node_value, _this->_myself._sequence_id);
        }
        _this->get_lock_owner(watch_myself);
    }
}

void lock_struct::get_lockdir_nodes()
{
    lock_struct_ptr _this = this;
    auto result_wrapper = [_this](zookeeper_session::zoo_opcontext *op) {
        if (is_zookeeper_timeout(op->_output.error)) {
            __add_ref_and_delay_call(op, _this);
        } else if (op->_output.error != ZOK) {
            __execute(
                std::bind(&lock_struct::after_get_lockdir_nodes, _this, op->_output.error, nullptr),
                _this);
        } else {
            const String_vector *vec = op->_output.getchildren_op.strings;
            std::shared_ptr<std::vector<std::string>> children(
                new std::vector<std::string>(vec->count));
            for (int i = 0; i != vec->count; ++i)
                (*children)[i].assign(vec->data[i]);
            __execute(
                std::bind(
                    &lock_struct::after_get_lockdir_nodes, _this, op->_output.error, children),
                _this);
        }
    };

    zookeeper_session::zoo_opcontext *op = zookeeper_session::create_context();
    op->_optype = zookeeper_session::ZOO_GETCHILDREN;
    op->_callback_function = result_wrapper;
    op->_input._path = _lock_dir;
    op->_input._is_set_watch = 0;
    _dist_lock_service->session()->visit(op);
}
/*static*/
void lock_struct::after_create_locknode(lock_struct_ptr _this,
                                        int ec,
                                        std::shared_ptr<std::string> path)
{
    // as we create an ephe|seq node, so ZNODEEXISTS is not allowed
    static const int allow_ec[] = {
        ZOK,          // succeed
        ZINVALIDSTATE // operation timeout
    };
    static const int allow_state[] = {
        lock_state::pending, lock_state::cancelled, lock_state::expired};

    _this->_checker.only_one_thread_access();
    __check_code(ec, allow_ec, 2, zerror(ec));
    __check_code(_this->_state, allow_state, 3, string_state(_this->_state));

    LOG_DEBUG("after create seq and ephe node, error({}), path({})", zerror(ec), *path);
    if (_this->_state == lock_state::cancelled || _this->_state == lock_state::expired) {
        LOG_INFO("current state({}), ignore event create lockdir({})",
                 string_state(_this->_state),
                 _this->_lock_dir);
        if (ZOK == ec && _this->_state == lock_state::cancelled) {
            _this->remove_my_locknode(std::move(*path), IGNORE_CALLBACK, REMOVE_FOR_CANCEL);
        }
        return;
    }
    if (ZINVALIDSTATE == ec) {
        LOG_ERROR("create seq/ephe node ({}) in dir({}) got session expired",
                  distributed_lock_service_zookeeper::LOCK_NODE_PREFIX,
                  _this->_lock_dir);
        _this->on_expire();
        return;
    }

    char splitter[] = {'/', 0};
    _this->_myself._node_seq_name = utils::get_last_component(*path, splitter);
    _this->_myself._sequence_id = parse_seq_path(_this->_myself._node_seq_name);
    CHECK_NE_MSG(_this->_myself._sequence_id, -1, "invalid seq path created");
    LOG_INFO("create seq/ephe node in dir({}) ok, my_sequence_id({})",
             _this->_lock_dir,
             _this->_myself._sequence_id);
    _this->get_lockdir_nodes();
}

void lock_struct::create_locknode()
{
    // create an ZOO_EPHEMERAL|ZOO_SEQUENCE node
    zookeeper_session::zoo_opcontext *op = zookeeper_session::create_context();
    op->_optype = zookeeper_session::ZOO_OPERATION::ZOO_CREATE;

    lock_struct_ptr _this = this;
    auto result_wrapper = [_this](zookeeper_session::zoo_opcontext *op) {
        if (is_zookeeper_timeout(op->_output.error)) {
            __add_ref_and_delay_call(op, _this);
        } else if (op->_output.error != ZOK) {
            __execute(
                std::bind(&lock_struct::after_create_locknode, _this, op->_output.error, nullptr),
                _this);
        } else {
            std::shared_ptr<std::string> path(new std::string(op->_output.create_op._created_path));
            __execute(std::bind(&lock_struct::after_create_locknode, _this, ZOK, path), _this);
        }
    };

    zookeeper_session::zoo_input &input = op->_input;
    input._path = _lock_dir + "/" + distributed_lock_service_zookeeper::LOCK_NODE_PREFIX;
    input._value.assign(_myself._node_value.c_str(), 0, _myself._node_value.length());
    input._flags = ZOO_EPHEMERAL | ZOO_SEQUENCE;
    op->_callback_function = result_wrapper;
    _dist_lock_service->session()->visit(op);
}
/*static*/
void lock_struct::after_create_lockdir(lock_struct_ptr _this, int ec)
{
    _this->_checker.only_one_thread_access();
    static const int allow_ec[] = {
        ZOK,
        ZNODEEXISTS,  // succeed state
        ZINVALIDSTATE // session expire
    };
    static const lock_state allow_state[] = {
        lock_state::pending, lock_state::cancelled, lock_state::expired};
    __check_code(ec, allow_ec, 3, zerror(ec));
    __check_code(_this->_state, allow_state, 3, string_state(_this->_state));

    if (_this->_state == lock_state::cancelled || _this->_state == lock_state::expired) {
        LOG_INFO("current state({}), ignore event create lockdir({})",
                 string_state(_this->_state),
                 _this->_lock_dir);
        return;
    }
    if (ZINVALIDSTATE == ec) {
        LOG_ERROR("create lock dir failed got session expire, _path({})", _this->_lock_dir);
        _this->_lock_dir.clear();
        _this->on_expire();
        return;
    }
    _this->create_locknode();
}
/*static*/
void lock_struct::try_lock(lock_struct_ptr _this,
                           lock_future_ptr lock_callback,
                           lock_future_ptr expire_callback)
{
    _this->_checker.only_one_thread_access();

    if (_this->_state != lock_state::uninitialized) {
        lock_callback->enqueue_with(ERR_RECURSIVE_LOCK, "", -1);
        return;
    }

    _this->_lock_callback = lock_callback;
    _this->_lease_expire_callback = expire_callback;
    _this->_state = lock_state::pending;

    if (_this->_lock_dir.empty()) {
        _this->_lock_dir = _this->_dist_lock_service->_lock_root + "/" + _this->_lock_id;
        auto result_wrapper = [_this](zookeeper_session::zoo_opcontext *op) {
            if (is_zookeeper_timeout(op->_output.error)) {
                __add_ref_and_delay_call(op, _this);
            } else {
                __execute(std::bind(&lock_struct::after_create_lockdir, _this, op->_output.error),
                          _this);
            }
        };
        zookeeper_session::zoo_opcontext *op = zookeeper_session::create_context();
        op->_optype = zookeeper_session::ZOO_CREATE;
        op->_input._path = _this->_lock_dir;
        op->_callback_function = result_wrapper;
        _this->_dist_lock_service->session()->visit(op);
    } else if (_this->_myself._sequence_id == -1)
        _this->create_locknode();
    else if (_this->_owner._sequence_id == -1)
        _this->get_lockdir_nodes();
    else
        _this->get_lock_owner(false);
}

void lock_struct::after_remove_my_locknode(lock_struct_ptr _this, int ec, bool remove_for_unlock)
{
    static const int allow_ec[] = {
        ZOK,
        ZNONODE,      // ok
        ZINVALIDSTATE // operation timeout
    };
    static const int allow_state[] = {
        lock_state::cancelled, lock_state::unlocking, lock_state::expired};
    _this->_checker.only_one_thread_access();
    __check_code(ec, allow_ec, 3, zerror(ec));
    __check_code(_this->_state, allow_state, 3, string_state(_this->_state));

    error_code dsn_ec;
    if (lock_state::expired == _this->_state) {
        LOG_INFO("during unlock/cancel lock({}), encountered expire, owner({}:{}), myself({}:{})",
                 _this->_lock_id,
                 _this->_owner._node_seq_name,
                 _this->_owner._node_value,
                 _this->_myself._node_seq_name,
                 _this->_myself._node_value);
        dsn_ec = ERR_INVALID_STATE;
    } else {
        if (ZINVALIDSTATE == ec) {
            _this->on_expire(); // when expire, only expire_callback is called, the unlock/cancel's
                                // callback is ignored
            return;
        } else
            dsn_ec = ERR_OK;
    }

    if (dsn_ec == ERR_OK)
        _this->remove_lock();

    if (REMOVE_FOR_UNLOCK == remove_for_unlock)
        _this->_unlock_callback->enqueue_with(dsn_ec);
    else {
        _this->_cancel_callback->enqueue_with(
            dsn_ec, _this->_owner._node_value, _this->_owner._sequence_id);
    }

    if (dsn_ec == ERR_OK) {
        _this->clear();
    }
}

void lock_struct::remove_my_locknode(std::string &&znode_path,
                                     bool ignore_callback,
                                     bool remove_for_unlock)
{
    lock_struct_ptr _this = this;
    auto result_wrapper =
        [_this, ignore_callback, remove_for_unlock](zookeeper_session::zoo_opcontext *op) {
            LOG_INFO("delete node {}, result({})", op->_input._path, zerror(op->_output.error));
            if (is_zookeeper_timeout(op->_output.error)) {
                __add_ref_and_delay_call(op, _this);
                return;
            }

            if (IGNORE_CALLBACK != ignore_callback) {
                __execute(std::bind(&lock_struct::after_remove_my_locknode,
                                    _this,
                                    op->_output.error,
                                    remove_for_unlock),
                          _this);
            }
        };
    zookeeper_session::zoo_opcontext *op = zookeeper_session::create_context();
    op->_optype = zookeeper_session::ZOO_DELETE;
    op->_input._path = std::move(znode_path);
    op->_callback_function = result_wrapper;
    _dist_lock_service->session()->visit(op);
}

/*static*/
void lock_struct::cancel_pending_lock(lock_struct_ptr _this, lock_future_ptr cancel_callback)
{
    _this->_checker.only_one_thread_access();
    if (_this->_state != lock_state::uninitialized && _this->_state != lock_state::pending &&
        _this->_state != lock_state::cancelled) {
        cancel_callback->enqueue_with(ERR_INVALID_PARAMETERS, "", _this->_owner._sequence_id);
        return;
    }

    _this->_state = lock_state::cancelled;
    _this->_cancel_callback = cancel_callback;
    if (!_this->_myself._node_seq_name.empty())
        _this->remove_my_locknode(_this->_lock_dir + "/" + _this->_myself._node_seq_name,
                                  DONT_IGNORE_CALLBACK,
                                  REMOVE_FOR_CANCEL);
    else {
        _this->remove_lock();
        cancel_callback->enqueue_with(
            ERR_OK, _this->_owner._node_value, _this->_owner._sequence_id);
        _this->clear();
    }
}

/*static*/
void lock_struct::unlock(lock_struct_ptr _this, error_code_future_ptr unlock_callback)
{
    _this->_checker.only_one_thread_access();
    if (_this->_state != lock_state::locked && _this->_state != lock_state::unlocking) {
        LOG_INFO("lock({}) myself({}) seqid({}) state({}), just return",
                 _this->_lock_id,
                 _this->_myself._node_value,
                 _this->_owner._sequence_id,
                 string_state(_this->_state));
        unlock_callback->enqueue_with(ERR_INVALID_PARAMETERS);
        return;
    }

    _this->_state = lock_state::unlocking;
    _this->_unlock_callback = unlock_callback;
    _this->remove_my_locknode(_this->_lock_dir + "/" + _this->_myself._node_seq_name,
                              DONT_IGNORE_CALLBACK,
                              REMOVE_FOR_UNLOCK);
}

/*static*/
void lock_struct::lock_expired(lock_struct_ptr _this)
{
    _this->_checker.only_one_thread_access();
    _this->on_expire();
}
}
}
