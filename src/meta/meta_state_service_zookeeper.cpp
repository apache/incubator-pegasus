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
 *     meta state service implemented with zookeeper
 *
 * Revision history:
 *     2015-12-04, @shengofsun (sunweijie@xiaomi.com)
 */
#include "runtime/task/async_calls.h"
#include "common/replication.codes.h"

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

#include "meta_state_service_zookeeper.h"
#include "utils/flags.h"
#include "zookeeper/zookeeper_session_mgr.h"
#include "zookeeper/zookeeper_session.h"
#include "zookeeper/zookeeper_error.h"

namespace dsn {
namespace dist {

DSN_DECLARE_int32(timeout_ms);

class zoo_transaction : public meta_state_service::transaction_entries
{
public:
    zoo_transaction(unsigned int capacity);
    virtual ~zoo_transaction() override {}
    virtual error_code create_node(const std::string &name, const blob &value = blob()) override;
    virtual error_code set_data(const std::string &name, const blob &value = blob()) override;
    virtual error_code delete_node(const std::string &name) override;
    virtual error_code get_result(unsigned int entry_index) override;

    std::shared_ptr<zookeeper_session::zoo_atomic_packet> packet() { return _pkt; }
private:
    std::shared_ptr<zookeeper_session::zoo_atomic_packet> _pkt;
};

zoo_transaction::zoo_transaction(unsigned int capacity)
{
    _pkt.reset(new zookeeper_session::zoo_atomic_packet(capacity));
}

error_code zoo_transaction::create_node(const std::string &path, const blob &value)
{
    if (_pkt->_count >= _pkt->_capacity)
        return ERR_ARRAY_INDEX_OUT_OF_RANGE;

    unsigned int &offset = _pkt->_count;
    std::string &p = (_pkt->_paths)[offset];
    blob &b = (_pkt->_datas)[offset];

    p = path;
    b = value;

    zoo_op_t &op = _pkt->_ops[offset];
    op.type = ZOO_CREATE_OP;
    op.create_op.path = p.c_str();
    op.create_op.flags = 0;
    op.create_op.acl = &ZOO_OPEN_ACL_UNSAFE;
    op.create_op.data = b.data();
    op.create_op.datalen = b.length();

    /* output path is either same with path(for non-sequencial node)
     * or 10 bytes more than the path(for sequencial node) */
    int buffer_length = path.size() + 20;

    op.create_op.buf = _pkt->alloc_buffer(buffer_length);
    op.create_op.buflen = buffer_length;

    ++offset;
    return ERR_OK;
}

error_code zoo_transaction::delete_node(const std::string &path)
{
    if (_pkt->_count >= _pkt->_capacity)
        return ERR_ARRAY_INDEX_OUT_OF_RANGE;
    unsigned int &offset = _pkt->_count;
    std::string &p = (_pkt->_paths)[offset];

    p = path;

    zoo_op_t &op = _pkt->_ops[offset];
    op.type = ZOO_DELETE_OP;
    op.delete_op.path = p.c_str();
    op.delete_op.version = -1;

    ++offset;
    return ERR_OK;
}

error_code zoo_transaction::set_data(const std::string &name, const blob &value)
{
    if (_pkt->_count >= _pkt->_capacity)
        return ERR_ARRAY_INDEX_OUT_OF_RANGE;
    unsigned int &offset = _pkt->_count;
    std::string &p = (_pkt->_paths)[offset];
    blob &b = (_pkt->_datas[offset]);
    p = name;
    b = value;

    zoo_op_t &op = _pkt->_ops[offset];
    op.type = ZOO_SETDATA_OP;
    op.set_op.path = p.c_str();
    op.set_op.data = value.data();
    op.set_op.datalen = value.length();
    op.set_op.version = -1;
    op.set_op.stat = (struct Stat *)_pkt->alloc_buffer(sizeof(struct Stat));

    ++offset;
    return ERR_OK;
}

error_code zoo_transaction::get_result(unsigned int entry_index)
{
    if (entry_index >= _pkt->_count)
        return ERR_ARRAY_INDEX_OUT_OF_RANGE;
    return from_zerror(_pkt->_results[entry_index].err);
}

meta_state_service_zookeeper::meta_state_service_zookeeper() : ref_counter() { _first_call = true; }

meta_state_service_zookeeper::~meta_state_service_zookeeper()
{
    _tracker.wait_outstanding_tasks();
    if (_session) {
        _session->detach(this);
        _session = nullptr;
    }
}

error_code meta_state_service_zookeeper::initialize(const std::vector<std::string> &)
{
    _session =
        zookeeper_session_mgr::instance().get_session(service_app::current_service_app_info());
    _zoo_state = _session->attach(this,
                                  std::bind(&meta_state_service_zookeeper::on_zoo_session_evt,
                                            ref_this(this),
                                            std::placeholders::_1));
    if (_zoo_state != ZOO_CONNECTED_STATE) {
        _notifier.wait_for(FLAGS_timeout_ms);
        if (_zoo_state != ZOO_CONNECTED_STATE)
            return ERR_TIMEOUT;
    }

    LOG_INFO("init meta_state_service_zookeeper succeed");

    // Notice: this reference is released in finalize
    add_ref();
    return ERR_OK;
}

error_code meta_state_service_zookeeper::finalize()
{
    release_ref();
    return ERR_OK;
}

std::shared_ptr<meta_state_service::transaction_entries>
meta_state_service_zookeeper::new_transaction_entries(unsigned int capacity)
{
    std::shared_ptr<zoo_transaction> t(new zoo_transaction(capacity));
    return t;
}

#define VISIT_INIT(tsk, op_type, node)                                                             \
    zookeeper_session::zoo_opcontext *op = zookeeper_session::create_context();                    \
    zookeeper_session::zoo_input *input = &op->_input;                                             \
    op->_callback_function = std::bind(&meta_state_service_zookeeper::visit_zookeeper_internal,    \
                                       ref_this(this),                                             \
                                       tsk,                                                        \
                                       std::placeholders::_1);                                     \
    op->_optype = op_type;                                                                         \
    input->_path = node;

task_ptr meta_state_service_zookeeper::create_node(const std::string &node,
                                                   task_code cb_code,
                                                   const err_callback &cb_create,
                                                   const blob &value,
                                                   dsn::task_tracker *tracker)
{
    error_code_future_ptr tsk(new error_code_future(cb_code, cb_create, 0));
    tsk->set_tracker(tracker);
    LOG_DEBUG("call create, node({})", node);
    VISIT_INIT(tsk, zookeeper_session::ZOO_OPERATION::ZOO_CREATE, node);
    input->_value = value;
    input->_flags = 0;

    _session->visit(op);
    return tsk;
}

task_ptr meta_state_service_zookeeper::submit_transaction(
    const std::shared_ptr<transaction_entries> &entries,
    task_code cb_code,
    const err_callback &cb_transaction,
    dsn::task_tracker *tracker)
{
    error_code_future_ptr tsk(new error_code_future(cb_code, cb_transaction, 0));
    tsk->set_tracker(tracker);
    LOG_DEBUG("call submit batch");
    zookeeper_session::zoo_opcontext *op = zookeeper_session::create_context();
    zookeeper_session::zoo_input *input = &op->_input;
    op->_callback_function = std::bind(&meta_state_service_zookeeper::visit_zookeeper_internal,
                                       ref_this(this),
                                       tsk,
                                       std::placeholders::_1);
    op->_optype = zookeeper_session::ZOO_OPERATION::ZOO_TRANSACTION;

    zoo_transaction *t = dynamic_cast<zoo_transaction *>(entries.get());
    input->_pkt = t->packet();

    _session->visit(op);
    return tsk;
}

task_ptr meta_state_service_zookeeper::delete_empty_node(const std::string &node,
                                                         task_code cb_code,
                                                         const err_callback &cb_delete,
                                                         dsn::task_tracker *tracker)
{
    error_code_future_ptr tsk(new error_code_future(cb_code, cb_delete, 0));
    tsk->set_tracker(tracker);
    LOG_DEBUG("call delete, node({})", node);
    VISIT_INIT(tsk, zookeeper_session::ZOO_OPERATION::ZOO_DELETE, node);
    _session->visit(op);
    return tsk;
}

task_ptr meta_state_service_zookeeper::delete_node(const std::string &node,
                                                   bool recursively_delete,
                                                   task_code cb_code,
                                                   const err_callback &cb_delete,
                                                   dsn::task_tracker *tracker)
{
    error_code_future_ptr tsk(new error_code_future(cb_code, cb_delete, 0));
    tsk->set_tracker(tracker);
    err_stringv_callback after_get_children = [node, recursively_delete, cb_code, tsk, this](
        error_code err, const std::vector<std::string> &children) {
        if (ERR_OK != err)
            tsk->enqueue_with(err);
        else if (children.empty())
            delete_empty_node(
                node, cb_code, [tsk](error_code err) { tsk->enqueue_with(err); }, &_tracker);
        else if (!recursively_delete)
            tsk->enqueue_with(ERR_INVALID_PARAMETERS);
        else {
            std::atomic_int *child_count = new std::atomic_int();
            std::atomic_int *error_count = new std::atomic_int();

            child_count->store((int)children.size());
            error_count->store(0);

            for (auto &child : children) {
                delete_node(node + "/" + child,
                            true,
                            cb_code,
                            [=](error_code err) {
                                if (ERR_OK != err)
                                    ++(*error_count);
                                int result = --(*child_count);
                                if (0 == result) {
                                    if (0 == *error_count)
                                        delete_empty_node(
                                            node,
                                            cb_code,
                                            [tsk](error_code err) { tsk->enqueue_with(err); },
                                            &_tracker);
                                    else
                                        tsk->enqueue_with(ERR_FILE_OPERATION_FAILED);
                                    delete child_count;
                                    delete error_count;
                                }
                            },
                            &_tracker);
            }
        }
    };

    get_children(node, cb_code, after_get_children, &_tracker);
    return tsk;
}

task_ptr meta_state_service_zookeeper::get_data(const std::string &node,
                                                task_code cb_code,
                                                const err_value_callback &cb_get_data,
                                                dsn::task_tracker *tracker)
{
    err_value_future_ptr tsk(new err_value_future(cb_code, cb_get_data, 0));
    tsk->set_tracker(tracker);
    LOG_DEBUG("call get, node({})", node);
    VISIT_INIT(tsk, zookeeper_session::ZOO_OPERATION::ZOO_GET, node);
    input->_is_set_watch = 0;
    _session->visit(op);
    return tsk;
}

task_ptr meta_state_service_zookeeper::set_data(const std::string &node,
                                                const blob &value,
                                                task_code cb_code,
                                                const err_callback &cb_set_data,
                                                dsn::task_tracker *tracker)
{
    error_code_future_ptr tsk(new error_code_future(cb_code, cb_set_data, 0));
    tsk->set_tracker(tracker);
    LOG_DEBUG("call set, node({})", node);
    VISIT_INIT(tsk, zookeeper_session::ZOO_OPERATION::ZOO_SET, node);

    input->_value = value;
    _session->visit(op);
    return tsk;
}

task_ptr meta_state_service_zookeeper::node_exist(const std::string &node,
                                                  task_code cb_code,
                                                  const err_callback &cb_exist,
                                                  dsn::task_tracker *tracker)
{
    error_code_future_ptr tsk(new error_code_future(cb_code, cb_exist, 0));
    tsk->set_tracker(tracker);
    LOG_DEBUG("call node_exist, node({})", node);
    VISIT_INIT(tsk, zookeeper_session::ZOO_OPERATION::ZOO_EXISTS, node);
    input->_is_set_watch = 0;
    _session->visit(op);
    return tsk;
}

task_ptr meta_state_service_zookeeper::get_children(const std::string &node,
                                                    task_code cb_code,
                                                    const err_stringv_callback &cb_get_children,
                                                    dsn::task_tracker *tracker)
{
    err_stringv_future_ptr tsk(new err_stringv_future(cb_code, cb_get_children, 0));
    tsk->set_tracker(tracker);
    LOG_DEBUG("call get children, node({})", node);
    VISIT_INIT(tsk, zookeeper_session::ZOO_OPERATION::ZOO_GETCHILDREN, node);
    input->_is_set_watch = 0;
    _session->visit(op);
    return tsk;
}

/*static*/
/* this function runs in zookeeper do-completion thread */
void meta_state_service_zookeeper::on_zoo_session_evt(ref_this _this, int zoo_state)
{
    _this->_zoo_state = zoo_state;

    if (ZOO_CONNECTING_STATE == zoo_state) {
        // TODO: support the switch of zookeeper session
        LOG_WARNING("the zk session is reconnecting");
    } else if (_this->_first_call && ZOO_CONNECTED_STATE == zoo_state) {
        _this->_first_call = false;
        _this->_notifier.notify();
    } else {
        // ignore
    }
}
/*static*/
/*this function runs in zookeper do-completion thread*/
void meta_state_service_zookeeper::visit_zookeeper_internal(ref_this,
                                                            task_ptr callback,
                                                            void *result)
{
    zookeeper_session::zoo_opcontext *op =
        reinterpret_cast<zookeeper_session::zoo_opcontext *>(result);
    LOG_DEBUG(
        "visit zookeeper internal: ans({}), call type({})", zerror(op->_output.error), op->_optype);

    switch (op->_optype) {
    case zookeeper_session::ZOO_OPERATION::ZOO_CREATE:
    case zookeeper_session::ZOO_OPERATION::ZOO_DELETE:
    case zookeeper_session::ZOO_OPERATION::ZOO_EXISTS:
    case zookeeper_session::ZOO_OPERATION::ZOO_SET:
    case zookeeper_session::ZOO_OPERATION::ZOO_TRANSACTION: {
        auto tsk = reinterpret_cast<error_code_future *>(callback.get());
        tsk->enqueue_with(from_zerror(op->_output.error));
    } break;
    case zookeeper_session::ZOO_OPERATION::ZOO_GET: {
        auto tsk = reinterpret_cast<err_value_future *>(callback.get());
        blob data;
        if (ZOK == op->_output.error) {
            std::shared_ptr<char> buf(
                dsn::utils::make_shared_array<char>(op->_output.get_op.value_length));
            memcpy(buf.get(), op->_output.get_op.value, op->_output.get_op.value_length);
            data.assign(buf, 0, op->_output.get_op.value_length);
        }
        tsk->enqueue_with(from_zerror(op->_output.error), data);
    } break;
    case zookeeper_session::ZOO_OPERATION::ZOO_GETCHILDREN: {
        auto tsk = reinterpret_cast<err_stringv_future *>(callback.get());
        std::vector<std::string> result;
        if (ZOK == op->_output.error) {
            const String_vector *vec = op->_output.getchildren_op.strings;
            result.resize(vec->count);
            for (int i = 0; i != vec->count; ++i)
                result[i].assign(vec->data[i]);
        }
        tsk->enqueue_with(from_zerror(op->_output.error), std::move(result));
    } break;
    default:
        break;
    }
}
}
}
