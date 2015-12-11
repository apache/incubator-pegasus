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
#include <dsn/dist/replication/replication.codes.h>

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

#include "meta_state_service_zookeeper.h"
#include "zookeeper_session_mgr.h"
#include "zookeeper_session.h"
#include "zookeeper_error.h"

namespace dsn{ namespace dist {

meta_state_service_zookeeper::meta_state_service_zookeeper(): clientlet(), ref_counter()
{
    _first_call = true;
}

meta_state_service_zookeeper::~meta_state_service_zookeeper()
{
    if (_session)
    {
        _session->detach(this);
        _session = nullptr;
    }
}

error_code meta_state_service_zookeeper::initialize(const char* /*work_dir*/)
{
    dsn_app_info node;
    dsn_get_current_app_info(&node);

    _session = zookeeper_session_mgr::instance().get_session(&node);
    _zoo_state = _session->attach(this, std::bind(&meta_state_service_zookeeper::on_zoo_session_evt,
                                                  ref_this(this),
                                                  std::placeholders::_1) );
    if (_zoo_state != ZOO_CONNECTED_STATE)
    {
        _notifier.wait_for( zookeeper_session_mgr::fast_instance().timeout() );
        if (_zoo_state != ZOO_CONNECTED_STATE)
            return ERR_TIMEOUT;
    }

    // TODO: add_ref() here because we need add_ref/release_ref in callbacks, so this object should be
    // stored in ref_ptr to avoid memory leak.
    add_ref();
    return ERR_OK;
}

#define VISIT_INIT(tsk, op_type, node) \
    zookeeper_session::zoo_opcontext* op = zookeeper_session::create_context();\
    zookeeper_session::zoo_input* input = &op->_input;\
    op->_callback_function = std::bind(&meta_state_service_zookeeper::visit_zookeeper_internal, ref_this(this), tsk, std::placeholders::_1);\
    op->_optype = op_type;\
    input->_path = node;

task_ptr meta_state_service_zookeeper::create_node(
    const std::string &node,
    task_code cb_code,
    const err_callback &cb_create,
    const blob &value,
    clientlet *tracker)
{
    task_ptr tsk = tasking::create_late_task(cb_code, cb_create, 0, tracker);
    dinfo("call create, node(%s)", node.c_str());
    VISIT_INIT(tsk, zookeeper_session::ZOO_OPERATION::ZOO_CREATE, node);
    input->_value = value;
    input->_flags = 0;

    _session->visit(op);
    return tsk;
}

task_ptr meta_state_service_zookeeper::delete_empty_node(
    const std::string &node,
    task_code cb_code,
    const err_callback &cb_delete,
    clientlet* tracker)
{
    task_ptr tsk = tasking::create_late_task(cb_code, cb_delete, 0, tracker);
    dinfo("call delete, node(%s)", node.c_str());
    VISIT_INIT(tsk, zookeeper_session::ZOO_OPERATION::ZOO_DELETE, node);
    _session->visit(op);
    return tsk;
}

static void bind_and_enqueue(task_ptr tsk, error_code ec)
{
    auto t = reinterpret_cast< safe_late_task<meta_state_service::err_callback>* >(tsk.get());
    t->bind_and_enqueue([&ec](meta_state_service::err_callback& cb) {
        return std::bind(cb, ec);
    });
}

task_ptr meta_state_service_zookeeper::delete_node(
    const std::string &node,
    bool recursively_delete,
    task_code cb_code,
    const err_callback &cb_delete,
    clientlet *tracker)
{
    task_ptr tsk = tasking::create_late_task(cb_code, cb_delete, 0, tracker);
    err_stringv_callback after_get_children = [node, recursively_delete, cb_code, tsk, this](error_code err, const std::vector<std::string>& children) {
        if (ERR_OK != err)
            bind_and_enqueue(tsk, err);
        else if (children.empty())
            delete_empty_node(node, cb_code, [tsk](error_code err){ bind_and_enqueue(tsk, err); }, this);
        else if (!recursively_delete)
            bind_and_enqueue(tsk, ERR_INVALID_PARAMETERS);
        else {
            std::atomic_int* child_count = new std::atomic_int();
            std::atomic_int* error_count = new std::atomic_int();

            child_count->store( (int)children.size() );
            error_count->store(0);

            for (auto& child: children)
            {
                delete_node(node+"/"+child, true, cb_code, [=](error_code err){
                    if (ERR_OK != err)
                        ++(*error_count);
                    int result = --(*child_count);
                    if (0 == result) {
                        if (0 == *error_count)
                            delete_empty_node(node, cb_code, [tsk](error_code err){ bind_and_enqueue(tsk, err); }, this);
                        else
                            bind_and_enqueue(tsk, ERR_FILE_OPERATION_FAILED);
                        delete child_count;
                        delete error_count;
                    }
                }, this);
            }
        }
    };

    get_children(node, cb_code, after_get_children, this);
    return tsk;
}

task_ptr meta_state_service_zookeeper::get_data(
    const std::string &node,
    task_code cb_code,
    const err_value_callback &cb_get_data,
    clientlet *tracker)
{
    task_ptr tsk = tasking::create_late_task(cb_code, cb_get_data, 0, tracker);
    dinfo("call get, node(%s)", node.c_str());
    VISIT_INIT(tsk, zookeeper_session::ZOO_OPERATION::ZOO_GET, node);
    input->_is_set_watch = 0;
    _session->visit(op);
    return tsk;
}

task_ptr meta_state_service_zookeeper::set_data(
    const std::string &node,
    const blob &value,
    task_code cb_code,
    const err_callback &cb_set_data,
    clientlet *tracker)
{
    task_ptr tsk = tasking::create_late_task(cb_code, cb_set_data, 0, tracker);
    dinfo("call set, node(%s)", node.c_str());
    VISIT_INIT(tsk, zookeeper_session::ZOO_OPERATION::ZOO_SET, node);

    input->_value = value;
    _session->visit(op);
    return tsk;
}

task_ptr meta_state_service_zookeeper::node_exist(
    const std::string &node,
    task_code cb_code,
    const err_callback &cb_exist,
    clientlet *tracker)
{
    task_ptr tsk = tasking::create_late_task(cb_code, cb_exist, 0, tracker);
    dinfo("call node_exist, node(%s)", node.c_str());
    VISIT_INIT(tsk, zookeeper_session::ZOO_OPERATION::ZOO_EXISTS, node);
    input->_is_set_watch = 0;
    _session->visit(op);
    return tsk;
}

task_ptr meta_state_service_zookeeper::get_children(
    const std::string &node,
    task_code cb_code,
    const err_stringv_callback &cb_get_children,
    clientlet *tracker)
{
    task_ptr tsk = tasking::create_late_task(cb_code, cb_get_children, 0, tracker);
    dinfo("call get children, node(%s)", node.c_str());
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
        //TODO: support the switch of zookeeper session
        dassert(false, "");
    }
    else if (_this->_first_call && ZOO_CONNECTED_STATE==zoo_state)
    {
        _this->_first_call = false;
        _this->_notifier.notify();
    }
    else
    {
        //ignore
    }
}
/*static*/
/*this function runs in zookeper do-completion thread*/
void meta_state_service_zookeeper::visit_zookeeper_internal(
    ref_this,
    task_ptr callback,
    void* result)
{
    zookeeper_session::zoo_opcontext* op = reinterpret_cast<zookeeper_session::zoo_opcontext*>(result);
    dinfo("visit zookeeper internal: ans(%d), call type(%d)", op->_output.error, op->_optype);

    switch (op->_optype)
    {
    case zookeeper_session::ZOO_OPERATION::ZOO_CREATE:
    case zookeeper_session::ZOO_OPERATION::ZOO_DELETE:
    case zookeeper_session::ZOO_OPERATION::ZOO_EXISTS:
    case zookeeper_session::ZOO_OPERATION::ZOO_SET:
    {
        auto tsk = reinterpret_cast< safe_late_task<meta_state_service::err_callback>* >(callback.get());
        tsk->bind_and_enqueue([op](meta_state_service::err_callback& cb){
            return std::bind(cb, from_zerror(op->_output.error));
        });
    }
        break;
    case zookeeper_session::ZOO_OPERATION::ZOO_GET:
    {
        auto tsk = reinterpret_cast< safe_late_task<meta_state_service::err_value_callback>* >(callback.get());
        tsk->bind_and_enqueue(
            [op](meta_state_service::err_value_callback& cb){
                blob data;
                if (ZOK == op->_output.error) {
                    std::shared_ptr<char> buf(new char[op->_output.get_op.value_length]);
                    memcpy(buf.get(), op->_output.get_op.value, op->_output.get_op.value_length);
                    data.assign(buf, 0, op->_output.get_op.value_length);
                }
                return std::bind(cb, from_zerror(op->_output.error), data);
            }
        );
    }
        break;
    case zookeeper_session::ZOO_OPERATION::ZOO_GETCHILDREN:
    {
        auto tsk = reinterpret_cast< safe_late_task<meta_state_service::err_stringv_callback>* >(callback.get());
        tsk->bind_and_enqueue([op](meta_state_service::err_stringv_callback& cb){
            std::vector<std::string> result;
            if (ZOK == op->_output.error) {
                const String_vector* vec = op->_output.getchildren_op.strings;
                result.resize(vec->count);
                for (int i=0; i!=vec->count; ++i)
                    result[i].assign(vec->data[i]);
            }
            return std::bind(cb, from_zerror(op->_output.error), result);
        });
    }
        break;
    default:
        break;
    }
}

}}
