#include <dsn/dist/replication/replication.codes.h>

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

#include <dsn/internal/task.h>

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

error_code meta_state_service_zookeeper::initialize()
{
    _session = zookeeper_session_mgr::instance().get_session( task::get_current_node() );
    _zoo_state = _session->attach(this, std::bind(&meta_state_service_zookeeper::on_zoo_session_evt,
                                                  ref_this(this),
                                                  std::placeholders::_1) );
    if (_zoo_state != ZOO_CONNECTED_STATE)
    {
        _notifier.wait_for( zookeeper_session_mgr::fast_instance().timeout() );
        if (_zoo_state != ZOO_CONNECTED_STATE)
            return ERR_TIMEOUT;
    }
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
    auto tsk = tasking::create_late_task(cb_code, cb_create, 0, tracker);

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
    auto tsk = tasking::create_late_task(cb_code, cb_delete, 0, tracker);
    VISIT_INIT(tsk, zookeeper_session::ZOO_OPERATION::ZOO_DELETE, node);
    _session->visit(op);
    return tsk;
}

static auto __bind_and_enqueue = [](safe_late_task<meta_state_service::err_callback>* tsk, error_code ec){
    tsk->bind_and_enqueue([ec](meta_state_service::err_callback& cb){
        return std::bind(cb, ec);
    });
};

task_ptr meta_state_service_zookeeper::delete_node(
    const std::string &node,
    bool recursively_delete,
    task_code cb_code,
    const err_callback &cb_delete,
    clientlet *tracker)
{
    auto tsk = tasking::create_late_task(cb_code, cb_delete, 0, tracker);
    err_stringv_callback after_get_children = [node, recursively_delete, cb_code, tsk, this](error_code err, const std::vector<std::string>& children) {
        if (ERR_OK != err)
            __bind_and_enqueue(tsk, err);
        else if (children.empty())
            delete_empty_node(node, cb_code, [tsk](error_code err){ __bind_and_enqueue(tsk, err); }, this);
        else if (!recursively_delete)
            __bind_and_enqueue(tsk, ERR_INVALID_PARAMETERS);
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
                            delete_empty_node(node, cb_code, [tsk](error_code err){ __bind_and_enqueue(tsk, err); }, this);
                        else
                            __bind_and_enqueue(tsk, ERR_FILE_OPERATION_FAILED);
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
    auto tsk = tasking::create_late_task(cb_code, cb_get_data, 0, tracker);
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
    auto tsk = tasking::create_late_task(cb_code, cb_set_data, 0, tracker);
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
    auto tsk = tasking::create_late_task(cb_code, cb_exist, 0, tracker);
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
    auto tsk = tasking::create_late_task(cb_code, cb_get_children, 0, tracker);
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
                if (ZOK == op->_output.error) {
                    std::shared_ptr<char> buf(new char[op->_output.get_op.value_length]);
                    memcpy(buf.get(), op->_output.get_op.value, op->_output.get_op.value_length);
                    blob data(buf, op->_output.get_op.value_length);
                    return std::bind(cb, ERR_OK, data);
                }
                else {
                    return std::bind(cb, from_zerror(op->_output.error), blob());
                }
            }
        );
    }
        break;
    case zookeeper_session::ZOO_OPERATION::ZOO_GETCHILDREN:
    {
        auto tsk = reinterpret_cast< safe_late_task<meta_state_service::err_stringv_callback>* >(callback.get());
        tsk->bind_and_enqueue([op](meta_state_service::err_stringv_callback& cb){
            if (ZOK == op->_output.error) {
                const String_vector* vec = op->_output.getchildren_op.strings;
                std::vector<std::string> result(vec->count);
                for (int i=0; i!=vec->count; ++i)
                    result[i].assign(vec->data[i]);
                return std::bind(cb, ERR_OK, result);
            }
            else
                return std::bind(cb, from_zerror(op->_output.error), std::vector<std::string>());
        });
    }
        break;
    default:
        break;
    }
}

}}
