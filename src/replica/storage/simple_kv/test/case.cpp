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
 *     Replication testing framework.
 *
 * Revision history:
 *     Nov., 2015, @qinzuoyan (Zuoyan Qin), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include "case.h"
#include "simple_kv.server.impl.h"
#include "checker.h"

#include <fmt/printf.h>

#include "runtime/task/task.h"
#include "runtime/rpc/rpc_message.h"
#include "replica/replica_stub.h"
#include "runtime/service_engine.h"
#include "meta/server_load_balancer.h"

#include <iostream>
#include <string>
#include <cstdio>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>

namespace dsn {
namespace replication {
namespace test {

static bool
parse_kv_map(int line_no, const std::string &str, std::map<std::string, std::string> &kv_map)
{
    kv_map.clear();
    std::vector<std::string> splits;
    dsn::utils::split_args(str.c_str(), splits, ',');
    for (std::string &i : splits) {
        if (i.empty())
            continue;
        size_t pos = i.find('=');
        if (pos == std::string::npos) {
            std::cerr << "bad line: line_no=" << line_no << ": invalid key-value pair: " << i
                      << std::endl;
            return false;
        }
        std::string key = i.substr(0, pos);
        std::string value = i.substr(pos + 1);
        if (kv_map.find(key) != kv_map.end()) {
            std::cerr << "bad line: line_no=" << line_no << ": duplicate key " << key << std::endl;
            return false;
        }
        kv_map[key] = value;
    }
    return true;
}

std::string set_case_line::to_string() const
{
    std::ostringstream oss;
    oss << name() << ":";
    int count = 0;
    if (_null_loop_set) {
        if (count > 0)
            oss << ",";
        oss << "null_loop=" << _null_loop;
        count++;
    }
    if (_lb_for_test_set) {
        if (count > 0)
            oss << ",";
        oss << "load_balance_for_test=" << _lb_for_test;
        count++;
    }
    if (_disable_lb_set) {
        if (count > 0)
            oss << ",";
        oss << "disable_load_balance=" << _disable_lb;
        count++;
    }
    if (_close_replica_stub_set) {
        if (count > 0)
            oss << ",";
        oss << "close_replica_stub_on_exit=" << _close_replica_stub;
        count++;
    }
    if (_not_exit_on_log_failure_set) {
        if (count > 0)
            oss << ",";
        oss << "not_exit_on_log_failure=" << _not_exit_on_log_failure;
        count++;
    }
    if (_simple_kv_open_fail_set) {
        if (count > 0)
            oss << ",";
        oss << "simple_kv_open_fail=" << _simple_kv_open_fail;
        count++;
    }
    if (_simple_kv_close_fail_set) {
        if (count > 0)
            oss << ",";
        oss << "simple_kv_close_fail=" << _simple_kv_close_fail;
        count++;
    }
    if (_simple_kv_get_checkpoint_fail_set) {
        if (count > 0)
            oss << ",";
        oss << "simple_kv_get_checkpoint_fail=" << _simple_kv_get_checkpoint_fail;
        count++;
    }
    if (_simple_kv_apply_checkpoint_fail_set) {
        if (count > 0)
            oss << ",";
        oss << "simple_kv_apply_checkpoint_fail=" << _simple_kv_apply_checkpoint_fail;
        count++;
    }
    return oss.str();
}

bool set_case_line::parse(const std::string &params)
{
    if (params.empty())
        return false;
    std::map<std::string, std::string> kv_map;
    if (!parse_kv_map(line_no(), params, kv_map)) {
        return false;
    }
    _null_loop_set = false;
    _lb_for_test_set = false;
    _disable_lb_set = false;
    _close_replica_stub_set = false;
    _not_exit_on_log_failure_set = false;
    _simple_kv_open_fail_set = false;
    _simple_kv_close_fail_set = false;
    _simple_kv_get_checkpoint_fail_set = false;
    _simple_kv_apply_checkpoint_fail_set = false;
    for (auto &kv : kv_map) {
        const std::string &k = kv.first;
        const std::string &v = kv.second;
        if (k == "null_loop") {
            _null_loop = boost::lexical_cast<int>(v);
            _null_loop_set = true;
        } else if (k == "load_balance_for_test") {
            _lb_for_test = boost::lexical_cast<bool>(v);
            _lb_for_test_set = true;
        } else if (k == "disable_load_balance") {
            _disable_lb = boost::lexical_cast<bool>(v);
            _disable_lb_set = true;
        } else if (k == "close_replica_stub_on_exit") {
            _close_replica_stub = boost::lexical_cast<bool>(v);
            _close_replica_stub_set = true;
        } else if (k == "not_exit_on_log_failure") {
            _not_exit_on_log_failure = boost::lexical_cast<bool>(v);
            _not_exit_on_log_failure_set = true;
        } else if (k == "simple_kv_open_fail") {
            _simple_kv_open_fail = boost::lexical_cast<bool>(v);
            _simple_kv_open_fail_set = true;
        } else if (k == "simple_kv_close_fail") {
            _simple_kv_close_fail = boost::lexical_cast<bool>(v);
            _simple_kv_close_fail_set = true;
        } else if (k == "simple_kv_get_checkpoint_fail") {
            _simple_kv_get_checkpoint_fail = boost::lexical_cast<bool>(v);
            _simple_kv_get_checkpoint_fail_set = true;
        } else if (k == "simple_kv_apply_checkpoint_fail") {
            _simple_kv_apply_checkpoint_fail = boost::lexical_cast<bool>(v);
            _simple_kv_apply_checkpoint_fail_set = true;
        } else {
            std::cerr << "bad line: line_no=" << line_no() << ": unknown key " << k << std::endl;
            return false;
        }
    }
    return true;
}

void set_case_line::apply_set() const
{
    if (_null_loop_set) {
        test_case::s_null_loop = _null_loop;
    }
    if (_lb_for_test_set) {
        // pass
    }
    if (_disable_lb_set) {
        test_checker::instance().control_balancer(_disable_lb);
    }
    if (_close_replica_stub_set) {
        test_case::s_close_replica_stub_on_exit = _close_replica_stub;
    }
    if (_not_exit_on_log_failure_set) {
        replica_stub::s_not_exit_on_log_failure = _not_exit_on_log_failure;
    }
    if (_simple_kv_open_fail_set) {
        simple_kv_service_impl::s_simple_kv_open_fail = _simple_kv_open_fail;
    }
    if (_simple_kv_close_fail_set) {
        simple_kv_service_impl::s_simple_kv_close_fail = _simple_kv_close_fail;
    }
    if (_simple_kv_get_checkpoint_fail_set) {
        simple_kv_service_impl::s_simple_kv_get_checkpoint_fail = _simple_kv_get_checkpoint_fail;
    }
    if (_simple_kv_apply_checkpoint_fail_set) {
        simple_kv_service_impl::s_simple_kv_apply_checkpoint_fail =
            _simple_kv_apply_checkpoint_fail;
    }
}

std::string skip_case_line::to_string() const
{
    std::ostringstream oss;
    oss << name() << ":" << _count;
    return oss.str();
}

bool skip_case_line::parse(const std::string &params)
{
    if (params.empty())
        return false;
    _count = boost::lexical_cast<int>(params);
    if (_count <= 0) {
        std::cerr << "bad line: line_no=" << line_no() << ": skip count should > 0" << std::endl;
        return false;
    }
    _skipped = 0;
    return true;
}

std::string exit_case_line::to_string() const
{
    std::ostringstream oss;
    oss << name() << ":";
    return oss.str();
}

bool exit_case_line::parse(const std::string &params) { return true; }

std::string state_case_line::to_string() const
{
    std::ostringstream oss;
    oss << name() << ":" << _state.to_string();
    return oss.str();
}

bool state_case_line::parse(const std::string &params) { return _state.from_string(params); }

bool state_case_line::check_state(const state_snapshot &cur_state, bool &forward)
{
    if (cur_state == _state) {
        forward = true;
        return true;
    }
    if (cur_state < _state) {
        forward = false;
        return true;
    }
    return false;
}

std::string config_case_line::to_string() const
{
    std::ostringstream oss;
    oss << name() << ":" << _config.to_string();
    return oss.str();
}

bool config_case_line::parse(const std::string &params) { return _config.from_string(params); }

bool config_case_line::check_config(const parti_config &cur_config, bool &forward)
{
    if (cur_config == _config) {
        forward = true;
        return true;
    }
    if (cur_config < _config) {
        forward = false;
        return true;
    }
    return false;
}

struct event_type_helper
{
    std::map<event_type, std::string> type_to_name;
    std::map<std::string, event_type> name_to_type;
    std::set<event_type> support_inject_fault;
    event_type_helper()
    {
        add(event_type::task_enqueue, "on_task_enqueue", false);
        add(event_type::task_begin, "on_task_begin", false);
        add(event_type::task_end, "on_task_end", false);
        add(event_type::task_cancelled, "on_task_cancelled", false);
        add(event_type::aio_call, "on_aio_call", true);
        add(event_type::aio_enqueue, "on_aio_enqueue", false);
        add(event_type::rpc_call, "on_rpc_call", true);
        add(event_type::rpc_request_enqueue, "on_rpc_request_enqueue", true);
        add(event_type::rpc_reply, "on_rpc_reply", true);
        add(event_type::rpc_response_enqueue, "on_rpc_response_enqueue", true);
    }
    void add(event_type type, const std::string &name, bool is_support_inject_fault)
    {
        type_to_name[type] = name;
        name_to_type[name] = type;
        if (is_support_inject_fault)
            support_inject_fault.insert(type);
    }
    const char *get(event_type type)
    {
        auto it = type_to_name.find(type);
        CHECK(it != type_to_name.end(), "");
        return it->second.c_str();
    }
    bool get(const std::string &name, event_type &type)
    {
        auto it = name_to_type.find(name);
        if (it == name_to_type.end())
            return false;
        type = it->second;
        return true;
    }
    bool is_support_inject_fault(event_type type)
    {
        return support_inject_fault.find(type) != support_inject_fault.end();
    }
};
static event_type_helper s_event_type_helper;

const char *event_type_to_string(event_type type) { return s_event_type_helper.get(type); }

bool event_type_from_string(const std::string &name, event_type &type)
{
    return s_event_type_helper.get(name, type);
}

bool event_type_support_inject_fault(event_type type)
{
    return s_event_type_helper.is_support_inject_fault(type);
}

std::string event::to_string() const
{
    std::ostringstream oss;
    oss << event_type_to_string(type()) << ":";
    internal_to_string(oss);
    std::string str = oss.str();
    if (str[str.size() - 1] == ',')
        str.resize(str.size() - 1);
    return str;
}

event *event::parse(int line_no, const std::string &params)
{
    size_t pos = params.find(':');
    if (pos == std::string::npos) {
        std::cerr << "bad line: line_no=" << line_no << std::endl;
        return nullptr;
    }
    std::string type_name = params.substr(0, pos);
    event_type type;
    if (!event_type_from_string(type_name, type)) {
        std::cerr << "bad line: line_no=" << line_no << ": invalid event type " << type_name
                  << std::endl;
        return nullptr;
    }
    std::map<std::string, std::string> kv_map;
    if (!parse_kv_map(line_no, params.substr(pos + 1), kv_map)) {
        return nullptr;
    }
    event *e = nullptr;
    switch (type) {
    case event_type::task_enqueue:
        e = new event_on_task_enqueue();
        break;
    case event_type::task_begin:
        e = new event_on_task_begin();
        break;
    case event_type::task_end:
        e = new event_on_task_end();
        break;
    case event_type::task_cancelled:
        e = new event_on_task_cancelled();
        break;
    case event_type::aio_call:
        e = new event_on_aio_call();
        break;
    case event_type::aio_enqueue:
        e = new event_on_aio_enqueue();
        break;
    case event_type::rpc_call:
        e = new event_on_rpc_call();
        break;
    case event_type::rpc_request_enqueue:
        e = new event_on_rpc_request_enqueue();
        break;
    case event_type::rpc_reply:
        e = new event_on_rpc_reply();
        break;
    case event_type::rpc_response_enqueue:
        e = new event_on_rpc_response_enqueue();
        break;
    default:
        CHECK(false, "");
    }
    if (!e->internal_parse(kv_map)) {
        std::cerr << "bad line: line_no=" << line_no
                  << ": invalid event params: " << params.substr(pos + 1) << std::endl;
        delete e;
        return nullptr;
    }
    return e;
}

void event_on_task::internal_to_string(std::ostream &oss) const
{
    if (!_node.empty())
        oss << "node=" << _node << ",";
    if (!_task_id.empty())
        oss << "task_id=" << _task_id << ",";
    if (!_task_code.empty())
        oss << "task_code=" << _task_code << ",";
    if (!_delay.empty())
        oss << "delay=" << _delay << ",";
}

bool event_on_task::internal_parse(const std::map<std::string, std::string> &kv_map)
{
    std::map<std::string, std::string>::const_iterator it;
    if ((it = kv_map.find("task_id")) != kv_map.end())
        _task_id = it->second;
    if ((it = kv_map.find("node")) != kv_map.end())
        _node = it->second;
    if ((it = kv_map.find("task_code")) != kv_map.end())
        _task_code = boost::algorithm::to_upper_copy(it->second);
    if ((it = kv_map.find("delay")) != kv_map.end())
        _delay = it->second;
    return true;
}

bool event_on_task::check_satisfied(const event *ev) const
{
    if (type() != ev->type())
        return false;
    const event_on_task *e = (const event_on_task *)ev;
    if (!_task_id.empty() && _task_id != e->_task_id)
        return false;
    if (!_node.empty() && _node != e->_node)
        return false;
    if (!_task_code.empty() && _task_code != e->_task_code)
        return false;
    if (!_delay.empty() && _delay != e->_delay)
        return false;
    return true;
}

void event_on_task::init(task *tsk)
{
    _task = tsk;
    if (tsk != nullptr) {
        char buf[100];
        sprintf(buf, "%016" PRIx64, tsk->id());
        _task_id = buf;
        _node = tsk->node()->full_name();
        _task_code = tsk->code().to_string();
        _delay = boost::lexical_cast<std::string>(tsk->delay_milliseconds());
    }
}

void event_on_rpc::internal_to_string(std::ostream &oss) const
{
    event_on_task::internal_to_string(oss);
    if (!_trace_id.empty())
        oss << "trace_id=" << _trace_id << ",";
    if (!_rpc_name.empty())
        oss << "rpc_name=" << _rpc_name << ",";
    if (!_from.empty())
        oss << "from=" << _from << ",";
    if (!_to.empty())
        oss << "to=" << _to << ",";
}

bool event_on_rpc::internal_parse(const std::map<std::string, std::string> &kv_map)
{
    if (!event_on_task::internal_parse(kv_map))
        return false;
    std::map<std::string, std::string>::const_iterator it;
    if ((it = kv_map.find("trace_id")) != kv_map.end())
        _trace_id = it->second;
    if ((it = kv_map.find("rpc_name")) != kv_map.end())
        _rpc_name = boost::algorithm::to_upper_copy(it->second);
    if ((it = kv_map.find("from")) != kv_map.end())
        _from = it->second;
    if ((it = kv_map.find("to")) != kv_map.end())
        _to = it->second;
    return true;
}

bool event_on_rpc::check_satisfied(const event *ev) const
{
    if (!event_on_task::check_satisfied(ev))
        return false;
    const event_on_rpc *e = (const event_on_rpc *)ev;
    if (!_trace_id.empty() && _trace_id != e->_trace_id)
        return false;
    if (!_rpc_name.empty() && _rpc_name != e->_rpc_name)
        return false;
    if (!_from.empty() && _from != e->_from)
        return false;
    if (!_to.empty() && _to != e->_to)
        return false;
    return true;
}

void event_on_rpc::init(message_ex *msg, task *tsk)
{
    event_on_task::init(tsk);
    if (msg != nullptr) {
        _trace_id = fmt::sprintf("%016llx", msg->header->trace_id);
        _rpc_name = msg->header->rpc_name;
        _from = address_to_node(msg->header->from_address);
        _to = address_to_node(msg->to_address);
    }
}

void event_on_rpc_request_enqueue::init(rpc_request_task *tsk)
{
    event_on_rpc::init(tsk->get_request(), tsk);
}

void event_on_rpc_response_enqueue::internal_to_string(std::ostream &oss) const
{
    event_on_rpc::internal_to_string(oss);
    if (!_err.empty())
        oss << "err=" << _err << ",";
}

bool event_on_rpc_response_enqueue::internal_parse(const std::map<std::string, std::string> &kv_map)
{
    if (!event_on_rpc::internal_parse(kv_map))
        return false;
    std::map<std::string, std::string>::const_iterator it;
    if ((it = kv_map.find("err")) != kv_map.end())
        _err = it->second;
    return true;
}

bool event_on_rpc_response_enqueue::check_satisfied(const event *ev) const
{
    if (!event_on_rpc::check_satisfied(ev))
        return false;
    event_on_rpc_response_enqueue *e = (event_on_rpc_response_enqueue *)ev;
    if (!_err.empty() && _err != e->_err)
        return false;
    return true;
}

void event_on_rpc_response_enqueue::init(rpc_response_task *tsk)
{
    event_on_rpc::init(tsk->get_request(), tsk); // use request here because response may be nullptr
    _rpc_name += "_ACK";
    _from.swap(_to);
    _err = tsk->error().to_string();
}

void event_on_aio::internal_to_string(std::ostream &oss) const
{
    event_on_task::internal_to_string(oss);
    if (!_type.empty())
        oss << "type=" << _type << ",";
    if (!_file_offset.empty())
        oss << "file_offset=" << _file_offset << ",";
    if (!_buffer_size.empty())
        oss << "buffer_size=" << _buffer_size << ",";
}

bool event_on_aio::internal_parse(const std::map<std::string, std::string> &kv_map)
{
    if (!event_on_task::internal_parse(kv_map))
        return false;
    std::map<std::string, std::string>::const_iterator it;
    if ((it = kv_map.find("type")) != kv_map.end())
        _type = boost::algorithm::to_upper_copy(it->second);
    if ((it = kv_map.find("file_offset")) != kv_map.end())
        _file_offset = it->second;
    if ((it = kv_map.find("buffer_size")) != kv_map.end())
        _buffer_size = it->second;
    return true;
}

bool event_on_aio::check_satisfied(const event *ev) const
{
    if (!event_on_task::check_satisfied(ev))
        return false;
    event_on_aio *e = (event_on_aio *)ev;
    if (!_type.empty() && _type != e->_type)
        return false;
    if (!_file_offset.empty() && _file_offset != e->_file_offset)
        return false;
    if (!_buffer_size.empty() && _buffer_size != e->_buffer_size)
        return false;
    return true;
}

void event_on_aio::init(aio_task *tsk)
{
    event_on_task::init(tsk);
    if (tsk->get_aio_context()->type == dsn::AIO_Invalid)
        return; // for flush task, the type is AIO_Invalid
    _type = (tsk->get_aio_context()->type == dsn::AIO_Read ? "READ" : "WRITE");
    _file_offset = boost::lexical_cast<std::string>(tsk->get_aio_context()->file_offset);
    _buffer_size = boost::lexical_cast<std::string>(tsk->get_aio_context()->buffer_size);
}

void event_on_aio_enqueue::internal_to_string(std::ostream &oss) const
{
    event_on_aio::internal_to_string(oss);
    if (!_err.empty())
        oss << "err=" << _err << ",";
    if (!_transferred_size.empty())
        oss << "transferred_size=" << _transferred_size << ",";
}

bool event_on_aio_enqueue::internal_parse(const std::map<std::string, std::string> &kv_map)
{
    if (!event_on_aio::internal_parse(kv_map))
        return false;
    std::map<std::string, std::string>::const_iterator it;
    if ((it = kv_map.find("err")) != kv_map.end())
        _err = boost::algorithm::to_upper_copy(it->second);
    if ((it = kv_map.find("transferred_size")) != kv_map.end())
        _transferred_size = it->second;
    return true;
}

bool event_on_aio_enqueue::check_satisfied(const event *ev) const
{
    if (!event_on_aio::check_satisfied(ev))
        return false;
    event_on_aio_enqueue *e = (event_on_aio_enqueue *)ev;
    if (!_err.empty() && _err != e->_err)
        return false;
    if (!_transferred_size.empty() && _transferred_size != e->_transferred_size)
        return false;
    return true;
}

void event_on_aio_enqueue::init(aio_task *tsk)
{
    event_on_aio::init(tsk);
    _err = tsk->error().to_string();
    _transferred_size = boost::lexical_cast<std::string>(tsk->get_transferred_size());
}

std::string event_case_line::to_string() const { return name() + ":" + _event_cond->to_string(); }

bool event_case_line::parse(const std::string &params)
{
    _event_cond = event::parse(line_no(), params);
    return _event_cond != nullptr;
}

bool event_case_line::check_satisfied(const event *ev) const
{
    return _event_cond->check_satisfied(ev);
}

bool inject_case_line::parse(const std::string &params)
{
    if (!event_case_line::parse(params))
        return false;
    if (!event_type_support_inject_fault(_event_cond->type())) {
        std::cerr << "bad line: line_no=" << line_no() << ": event type "
                  << event_type_to_string(_event_cond->type()) << " not support inject fault"
                  << std::endl;
        return false;
    }
    return true;
}

std::string modify_case_line::to_string() const
{
    std::ostringstream oss;
    oss << event_case_line::to_string() << ",modify_delay=" << _modify_delay;
    return oss.str();
}

bool modify_case_line::parse(const std::string &params)
{
    if (!event_case_line::parse(params))
        return false;
    size_t pos = params.find(':');
    CHECK(pos != std::string::npos, "");
    std::map<std::string, std::string> kv_map;
    CHECK(parse_kv_map(line_no(), params.substr(pos + 1), kv_map), "");
    std::map<std::string, std::string>::const_iterator it;
    if ((it = kv_map.find("modify_delay")) != kv_map.end())
        _modify_delay = it->second;
    return true;
}

void modify_case_line::modify(const event *ev)
{
    if (!_modify_delay.empty()) {
        const event_on_task *e = dynamic_cast<const event_on_task *>(ev);
        CHECK_NOTNULL(e, "");
        CHECK_NOTNULL(e->_task, "");
        e->_task->set_delay(boost::lexical_cast<int>(_modify_delay));
    }
}

std::string client_case_line::to_string() const
{
    std::ostringstream oss;
    oss << name() << ":" << type_name() << ":";
    switch (_type) {
    case begin_write: {
        oss << "id=" << _id << ",key=" << _key << ",value=" << _value << ",timeout=" << _timeout;
        break;
    }
    case begin_read: {
        oss << "id=" << _id << ",key=" << _key << ",timeout=" << _timeout;
        break;
    }
    case end_write: {
        oss << "id=" << _id << ",err=" << _err.to_string() << ",resp=" << _write_resp;
        break;
    }
    case end_read: {
        oss << "id=" << _id << ",err=" << _err.to_string() << ",resp=" << _read_resp;
        break;
    }
    case replica_config: {
        oss << "receiver=" << address_to_node(_config_receiver)
            << ",type=" << config_command_to_string(_config_type)
            << ",node=" << address_to_node(_config_node);
        break;
    }
    default:
        CHECK(false, "");
    }
    return oss.str();
}

bool client_case_line::parse(const std::string &params)
{
    size_t pos = params.find(':');
    if (pos == std::string::npos) {
        std::cerr << "bad line: line_no=" << line_no() << std::endl;
        return false;
    }
    std::string type_name = params.substr(0, pos);
    if (!parse_type_name(type_name)) {
        std::cerr << "bad line: line_no=" << line_no() << ": invalid client type " << type_name
                  << std::endl;
        return false;
    }
    std::map<std::string, std::string> kv_map;
    if (!parse_kv_map(line_no(), params.substr(pos + 1), kv_map)) {
        return false;
    }
    bool parse_ok = true;
    switch (_type) {
    case begin_write: {
        _id = boost::lexical_cast<int>(kv_map["id"]);
        _key = kv_map["key"];
        _value = kv_map["value"];
        _timeout = boost::lexical_cast<int>(kv_map["timeout"]);
        break;
    }
    case begin_read: {
        _id = boost::lexical_cast<int>(kv_map["id"]);
        _key = kv_map["key"];
        _timeout = boost::lexical_cast<int>(kv_map["timeout"]);
        break;
    }
    case end_write: {
        _id = boost::lexical_cast<int>(kv_map["id"]);
        _err = dsn::error_code::try_get(boost::algorithm::to_upper_copy(kv_map["err"]).c_str(),
                                        ERR_UNKNOWN);
        _write_resp = boost::lexical_cast<int>(kv_map["resp"]);
        if (_err == ERR_UNKNOWN)
            parse_ok = false;
        break;
    }
    case end_read: {
        _id = boost::lexical_cast<int>(kv_map["id"]);
        _err = dsn::error_code::try_get(boost::algorithm::to_upper_copy(kv_map["err"]).c_str(),
                                        ERR_UNKNOWN);
        _read_resp = kv_map["resp"];
        if (_err == ERR_UNKNOWN)
            parse_ok = false;
        break;
    }
    case replica_config: {
        _config_receiver = node_to_address(kv_map["receiver"]);
        _config_type = parse_config_command(kv_map["type"]);
        _config_node = node_to_address(kv_map["node"]);
        if (_config_receiver.is_invalid() || _config_type == config_type::CT_INVALID ||
            _config_node.is_invalid())
            parse_ok = false;
        break;
    }
    default:
        CHECK(false, "");
    }
    if (!parse_ok) {
        std::cerr << "bad line: line_no=" << line_no() << ": unknown error: " << kv_map["err"]
                  << std::endl;
        return false;
    }
    return true;
}

std::string client_case_line::type_name() const
{
    switch (_type) {
    case begin_write:
        return "begin_write";
    case begin_read:
        return "begin_read";
    case end_write:
        return "end_write";
    case end_read:
        return "end_read";
    case replica_config:
        return "replica_config";
    default:
        CHECK(false, "");
    }
    return "";
}

bool client_case_line::parse_type_name(const std::string &name)
{
    if (name == "begin_write")
        _type = begin_write;
    else if (name == "begin_read")
        _type = begin_read;
    else if (name == "end_write")
        _type = end_write;
    else if (name == "end_read")
        _type = end_read;
    else if (name == "replica_config")
        _type = replica_config;
    else
        return false;
    return true;
}

static const char *s_replica_config_commands[] = {"none",
                                                  "assign_primary",
                                                  "upgrade_to_primary",
                                                  "add_secondary",
                                                  "upgrade_to_secondary",
                                                  "downgrade_to_secondary",
                                                  "downgrade_to_inactive",
                                                  "remove",
                                                  nullptr};

dsn::replication::config_type::type
client_case_line::parse_config_command(const std::string &command_name) const
{
    for (int i = 0; s_replica_config_commands[i] != nullptr; ++i) {
        if (boost::iequals(command_name.c_str(), s_replica_config_commands[i])) {
            return (dsn::replication::config_type::type)i;
        }
    }
    return config_type::CT_INVALID;
}

std::string
client_case_line::config_command_to_string(dsn::replication::config_type::type cfg_command) const
{
    return s_replica_config_commands[cfg_command];
}

void client_case_line::get_write_params(int &id,
                                        std::string &key,
                                        std::string &value,
                                        int &timeout_ms) const
{
    CHECK_EQ(_type, begin_write);
    id = _id;
    key = _key;
    value = _value;
    timeout_ms = _timeout;
}

void client_case_line::get_read_params(int &id, std::string &key, int &timeout_ms) const
{
    CHECK_EQ(_type, begin_read);
    id = _id;
    key = _key;
    timeout_ms = _timeout;
}

void client_case_line::get_replica_config_params(rpc_address &receiver,
                                                 dsn::replication::config_type::type &type,
                                                 rpc_address &node) const
{
    CHECK_EQ(_type, replica_config);
    receiver = _config_receiver;
    type = _config_type;
    node = _config_node;
}

bool client_case_line::check_write_result(int id, ::dsn::error_code err, int32_t resp)
{
    return id == _id && err == _err && (err != dsn::ERR_OK || resp == _write_resp);
}

bool client_case_line::check_read_result(int id, ::dsn::error_code err, const std::string &resp)
{
    return id == _id && err == _err && (err != dsn::ERR_OK || resp == _read_resp);
}

bool test_case::s_inited = false;
int test_case::s_null_loop = 10000;
bool test_case::s_close_replica_stub_on_exit = false;

test_case::test_case() : _next(0), _null_loop_count(0)
{
    register_creator<set_case_line>();
    register_creator<skip_case_line>();
    register_creator<exit_case_line>();
    register_creator<state_case_line>();
    register_creator<config_case_line>();
    register_creator<wait_case_line>();
    register_creator<inject_case_line>();
    register_creator<modify_case_line>();
    register_creator<client_case_line>();
}

test_case::~test_case() {}

bool test_case::init(const std::string &case_input)
{
    if (s_inited) {
        return false;
    }

    std::string input_postfix = ".act";
    std::string output_postfix = ".out";

    size_t pos = case_input.find(input_postfix);
    if (pos == std::string::npos || pos + input_postfix.size() != case_input.size()) {
        std::cerr << "invalid case input file name: " << case_input << std::endl;
        return false;
    }

    _name = case_input.substr(0, pos);
    std::string case_output = _name + output_postfix;

    _output.open(case_output);
    if (!_output) {
        std::cerr << "open case output file failed: " << case_input << std::endl;
        return false;
    }

    std::ifstream fin(case_input.c_str());
    if (!fin) {
        std::cerr << "open case input file failed: " << case_input << std::endl;
        return false;
    }

    _case_lines.push_back(nullptr); // the first one is null

    int line_no = 0;
    std::string line;
    while (!fin.eof()) {
        std::getline(fin, line);
        line_no++;
        boost::algorithm::trim(line);
        if (line.empty() || line[0] == '#') {
            // ignore comments
            continue;
        }
        size_t pos = line.find(':');
        if (pos == std::string::npos) {
            std::cerr << "bad line: line_no=" << line_no << std::endl;
            return false;
        }
        std::string type = line.substr(0, pos);
        if (_creators.find(type) == _creators.end()) {
            std::cerr << "bad line: line_no=" << line_no << ": invalid case line type " << type
                      << std::endl;
            return false;
        }

        std::string params = line.substr(pos + 1);
        case_line *cl = _creators[type](line_no, params);
        if (cl == nullptr) {
            std::cerr << "bad line: line_no=" << line_no << ": invalid params: " << params
                      << std::endl;
            return false;
        }

        _case_lines.push_back(cl);
    }

    if (_case_lines.size() == 1) // only the first null one
    {
        std::cerr << "empty case input file: " << case_input << std::endl;
        return false;
    }

    forward();

    LOG_INFO("=== init %s succeed", _name.c_str());

    s_inited = true;
    return true;
}

void test_case::forward()
{
    _null_loop_count = 0; // reset null loop count
    CHECK_LT(_next, _case_lines.size());
    while (true) {
        case_line *cl = _case_lines[_next];
        if (cl != nullptr) {
            if (cl->name() != skip_case_line::NAME()) {
                output(cl->to_string());
                print(cl, "");
            }
            LOG_INFO("=== on_case_forward:[%d]%s", cl->line_no(), cl->to_string().c_str());
        }
        _next++;
        if (_next >= _case_lines.size()) {
            LOG_INFO("=== on_case_done");
            g_done = true;
            break;
        }
        // pre-view the next one
        cl = _case_lines[_next];
        if (cl->name() == set_case_line::NAME()) {
            set_case_line *scl = static_cast<set_case_line *>(cl);
            scl->apply_set();
        } else if (cl->name() == exit_case_line::NAME()) {
            LOG_INFO("=== on_case_exit");
            g_done = true;
            break;
        } else {
            if (cl->name() == skip_case_line::NAME()) {
                output(cl->to_string());
                print(cl, "");
            }
            break;
        }
    }
    notify_check_client();
}

void test_case::fail(const std::string &other)
{
    _null_loop_count = 0; // reset null loop count
    CHECK_LT(_next, _case_lines.size());
    case_line *cl = _case_lines[_next];
    output(other);
    print(cl, other);
    LOG_ERROR("=== on_case_failure:line=%d,case=%s", cl->line_no(), cl->to_string().c_str());
    g_fail = true;
    g_done = true;
    notify_check_client();
}

void test_case::output(const std::string &line)
{
    _output << line << std::endl;
    _output.flush();
}

void test_case::print(case_line *cl, const std::string &other, bool is_skip)
{
    if (is_skip) {
        CHECK(cl == nullptr, "");
        CHECK(!other.empty(), "");
        std::cout << "    s  " << other << std::endl;
        return;
    }

    if (cl == nullptr) {
        CHECK(!other.empty(), "");
        std::cout << "    +  " << other << std::endl;
    } else // cl != nullptr
    {
        char buf[100];
        sprintf(buf, "%5d  ", cl->line_no());
        std::cout << buf << cl->to_string() << std::endl;
        if (!other.empty()) {
            std::cout << " <==>  " << other << std::endl;
        }
    }
}

bool test_case::check_skip(bool consume_one)
{
    if (g_done)
        return true;

    case_line *c = _case_lines[_next];
    if (c->name() != skip_case_line::NAME()) {
        return false;
    }
    skip_case_line *cl = static_cast<skip_case_line *>(c);
    if (consume_one) {
        cl->skip_one();
        LOG_INFO("=== on_skip_one:skipped=%d/%d", cl->skipped(), cl->count());
    }
    if (cl->is_skip_done()) {
        forward();
    }
    return true;
}

void test_case::wait_check_client() { _client_sema.wait(); }

void test_case::notify_check_client() { _client_sema.signal(); }

bool test_case::check_client_instruction(client_case_line::client_type type)
{
    if (g_done)
        return false;

    if (check_skip(false))
        return false;

    case_line *c = _case_lines[_next];
    if (c->name() != client_case_line::NAME()) {
        return false;
    }
    client_case_line *cl = static_cast<client_case_line *>(c);
    if (cl->type() != type) {
        return false;
    }
    return true;
}

bool test_case::check_client_write(int &id, std::string &key, std::string &value, int &timeout_ms)
{
    if (!check_client_instruction(client_case_line::begin_write))
        return false;
    client_case_line *cl = static_cast<client_case_line *>(_case_lines[_next]);
    cl->get_write_params(id, key, value, timeout_ms);
    forward();
    return true;
}

bool test_case::check_replica_config(rpc_address &receiver,
                                     dsn::replication::config_type::type &type,
                                     rpc_address &node)
{
    if (!check_client_instruction(client_case_line::replica_config))
        return false;
    client_case_line *cl = static_cast<client_case_line *>(_case_lines[_next]);
    cl->get_replica_config_params(receiver, type, node);
    forward();
    return true;
}

bool test_case::check_client_read(int &id, std::string &key, int &timeout_ms)
{
    if (!check_client_instruction(client_case_line::begin_read))
        return false;
    client_case_line *cl = static_cast<client_case_line *>(_case_lines[_next]);
    cl->get_read_params(id, key, timeout_ms);
    forward();
    return true;
}

void test_case::on_end_write(int id, ::dsn::error_code err, int32_t resp)
{
    if (g_done)
        return;

    char buf[1024];
    snprintf_p(buf,
               1024,
               "%s:end_write:id=%d,err=%s,resp=%d",
               client_case_line::NAME(),
               id,
               err.to_string(),
               resp);

    LOG_INFO("=== on_end_write:id=%d,err=%s,resp=%d", id, err.to_string(), resp);

    if (check_skip(true)) {
        output(buf);
        print(nullptr, buf, true);
        return;
    }

    case_line *c = _case_lines[_next];
    if (c->name() != client_case_line::NAME()) {
        output(buf);
        print(nullptr, buf, true);
        return;
    }
    client_case_line *cl = static_cast<client_case_line *>(c);
    if (cl->type() != client_case_line::end_write) {
        output(buf);
        print(nullptr, buf, true);
        return;
    }
    if (!cl->check_write_result(id, err, resp)) {
        output(buf);
        print(nullptr, buf, true);
        return;
    }
    forward();
}

void test_case::on_end_read(int id, ::dsn::error_code err, const std::string &resp)
{
    if (g_done)
        return;

    char buf[1024];
    snprintf_p(buf,
               1024,
               "%s:end_read:id=%d,err=%s,resp=%s",
               client_case_line::NAME(),
               id,
               err.to_string(),
               resp.c_str());

    LOG_INFO("=== on_end_read:id=%d,err=%s,resp=%s", id, err.to_string(), resp.c_str());

    if (check_skip(true)) {
        output(buf);
        print(nullptr, buf, true);
        return;
    }

    case_line *c = _case_lines[_next];
    if (c->name() != client_case_line::NAME()) {
        output(buf);
        print(nullptr, buf, true);
        return;
    }
    client_case_line *cl = static_cast<client_case_line *>(c);
    if (cl->type() != client_case_line::end_read) {
        output(buf);
        print(nullptr, buf, true);
        return;
    }
    if (!cl->check_read_result(id, err, resp)) {
        output(buf);
        print(nullptr, buf, true);
        return;
    }
    forward();
}

bool test_case::on_event(const event *ev)
{
    if (g_done)
        return true;

    LOG_INFO("=== %s", ev->to_string().c_str());

    if (check_skip(false))
        return true;

    case_line *c = _case_lines[_next];
    if (c->name() != inject_case_line::NAME() && c->name() != modify_case_line::NAME() &&
        c->name() != wait_case_line::NAME()) {
        return true;
    }

    event_case_line *cl = static_cast<event_case_line *>(c);
    if (!cl->check_satisfied(ev)) {
        return true;
    }

    bool ret = true;
    if (cl->name() == inject_case_line::NAME()) {
        // should inject fault
        ret = false;
    } else if (cl->name() == modify_case_line::NAME()) {
        modify_case_line *mcl = static_cast<modify_case_line *>(cl);
        mcl->modify(ev);
    }

    forward();
    return ret;
}

void test_case::on_check()
{
    if (g_done)
        return;

    ++_null_loop_count;
    if (s_null_loop > 0 && _null_loop_count > s_null_loop) {
        fail("null_loop:" + boost::lexical_cast<std::string>(s_null_loop));
    }
}

void test_case::on_config_change(const parti_config &last, const parti_config &cur)
{
    if (g_done)
        return;

    _null_loop_count = 0; // reset null loop count

    std::string buf = std::string(config_case_line::NAME()) + ":" + cur.to_string();
    LOG_INFO("=== on_config_change:%s", cur.to_string().c_str());

    if (check_skip(true)) {
        output(buf);
        print(nullptr, buf, true);
        return;
    }

    case_line *c = _case_lines[_next];
    if (c->name() != config_case_line::NAME()) {
        output(buf);
        print(nullptr, buf);
        return;
    }
    config_case_line *cl = static_cast<config_case_line *>(c);
    bool do_forward;
    if (!cl->check_config(cur, do_forward)) {
        fail(buf);
        return;
    }
    if (do_forward) {
        forward();
    } else {
        output(buf);
        print(nullptr, buf);
    }
}

void test_case::on_state_change(const state_snapshot &last, const state_snapshot &cur)
{
    if (g_done)
        return;

    _null_loop_count = 0; // reset null loop count

    std::string buf = std::string(state_case_line::NAME()) + ":" + cur.to_string();
    LOG_INFO("=== on_state_change:%s\n%s", cur.to_string().c_str(), cur.diff_string(last).c_str());

    if (check_skip(true)) {
        output(buf);
        print(nullptr, buf, true);
        return;
    }

    case_line *c = _case_lines[_next];
    if (c->name() != state_case_line::NAME()) {
        output(buf);
        print(nullptr, buf);
        return;
    }
    state_case_line *cl = static_cast<state_case_line *>(c);
    bool do_forward;
    if (!cl->check_state(cur, do_forward)) {
        fail(buf);
        return;
    }
    if (do_forward) {
        forward();
    } else {
        output(buf);
        print(nullptr, buf);
    }
}

void test_case::internal_register_creator(const std::string &name, case_line_creator creator)
{
    CHECK(_creators.find(name) == _creators.end(), "");
    _creators[name] = creator;
}
}
}
}
