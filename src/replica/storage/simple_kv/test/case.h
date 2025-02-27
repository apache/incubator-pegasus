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

#pragma once

#include <stddef.h>
#include <stdint.h>
#include <fstream>
#include <map>
#include <string>
#include <vector>

#include "common.h"
#include "meta_admin_types.h"
#include "rpc/rpc_host_port.h"
#include "utils/error_code.h"
#include "utils/fmt_utils.h"
#include "utils/singleton.h"
#include "utils/zlocks.h"

namespace dsn {
class aio_task;

class message_ex;
class rpc_request_task;
class rpc_response_task;
class task;

namespace replication {
namespace test {

class case_line
{
public:
    template <typename T>
    static case_line *create(int line_no, const std::string &params)
    {
        case_line *cl = new T();
        cl->set_line_no(line_no);
        if (!cl->parse(params)) {
            delete cl;
            return nullptr;
        }
        return cl;
    }

public:
    virtual ~case_line() {}
    int line_no() const { return _line_no; }
    void set_line_no(int line_no) { _line_no = line_no; }
    virtual std::string name() const = 0;
    virtual std::string to_string() const = 0;
    virtual bool parse(const std::string &params) = 0;

    friend std::ostream &operator<<(std::ostream &os, const case_line &cl)
    {
        return os << cl.to_string();
    }

private:
    int _line_no;
};

class set_case_line : public case_line
{
public:
    static const char *NAME() { return "set"; }
    virtual ~set_case_line() {}
    virtual std::string name() const { return NAME(); }
    virtual bool parse(const std::string &params);

    void apply_set() const;

private:
    std::string to_string() const override;

    bool _null_loop_set;
    int _null_loop;
    bool _lb_for_test;
    bool _lb_for_test_set;
    bool _disable_lb;
    bool _disable_lb_set;
    bool _close_replica_stub;
    bool _close_replica_stub_set;
    bool _not_exit_on_log_failure;
    bool _not_exit_on_log_failure_set;
    bool _simple_kv_open_fail;
    bool _simple_kv_open_fail_set;
    bool _simple_kv_close_fail;
    bool _simple_kv_close_fail_set;
    bool _simple_kv_get_checkpoint_fail;
    bool _simple_kv_get_checkpoint_fail_set;
    bool _simple_kv_apply_checkpoint_fail;
    bool _simple_kv_apply_checkpoint_fail_set;
};

// SKIP:100
class skip_case_line : public case_line
{
public:
    static const char *NAME() { return "skip"; }
    virtual ~skip_case_line() {}
    virtual std::string name() const { return NAME(); }
    virtual bool parse(const std::string &params);

    int count() const { return _count; }
    int skipped() const { return _skipped; }
    void skip_one() { _skipped++; }
    bool is_skip_done() const { return _skipped >= _count; }

private:
    std::string to_string() const override;

    int _count;
    int _skipped;
};

// EXIT:
class exit_case_line : public case_line
{
public:
    static const char *NAME() { return "exit"; }
    virtual ~exit_case_line() {}
    virtual std::string name() const { return NAME(); }
    virtual bool parse(const std::string &params);

private:
    std::string to_string() const override;
};

class state_case_line : public case_line
{
public:
    static const char *NAME() { return "state"; }
    virtual ~state_case_line() {}
    virtual std::string name() const { return NAME(); }
    virtual bool parse(const std::string &params);

    // return false if check failed
    // 'forward' is to indicates if should go forward
    bool check_state(const state_snapshot &cur_state, bool &forward);

private:
    std::string to_string() const override;

    state_snapshot _state;
};

class config_case_line : public case_line
{
public:
    static const char *NAME() { return "config"; }
    virtual ~config_case_line() {}
    virtual std::string name() const { return NAME(); }
    virtual bool parse(const std::string &params);

    // return false if check failed
    // 'forward' is to indicates if should go forward
    bool check_config(const parti_config &cur_config, bool &forward);

private:
    std::string to_string() const override;

    parti_config _config;
};

enum event_type
{
    task_enqueue,         // node=xxx,code=xxx
    task_begin,           // node=xxx,code=xxx
    task_end,             // node=xxx,code=xxx
    task_cancelled,       // node=xxx,code=xxx
    aio_call,             // node=xxx
    aio_enqueue,          // node=xxx
    rpc_call,             // name=xxx,from=xxx,to=xxx
    rpc_request_enqueue,  // name=xxx,from=xxx,to=xxx
    rpc_reply,            // name=xxx,from=xxx,to=xxx
    rpc_response_enqueue, // name=xxx,from=xxx,to=xxx,err=xxx
};

const char *event_type_to_string(event_type type);
bool event_type_from_string(const std::string &name, event_type &type);
bool event_type_support_inject_fault(event_type type);

class event
{
public:
    virtual ~event() {}
    virtual event_type type() const = 0;
    virtual void internal_to_string(std::ostream &oss) const = 0;
    virtual bool internal_parse(const std::map<std::string, std::string> &kv_map) = 0;
    // 'this' is the event condition, and 'ev' is the real event occured
    // return true if 'ev' satisfy 'this' condition
    virtual bool check_satisfied(const event *ev) const = 0;

    friend std::ostream &operator<<(std::ostream &os, const event &evt)
    {
        return os << evt.to_string();
    }

    static event *parse(int line_no, const std::string &params);

private:
    std::string to_string() const;
};

class event_on_task : public event
{
public:
    virtual void internal_to_string(std::ostream &oss) const;
    virtual bool internal_parse(const std::map<std::string, std::string> &kv_map);
    virtual bool check_satisfied(const event *ev) const;

    void init(task *tsk);

public:
    task *_task;
    std::string _task_id;
    std::string _node;
    std::string _task_code;
    std::string _delay;
};

class event_on_task_enqueue : public event_on_task
{
public:
    virtual event_type type() const { return task_enqueue; }
};

class event_on_task_begin : public event_on_task
{
public:
    virtual event_type type() const { return task_begin; }
};

class event_on_task_end : public event_on_task
{
public:
    virtual event_type type() const { return task_end; }
};

class event_on_task_cancelled : public event_on_task
{
public:
    virtual event_type type() const { return task_cancelled; }
};

class event_on_rpc : public event_on_task
{
public:
    virtual void internal_to_string(std::ostream &oss) const;
    virtual bool internal_parse(const std::map<std::string, std::string> &kv_map);
    virtual bool check_satisfied(const event *ev) const;

    // 'tsk' is the following task by the event, by which we can
    // connect related events to event sequence.
    void init(message_ex *msg, task *tsk);

public:
    std::string _trace_id;
    std::string _rpc_name;
    std::string _from;
    std::string _to;
};

class event_on_rpc_call : public event_on_rpc
{
public:
    virtual event_type type() const { return rpc_call; }
};

class event_on_rpc_request_enqueue : public event_on_rpc
{
public:
    virtual event_type type() const { return rpc_request_enqueue; }

    void init(rpc_request_task *tsk);
};

class event_on_rpc_reply : public event_on_rpc
{
public:
    virtual event_type type() const { return rpc_reply; }
};

class event_on_rpc_response_enqueue : public event_on_rpc
{
public:
    virtual event_type type() const { return rpc_response_enqueue; }
    virtual void internal_to_string(std::ostream &oss) const;
    virtual bool internal_parse(const std::map<std::string, std::string> &kv_map);
    virtual bool check_satisfied(const event *ev) const;

    void init(rpc_response_task *tsk);

public:
    std::string _err;
};

class event_on_aio : public event_on_task
{
public:
    virtual void internal_to_string(std::ostream &oss) const;
    virtual bool internal_parse(const std::map<std::string, std::string> &kv_map);
    virtual bool check_satisfied(const event *ev) const;

    void init(aio_task *tsk);

public:
    std::string _type;
    std::string _file_offset;
    std::string _buffer_size;
};

class event_on_aio_call : public event_on_aio
{
public:
    virtual event_type type() const { return aio_call; }
};

class event_on_aio_enqueue : public event_on_aio
{
public:
    virtual event_type type() const { return aio_enqueue; }
    virtual void internal_to_string(std::ostream &oss) const;
    virtual bool internal_parse(const std::map<std::string, std::string> &kv_map);
    virtual bool check_satisfied(const event *ev) const;

    void init(aio_task *tsk);

public:
    std::string _err;
    std::string _transferred_size;
};

class event_case_line : public case_line
{
public:
    virtual ~event_case_line()
    {
        if (_event_cond)
            delete _event_cond;
    }
    virtual bool parse(const std::string &params);

    bool check_satisfied(const event *ev) const;

    event *_event_cond;

protected:
    std::string to_string() const override;
};

class wait_case_line : public event_case_line
{
public:
    static const char *NAME() { return "wait"; }
    virtual std::string name() const { return NAME(); }
};

class inject_case_line : public event_case_line
{
public:
    static const char *NAME() { return "inject"; }
    virtual std::string name() const { return NAME(); }
    virtual bool parse(const std::string &params);
};

class modify_case_line : public event_case_line
{
public:
    static const char *NAME() { return "modify"; }
    virtual std::string name() const { return NAME(); };
    virtual bool parse(const std::string &params);
    virtual void modify(const event *ev);

private:
    std::string to_string() const override;

    std::string _modify_delay;
};

class client_case_line : public case_line
{
public:
    enum client_type
    {
        begin_write,    // id=xxx,key=xxx,value=xxx,timeout=xxx
        begin_read,     // id=xxx,key=xxx,timeout=xxx
        end_write,      // id=xxx,err=xxx,resp=xxx
        end_read,       // id=xxx,err=xxx,resp=xxx
        replica_config, // receiver=xxx,type=xxx,node=xxx
    };

public:
    static const char *NAME() { return "client"; }
    virtual std::string name() const { return NAME(); }
    virtual bool parse(const std::string &params);

    client_type type() const { return _type; }
    std::string type_name() const;
    bool parse_type_name(const std::string &name);
    void get_write_params(int &id, std::string &key, std::string &value, int &timeout_ms) const;
    void get_read_params(int &id, std::string &key, int &timeout_ms) const;
    void get_replica_config_params(host_port &receiver,
                                   dsn::replication::config_type::type &type,
                                   host_port &node) const;
    bool check_write_result(int id, ::dsn::error_code err, int32_t resp);
    bool check_read_result(int id, ::dsn::error_code err, const std::string &resp);

    dsn::replication::config_type::type parse_config_command(const std::string &command_str) const;
    std::string config_command_to_string(dsn::replication::config_type::type cfg_command) const;

private:
    std::string to_string() const override;

    client_type _type;
    int _id;
    std::string _key;
    std::string _value;
    int _timeout;
    dsn::error_code _err;
    int _write_resp;
    std::string _read_resp;

    host_port _config_receiver;
    dsn::replication::config_type::type _config_type;
    host_port _config_node;
};
USER_DEFINED_ENUM_FORMATTER(client_case_line::client_type)

class test_case : public dsn::utils::singleton<test_case>
{
public:
    static bool s_inited;

    // options, can be modified be set_case_line
    static int s_null_loop;
    static bool s_close_replica_stub_on_exit;

public:
    test_case();
    ~test_case();
    bool init(const std::string &case_input);
    void forward();
    void fail(const std::string &other);
    void output(const std::string &line);
    void print(case_line *cl, const std::string &other, bool is_skip = false);

    // return true if should skip
    bool check_skip(bool consume_one);

    // client
    void wait_check_client();
    void notify_check_client();
    bool check_client_write(int &id, std::string &key, std::string &value, int &timeout_ms);
    bool check_replica_config(host_port &receiver,
                              dsn::replication::config_type::type &type,
                              host_port &node);
    bool check_client_read(int &id, std::string &key, int &timeout_ms);
    void on_end_write(int id, ::dsn::error_code err, int32_t resp);
    void on_end_read(int id, ::dsn::error_code err, const std::string &resp);

    // checker
    void on_check();
    void on_config_change(const parti_config &last, const parti_config &cur);
    void on_state_change(const state_snapshot &last, const state_snapshot &cur);

    // injecter
    bool on_event(const event *ev);

private:
    typedef case_line *(*case_line_creator)(int, const std::string &);
    void internal_register_creator(const std::string &name, case_line_creator creator);
    template <typename T>
    void register_creator()
    {
        internal_register_creator(T::NAME(), T::template create<T>);
    }

    bool check_client_instruction(client_case_line::client_type type);

private:
    std::string _name;
    std::ofstream _output;
    std::map<std::string, case_line_creator> _creators;
    std::vector<case_line *> _case_lines;
    size_t _next;
    int _null_loop_count;
    dsn::zsemaphore _client_sema;
};
} // namespace test
} // namespace replication
} // namespace dsn

USER_DEFINED_STRUCTURE_FORMATTER(::dsn::replication::test::case_line);
USER_DEFINED_STRUCTURE_FORMATTER(::dsn::replication::test::event);
