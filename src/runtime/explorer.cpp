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
 *     the tracer toollets traces all the asynchonous execution flow
 *     in the system through the join-point mechanism
 *
 * Revision history:
 *     May, 2016, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include <dsn/toollet/explorer.h>
#include <dsn/service_api_c.h>
#include <dsn/tool-api/command_manager.h>
#include <dsn/tool/simulator.h>

namespace dsn {
namespace tools {
class task_explorer
{
public:
    task_explorer()
    {
        int maxt = dsn::task_code::max();
        _locals.resize((size_t)maxt, 0);
        _msg_count.store(0);
        _lpc_count.store(0);
    }

    void on_message_recv(message_ex *msg, int caller)
    {
        ++_msg_count;
        utils::auto_lock<utils::ex_lock_nr_spin> l(_lock);
        ++_ins[msg->header->from_address.value()][caller];
    }

    void on_local_call(dsn::task_code callee)
    {
        ++_lpc_count;

        utils::auto_lock<utils::ex_lock_nr_spin> l(_lock);
        ++_locals[callee];
    }

    uint64_t count() { return _msg_count.load() + _lpc_count.load(); }

    uint64_t msg_count() { return _msg_count.load(); }

    void increase_lpc_count() { ++_lpc_count; }

private:
    friend class per_node_task_explorer;
    std::unordered_map<uint64_t, std::unordered_map<int, uint64_t>>
        _ins;                      // <rpc-address, <task-code, in-count> >
    std::vector<uint64_t> _locals; // local task invocation count
    utils::ex_lock_nr_spin _lock;
    std::atomic<uint64_t> _msg_count, _lpc_count;
};

static std::string explorer_get_task_id(int nid, int task_code)
{
    char buffer[32];
    sprintf(buffer, "%d.%d", nid, task_code);
    return buffer;
}

static std::string explorer_get_task_label(int nid, int task_code)
{
    char buffer[32];
    sprintf(buffer, "%d", task_code);
    return buffer;

    /*std::string lb = task_spec::get(task_code)->name;
    if (lb.length() > 4)
    {
        if (lb.substr(0, 4) == "RPC_")
            lb = lb.substr(4);
        else if (lb.substr(0, 4) == "LPC_")
            lb = lb.substr(4);
        else
            lb = lb;
    }
    return lb;*/
}

class per_node_task_explorer
{
public:
    per_node_task_explorer()
    {
        int maxt = dsn::task_code::max();
        _explorers.resize(maxt + 1);
        _node_id = 0;
        _name = "ukn";

        for (int i = 0; i < maxt + 1; i++) {
            _explorers[i] = new task_explorer();
        }
    }

    ~per_node_task_explorer()
    {
        for (auto exp : _explorers)
            delete exp;
        _explorers.clear();
    }

    void set_id(int nid, rpc_address addr, const std::string &name)
    {
        _node_id = nid;
        _address = addr;
        _name = name;
    }

    int node_id() const { return _node_id; }

    rpc_address address() const { return _address; }

    std::string name() const { return _name; }

    void on_message_recv(message_ex *msg, int caller, int callee)
    {
        _explorers[callee]->on_message_recv(msg, caller);
    }

    void on_local_call(dsn::task_code caller, dsn::task_code callee)
    {
        _explorers[caller]->on_local_call(callee);
        _explorers[callee]->increase_lpc_count();
    }

    // <source node address, sent task kinds>
    void collect_out_tasks(std::unordered_map<uint64_t, std::unordered_set<int>> &outs)
    {
        for (int i = 0; i < (int)_explorers.size(); i++) {
            auto &exp = _explorers[i];
            if (exp->msg_count() > 0) {
                // addr -> count
                std::unordered_map<uint64_t, std::unordered_map<int, uint64_t>> i_st;
                {
                    utils::auto_lock<utils::ex_lock_nr_spin> l(exp->_lock);
                    i_st = exp->_ins;
                }

                for (auto &kv : i_st) {
                    std::unordered_set<int> *ptr;
                    auto src = kv.first;
                    auto it = outs.find(src);
                    if (it == outs.end()) {
                        std::unordered_set<int> ots;
                        ptr = &outs.emplace(src, ots).first->second;
                    } else {
                        ptr = &it->second;
                    }

                    for (auto &kv2 : kv.second) {
                        ptr->insert(kv2.first);
                    }
                }
            }
        }
    }

    void draw_dot_graph1(std::stringstream &ss,
                         const std::unordered_set<int> &out_tasks,
                         std::set<int> &all_tasks)
    {
        ss << "\tsubgraph cluster" << node_id() << " { " << std::endl;

        // global properties
        ss << "\t\t"
           << "label = \"" << name() << " @ " << address().to_std_string() << " (id=" << node_id()
           << ")\";" << std::endl;
        ss << "\t\t" << std::endl;

        for (int i = 0; i < (int)_explorers.size(); i++) {
            auto &exp = _explorers[i];
            if (exp->count() > 0 || out_tasks.find(i) != out_tasks.end()) {
                // all used tasks in an node
                ss << "\t\t" << explorer_get_task_id(_node_id, i) << " [label=\""
                   << explorer_get_task_label(_node_id, i) << "\",style=filled];" << std::endl;
                all_tasks.emplace(i);

                // all intra edges
                for (int j = 0; j < (int)exp->_locals.size(); j++) {
                    if (exp->_locals[j] > 0) {
                        ss << "\t\t" << explorer_get_task_id(_node_id, i) << " -> "
                           << explorer_get_task_id(_node_id, j) << " [label=" << exp->_locals[j]
                           << "];" << std::endl;
                    }
                }
                ss << "\t\t" << std::endl;
            }
        }

        ss << "\t}" << std::endl;
        ss << "\t" << std::endl;
    }

    void draw_dot_graph2(std::stringstream &ss,
                         const std::map<uint64_t, per_node_task_explorer *> &lookup)
    {
        for (int i = 0; i < (int)_explorers.size(); i++) {
            auto &exp = _explorers[i];
            if (exp->msg_count() > 0) {
                // addr -> count
                std::unordered_map<uint64_t, std::unordered_map<int, uint64_t>> i_st;
                {
                    utils::auto_lock<utils::ex_lock_nr_spin> l(exp->_lock);
                    i_st = exp->_ins;
                }

                // all incoming edges
                for (auto &kv : i_st) {
                    int from_node_id = -1;
                    auto from_node_it = lookup.find(kv.first);
                    if (from_node_it != lookup.end()) {
                        from_node_id = from_node_it->second->node_id();
                    }

                    for (auto &fc_kv : kv.second) {
                        if (-1 != from_node_id) {
                            ss << "\t" << explorer_get_task_id(from_node_id, fc_kv.first) << " -> "
                               << explorer_get_task_id(_node_id, i) << " [label=" << fc_kv.second
                               << "];" << std::endl;
                        }

                        // with external partners
                        else {
                            dsn::rpc_address remote;
                            remote.value() = kv.first;

                            ss << "\t" << kv.first << " [label=\""
                               << rpc_address(remote).to_std_string() << "\"];" << std::endl;
                            ss << "\t" << kv.first << " -> " << explorer_get_task_id(_node_id, i)
                               << " [label=" << fc_kv.second << "];" << std::endl;
                        }
                    }
                }
                // ss << "\t" << std::endl;
            }
            // ss << "\t" << std::endl;
        }
        // ss << "\t" << std::endl;
    }

private:
    int _node_id;
    rpc_address _address;
    std::string _name;

    std::vector<task_explorer *> _explorers;
};

class all_task_explorer : public utils::singleton<all_task_explorer>
{
public:
    all_task_explorer()
    {
        std::vector<service_app *> all;
        service_app::get_all_service_apps(&all);
        _explorers.resize(all.size());

        for (int i = 0; i < all.size(); i++) {
            service_app *se = all[i];
            auto &exp = _explorers[se->info().entity_id];
            exp.set_id(se->info().entity_id, se->primary_address(), se->info().full_name);
            _explorers_by_addr[exp.address().value()] = &exp;
        }
    }

    void on_message_recv(message_ex *msg, int caller_code, int callee_code)
    {
        _explorers[task::get_current_node_id()].on_message_recv(msg, caller_code, callee_code);
    }

    void on_local_call(dsn::task_code caller, dsn::task_code callee)
    {
        _explorers[task::get_current_node_id()].on_local_call(caller, callee);
    }

    void get_dot_graph(std::stringstream &ss, const std::vector<std::string> &args)
    {
        ss << "digraph G { " << std::endl;

        // we only collect incoming messages at runtime
        // therefore the statistics at the outgoing node may be missing
        // need to collect those missing statistics first to avoid
        // missing nodes in the sources

        // <source node address, sent task kinds>
        std::unordered_map<uint64_t, std::unordered_set<int>> outgoing_tasks;
        for (auto &exp : _explorers)
            exp.collect_out_tasks(outgoing_tasks);

        // cross-node tasks
        for (auto &exp : _explorers)
            exp.draw_dot_graph2(ss, _explorers_by_addr);

        // all nodes and intra-node tasks
        std::set<int> all_tasks;
        for (auto &exp : _explorers) {
            std::unordered_set<int> ots;
            std::unordered_set<int> *ptr = &ots;
            auto it = outgoing_tasks.find(exp.address().value());
            if (it != outgoing_tasks.end()) {
                ptr = &it->second;
            }

            exp.draw_dot_graph1(ss, *ptr, all_tasks);
        }

        // legend for all tasks
        ss << "\tlegend [shape=record,label=\"{tasks";
        for (auto &t : all_tasks) {
            ss << "| {" << t << "|" << task_spec::get(t)->name << "}";
        }
        ss << "}\"];" << std::endl;

        ss << "}" << std::endl;
    }

private:
    std::vector<per_node_task_explorer> _explorers; // app_id as index
    std::map<uint64_t, per_node_task_explorer *> _explorers_by_addr;
};

typedef uint64_extension_helper<explorer, message_ex> message_ext_for_explorer;
typedef uint64_extension_helper<explorer, task> task_ext_for_explorer;

static void explorer_on_task_create(task *caller, task *callee)
{
    switch (callee->spec().type) {
    case dsn_task_type_t::TASK_TYPE_COMPUTE:
    case dsn_task_type_t::TASK_TYPE_AIO:
        task_ext_for_explorer::get(callee) =
            (uint64_t)(caller ? caller->spec().code : TASK_CODE_INVALID);
        break;
    case dsn_task_type_t::TASK_TYPE_RPC_REQUEST: {
        auto callee2 = static_cast<rpc_request_task *>(callee);
        all_task_explorer::instance().on_message_recv(
            callee2->get_request(),
            (int)(message_ext_for_explorer::get(callee2->get_request())),
            callee->spec().code);
    } break;
    case dsn_task_type_t::TASK_TYPE_RPC_RESPONSE: {
        auto callee2 = static_cast<rpc_response_task *>(callee);
        message_ext_for_explorer::get(callee2->get_request()) =
            (uint64_t)(caller ? caller->spec().code : TASK_CODE_INVALID);
    } break;
    default:
        break;
    }
}

static void explorer_on_task_begin(task *t)
{
    switch (t->spec().type) {
    case dsn_task_type_t::TASK_TYPE_COMPUTE:
    case dsn_task_type_t::TASK_TYPE_AIO: {
        dsn::task_code caller_code(task_ext_for_explorer::get(t));
        if (caller_code != TASK_CODE_INVALID) {
            all_task_explorer::instance().on_local_call(caller_code, t->spec().code);
        }
    } break;
    default:
        break;
    }
}

static void explorer_on_rpc_call(task *caller, message_ex *req, rpc_response_task *callee)
{
    // attach caller task-code for one-way rpc only as two-way rpc is tracked in on_ask_create above
    if (callee == nullptr)
        message_ext_for_explorer::get(req) =
            (uint64_t)(caller ? caller->spec().code : TASK_CODE_INVALID);
}

static void explorer_on_rpc_reply(task *caller, message_ex *msg)
{
    // attach caller task-code
    message_ext_for_explorer::get(msg) =
        (uint64_t)(caller ? caller->spec().code : TASK_CODE_INVALID);
}

static void explorer_on_rpc_response_enqueue(rpc_response_task *resp)
{
    if (resp->get_response()) {
        all_task_explorer::instance().on_message_recv(
            resp->get_response(),
            (int)(message_ext_for_explorer::get(resp->get_response())),
            resp->spec().code);
    } else {
        dsn::task_code caller_code(message_ext_for_explorer::get(resp->get_request()));
        all_task_explorer::instance().on_local_call(caller_code, resp->spec().code);
    }
}

void explorer::install(service_spec &spec)
{
    auto explore = dsn_config_get_value_bool(
        "task..default", "is_explore", true, "whether to explore this kind of task");

    for (int i = 0; i <= dsn::task_code::max(); i++) {
        if (i == TASK_CODE_INVALID)
            continue;

        std::string name(dsn::task_code(i).to_string());
        std::string section_name = std::string("task.") + name;
        task_spec *spec = task_spec::get(i);
        dassert(spec != nullptr, "task_spec cannot be null");
        bool explore2 = dsn_config_get_value_bool(
            section_name.c_str(), "is_explore", explore, "whether to explore this kind of task");

        if (explore2) {
            spec->on_task_create.put_back(explorer_on_task_create, "explorer");
            spec->on_task_begin.put_back(explorer_on_task_begin, "explorer");
            spec->on_rpc_call.put_back(explorer_on_rpc_call, "explorer");
            spec->on_rpc_reply.put_back(explorer_on_rpc_reply, "explorer");
            spec->on_rpc_response_enqueue.put_back(explorer_on_rpc_response_enqueue, "explorer");
        }
    }

    message_ext_for_explorer::register_ext();
    task_ext_for_explorer::register_ext();
    ::dsn::command_manager::instance().register_command(
        {"explore", "exp"},
        "explore the task dependencies as GraphViz dot graph",
        "explore the task dependencies as GraphViz dot graph",
        [](const std::vector<std::string> &args) {
            std::stringstream ss;
            all_task_explorer::instance().get_dot_graph(ss, args);
            return ss.str();
        });
}

explorer::explorer(const char *name) : toollet(name) {}
}
}
