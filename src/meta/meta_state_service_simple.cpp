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

#include "meta_state_service_simple.h"

#include <fcntl.h>

#include <stack>
#include <utility>

#include "runtime/task/async_calls.h"
#include "runtime/task/task.h"
#include "utils/filesystem.h"
#include "utils/fmt_logging.h"

namespace dsn {
namespace dist {
// path: /, /n1/n2, /n1/n2/, /n2/n2/n3
std::string meta_state_service_simple::normalize_path(const std::string &s)
{
    if (s.empty() || s[0] != '/')
        return "";
    if (s.length() > 1 && *s.rbegin() == '/')
        return s.substr(0, s.length() - 1);
    return s;
}

error_code meta_state_service_simple::extract_name_parent_from_path(const std::string &s,
                                                                    /*out*/ std::string &name,
                                                                    /*out*/ std::string &parent)
{
    auto pos = s.find_last_of('/');
    if (pos == std::string::npos)
        return ERR_INVALID_PARAMETERS;

    name = s.substr(pos + 1);
    if (pos > 0)
        parent = s.substr(0, pos);
    else
        parent = "/";
    return ERR_OK;
}

static void
__err_cb_bind_and_enqueue(task_ptr lock_task, error_code err, int delay_milliseconds = 0)
{
    auto t = dynamic_cast<error_code_future *>(lock_task.get());
    t->enqueue_with(err, delay_milliseconds);
}

void meta_state_service_simple::write_log(blob &&log_blob,
                                          std::function<error_code()> internal_operation,
                                          task_ptr task)
{
    _log_lock.lock();
    uint64_t log_offset = _offset;
    _offset += log_blob.length();
    auto continuation_task = std::unique_ptr<operation>(new operation(false, [=](bool log_succeed) {
        CHECK(log_succeed, "we cannot handle logging failure now");
        __err_cb_bind_and_enqueue(task, internal_operation(), 0);
    }));
    auto continuation_task_ptr = continuation_task.get();
    _task_queue.emplace(move(continuation_task));
    _log_lock.unlock();

    file::write(_log,
                log_blob.data(),
                log_blob.length(),
                log_offset,
                LPC_META_STATE_SERVICE_SIMPLE_INTERNAL,
                &_tracker,
                [=](error_code err, size_t bytes) {
                    CHECK(err == ERR_OK && bytes == log_blob.length(),
                          "we cannot handle logging failure now");
                    _log_lock.lock();
                    continuation_task_ptr->done = true;
                    while (!_task_queue.empty()) {
                        if (!_task_queue.front()->done) {
                            break;
                        }
                        _task_queue.front()->cb(true);
                        _task_queue.pop();
                    }
                    _log_lock.unlock();
                });
}

error_code meta_state_service_simple::create_node_internal(const std::string &node,
                                                           const blob &value)
{
    auto path = normalize_path(node);
    zauto_lock _(_state_lock);
    auto me_it = _quick_map.find(path);
    if (me_it != _quick_map.end())
        return ERR_NODE_ALREADY_EXIST;

    std::string name, parent;
    auto err = extract_name_parent_from_path(path, name, parent);
    if (err != ERR_OK) {
        return err;
    }

    auto parent_it = _quick_map.find(parent);
    if (parent_it == _quick_map.end())
        return ERR_OBJECT_NOT_FOUND;

    state_node *n = new state_node(name, parent_it->second, value);
    parent_it->second->children.insert(quick_map::value_type(name, n));
    _quick_map.insert(quick_map::value_type(path, n));
    return ERR_OK;
}

error_code meta_state_service_simple::delete_node_internal(const std::string &node, bool recursive)
{
    auto path = normalize_path(node);
    if (path == "/")
        return ERR_INVALID_PARAMETERS; // cannot delete root
    zauto_lock _(_state_lock);
    auto me_it = _quick_map.find(path);
    if (me_it == _quick_map.end())
        return ERR_OBJECT_NOT_FOUND;
    if (!recursive && !me_it->second->children.empty())
        return ERR_INVALID_PARAMETERS;

    struct delete_state
    {
        std::string path;
        state_node *node;
        decltype(state_node::children)::iterator next_child_to_delete;
    };
    std::stack<delete_state> delete_stack;
    delete_stack.push({path, me_it->second, me_it->second->children.begin()});
    for (; !delete_stack.empty();) {
        auto &node_pair = delete_stack.top();
        if (node_pair.node->children.end() == node_pair.next_child_to_delete) {
            auto delnum = _quick_map.erase(node_pair.path);
            CHECK_EQ_MSG(delnum, 1, "inconsistent state between quick map and tree");
            delete node_pair.node;
            delete_stack.pop();
        } else {
            auto child_it = node_pair.next_child_to_delete;
            delete_stack.push({node_pair.path + "/" + child_it->second->name,
                               child_it->second,
                               child_it->second->children.begin()});
            ++node_pair.next_child_to_delete;
        }
    }

    std::string name, parent;
    auto err = extract_name_parent_from_path(path, name, parent);
    if (err != ERR_OK) {
        return err;
    }

    auto parent_it = _quick_map.find(parent);
    CHECK(parent_it != _quick_map.end(), "unable to find parent node");
    // XXX we cannot delete root, right?

    auto erase_num = parent_it->second->children.erase(name);
    CHECK_EQ_MSG(erase_num, 1, "inconsistent state between quick map and tree");
    return ERR_OK;
}

error_code meta_state_service_simple::set_data_internal(const std::string &node, const blob &value)
{
    auto path = normalize_path(node);
    zauto_lock _(_state_lock);
    auto it = _quick_map.find(path);
    if (it == _quick_map.end())
        return ERR_OBJECT_NOT_FOUND;
    it->second->data = value;
    return ERR_OK;
}

error_code meta_state_service_simple::apply_transaction(
    const std::shared_ptr<meta_state_service::transaction_entries> &t_entries)
{
    LOG_DEBUG("internal operation after logged");
    simple_transaction_entries *entries =
        dynamic_cast<simple_transaction_entries *>(t_entries.get());
    CHECK_NOTNULL(entries, "invalid input parameter");
    error_code ec;
    for (int i = 0; i != entries->_offset; ++i) {
        operation_entry &e = entries->_ops[i];
        switch (e._type) {
        case operation_type::create_node:
            ec = create_node_internal(e._node, e._value);
            break;
        case operation_type::delete_node:
            ec = delete_node_internal(e._node, false);
            break;
        case operation_type::set_data:
            ec = set_data_internal(e._node, e._value);
            break;
        default:
            CHECK(false, "unsupported operation");
        }
        CHECK_EQ_MSG(ec, ERR_OK, "unexpected error when applying");
    }

    return ERR_OK;
}

error_code meta_state_service_simple::initialize(const std::vector<std::string> &args)
{
    const char *work_dir =
        args.empty() ? service_app::current_service_app_info().data_dir.c_str() : args[0].c_str();

    _offset = 0;
    std::string log_path = dsn::utils::filesystem::path_combine(work_dir, "meta_state_service.log");
    if (utils::filesystem::file_exists(log_path)) {
        if (FILE *fd = fopen(log_path.c_str(), "rb")) {
            for (;;) {
                log_header header;
                if (fread(&header, sizeof(log_header), 1, fd) != 1) {
                    break;
                }
                if (header.magic != log_header::default_magic) {
                    break;
                }
                std::shared_ptr<char> buffer(dsn::utils::make_shared_array<char>(header.size));
                if (fread(buffer.get(), header.size, 1, fd) != 1) {
                    break;
                }
                _offset += sizeof(header) + header.size;
                binary_reader reader(blob(buffer, (int)header.size));
                int op_type = 0;
                reader.read(op_type);

                switch (static_cast<operation_type>(op_type)) {
                case operation_type::create_node: {
                    std::string node;
                    blob data;
                    create_node_log::parse(reader, node, data);
                    create_node_internal(node, data);
                    break;
                }
                case operation_type::delete_node: {
                    std::string node;
                    bool recursively_delete;
                    delete_node_log::parse(reader, node, recursively_delete);
                    delete_node_internal(node, recursively_delete);
                    break;
                }
                case operation_type::set_data: {
                    std::string node;
                    blob data;
                    set_data_log::parse(reader, node, data);
                    set_data_internal(node, data);
                    break;
                }
                default:
                    // The log is complete but its content is modified by cosmic ray. This is
                    // unacceptable
                    CHECK(false, "meta state server log corrupted");
                }
            }
            fclose(fd);
        }
    }

    _log = file::open(log_path.c_str(), O_RDWR | O_CREAT | O_BINARY, 0666);
    if (!_log) {
        LOG_ERROR("open file failed: %s", log_path.c_str());
        return ERR_FILE_OPERATION_FAILED;
    }
    return ERR_OK;
}

std::shared_ptr<meta_state_service::transaction_entries>
meta_state_service_simple::new_transaction_entries(unsigned int capacity)
{
    return std::shared_ptr<meta_state_service::transaction_entries>(
        new meta_state_service_simple::simple_transaction_entries(capacity));
}

task_ptr meta_state_service_simple::submit_transaction(
    /*in-out*/ const std::shared_ptr<meta_state_service::transaction_entries> &t_entries,
    task_code cb_code,
    const err_callback &cb_transaction,
    dsn::task_tracker *tracker)
{
    // when checking the snapshot, we block all write operations which come later
    zauto_lock l(_log_lock);
    std::set<std::string> snapshot;
    for (const auto &kv : _quick_map)
        snapshot.insert(kv.first);

    // try
    simple_transaction_entries *entries =
        dynamic_cast<simple_transaction_entries *>(t_entries.get());
    std::string parent, name;
    size_t i;

    std::vector<blob> batch_buffer;
    int total_size = 0;
    batch_buffer.reserve(entries->_offset);

    for (i = 0; i != entries->_offset; ++i) {
        operation_entry &op = entries->_ops[i];
        op._node = normalize_path(op._node);

        switch (op._type) {
        case operation_type::create_node: {
            op._result = extract_name_parent_from_path(op._node, name, parent);
            if (op._result == ERR_OK) {
                if (snapshot.find(parent) == snapshot.end())
                    op._result = ERR_OBJECT_NOT_FOUND;
                else if (snapshot.find(op._node) != snapshot.end())
                    op._result = ERR_NODE_ALREADY_EXIST;
                else {
                    batch_buffer.push_back(create_node_log::get_log(op._node, op._value));
                    total_size += batch_buffer.back().length();
                    snapshot.insert(op._node);
                    op._result = ERR_OK;
                }
            }
        } break;
        case operation_type::delete_node: {
            if (snapshot.find(op._node) == snapshot.end()) {
                op._result = ERR_OBJECT_NOT_FOUND;
            } else if (op._node == "/") {
                // delete root is forbidden
                op._result = ERR_INVALID_PARAMETERS;
            } else {
                op._node.push_back('/');
                std::set<std::string>::iterator iter = snapshot.lower_bound(op._node);
                if (iter != snapshot.end() && (*iter).length() >= op._node.length() &&
                    memcmp((*iter).c_str(), op._node.c_str(), op._node.length()) == 0) {
                    // op._node is the prefix of some path, so we regard this directory as not empty
                    op._result = ERR_INVALID_PARAMETERS;
                } else {
                    batch_buffer.push_back(delete_node_log::get_log(op._node, false));
                    total_size += batch_buffer.back().length();
                    op._node.pop_back();
                    snapshot.erase(op._node);
                    op._result = ERR_OK;
                }
            }
        } break;
        case operation_type::set_data: {
            if (snapshot.find(op._node) == snapshot.end())
                op._result = ERR_OBJECT_NOT_FOUND;
            else {
                batch_buffer.push_back(set_data_log::get_log(op._node, op._value));
                total_size += batch_buffer.back().length();
                op._result = ERR_OK;
            }
        } break;
        default:
            CHECK(false, "not supported operation");
            break;
        }

        if (op._result != ERR_OK)
            break;
    }

    if (i < entries->_offset) {
        for (int j = i + 1; j != entries->_offset; ++j)
            entries->_ops[j]._result = ERR_INCONSISTENT_STATE;
        return tasking::enqueue(
            cb_code, tracker, [=]() { cb_transaction(ERR_INCONSISTENT_STATE); });
    } else {
        // apply
        std::shared_ptr<char> batch(dsn::utils::make_shared_array<char>(total_size));
        char *dest = batch.get();
        std::for_each(batch_buffer.begin(), batch_buffer.end(), [&dest](const blob &entry) {
            memcpy(dest, entry.data(), entry.length());
            dest += entry.length();
        });
        CHECK_EQ_MSG(dest - batch.get(), total_size, "memcpy error");
        task_ptr task(new error_code_future(cb_code, cb_transaction, 0));
        task->set_tracker(tracker);
        write_log(blob(batch, total_size),
                  [this, t_entries] { return apply_transaction(t_entries); },
                  task);
        return task;
    }
}

task_ptr meta_state_service_simple::create_node(const std::string &node,
                                                task_code cb_code,
                                                const err_callback &cb_create,
                                                const blob &value,
                                                dsn::task_tracker *tracker)
{
    task_ptr task(new error_code_future(cb_code, cb_create, 0));
    task->set_tracker(tracker);
    write_log(create_node_log::get_log(node, value),
              [=] { return create_node_internal(node, value); },
              task);
    return task;
}

task_ptr meta_state_service_simple::delete_node(const std::string &node,
                                                bool recursively_delete,
                                                task_code cb_code,
                                                const err_callback &cb_delete,
                                                dsn::task_tracker *tracker)
{
    task_ptr task(new error_code_future(cb_code, cb_delete, 0));
    task->set_tracker(tracker);
    write_log(delete_node_log::get_log(node, recursively_delete),
              [=] { return delete_node_internal(node, recursively_delete); },
              task);
    return task;
}

task_ptr meta_state_service_simple::node_exist(const std::string &node,
                                               task_code cb_code,
                                               const err_callback &cb_exist,
                                               dsn::task_tracker *tracker)
{
    error_code err;
    {
        zauto_lock _(_state_lock);
        err = _quick_map.find(normalize_path(node)) != _quick_map.end() ? ERR_OK
                                                                        : ERR_OBJECT_NOT_FOUND;
    }
    return tasking::enqueue(cb_code, tracker, [=]() { cb_exist(err); });
}

task_ptr meta_state_service_simple::get_data(const std::string &node,
                                             task_code cb_code,
                                             const err_value_callback &cb_get_data,
                                             dsn::task_tracker *tracker)
{
    auto path = normalize_path(node);
    zauto_lock _(_state_lock);
    auto me_it = _quick_map.find(path);
    if (me_it == _quick_map.end()) {
        return tasking::enqueue(cb_code, tracker, [=]() { cb_get_data(ERR_OBJECT_NOT_FOUND, {}); });
    } else {
        auto data_copy = me_it->second->data;
        return tasking::enqueue(
            cb_code, tracker, [=]() mutable { cb_get_data(ERR_OK, std::move(data_copy)); });
    }
}

task_ptr meta_state_service_simple::set_data(const std::string &node,
                                             const blob &value,
                                             task_code cb_code,
                                             const err_callback &cb_set_data,
                                             dsn::task_tracker *tracker)
{
    task_ptr task(new error_code_future(cb_code, cb_set_data, 0));
    task->set_tracker(tracker);
    write_log(
        set_data_log::get_log(node, value), [=] { return set_data_internal(node, value); }, task);
    return task;
}

task_ptr meta_state_service_simple::get_children(const std::string &node,
                                                 task_code cb_code,
                                                 const err_stringv_callback &cb_get_children,
                                                 dsn::task_tracker *tracker)
{
    auto path = normalize_path(node);
    zauto_lock _(_state_lock);
    auto me_it = _quick_map.find(path);
    if (me_it == _quick_map.end()) {
        return tasking::enqueue(
            cb_code, tracker, [=]() { cb_get_children(ERR_OBJECT_NOT_FOUND, {}); });
    } else {
        std::vector<std::string> result;
        for (auto &child_pair : me_it->second->children) {
            result.push_back(child_pair.first);
        }
        return tasking::enqueue(
            cb_code, tracker, [=]() mutable { cb_get_children(ERR_OK, move(result)); });
    }
}

meta_state_service_simple::~meta_state_service_simple()
{
    _tracker.cancel_outstanding_tasks();
    file::close(_log);

    for (const auto &kv : _quick_map) {
        if ("/" != kv.first) {
            delete kv.second;
        }
    }
    _quick_map.clear();
}
}
}
