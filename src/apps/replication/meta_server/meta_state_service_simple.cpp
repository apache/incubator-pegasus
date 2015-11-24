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
 *     a simple version of meta state service for development
 *
 * Revision history:
 *     2015-11-04, @imzhenyu (Zhenyu.Guo@microsoft.com), setup the sketch
 *     2015-11-11, Tianyi WANG, first version done
 *     xxxx-xx-xx, author, fix bug about xxx
 */

# include "meta_state_service_simple.h"
# include <dsn/internal/task.h>

# include <stack>
# include <utility>

namespace dsn
{
    namespace dist
    {
        // path: /, /n1/n2, /n1/n2/, /n2/n2/n3
        std::string meta_state_service_simple::normalize_path(const std::string& s)
        {
            if (s.empty() || s[0] != '/')
                return "";
            if (s.length() > 1 && *s.rbegin() == '/')
                return s.substr(0, s.length() - 1);
            return s;
        }

        error_code meta_state_service_simple::extract_name_parent_from_path(
            const std::string& s,
            /*out*/ std::string& name,
            /*out*/ std::string& parent
            )
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

        static void __err_cb_bind_and_enqueue(
            task_ptr lock_task,
            error_code err,
            int delay_milliseconds = 0
            )
        {
            auto t = dynamic_cast<safe_late_task<meta_state_service::err_callback>*>(lock_task.get());

            t->bind_and_enqueue(
                [&](meta_state_service::err_callback& cb)
                {
                    return bind(cb, err);
                },
                delay_milliseconds
                );
        }

        void meta_state_service_simple::write_log(blob&& log_blob, std::function<error_code()> internal_operation, task_ptr task)
        {
            _log_lock.lock();
            uint64_t log_offset = _offset;
            _offset += log_blob.length();
            auto continuation_task = std::unique_ptr<operation>(new operation(false, [=](bool log_succeed)
            {
                dassert(log_succeed, "we cannot handle logging failure now");
                __err_cb_bind_and_enqueue(task, internal_operation(), 0);
            }));
            auto continuation_task_ptr = continuation_task.get();
            _task_queue.emplace(move(continuation_task));
            _log_lock.unlock();

            file::write(
                _log,
                log_blob.data(),
                log_blob.length(),
                log_offset,
                LPC_META_STATE_SERVICE_SIMPLE_INTERNAL,
                this,
                [=](error_code err, size_t bytes)
                {
                    dassert(err == ERR_OK && bytes == log_blob.length(), "we cannot handle logging failure now");
                    _log_lock.lock();
                    continuation_task_ptr->done = true;
                    while (!_task_queue.empty())
                    {
                        if (!_task_queue.front()->done)
                        {
                            break;
                        }
                        _task_queue.front()->cb(true);
                        _task_queue.pop();
                    }
                    _log_lock.unlock();
                }
                );
        }



        error_code meta_state_service_simple::create_node_internal(const std::string& node, const blob& value)
        {
            auto path = normalize_path(node);
            zauto_lock _(_state_lock);
            auto me_it = _quick_map.find(path);
            if (me_it != _quick_map.end())
                return ERR_NODE_ALREADY_EXIST;

            std::string name, parent;
            auto err = extract_name_parent_from_path(path, name, parent);
            if (err != ERR_OK)
            {
                return err;
            }

            auto parent_it = _quick_map.find(parent);
            if (parent_it == _quick_map.end())
                return ERR_OBJECT_NOT_FOUND;

            state_node* n = new state_node(name, parent_it->second, value);
            parent_it->second->children.insert(quick_map::value_type(name, n));
            _quick_map.insert(quick_map::value_type(path, n));
            return ERR_OK;
        }

        error_code meta_state_service_simple::delete_node_internal(const std::string& node, bool recursive)
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
            delete_stack.push({ path, me_it->second, me_it->second->children.begin() });
            for (; !delete_stack.empty();)
            {
                auto &node_pair = delete_stack.top();
                if (node_pair.node->children.end() == node_pair.next_child_to_delete)
                {
                    auto delnum = _quick_map.erase(node_pair.path);
                    dassert(delnum == 1, "inconsistent state between quick map and tree");
                    delete node_pair.node;
                    delete_stack.pop();
                }
                else
                {
                    auto child_it = node_pair.next_child_to_delete;
                    delete_stack.push({ node_pair.path + "/" + child_it->second->name, child_it->second, child_it->second->children.begin() });
                    ++node_pair.next_child_to_delete;
                }
            }

            std::string name, parent;
            auto err = extract_name_parent_from_path(path, name, parent);
            if (err != ERR_OK)
            {
                return err;
            }

            auto parent_it = _quick_map.find(parent);
            dassert(parent_it != _quick_map.end(), "unable to find parent node");
            //XXX we cannot delete root, right?

            auto erase_num = parent_it->second->children.erase(name);
            dassert(erase_num == 1, "inconsistent state between quick map and tree");
            return ERR_OK;
        }

        error_code meta_state_service_simple::set_data_internal(const std::string& node, const blob& value)
        {
            auto path = normalize_path(node);
            zauto_lock _(_state_lock);
            auto it = _quick_map.find(path);
            if (it == _quick_map.end())
                return ERR_OBJECT_NOT_FOUND;
            it->second->data = value;
            return ERR_OK;
        }

        error_code meta_state_service_simple::initialize(const char* dir)
        {
            _offset = 0;
            std::string log_path = dsn::utils::filesystem::path_combine(dir, "meta_state_service.log");
            if (utils::filesystem::file_exists(log_path))
            {
                if (FILE* fd = fopen(log_path.c_str(), "rb"))
                {
                    for (;;)
                    {
                        log_header header;
                        if (fread(&header, sizeof(log_header), 1, fd) != 1)
                        {
                            break;
                        }
                        if (header.magic != log_header::default_magic)
                        {
                            break;
                        }
                        std::shared_ptr<char> buffer(new char[header.size]);
                        if (fread(buffer.get(), header.size, 1, fd) != 1)
                        {
                            break;
                        }
                        _offset += sizeof(header) + header.size;
                        blob blob_wrapper(buffer, (int)header.size);
                        binary_reader reader(blob_wrapper);
                        int op_type;
                        unmarshall(reader, op_type);
                        switch (static_cast<operation_type>(op_type))
                        {
                        case operation_type::create_node:
                        {
                            std::string node;
                            blob data;
                            create_node_log::parse(reader, node, data);
                            create_node_internal(node, data).end_tracking();
                            break;
                        }
                        case operation_type::delete_node:
                        {
                            std::string node;
                            bool recursively_delete;
                            delete_node_log::parse(reader, node, recursively_delete);
                            delete_node_internal(node, recursively_delete).end_tracking();
                            break;
                        }
                        case operation_type::set_data:
                        {
                            std::string node;
                            blob data;
                            set_data_log::parse(reader, node, data);
                            set_data_internal(node, data).end_tracking();
                            break;
                        }
                        default:
                            //The log is complete but its content is modified by cosmic ray. This is unacceptable
                            dassert(false, "meta state server log corrupted");
                        }   
                    }
                    fclose(fd);
                }
            }

            _log = dsn_file_open(log_path.c_str(), O_RDWR | O_CREAT | O_BINARY, 0666);
            if (!_log)
            {
                derror("open file failed: %s", log_path.c_str());
                return ERR_FILE_OPERATION_FAILED;
            }
            return ERR_OK;
        }

        task_ptr meta_state_service_simple::create_node(
            const std::string& node,
            task_code cb_code,
            const err_callback& cb_create,            
            const blob& value,
            clientlet* tracker )
        {
            task_ptr task = tasking::create_late_task(cb_code, cb_create, 0, tracker);
            write_log(
                create_node_log::get_log(node, value),
                [=]{
                    return create_node_internal(node, value);
                },
                task
                );
            return task;
        }

        task_ptr meta_state_service_simple::delete_node(
            const std::string& node,
            bool recursively_delete,
            task_code cb_code,
            const err_callback& cb_delete,
            clientlet* tracker)
        {
            task_ptr task = tasking::create_late_task(cb_code, cb_delete, 0, tracker);
            write_log(
                delete_node_log::get_log(node, recursively_delete),
                [=] {
                    return delete_node_internal(node, recursively_delete);
                },
                task
                );
            return task;
        }

        task_ptr meta_state_service_simple::node_exist(
            const std::string& node,
            task_code cb_code,
            const err_callback& cb_exist,
            clientlet* tracker)
        {
            error_code err;
            {
                zauto_lock _(_state_lock);
                err = _quick_map.find(normalize_path(node)) != _quick_map.end() ? ERR_OK : ERR_PATH_NOT_FOUND;
            }
            return tasking::enqueue(
                cb_code,
                tracker,
                [=]()
                {
                    cb_exist(err);
                }
                );  
        }

        task_ptr meta_state_service_simple::get_data(
            const std::string& node,
            task_code cb_code,
            const err_value_callback& cb_get_data,
            clientlet* tracker)
        {
            auto path = normalize_path(node);
            zauto_lock _(_state_lock);
            auto me_it = _quick_map.find(path);
            if (me_it == _quick_map.end())
            {
                return tasking::enqueue(
                    cb_code,
                    tracker,
                    [=]()
                    {
                        cb_get_data(ERR_OBJECT_NOT_FOUND, {});
                    }
                    );
            }
            else
            {
                auto data_copy = me_it->second->data;
                return tasking::enqueue(
                    cb_code,
                    tracker,
                    [=]() mutable
                    {
                        cb_get_data(ERR_OK, std::move(data_copy));
                    }
                    );
            }
        }

        task_ptr meta_state_service_simple::set_data(
            const std::string& node,
            const blob& value,
            task_code cb_code,
            const err_callback& cb_set_data,
            clientlet* tracker)
        {
            task_ptr task = tasking::create_late_task(cb_code, cb_set_data, 0, tracker);
            write_log(
                set_data_log::get_log(node, value),
                [=] {
                    return set_data_internal(node, value);
                },
                task
                );
            return task;
        }

        task_ptr meta_state_service_simple::get_children(
            const std::string& node,
            task_code cb_code,
            const err_stringv_callback& cb_get_children,
            clientlet* tracker)
        {
            auto path = normalize_path(node);
            zauto_lock _(_state_lock);
            auto me_it = _quick_map.find(path);
            if (me_it == _quick_map.end())
            {
                return tasking::enqueue(
                    cb_code,
                    tracker,
                    [=]()
                    {
                        cb_get_children(ERR_OBJECT_NOT_FOUND, {});
                    }
                    );
            }
            else
            {
                std::vector<std::string> result;
                for (auto &child_pair : me_it->second->children)
                {
                    result.push_back(child_pair.first);
                }
                return tasking::enqueue(
                    cb_code,
                    tracker,
                    [=]() mutable
                    {
                        cb_get_children(ERR_OK, move(result));
                    }
                    );
            }
        }

        meta_state_service_simple::~meta_state_service_simple()
        {
            dsn_file_close(_log);
        }
    }
}
