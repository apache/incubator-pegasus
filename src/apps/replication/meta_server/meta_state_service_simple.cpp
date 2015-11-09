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
 *     2015-11-04, @imzhenyu (Zhenyu.Guo@microsoft.com), setup the sketch and a sample api impl
 *     xxxx-xx-xx, author, fix bug about xxx
 */

# include "meta_state_service_simple.h"

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
                parent = s.substr(pos - 1);
            else
                parent = "";

            // TODO: what if no parent

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
                    return std::bind(cb, err);
                },
                delay_milliseconds
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

            auto parent_it = _quick_map.find(parent);
            if (parent_it == _quick_map.end())
                return ERR_INVALID_PARAMETERS;

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
                return ERR_INVALID_PARAMETERS;
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
                return ERR_INVALID_PARAMETERS;
            it->second->data = value;
            return ERR_OK;
        }

        void meta_state_service_simple::log_continuation(operation* succeed_operation, bool log_succeed)
        {
            dassert(log_succeed, "we cannot handle logging failure now");
            _log_lock.lock();
            succeed_operation->done = true;
            while (!_task_queue.empty())
            {
                if (!_task_queue.front()->done)
                {
                    break;
                }
                _task_queue.front()->cb(log_succeed);
                _task_queue.pop();
            }
            _log_lock.unlock();
        }

        error_code meta_state_service_simple::initialize()
        {
            _offset = 0;
            std::string log_path = "meta_state_service.log";
            if (utils::filesystem::file_exists(log_path))
            {
                if (FILE* fd = fopen(log_path.c_str(), "rb"))
                {
                    for (;;)
                    {
                        char magic_size_buffer[sizeof(int) + sizeof(size_t)];
                        if (fread(magic_size_buffer, sizeof(size_t), 1, fd) != sizeof(sizeof(int) + sizeof(size_t)))
                        {
                            break;
                        }
                        
                        int magic = *reinterpret_cast<int*>(magic_size_buffer);
                        if (magic != 0xdeadbeef)
                        {
                            break;
                        }
                        size_t size = *reinterpret_cast<size_t*>(magic_size_buffer + sizeof(int));
                        std::shared_ptr<char> buffer(new char[size]);
                        //TODO error handling, check size or check std::bad_alloc
                        if (fread(buffer.get(), size, 1, fd) != size)
                        {
                            break;
                        }
                        _offset += size;
                        dsn::blob blob_wrapper(buffer, size);
                        binary_reader reader(blob_wrapper);
                        int8_t op_type;
                        unmarshall(reader, op_type);
                        switch (static_cast<operation_type>(op_type))
                        {
                        case operation_type::create_node:
                        {
                            std::string node;
                            ::dsn::blob data;
                            unmarshall(reader, node);
                            unmarshall(reader, data);
                            create_node_internal(node, data);
                            break;
                        }
                        case operation_type::delete_node:
                        {
                            std::string node;
                            bool recursively_delete;
                            unmarshall(reader, node);
                            unmarshall(reader, recursively_delete);
                            delete_node_internal(node, recursively_delete);
                            break;
                        }
                        case operation_type::set_data:
                        {
                            std::string node;
                            ::dsn::blob data;
                            unmarshall(reader, node);
                            unmarshall(reader, data);
                            set_data_internal(node, data);
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
            return ERR_OK;
        }

        task_ptr meta_state_service_simple::create_node(
            const std::string& node,
            task_code cb_code,
            const err_callback& cb_create,            
            const blob& value,
            clientlet* tracker )
        {
            binary_writer temperal_writer;
            marshall(temperal_writer, static_cast<int8_t>(operation_type::create_node));
            marshall(temperal_writer, node);
            marshall(temperal_writer, value);

            binary_writer log_writer;
            int magic = 0xdeadbeef;
            log_writer.write(magic);
            log_writer.write(static_cast<size_t>(log_writer.total_size()));
            log_writer.write(log_writer.get_buffer());
            
            size_t log_size = log_writer.total_size();
            _log_lock.lock();
            uint64_t log_offset = _offset;
            _offset += log_writer.total_size();

            auto returned_task_ptr = tasking::create_late_task(cb_code, cb_create, 0, tracker);
            auto continuation_task = std::make_unique<operation>(false, [=](bool log_succeed)
            {
                dassert(log_succeed, "we cannot handle logging failure now");
                __err_cb_bind_and_enqueue(returned_task_ptr, create_node_internal(node, value), 0);
            });
            auto continuation_task_ptr = continuation_task.get();
            _task_queue.emplace(std::move(continuation_task));
            _log_lock.unlock();
             file::write(
                _log,
                log_writer.get_buffer().buffer_ptr(),
                log_writer.total_size(),
                log_offset,
                META_STATE_SERVICE_SIMPLE_INTERNAL_TASK,
                this,
                [=](error_code err, size_t bytes)
                {
                    log_continuation(continuation_task_ptr, !err && bytes == log_size);
                }
                );
             return returned_task_ptr;
        }

        task_ptr meta_state_service_simple::delete_node(
            const std::string& node,
            bool recursively_delete,
            task_code cb_code,
            const err_callback& cb_delete,
            clientlet* tracker)
        {
            binary_writer temperal_writer;
            marshall(temperal_writer, static_cast<int8_t>(operation_type::delete_node));
            marshall(temperal_writer, node);
            marshall(temperal_writer, recursively_delete);

            binary_writer log_writer;
            int magic = 0xdeadbeef;
            log_writer.write(magic);
            log_writer.write(static_cast<size_t>(log_writer.total_size()));
            log_writer.write(log_writer.get_buffer());

            size_t log_size = log_writer.total_size();
            _log_lock.lock();
            uint64_t log_offset = _offset;
            _offset += log_writer.total_size();

            auto returned_task_ptr = tasking::create_late_task(cb_code, cb_delete, 0, tracker);
            auto continuation_task = std::make_unique<operation>(false, [=](bool log_succeed)
            {
                dassert(log_succeed, "we cannot handle logging failure now");
                __err_cb_bind_and_enqueue(returned_task_ptr, delete_node_internal(node, recursively_delete), 0);
            });
            auto continuation_task_ptr = continuation_task.get();
            _task_queue.emplace(std::move(continuation_task));

            _log_lock.unlock();

            file::write(
                _log,
                log_writer.get_buffer().buffer_ptr(),
                log_writer.total_size(),
                log_offset,
                META_STATE_SERVICE_SIMPLE_INTERNAL_TASK,
                this,
                [=](error_code err, size_t bytes)
                {
                    log_continuation(continuation_task_ptr, !err && bytes == log_size);
                }
                );
            return returned_task_ptr;
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
                        cb_get_data(ERR_INVALID_PARAMETERS, {});
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
            binary_writer temperal_writer;
            marshall(temperal_writer, static_cast<int8_t>(operation_type::set_data));
            marshall(temperal_writer, node);
            marshall(temperal_writer, value);

            binary_writer log_writer;
            int magic = 0xdeadbeef;
            log_writer.write(magic);
            log_writer.write(static_cast<size_t>(log_writer.total_size()));
            log_writer.write(log_writer.get_buffer());

            size_t log_size = log_writer.total_size();
            _log_lock.lock();
            uint64_t log_offset = _offset;
            _offset += log_writer.total_size();

            auto returned_task_ptr = tasking::create_late_task(cb_code, cb_set_data, 0, tracker);
            auto continuation_task = std::make_unique<operation>(false, [=](bool log_succeed)
            {
                dassert(log_succeed, "we cannot handle logging failure now");
                __err_cb_bind_and_enqueue(returned_task_ptr, set_data_internal(node, value), 0);
            });
            auto continuation_task_ptr = continuation_task.get();
            _task_queue.emplace(std::move(continuation_task));

            _log_lock.unlock();

            file::write(
                _log,
                log_writer.get_buffer().buffer_ptr(),
                log_writer.total_size(),
                log_offset,
                META_STATE_SERVICE_SIMPLE_INTERNAL_TASK,
                this,
                [=](error_code err, size_t bytes)
                {
                    log_continuation(continuation_task_ptr, !err && bytes == log_size);
                }
                );
            return returned_task_ptr;
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
                        cb_get_children(ERR_INVALID_PARAMETERS, {});
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
                        cb_get_children(ERR_OK, std::move(result));
                    }
                    );
            }
        }
    }
}