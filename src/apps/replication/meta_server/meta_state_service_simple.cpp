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

namespace dsn
{
    namespace dist
    {
        // path: /, /n1/n2, /n1/n2/, /n2/n2/n3
        std::string meta_state_service_simple::normalize_path(const std::string& s)
        {
            if (s.length() > 1 && *s.rbegin() == '/')
                return s.substr(0, s.length() - 1);
            else
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

        error_code meta_state_service_simple::initialize()
        {
            // TODO: load on-disk files and recover the memory state
            return ERR_OK;
        }

        task_ptr meta_state_service_simple::create_node(
            const std::string& node,
            task_code cb_code,
            const err_callback& cb_create,            
            const blob& value,
            clientlet* tracker )
        {
            auto path = normalize_path(node);

            _state_lock.lock();
            auto it = _quick_map.find(path);
            if (it != _quick_map.end())
            {
                _state_lock.unlock();

                return tasking::enqueue(
                    cb_code,
                    tracker,
                    [=]()
                    {
                        cb_create(ERR_NODE_ALREADY_EXIST);
                    }
                    );

                // TODO: need aio logger is done first
                auto t = tasking::create_late_task(
                    cb_code,
                    cb_create,
                    0,
                    tracker
                    );
                //return t;

                // after logger is done
                __err_cb_bind_and_enqueue(t, ERR_NODE_ALREADY_EXIST, 0);
            }

            std::string name, parent;
            auto err = extract_name_parent_from_path(path, name, parent);

            it = _quick_map.find(parent);
            dassert(it != _quick_map.end(), "inconsistent state between quick map and tree");

            state_node* n = new state_node(name, it->second);
            it->second->children.insert(quick_map::value_type(name, n));
            _quick_map.insert(quick_map::value_type(path, n));

            _state_lock.unlock();

            return tasking::enqueue(
                cb_code,
                tracker,
                [=]()
                {
                    cb_create(ERR_OK);
                }
                );
        }

        task_ptr meta_state_service_simple::delete_node(
            const std::string& node,
            bool recursively_delete,
            task_code cb_code,
            const err_callback& cb_delete,
            clientlet* tracker)
        {
            return nullptr;
        }

        task_ptr meta_state_service_simple::node_exist(
            const std::string& node,
            task_code cb_code,
            const err_callback& cb_exist,
            clientlet* tracker)
        {
            return nullptr;
        }

        task_ptr meta_state_service_simple::get_data(
            const std::string& node,
            task_code cb_code,
            const err_value_callback& cb_get_data,
            clientlet* tracker)
        {
            return nullptr;
        }

        task_ptr meta_state_service_simple::set_data(
            const std::string& node,
            const blob& value,
            task_code cb_code,
            const err_callback& cb_set_data,
            clientlet* tracker)
        {
            return nullptr;
        }

        task_ptr meta_state_service_simple::get_children(
            const std::string& node,
            task_code cb_code,
            const err_stringv_callback& cb_get_children,
            clientlet* tracker)
        {
            return nullptr;
        }
    }
}