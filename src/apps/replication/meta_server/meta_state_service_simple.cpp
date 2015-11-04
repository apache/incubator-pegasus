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
        DEFINE_TASK_CODE(LPC_META_STATE_SVC_CALLBACK, TASK_PRIORITY_COMMON, THREAD_POOL_META_SERVER);

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

        void meta_state_service_simple::create_node(const std::string& node,
            const err_callback& cb_create,
            const std::string& value)
        {
            auto path = normalize_path(node);

            _state_lock.lock();
            auto it = _quick_map.find(path);
            if (it != _quick_map.end())
            {
                _state_lock.unlock();

                tasking::enqueue(
                    LPC_META_STATE_SVC_CALLBACK,
                    this,
                    [=]()
                    {
                        cb_create(ERR_NODE_ALREADY_EXIST);
                    }
                    );
                return;
            }

            std::string name, parent;
            auto err = extract_name_parent_from_path(path, name, parent);

            it = _quick_map.find(parent);
            dassert(it != _quick_map.end(), "inconsistent state between quick map and tree");

            state_node* n = new state_node(name, it->second);
            it->second->children.insert(quick_map::value_type(name, n));
            _quick_map.insert(quick_map::value_type(path, n));

            _state_lock.unlock();

            tasking::enqueue(
                LPC_META_STATE_SVC_CALLBACK,
                this,
                [=]()
            {
                cb_create(ERR_OK);
            }
            );
        }

        void meta_state_service_simple::delete_node(const std::string& node,
            bool recursively_delete,
            const err_callback& cb_delete)
        {

        }

        void meta_state_service_simple::node_exist(const std::string& node,
            const err_callback& cb_exist)
        {

        }

        void meta_state_service_simple::get_data(const std::string& node,
            const err_string_callback& cb_get_data)
        {

        }

        void meta_state_service_simple::set_data(const std::string& node,
            const std::string& value,
            const err_callback& cb_set_data)
        {

        }

        void meta_state_service_simple::get_children(const std::string& node,
            const err_stringv_callback& cb_get_children)
        {

        }
    }
}