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
 *     2015-11-03, @imzhenyu (Zhenyu.Guo@microsoft.com), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

# include <dsn/dist/meta_state_service.h>
# include "replication_common.h"

using namespace ::dsn::service;

namespace dsn
{
    namespace dist
    {
        class meta_state_service_simple 
            : public meta_state_service, public clientlet
        {
        public:
            meta_state_service_simple() : _root("/", nullptr) {}

            virtual error_code initialize() override;

            virtual task_ptr create_node(
                const std::string& node,
                task_code cb_code,
                const err_callback& cb_create,
                const blob& value = blob(),
                clientlet* tracker = nullptr
                ) override;

            virtual task_ptr delete_node(
                const std::string& node,
                bool recursively_delete,
                task_code cb_code,
                const err_callback& cb_delete,
                clientlet* tracker = nullptr) override;

            virtual task_ptr node_exist(
                const std::string& node,
                task_code cb_code,
                const err_callback& cb_exist,
                clientlet* tracker = nullptr) override;

            virtual task_ptr get_data(
                const std::string& node,
                task_code cb_code,
                const err_value_callback& cb_get_data,
                clientlet* tracker = nullptr) override;

            virtual task_ptr set_data(
                const std::string& node,
                const blob& value,
                task_code cb_code,
                const err_callback& cb_set_data,
                clientlet* tracker = nullptr) override;

            virtual task_ptr get_children(
                const std::string& node,
                task_code cb_code,
                const err_stringv_callback& cb_get_children,
                clientlet* tracker = nullptr) override;

            virtual void watch(const std::string& node,
                const std::string& watcher_id,
                task_code cb_code,
                const err_value_callback& cb_notify
                ) override;

            virtual void unwatch(const std::string& node, const std::string& watcher_id) override;

        private:
            std::string normalize_path(const std::string& s);
            error_code extract_name_parent_from_path(
                const std::string& s,
                /*out*/ std::string& name,
                /*out*/ std::string& parent
                );

        private:
            struct state_node
            {
                std::string name;
                dsn::blob   data;

                state_node* parent;
                std::unordered_map<std::string, state_node*> children;

                state_node(const std::string& nm, state_node* pt)
                    : name(nm), parent(pt)
                {}
            };
            
            typedef std::unordered_map<std::string, state_node*> quick_map;

            zlock          _state_lock;
            state_node     _root;  // tree          
            quick_map      _quick_map; // <path, node*>

            zlock          _log_lock;
            dsn_handle_t   _log;
            uint64_t       _offset;
        };
    }
}

