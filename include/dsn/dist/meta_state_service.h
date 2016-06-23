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
 *     interface of the reliable meta state service
 *     it is usually for storing meta state of dist systems, such as membership
 *
 * Revision history:
 *     2015-10-28, Weijie Sun, first version
 *     2015-11-05, @imzhenyu (Zhenyu Guo), adjust the interface, so that
 *                (1) return task_ptr/tracker for callers to cancel or wait;
 *                (2) add factory for provider registration; 
 *                (3) add cb_code parameter, then users can specify where the callback 
 *                    should be executed
 *     2015-11-06, @imzhenyu (Zhenyu Guo), add watch/unwatch API
 *     2015-12-28, @shengofsun (Weijie SUn), add transaction api
 *     xxxx-xx-xx, author, fix bug about xxx
 */

# pragma once

#include <dsn/service_api_cpp.h>
#include <dsn/dist/error_code.h>
#include <string>
#include <functional>

namespace dsn
{
    namespace dist
    {
        class meta_state_service
        {
        public:
            template <typename T> static meta_state_service* create()
            {
                return new T();
            }

            typedef meta_state_service* (*factory)();

        public:
            typedef std::function<void (error_code ec, const blob& val)> err_value_callback;
            typedef std::function<void (error_code ec, const std::vector<std::string>& ret_strv)> err_stringv_callback;
            typedef std::function<void (error_code ec)> err_callback;

            /* providers should implement this to support transaction */
            class transaction_entries {
            public:
                virtual ~transaction_entries() {}
                virtual error_code create_node(const std::string& node, const blob& value = blob()) = 0;
                virtual error_code delete_node(const std::string& node) = 0;
                virtual error_code set_data(const std::string& node, const blob& value = blob()) = 0;

                virtual error_code get_result(unsigned int entry_index) = 0;
            };

        public:
            virtual ~meta_state_service() {}
            /*
             * initialization work
             */
            virtual error_code initialize(const std::vector<std::string>& args) = 0;

            /*
             * finalize work
             */
            virtual error_code finalize() = 0;

            /*
             * create a transaction_entries structure
             * capacity: the maximum entries the structure can hold
             */
            virtual std::shared_ptr<transaction_entries> new_transaction_entries(unsigned int capacity) = 0;

            /*
             * submit transaction, it should be all succeeded or all failed
             * cb_code: the task code specifies where to execute the callback
             * cb_transaction: callback, ec to indicate success or failure reason
             * tracker: to track (wait/cancel) whether the callback is executed
             */
            virtual task_ptr submit_transaction(const std::shared_ptr<transaction_entries>& entries,
                                      task_code cb_code,
                                      const err_callback& cb_transaction,
                                      clientlet* tracker = nullptr) = 0;

            /*
             * create a dir node
             * node: the dir name with full path
             * cb_code: the task code specifies where to execute the callback
             * cb_create: create callback, ec to indicate success or failure reason
             * value: the data value to store in the node
             * tracker: to track (wait/cancel) whether the callback is executed
             */
            virtual task_ptr create_node(const std::string& node,
                                     task_code cb_code,
                                     const err_callback& cb_create,                                     
                                     const blob& value = blob(),
                                     clientlet* tracker = nullptr) = 0;
            /*
             * delete a dir, the directory may be empty or not
             * node: the dir name with full path
             * recursively_delete: true for recursively delete non-empty node,
             *                     false for failure
             * cb_code: the task code specifies where to execute the callback
             * cb_delete: delete callback, ec to indicate success or failure reason
             */
            virtual task_ptr delete_node(const std::string& node,
                                     bool recursively_delete,
                                     task_code cb_code,
                                     const err_callback& cb_delete,
                                     clientlet* tracker = nullptr) = 0;
            /*
             * check if the node dir exists
             * node: the dir name with full path
             * cb_code: the task code specifies where to execute the callback
             * cb_exist: callback to indicate the check result
             */
            virtual task_ptr node_exist(const std::string& node,
                                    task_code cb_code,
                                    const err_callback& cb_exist,
                                    clientlet* tracker = nullptr) = 0;
            /*
             * get the data in node
             * node: dir name with full path
             * cb_code: the task code specifies where to execute the callback
             * cb_get_data: callback. If success, ec indicate the success and
             *              node data is returned in ret_str
             *              or-else user get the fail reason in ec
             */
            virtual task_ptr get_data(const std::string& node,
                                  task_code cb_code,
                                  const err_value_callback& cb_get_data,
                                  clientlet* tracker = nullptr) = 0;
            /*
             * set the data of the node
             * node: dir name with full path
             * value: the value
             * cb_code: the task code specifies where to execute the callback
             * cb_set_data: the callback to indicate the set result
             */
            virtual task_ptr set_data(const std::string& node,
                                  const blob& value,
                                  task_code cb_code,
                                  const err_callback& cb_set_data,
                                  clientlet* tracker = nullptr) = 0;
            /*
             * get all childrens of a node
             * node: dir name with full path
             * cb_code: the task code specifies where to execute the callback
             * cb_get_children: if success, ret_strv store the node names of children
             */
            virtual task_ptr get_children(const std::string& node,
                                      task_code cb_code,
                                      const err_stringv_callback& cb_get_children,
                                      clientlet* tracker = nullptr) = 0;
        };
    }
}
