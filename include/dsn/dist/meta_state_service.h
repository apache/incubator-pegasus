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
# pragma once

#include <dsn/service_api_cpp.h>
#include <string>
#include <functional>
#include <memory>
#include <dsn/internal/task.h>
#include "service_supplier.h"

namespace dsn
{
    namespace dist
    {
        class meta_state_service
        {
        public:
            typedef std::function<void (int ec, std::string&& ret_str)> err_string_callback;
            typedef std::function<void (int ec, std::vector<std::string>&& ret_strv)> err_stringv_callback;
            typedef std::function<void (int ec, std::string&& ret_strv, int ret_i)> err_data_callback;
            typedef std::function<void (int ec, int ret_i)> err_state_callback;
            typedef std::function<void (int ec)> err_callback;

        public:
            template<typename T>
            static meta_state_service* open_service(std::shared_ptr<service_supplier> supplier) {
                return T::open(supplier);
            }

            virtual ~meta_state_service() {}
            //create a dir
            virtual void amkdir(const std::string& znode,
                                const err_string_callback& cb_create,
                                const std::string& value = std::string()) = 0;
            //create a node which can't be a dir
            virtual void acreate(const std::string& znode,
                                 const err_string_callback& cb_create,
                                 const std::string& value = std::string()) = 0;
            //delete a node/dir, and the dir may not be empty
            virtual void adelete(const std::string& znode,
                                 const err_callback& cb_delete) = 0;
            virtual void aexist(const std::string& znode,
                                const err_state_callback& cb_exist) = 0;
            virtual void aget(const std::string& znode,
                              const err_data_callback& cb_get_data) = 0;
            virtual void aset(const std::string& znode,
                              const std::string& value,
                              const err_state_callback& cb_set_data) = 0;
            virtual void aget_children(const std::string& znode,
                                       const err_stringv_callback& cb_list_node) = 0;

        };
    }
}
