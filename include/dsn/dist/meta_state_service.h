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

namespace dsn
{
    namespace dist
    {
        class meta_state_service
        {
        public:
            typedef std::function<void (error_code ec, std::string&& ret_str)> err_string_callback;
            typedef std::function<void (error_code ec, std::vector<std::string>&& ret_strv)> err_stringv_callback;
            typedef std::function<void (error_code ec)> err_callback;

        public:
            /*
             * create a dir node
             * node: the dir name with full path
             * cb_create: create callback, ec to indicate success or failure reason
             * value: the data value to store in the node
             */
            virtual void create_directory(const std::string& node,
                                          const err_callback& cb_create,
                                          const std::string& value = std::string()) = 0;
            /*
             * delete a dir, the directory may be empty or not
             * node: the dir name with full path
             * recursively_delete: true for recursively delete non-empty node,
             *                     false for failure
             * cb_delete: delete callback, ec to indicate success or failure reason
             */
            virtual void delete_directory(const std::string& node,
                                          bool recursively_delete,
                                          const err_callback& cb_delete) = 0;
            /*
             * check if the node dir exists
             * node: the dir name with full path
             * cb_exist: callback to indicate the check result
             */
            virtual void node_exist(const std::string& node,
                                    const err_callback& cb_exist) = 0;
            /*
             * get the data in node
             * node: dir name with full path
             * cb_get_data: callback. If success, ec indicate the success and
             *              node data is returned in ret_str
             *              or-else user get the fail reason in ec
             */
            virtual void get_data(const std::string& node,
                                  const err_string_callback& cb_get_data) = 0;
            /*
             * set the data of the node
             * node: dir name with full path
             * value: the value
             * cb_set_data: the callback to indicate the set result
             */
            virtual void set_data(const std::string& node,
                                  const std::string& value,
                                  const err_callback& cb_set_data) = 0;
            /*
             * get all childrens of a node
             * node: dir name with full path
             * cb_get_children: if success, ret_strv store the node names of children
             */
            virtual void get_children(const std::string& node,
                                      const err_stringv_callback& cb_get_children) = 0;
        };
    }
}
