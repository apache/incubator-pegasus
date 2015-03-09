/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation

 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.

 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
#pragma once

#include "replication_app_client_base.h"

namespace rdsn { namespace replication {

class app_client_example1 : public replication_app_client_base
{
public:
    app_client_example1(const std::vector<end_point>& meta_servers);
    ~app_client_example1(void);

    // async ops
    void read(const std::string& key, rpc_reply_handler callback);
    void Update(const std::string& key, const std::string& value, rpc_reply_handler callback);
    void append(const std::string& key, const std::string& appendValue, rpc_reply_handler callback);

    // sync ops
    int read(const std::string& key, __out std::string& value);
    int Update(const std::string& key, const std::string& value);
    int append(const std::string& key, const std::string& appendValue);
    
private:
    int KeyToPartitionIndex(const std::string& key);
    int HandleResponse(rpc_response_task_ptr& resp, std::string* pvalue = nullptr);

private:
    int _timeoutMilliesecondsRead;
    int _timeoutMilliesecondsWrite;
};


}} // namespace
