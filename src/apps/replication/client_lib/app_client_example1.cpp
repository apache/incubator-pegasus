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
#include "app_client_example1.h"

namespace dsn { namespace replication {


app_client_example1::app_client_example1(const std::vector<end_point>& meta_servers)
    : replication_app_client_base(meta_servers, "TestTable")
{
    _timeoutMilliesecondsRead = 1000;
    _timeoutMilliesecondsWrite = 3000;
}


app_client_example1::~app_client_example1(void)
{

}

int app_client_example1::KeyToPartitionIndex(const std::string& key)
{
    // TODO:
    return 0;
}

void app_client_example1::read(const std::string& key, rpc_reply_handler callback)
{
    auto req = create_read_request(KeyToPartitionIndex(key));
    SimpleKvRequest msg;
    msg.op = SKV_READ;
    msg.key = key;
    
    marshall(req, msg);

    send(req, _timeoutMilliesecondsRead, callback);
} 

void app_client_example1::Update(const std::string& key, const std::string& value, rpc_reply_handler callback)
{
    auto req = create_write_request(KeyToPartitionIndex(key));
    SimpleKvRequest msg;
    msg.op = SKV_UPDATE;
    msg.key = key;
    msg.value = value;
    marshall(req, msg);

    send(req, _timeoutMilliesecondsWrite, callback);
}

void app_client_example1::append(const std::string& key, const std::string& appendValue, rpc_reply_handler callback)
{
    auto req = create_write_request(KeyToPartitionIndex(key));
    SimpleKvRequest msg;
    msg.op = SKV_APPEND;
    msg.key = key;
    msg.value = appendValue;
    marshall(req, msg);

    send(req, _timeoutMilliesecondsWrite, callback);
}

int app_client_example1::HandleResponse(rpc_response_task_ptr& reply, std::string* pvalue)
{
    if (reply->get_response() != nullptr)
    {
        int err = reply->error();
        if (err == ERR_SUCCESS)
        {
            reply->get_response()->reader().read(err);
        }

        if (err == ERR_SUCCESS)
        {
            SimpleKvResponse appResp;
            unmarshall(reply->get_response(), appResp);
            if (pvalue)
            {
                *pvalue = appResp.value;
            }
            return appResp.err;
        }
        else
        {
            return err;
        }
    }
    else
        return ERR_TIMEOUT;
}

int app_client_example1::read(const std::string& key, __out_param std::string& value)
{
    auto req = create_read_request(KeyToPartitionIndex(key));
    SimpleKvRequest msg;
    msg.op = SKV_READ;
    msg.key = key;
    marshall(req, msg);

    auto reply = send(req, _timeoutMilliesecondsRead, nullptr);
    reply->wait();

    return HandleResponse(reply, &value);
}

int app_client_example1::Update(const std::string& key, const std::string& value)
{
    auto req = create_write_request(KeyToPartitionIndex(key));
    SimpleKvRequest msg;
    msg.op = SKV_UPDATE;
    msg.key = key;
    msg.value = value;
    marshall(req, msg);

    auto reply = send(req, _timeoutMilliesecondsRead, nullptr);
    reply->wait();

    return HandleResponse(reply);
}

int app_client_example1::append(const std::string& key, const std::string& appendValue)
{
    auto req = create_write_request(KeyToPartitionIndex(key));
    SimpleKvRequest msg;
    msg.op = SKV_APPEND;
    msg.key = key;
    msg.value = appendValue;
    marshall(req, msg);

    auto reply = send(req, _timeoutMilliesecondsRead, nullptr);
    reply->wait();

    return HandleResponse(reply);
}

}} // namespace
