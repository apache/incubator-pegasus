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

#include "runtime/message_utils.h"

#include <string>
#include <utility>

#include "common/replication.codes.h"
#include "dsn.layer2_types.h"
#include "gtest/gtest.h"
#include "runtime/rpc/rpc_holder.h"
#include "runtime/rpc/rpc_message.h"
#include "utils/autoref_ptr.h"
#include "utils/threadpool_code.h"

namespace dsn {

DEFINE_TASK_CODE_RPC(RPC_CODE_FOR_TEST, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)

typedef rpc_holder<query_cfg_request, query_cfg_response> t_rpc;

TEST(message_utils, msg_blob_convertion)
{
    std::string data = "hello";

    blob b(data.c_str(), 0, data.size());
    message_ptr m = from_blob_to_received_msg(RPC_CODE_FOR_TEST, std::move(b));

    ASSERT_EQ(m->header->body_length, data.size());
    ASSERT_EQ(b.to_string(), move_message_to_blob(m.get()).to_string());
}

TEST(message_utils, thrift_msg_convertion)
{
    query_cfg_request request;
    request.app_name = "haha";

    message_ptr msg =
        from_thrift_request_to_received_message(request, RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX);

    t_rpc rpc(msg.get());
    ASSERT_EQ(rpc.request().app_name, "haha");
}

TEST(message_utils, complex_convertion)
{
    query_cfg_request request;
    request.app_name = "haha";

    message_ptr msg =
        from_thrift_request_to_received_message(request, RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX);
    blob b = move_message_to_blob(msg.get());
    msg = from_blob_to_received_msg(RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX, std::move(b));

    t_rpc rpc(msg.get());
    ASSERT_EQ(rpc.request().app_name, "haha");
}

} // namespace dsn
