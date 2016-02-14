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
 *     What is this file about?
 *
 * Revision history:
 *     Feb., 2016, @imzhenyu (Zhenyu Guo), done in Tron project and copied here
 *     xxxx-xx-xx, author, fix bug about xxx
 */
 
using System;
using System.IO;
using dsn.dev.csharp;

namespace dsn.replication 
{
    public static partial class replicationHelper
    {
        public static TaskCode LPC_REPLICATION_TEST_TIMER;
        // define your own thread pool using DEFINE_THREAD_POOL_CODE(xxx)
        // define RPC task code for service 'meta_s'
        public static TaskCode RPC_REPLICATION_META_S_CREATE_APP;
        public static TaskCode RPC_REPLICATION_META_S_DROP_APP;
        public static TaskCode RPC_REPLICATION_META_S_QUERY_CONFIGURATION_BY_NODE;
        public static TaskCode RPC_REPLICATION_META_S_QUERY_CONFIGURATION_BY_INDEX;
        public static TaskCode RPC_REPLICATION_META_S_UPDATE_CONFIGURATION;
    

        public static void InitCodes()
        {RPC_REPLICATION_META_S_CREATE_APP = new TaskCode("RPC_REPLICATION_META_S_CREATE_APP", dsn_task_type_t.TASK_TYPE_RPC_REQUEST, dsn_task_priority_t.TASK_PRIORITY_COMMON, ThreadPoolCode.THREAD_POOL_DEFAULT);
            RPC_REPLICATION_META_S_DROP_APP = new TaskCode("RPC_REPLICATION_META_S_DROP_APP", dsn_task_type_t.TASK_TYPE_RPC_REQUEST, dsn_task_priority_t.TASK_PRIORITY_COMMON, ThreadPoolCode.THREAD_POOL_DEFAULT);
            RPC_REPLICATION_META_S_QUERY_CONFIGURATION_BY_NODE = new TaskCode("RPC_REPLICATION_META_S_QUERY_CONFIGURATION_BY_NODE", dsn_task_type_t.TASK_TYPE_RPC_REQUEST, dsn_task_priority_t.TASK_PRIORITY_COMMON, ThreadPoolCode.THREAD_POOL_DEFAULT);
            RPC_REPLICATION_META_S_QUERY_CONFIGURATION_BY_INDEX = new TaskCode("RPC_REPLICATION_META_S_QUERY_CONFIGURATION_BY_INDEX", dsn_task_type_t.TASK_TYPE_RPC_REQUEST, dsn_task_priority_t.TASK_PRIORITY_COMMON, ThreadPoolCode.THREAD_POOL_DEFAULT);
            RPC_REPLICATION_META_S_UPDATE_CONFIGURATION = new TaskCode("RPC_REPLICATION_META_S_UPDATE_CONFIGURATION", dsn_task_type_t.TASK_TYPE_RPC_REQUEST, dsn_task_priority_t.TASK_PRIORITY_COMMON, ThreadPoolCode.THREAD_POOL_DEFAULT);
            LPC_REPLICATION_TEST_TIMER = new TaskCode("LPC_REPLICATION_TEST_TIMER", dsn_task_type_t.TASK_TYPE_COMPUTE, dsn_task_priority_t.TASK_PRIORITY_COMMON, ThreadPoolCode.THREAD_POOL_DEFAULT);
        }
    }    
}

