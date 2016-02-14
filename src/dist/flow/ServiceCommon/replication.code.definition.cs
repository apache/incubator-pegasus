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

