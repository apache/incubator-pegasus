using System;
using System.IO;
using dsn.dev.csharp;

namespace dsn.example 
{
    public static partial class echoHelper
    {
        // define your own thread pool using DEFINE_THREAD_POOL_CODE(xxx)
        // define RPC task code for service 'echo'
        public static TaskCode RPC_ECHO_ECHO_PING;
        public static TaskCode LPC_ECHO_TEST_TIMER;
    

        public static void InitCodes()
        {
            RPC_ECHO_ECHO_PING = new TaskCode("RPC_ECHO_ECHO_PING", dsn_task_type_t.TASK_TYPE_RPC_REQUEST, dsn_task_priority_t.TASK_PRIORITY_COMMON, ThreadPoolCode.THREAD_POOL_DEFAULT);
            LPC_ECHO_TEST_TIMER = new TaskCode("LPC_ECHO_TEST_TIMER", dsn_task_type_t.TASK_TYPE_COMPUTE, dsn_task_priority_t.TASK_PRIORITY_COMMON, ThreadPoolCode.THREAD_POOL_DEFAULT);
        }
    }    
}

