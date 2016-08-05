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
 *     Feb., 2016, @imzhenyu (Zhenyu Guo), done in rDSN.CSharp project and copied here
 *     xxxx-xx-xx, author, fix bug about xxx
 */

using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Security;

namespace dsn.dev.csharp
{
    using dsn_error_t = Int32;
    using dsn_task_code_t = Int32;
    using dsn_threadpool_code_t = Int32;
    using dsn_handle_t = IntPtr;
    using dsn_task_t = IntPtr;
    using dsn_task_tracker_t = IntPtr;
    using dsn_message_t = IntPtr;
    using size_t = IntPtr;
    using dsn_address_t = UInt64;
    using dsn_group_t = IntPtr;
    using dsn_uri_t = IntPtr;
    using dsn_gpid = UInt64;

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate void dsn_task_handler_t(IntPtr param);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate void dsn_rpc_request_handler_t(dsn_message_t request, IntPtr param);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate void dsn_rpc_response_handler_t(int err, dsn_message_t req, dsn_message_t resp, IntPtr param);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate void dsn_aio_handler_t(dsn_error_t err, size_t sz, IntPtr param);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate IntPtr dsn_checker_create(string name, IntPtr app_info, int app_info_count); // app_info: dsn_app_info[]
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate void dsn_checker_apply(IntPtr checker);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate IntPtr dsn_app_create(string type_name, dsn_gpid gpid); // return app_context
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate int dsn_app_start(IntPtr app_context, int argc, IntPtr argv); // argv: char**
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate dsn_error_t dsn_app_destroy(IntPtr app_context, bool cleanup);
    
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate void dsn_layer2_rpc_request_handler(
            IntPtr app_context,
            dsn_gpid gpid,
            bool is_write_op,
            dsn_message_t req,
            int delay_milliseconds
            );

    [StructLayout(LayoutKind.Sequential, Pack = 4, CharSet = CharSet.Ansi)]
    public struct dsn_app_learn_state
    {
        int total_learn_state_size; // memory used in the given buffer by this learn-state 
        Int64 from_decree_excluded;
        Int64 to_decree_included;
        int meta_state_size;
        int file_state_count;
        IntPtr meta_state_ptr;
        IntPtr files;
        //IntPtr meta_state_ptr;
        //const char** files;
    }

    public enum dsn_chkpt_apply_mode
    {
        DSN_CHKPT_COPY,
        DSN_CHKPT_LEARN
    }

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate dsn_error_t dsn_app_checkpoint(IntPtr app_context);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate dsn_error_t dsn_app_checkpoint_async(IntPtr app_context);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate int dsn_app_prepare_get_checkpoint(
        IntPtr app_context,
        IntPtr buffer,
        int buffer_capacity
        );

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate int dsn_app_get_checkpoint(
        IntPtr app_context,
        Int64 start_decree,
        IntPtr learn_req,
        int learn_req_size,
        IntPtr learn_state_buffer, // dsn_app_learn_state learn_state_buffer,
        int learn_state_buffer_size
        );

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate int dsn_app_apply_checkpoint(
        IntPtr app_context,
        IntPtr learn_state,
        dsn_chkpt_apply_mode mode
        );

    public delegate int dsn_app_start_managed(IntPtr app_context, string[] args);
    public delegate IntPtr dsn_checker_create_managed(string name, dsn_app_info[] app_info);

    public enum dsn_batch_state
    {
        BS_NOT_BATCH,  // request is not batched
        BS_BATCH,      // request is batched but not the last in the same batch
        BS_BATCH_LAST  // request is batched and the last in the same batch
    };

    [StructLayout(LayoutKind.Sequential, Pack = 4, CharSet = CharSet.Ansi)]
    public struct dsn_app_cross_layer_shared_info_type_1
    {
        // from layer 2 to layer 1
        public Int64 last_committed_decree;
        // from layer 1 to layer 2
        public Int64 last_durable_decree;
        // physical error (e.g., io error) indicates the app needs to be dropped
        public Int32 physical_error;

        public dsn_batch_state batch_state;
    };

    [StructLayout(LayoutKind.Sequential, Pack = 4, CharSet = CharSet.Ansi)]
    public struct dsn_app_info
    {
        public IntPtr app_context_ptr; // returned by dsn_app_create
        public int app_id;
        public int index;

        [MarshalAs(UnmanagedType.ByValTStr, SizeConst = 32)]
        public string role; // size = DSN_MAX_APP_TYPE_NAME_LENGTH

        [MarshalAs(UnmanagedType.ByValTStr, SizeConst = 32)]
        public string type; // size = DSN_MAX_APP_TYPE_NAME_LENGTH

        [MarshalAs(UnmanagedType.ByValTStr, SizeConst = 32)]
        public string name; // size = DSN_MAX_APP_TYPE_NAME_LENGTH

        [MarshalAs(UnmanagedType.ByValTStr, SizeConst = 1024)]
        public string data_dir; // size = DSN_MAX_PATH
        dsn_app_cross_layer_shared_info_type_1 type1_info;
    }

    [StructLayout(LayoutKind.Sequential, Pack = 4, CharSet = CharSet.Ansi)]
    public struct layer1_callbacks
    {
        //public dsn_app_create create;  ///< callback to create the context for the app
        //public dsn_app_start start;   ///< callback to start the app, similar to ```main```
        //public dsn_app_destroy destroy; ///< callback to stop and destroy the app
                                        
        public IntPtr create;  ///< callback to create the context for the app
        public IntPtr start;   ///< callback to start the app, similar to ```main```
        public IntPtr destroy; ///< callback to stop and destroy the app
    }

    [StructLayout(LayoutKind.Sequential, Pack = 4, CharSet = CharSet.Ansi)]
    public struct layer2_framework_callbacks
    {
        // dsn_layer2_rpc_request_handler on_rpc_request;
        public IntPtr on_rpc_request;
    };

    [StructLayout(LayoutKind.Sequential, Pack = 4, CharSet = CharSet.Ansi)]
    public struct layer2_app_type_1_callbacks
    {
        //dsn_app_checkpoint chkpt;
        //dsn_app_checkpoint_async chkpt_async;
        //dsn_app_prepare_get_checkpoint checkpoint_get_prepare; ///< optional
        //dsn_app_get_checkpoint chkpt_get;
        //dsn_app_apply_checkpoint chkpt_apply;

        public IntPtr chkpt;
        public IntPtr chkpt_async;
        public IntPtr checkpoint_get_prepare; ///< optional
        public IntPtr chkpt_get;
        public IntPtr chkpt_apply;
    };

    [StructLayout(LayoutKind.Sequential, Pack = 4, CharSet = CharSet.Ansi)]
    public struct layer3_callbacks
    {
        ulong dump;
    }

    [StructLayout(LayoutKind.Sequential, Pack = 4, CharSet = CharSet.Ansi)]
    public struct dsn_app
    {
        public ulong        mask; ///< application capability mask
        
        [MarshalAs(UnmanagedType.ByValTStr, SizeConst = 32)]
        public string type_name; // size = DSN_MAX_APP_TYPE_NAME_LENGTH

        /*! layer 1 app definition, mask = DSN_APP_MASK_DEFAULT */
        public layer1_callbacks layer1;
        
        public layer2_framework_callbacks layer2_frameworks;

        public layer2_app_type_1_callbacks layer2_apps_type_1;

        public layer3_callbacks layer3;
    }

    public enum dsn_task_type_t
    {
        TASK_TYPE_RPC_REQUEST,
        TASK_TYPE_RPC_RESPONSE,
        TASK_TYPE_COMPUTE,
        TASK_TYPE_AIO,
        TASK_TYPE_CONTINUATION,
        TASK_TYPE_COUNT,
        TASK_TYPE_INVALID
    }

    public enum dsn_task_priority_t
    {
        TASK_PRIORITY_LOW,
        TASK_PRIORITY_COMMON,
        TASK_PRIORITY_HIGH,
        TASK_PRIORITY_COUNT,
        TASK_PRIORITY_INVALID
    }

    public enum dsn_log_level_t
    {
        LOG_LEVEL_INFORMATION,
        LOG_LEVEL_DEBUG,
        LOG_LEVEL_WARNING,
        LOG_LEVEL_ERROR,
        LOG_LEVEL_FATAL,
        LOG_LEVEL_COUNT,
        LOG_LEVEL_INVALID
    }

    
    public enum dsn_host_type_t
    {
        HOST_TYPE_IPV4,  // 4 bytes
        HOST_TYPE_IPV6,  // 16 bytes
        HOST_TYPE_URI,   // customized bytes
        HOST_TYPE_COUNT,
        HOST_TYPE_INVALID
    }
        
    public static class Native
    {
        public const uint DSN_MAX_TASK_CODE_NAME_LENGTH  = 48 ;
        public const uint DSN_MAX_ADDRESS_NAME_LENGTH    = 16 ;
        public const uint DSN_MAX_BUFFER_COUNT_IN_MESSAGE= 64 ;
        public const uint DSN_INVALID_HASH               = 0xdeadbeef ;
        public const uint DSN_MAX_APP_TYPE_NAME_LENGTH   = 32 ;

        public const uint DSN_APP_MASK_DEFAULT = 0x00; ///< default mask, only layer1 app model is supported
        public const uint DSN_L2_REPLICATION_FRAMEWORK_TYPE_1 = 0x01; ///< implement type 1 replication framework
        public const uint DSN_L2_REPLICATION_APP_TYPE_1 = 0x02; ///< implement type 1 replication app
        public const uint DSN_L2_REPLICATION_FRAMEWORK_TYPE_2 = 0x04; ///< implement type 1 replication framework
        public const uint DSN_L2_REPLICATION_APP_TYPE_2 = 0x08; ///< implement type 1 replication app


#if __MonoCS__
        public const string DSN_CORE_DLL = "dsn.core.so";
#else
        public const string DSN_CORE_DLL = "dsn.core.dll";
#endif

        public static string[] CopyCStringArrayToManaged(IntPtr ptr, int size)
        {
            var ss = new List<string>();
            for (var i = 0; i < size; i++)
            {
                var sptr = Marshal.ReadIntPtr(ptr);
                ptr += IntPtr.Size;

                var s = Marshal.PtrToStringAnsi(sptr);
                ss.Add(s);                
            }
            return ss.ToArray();
        }

        public static dsn_app_info[] CopyAppInfoArrayToManaged(IntPtr ptr, int size)
        {
            var ss = new List<dsn_app_info>();
            for (var i = 0; i < size; i++)
            {
                var obj = new dsn_app_info();
                Marshal.PtrToStructure(ptr, obj);
                ss.Add(obj);
                
                // TODO: marshalled size of(dsn_app_info)
                ptr += (int)(IntPtr.Size + sizeof(int) + 2 * DSN_MAX_APP_TYPE_NAME_LENGTH);
            }
            return ss.ToArray();
        }
        public static bool dsn_register_app_managed(string type_name, dsn_app_create create, dsn_app_start_managed start, dsn_app_destroy destroy)
        {
            dsn_app_start start2 = (app_context, argc, argv) => 
            {
                var args = CopyCStringArrayToManaged(argv, argc);
                return start(app_context, args);
            };

            var app = new dsn_app
            {
                mask = DSN_APP_MASK_DEFAULT,
                type_name = type_name,
                layer1 =
                {
                    create = Marshal.GetFunctionPointerForDelegate(create),
                    start = Marshal.GetFunctionPointerForDelegate(start2),
                    destroy = Marshal.GetFunctionPointerForDelegate(destroy)
                }
            };

            return dsn_register_app(app);
        }

        public static int dsn_register_app_checker_managed(string name, dsn_checker_create_managed create, dsn_checker_apply apply)
        {
            dsn_checker_create create2 = (name2, app_info, app_info_count) => 
            {
                var app_infos = CopyAppInfoArrayToManaged(app_info, app_info_count);
                return create(name2, app_infos);
            };

            return dsn_register_app_checker(name, create2, apply);
        }
		
        //------------------------------------------------------------------------------
        //
        // system
        //
        //------------------------------------------------------------------------------
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern bool dsn_register_app(dsn_app app);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern int  dsn_register_app_checker(string name, dsn_checker_create create, dsn_checker_apply apply);
        [DllImport(DSN_CORE_DLL, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern bool dsn_mimic_app(string app_name, int index); // index starts from 1
        [DllImport(DSN_CORE_DLL, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern bool dsn_run_config(string config, bool sleep_after_init);
        //
        // run the system with arguments
        //   config [-cargs k1=v1;k2=v2, -app app_name, -app_index index]
        // e.g., config.ini -app replica -app_index 1 to start the first replica as a new process
        //       config.ini -app replica to start ALL replicas (count specified in config) as a new process
        //       config.ini -app replica -cargs replica-port=34556 to start ALL replicas with given port variable specified in config.ini
        //       config.ini to start ALL apps as a new process
        //
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern void dsn_run(int argc, string[] argv, bool sleep_after_init);

        [DllImport(DSN_CORE_DLL, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern void dsn_exit(int code);

        //------------------------------------------------------------------------------
        //
        // common utilities
        //
        //------------------------------------------------------------------------------
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern int           dsn_error_register(string name);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern IntPtr                dsn_error_to_string(int err);    
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern int dsn_threadpool_code_register(string name);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern IntPtr                dsn_threadpool_code_to_string(int pool_code);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern int dsn_threadpool_code_from_string(string s, int default_code);
		[DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern int                   dsn_threadpool_get_current_tid();
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern int                   dsn_threadpool_code_max();
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern int       dsn_task_code_register(string name, dsn_task_type_t type, dsn_task_priority_t pri, int pool);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern void                  dsn_task_code_query(int code, out dsn_task_type_t ptype, out dsn_task_priority_t ppri, out int ppool);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern void                  dsn_task_code_set_threadpool(int code, int pool);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern void                  dsn_task_code_set_priority(int code, dsn_task_priority_t pri);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern IntPtr                dsn_task_code_to_string(int code);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern int       dsn_task_code_from_string(string s, int default_code);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern int                   dsn_task_code_max();
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern string                dsn_task_type_to_string(dsn_task_type_t tt);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern string                dsn_task_priority_to_string(dsn_task_priority_t tt);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern string                dsn_config_get_value_string(string section, string key, string default_value, string dsptr);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern bool                  dsn_config_get_value_bool(string section, string key, bool default_value, string dsptr);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern ulong                dsn_config_get_value_uint64(string section, string key, ulong default_value, string dsptr);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern double                dsn_config_get_value_double(string section, string key, double default_value, string dsptr);
        // return all key count (may greater than buffer_count)
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern int                   dsn_config_get_all_keys(string section, string[] buffers, ref int buffer_count); 
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern dsn_log_level_t       dsn_log_get_start_level();
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern void                  dsn_logf(string file, string function, int line, dsn_log_level_t log_level, string title, string fmt, __arglist);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern void                  dsn_log(string file, string function, int line, dsn_log_level_t log_level, string title);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern void                  dsn_coredump();
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern uint                dsn_crc32_compute(IntPtr ptr, size_t size, uint init_crc);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern uint                dsn_crc32_concatenate(uint xy_init, uint x_init, uint x_final, size_t x_size, uint y_init, uint y_final, size_t y_size);

        //------------------------------------------------------------------------------
        //
        // tasking - asynchronous tasks and timers tasks executed in target thread pools
        //
        // (configured in config files)
        // [task.RPC_PREPARE
        // // TODO: what can be configured for a task
        //
        // [threadpool.THREAD_POOL_REPLICATION]
        // // TODO: what can be configured for a thread pool
        //
        //------------------------------------------------------------------------------
        //

        //
        // all returned dsn_task_t are NOT add_ref by rDSN,
        // so you DO NOT need to call task_release_ref to release the tasks.
        // the exception is made for easier programming, and you may consider the later
        // dsn_rpc_xxx calls do the resource gc work for you.
        //
        // however, after you use the tasks with rDSN calls (e.g., dsn_task_call, 
        // dsn_rpc_call_xxx, dsn_file_read/write, etc.) and you want to hold them
        // further, you need to call task_add_ref and task_release_ref.
        // 
        //
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern void        dsn_task_release_ref(dsn_task_t task);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern void        dsn_task_add_ref(dsn_task_t task);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern dsn_task_t  dsn_task_create(int code, dsn_task_handler_t cb, IntPtr param, int hash, dsn_task_tracker_t tracker = default(dsn_task_tracker_t));
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern dsn_task_t  dsn_task_create_timer(int code, dsn_task_handler_t cb, IntPtr param, int hash, int interval_milliseconds, dsn_task_tracker_t tracker = default(dsn_task_tracker_t));
        // repeated declarations later in correpondent rpc and file sections
        //[DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        //public extern static dsn_task_t  dsn_rpc_create_response_task(dsn_message_t request, dsn_rpc_response_handler_t cb, IntPtr param, int reply_thread_hash);
        //[DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        //public extern static dsn_task_t  dsn_file_create_aio_task(dsn_task_code_t code, dsn_aio_handler_t cb, IntPtr param, int hash);

        //
        // task trackers are used to track task context
        //
        // when a task executes, it usually accesses certain context
        // when the context is gone, all tasks accessing this context needs 
        // to be cancelled automatically to avoid invalid context access
        // 
        // to release this burden from developers, rDSN provides 
        // task tracker which can be embedded into a context, and
        // destroyed when the context is gone
        //
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern dsn_task_tracker_t dsn_task_tracker_create(int task_bucket_count);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern void               dsn_task_tracker_destroy(dsn_task_tracker_t tracker = default(dsn_task_tracker_t));
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern void               dsn_task_tracker_cancel_all(dsn_task_tracker_t tracker = default(dsn_task_tracker_t));
        [DllImport(DSN_CORE_DLL, CallingConvention = CallingConvention.Cdecl)]
        public static extern void               dsn_task_tracker_wait_all(dsn_task_tracker_t tracker = default(dsn_task_tracker_t));

        //
        // common task 
        //
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern void        dsn_task_call(dsn_task_t task, int delay_milliseconds);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern bool        dsn_task_cancel(dsn_task_t task, bool wait_until_finished);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern bool        dsn_task_cancel2(dsn_task_t task, bool wait_until_finished, out bool finished);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern bool        dsn_task_wait(dsn_task_t task); 
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern bool        dsn_task_wait_timeout(dsn_task_t task, int timeout_milliseconds);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern int dsn_task_error(dsn_task_t task);

        //------------------------------------------------------------------------------
        //
        // thread synchronization
        //
        //------------------------------------------------------------------------------
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern dsn_handle_t dsn_exlock_create(bool recursive);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern void         dsn_exlock_destroy(dsn_handle_t l);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern void         dsn_exlock_lock(dsn_handle_t l);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern bool         dsn_exlock_try_lock(dsn_handle_t l);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern void         dsn_exlock_unlock(dsn_handle_t l);

        // non-recursive rwlock
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern dsn_handle_t dsn_rwlock_nr_create();
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern void         dsn_rwlock_nr_destroy(dsn_handle_t l);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern void         dsn_rwlock_nr_lock_read(dsn_handle_t l);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern void         dsn_rwlock_nr_unlock_read(dsn_handle_t l);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern void         dsn_rwlock_nr_lock_write(dsn_handle_t l);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern void         dsn_rwlock_nr_unlock_write(dsn_handle_t l);

        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern dsn_handle_t dsn_semaphore_create(int initial_count);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern void         dsn_semaphore_destroy(dsn_handle_t s);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern void         dsn_semaphore_signal(dsn_handle_t s, int count);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern void         dsn_semaphore_wait(dsn_handle_t s);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern bool         dsn_semaphore_wait_timeout(dsn_handle_t s, int timeout_milliseconds);

        //------------------------------------------------------------------------------
        //
        // rpc
        //
        //------------------------------------------------------------------------------

        // rpc address utilities
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern uint      dsn_ipv4_from_host(string name);
        [DllImport(DSN_CORE_DLL, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern uint      dsn_ipv4_local(string network_interface);
        [DllImport(DSN_CORE_DLL, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern ulong dsn_address_build(string host, ushort port);
        [DllImport(DSN_CORE_DLL, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern ulong dsn_address_build_ipv4(uint ipv4, ushort port);
        [DllImport(DSN_CORE_DLL, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern ulong dsn_address_build_group(dsn_group_t g);
        [DllImport(DSN_CORE_DLL, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern ulong dsn_address_build_uri(dsn_uri_t uri);
        [DllImport(DSN_CORE_DLL, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern string dsn_address_to_string(ulong addr);

        [DllImport(DSN_CORE_DLL, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern dsn_uri_t     dsn_uri_build(string url); // must be paired with destroy later
        [DllImport(DSN_CORE_DLL, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern void dsn_uri_destroy(dsn_uri_t uri);

        [DllImport(DSN_CORE_DLL, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern dsn_group_t   dsn_group_build(string name); // must be paired with release later
        [DllImport(DSN_CORE_DLL, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern bool dsn_group_add(dsn_group_t g, ulong ep);
        [DllImport(DSN_CORE_DLL, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern bool dsn_group_remove(dsn_group_t g, ulong ep);
        [DllImport(DSN_CORE_DLL, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern void dsn_group_set_leader(dsn_group_t g, ulong ep);
        [DllImport(DSN_CORE_DLL, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern ulong dsn_group_get_leader(dsn_group_t g);
        [DllImport(DSN_CORE_DLL, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern bool dsn_group_is_leader(dsn_group_t g, ulong ep);
        [DllImport(DSN_CORE_DLL, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern ulong dsn_group_next(dsn_group_t g, ulong ep);
        [DllImport(DSN_CORE_DLL, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern void dsn_group_destroy(dsn_group_t g);

        [DllImport(DSN_CORE_DLL, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern ulong dsn_primary_address();

        // rpc message and buffer management
        //
        // all returned dsn_message_t are add_ref by rDSN except those dsn_msg_create_xxx, 
        // so you need to call msg_release_ref to release the msgs.
        // the exception is made for easier programming, and you may consider the later
        // dsn_rpc_xxx calls do the resource gc work for you.
        //
        // for those returned by dsn_msg_create_xxx, if you want to hold them after
        // calling dsn_rpc_xxx, you need to call msg_add_ref and msg_release_ref.
        // 
        // for all msgs accessable in callbacks, rDSN will handle reference by itself.
        // if you want to hold them in upper apps, you need to call msg_add_ref
        // and msg_release_ref explicitly.
        //
        [DllImport(DSN_CORE_DLL, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern dsn_message_t dsn_msg_create_request(dsn_task_code_t rpc_code, int timeout_milliseconds, Int32 thread_hash, UInt64 partition_hash);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern dsn_message_t dsn_msg_create_response(dsn_message_t request);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern void          dsn_msg_add_ref(dsn_message_t msg);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern void          dsn_msg_release_ref(dsn_message_t msg);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern void          dsn_msg_update_request(dsn_message_t msg, int timeout_milliseconds, int hash);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern void          dsn_msg_query_request(dsn_message_t msg, out int ptimeout_milliseconds, out int phash);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern void          dsn_msg_write_next(dsn_message_t msg, out IntPtr ptr, out size_t size, size_t min_size);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern void          dsn_msg_write_commit(dsn_message_t msg, size_t size);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern bool          dsn_msg_read_next(dsn_message_t msg, out IntPtr ptr, out size_t size);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern void          dsn_msg_read_commit(dsn_message_t msg, size_t size);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern size_t        dsn_msg_body_size(dsn_message_t msg);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern IntPtr         dsn_msg_rw_ptr(dsn_message_t msg, size_t offset_begin);
        [DllImport(DSN_CORE_DLL, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern ulong dsn_msg_from_address(dsn_message_t msg);
        [DllImport(DSN_CORE_DLL, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern ulong dsn_msg_to_address(dsn_message_t msg);

        // rpc calls
        [DllImport(DSN_CORE_DLL, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern bool           dsn_rpc_register_handler(dsn_task_code_t code, string name, dsn_rpc_request_handler_t cb, IntPtr param, dsn_gpid gpid);
        [DllImport(DSN_CORE_DLL, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern IntPtr         dsn_rpc_unregiser_handler(dsn_task_code_t code, dsn_gpid gpid);   // return IntPtr param on registration  
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern dsn_task_t     dsn_rpc_create_response_task(dsn_message_t request, dsn_rpc_response_handler_t cb, IntPtr param, int reply_thread_hash, dsn_task_tracker_t tracker = default(dsn_task_tracker_t));
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern void          dsn_rpc_call(dsn_address_t server, dsn_task_t rpc_call);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern dsn_message_t dsn_rpc_call_wait(dsn_address_t server, dsn_message_t request); // returned msg must be explicitly msg_release_ref
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern void          dsn_rpc_call_one_way(dsn_address_t server, dsn_message_t request);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern void          dsn_rpc_reply(dsn_message_t response, dsn_error_t err);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern dsn_message_t dsn_rpc_get_response(dsn_task_t rpc_call); // returned msg must be explicitly msg_release_ref
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern void          dsn_rpc_enqueue_response(dsn_task_t rpc_call, dsn_error_t err, dsn_message_t response);

        //------------------------------------------------------------------------------
        //
        // file operations
        //
        //------------------------------------------------------------------------------
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern dsn_handle_t dsn_file_open(string file_name, int flag, int pmode);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern dsn_error_t  dsn_file_close(dsn_handle_t file);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern dsn_task_t   dsn_file_create_aio_task(dsn_task_code_t code, dsn_aio_handler_t cb, IntPtr param, int hash, dsn_task_tracker_t tracker = default(dsn_task_tracker_t));
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern void         dsn_file_read(dsn_handle_t file, byte[] buffer, int count, UInt64 offset, dsn_task_t cb);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern void         dsn_file_write(dsn_handle_t file, byte[] buffer, int count, UInt64 offset, dsn_task_t cb);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern void         dsn_file_copy_remote_directory(dsn_address_t remote, string source_dir, string dest_dir, bool overwrite, dsn_task_t cb);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern void         dsn_file_copy_remote_files(dsn_address_t remote, string source_dir, string[] source_files, string dest_dir, bool overwrite, dsn_task_t cb);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern size_t       dsn_file_get_io_size(dsn_task_t cb_task);
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern void         dsn_file_task_enqueue(dsn_task_t cb_task, dsn_error_t err, size_t size);

        //------------------------------------------------------------------------------
        //
        // environment inputs
        //
        //------------------------------------------------------------------------------
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern UInt64 dsn_now_ns();
        [DllImport(DSN_CORE_DLL, CallingConvention=CallingConvention.Cdecl, CharSet=CharSet.Ansi), SuppressUnmanagedCodeSecurity]
        public static extern UInt64 dsn_random64(UInt64 min, UInt64 max); // [min, max]

        public static UInt64 dsn_now_us() { return dsn_now_ns() / 1000; }
        public static UInt64 dsn_now_ms() { return dsn_now_ns() / 1000000; }
        public static UInt32 dsn_random32(UInt32 min, UInt32 max) { return (UInt32)(dsn_random64(min, max)); }
        public static double dsn_probability() { return dsn_random64(0, 1000000000) / 1000000000.0; }

    }
}
