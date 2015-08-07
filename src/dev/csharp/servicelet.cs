using System;
using System.Collections.Generic;
using System.Collections;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Runtime.InteropServices;

namespace dsn.dev.csharp
{
    using dsn_task_t = IntPtr;
    using dsn_handle_t = IntPtr;
        
    public class Servicelet : SafeHandleZeroIsInvalid
    {
        public Servicelet(int task_bucket_count = 13)
            : base(IntPtr.Zero, true)
        {
            SetHandle(Native.dsn_task_tracker_create(task_bucket_count));
            _access_thread_id_inited = false;
        }

        protected override bool ReleaseHandle()
        {
            if (!IsInvalid)
            {
                Native.dsn_task_tracker_destroy(handle);
                return true;
            }
            else
                return false;
        }

        protected IntPtr tracker() { return handle; }

        public void wait_all_pending_tasks()
        {
            Native.dsn_task_tracker_wait_all(handle);
        }

        public void cancel_all_pending_tasks()
        {
            Native.dsn_task_tracker_cancel_all(handle);
        }

        public static void primary_address(out dsn_address_t addr) { Native.dsn_primary_address2(out addr); }
        public static UInt32 random32(UInt32 min, UInt32 max) { return Native.dsn_random32(min, max); }
        public static UInt64 random64(UInt64 min, UInt64 max) { return Native.dsn_random64(min, max); }
        public static UInt64 now_ns() { return Native.dsn_now_ns(); }
        public static UInt64 now_us() { return Native.dsn_now_us(); }
        public static UInt64 now_ms() { return Native.dsn_now_ms(); }

        protected void check_hashed_access()
        {
            if (_access_thread_id_inited)
            {
                Logging.dassert((Native.dsn_threadpool_get_current_tid() == _access_thread_id),
                    "the service is assumed to be accessed by one thread only!"
                    );
            }
            else
            {
                _access_thread_id = Native.dsn_threadpool_get_current_tid();
                _access_thread_id_inited = true;
            }
        }

        private int _access_thread_id;
        private bool _access_thread_id_inited;
    
        public delegate void task_handler();

        // TODO: what if the task is cancelled
        static void c_task_handler(IntPtr h)
        {
            int idx2 = (int)h;
            var hr = GlobalInterOpLookupTable.GetRelease(idx2) as task_handler;
            hr();
        }

        static dsn_task_handler_t _c_task_handler_holder = c_task_handler;

        public static void CallAsync(
            TaskCode evt,
            Servicelet callbackOwner,
            task_handler callback,
            int hash = 0,
            int delay_milliseconds = 0,
            int timer_interval_milliseconds = 0
            )
        {
            int idx = GlobalInterOpLookupTable.Put(callback);
            IntPtr task;

            if (timer_interval_milliseconds == 0)
                task = Native.dsn_task_create(evt, _c_task_handler_holder, (IntPtr)idx, hash);
            else
                task = Native.dsn_task_create_timer(evt, _c_task_handler_holder, (IntPtr)idx, hash, timer_interval_milliseconds);

            Native.dsn_task_call(task, callbackOwner != null ? callbackOwner.tracker() : IntPtr.Zero, delay_milliseconds);
        }

        //
        // this gives you the task handle so you can wait or cancel
        // the task, with the cost of add/ref the task handle
        // 
        public static SafeTaskHandle CallAsync2(
            TaskCode evt,
            Servicelet callbackOwner,
            task_handler callback,
            int hash = 0,
            int delay_milliseconds = 0,
            int timer_interval_milliseconds = 0
            )
        {
            int idx = GlobalInterOpLookupTable.Put(callback);
            IntPtr task;

            if (timer_interval_milliseconds == 0)
                task = Native.dsn_task_create(evt, _c_task_handler_holder, (IntPtr)idx, hash);
            else
                task = Native.dsn_task_create_timer(evt, _c_task_handler_holder, (IntPtr)idx, hash, timer_interval_milliseconds);

            var ret = new SafeTaskHandle(task);
            Native.dsn_task_call(task, callbackOwner != null ? callbackOwner.tracker() : IntPtr.Zero, delay_milliseconds);
            return ret;
        }
                
        // no callback
        public static void RpcCallOneWay(
            RpcAddress server,
            RpcWriteStream requestStream
            )
        {
            Logging.dassert(requestStream.IsFlushed(),
                "RpcWriteStream must be flushed after write in the same thread");

            Native.dsn_rpc_call_one_way(server, requestStream.DangerousGetHandle());
        }

        public static RpcReadStream RpcCallSync(
            RpcAddress server,
            RpcWriteStream requestStream
            )
        {
            Logging.dassert(requestStream.IsFlushed(), 
                "RpcWriteStream must be flushed after write in the same thread");

            IntPtr respMsg = Native.dsn_rpc_call_wait(server, requestStream.DangerousGetHandle());
            if (IntPtr.Zero == respMsg)
            {
                return null;
            }   
            else
            {
                return new RpcReadStream(respMsg, true);
            }
        }

        public delegate void RpcResponseHandler(ErrorCode err, RpcReadStream responseStream);

        static void c_rpc_response_handler(int err, IntPtr reqc, IntPtr respc, IntPtr h)
        {
            int idx2 = (int)h;
            var hr = GlobalInterOpLookupTable.GetRelease(idx2) as RpcResponseHandler;
            
            if (err == 0)
            {
                var rms = new RpcReadStream(respc, false);
                hr(new ErrorCode(err), rms);
            }
            else
            {
                hr(new ErrorCode(err), null);
            }
        }

        static dsn_rpc_response_handler_t _c_rpc_response_handler_holder = c_rpc_response_handler;

        public static void RpcCallAsync(
            RpcAddress server,
            RpcWriteStream requestStream,
            Servicelet callbackOwner,
            RpcResponseHandler callback,
            int replyHash = 0
            )
        {
            Logging.dassert(requestStream.IsFlushed(),
                "RpcWriteStream must be flushed after write in the same thread");

            var idx = GlobalInterOpLookupTable.Put(callback);
            dsn_task_t task = Native.dsn_rpc_create_response_task(
                requestStream.DangerousGetHandle(),
                _c_rpc_response_handler_holder, 
                (IntPtr)idx, 
                replyHash
                );
            Native.dsn_rpc_call(server, task, callbackOwner != null ? callbackOwner.tracker() : IntPtr.Zero);
        }

        //
        // this gives you the task handle so you can wait or cancel
        // the task, with the cost of add/ref the task handle
        // 
        public static SafeTaskHandle RpcCallAsync2(
            RpcAddress server,
            RpcWriteStream requestStream,
            Servicelet callbackOwner,
            RpcResponseHandler callback,
            int replyHash = 0
            )
        {
            Logging.dassert(requestStream.IsFlushed(),
                "RpcWriteStream must be flushed after write in the same thread");

            var idx = GlobalInterOpLookupTable.Put(callback);
            dsn_task_t task = Native.dsn_rpc_create_response_task(
                requestStream.DangerousGetHandle(),
                _c_rpc_response_handler_holder,
                (IntPtr)idx,
                replyHash
                );

            var ret = new SafeTaskHandle(task);
            Native.dsn_rpc_call(server, task, callbackOwner != null ? callbackOwner.tracker() : IntPtr.Zero);
            return ret;
        }

        public static dsn_handle_t FileOpen(string file_name, int flag, int pmode)
        {
            return Native.dsn_file_open(file_name, flag, pmode);
        }

        public static ErrorCode FileClose(dsn_handle_t file)
        {
            int err = Native.dsn_file_close(file);
            return new ErrorCode(err);
        }

        public delegate void AioHandler(ErrorCode err, int size);

        static void c_aio_handler(int err, IntPtr size, IntPtr h)
        {
            int idx2 = (int)h;
            var hr = GlobalInterOpLookupTable.GetRelease(idx2) as AioHandler;
            
            hr(new ErrorCode(err), size.ToInt32());
        }

        static dsn_aio_handler_t _c_aio_handler_holder = c_aio_handler;

        public static dsn_task_t FileRead(
            dsn_handle_t hFile,
            byte[] buffer,
            int count,
            UInt64 offset,
            TaskCode callbackCode,
            Servicelet callbackOwner,
            AioHandler callback,
            int hash = 0
            )
        {
            int idx = GlobalInterOpLookupTable.Put(callback);
            dsn_task_t task = Native.dsn_file_create_aio_task(callbackCode, _c_aio_handler_holder, (IntPtr)idx, hash);
            Native.dsn_file_read(hFile, buffer, count, offset, task, callbackOwner != null ? callbackOwner.tracker() : IntPtr.Zero);
            return task;
        }

        public static dsn_task_t FileWrite(
            dsn_handle_t hFile,
            byte[] buffer,
            int count,
            UInt64 offset,
            TaskCode callbackCode,
            Servicelet callbackOwner,
            AioHandler callback,
            int hash = 0
            )
        {
            int idx = GlobalInterOpLookupTable.Put(callback);
            dsn_task_t task = Native.dsn_file_create_aio_task(callbackCode, _c_aio_handler_holder, (IntPtr)idx, hash);
            Native.dsn_file_write(hFile, buffer, count, offset, task, callbackOwner != null ? callbackOwner.tracker() : IntPtr.Zero);
            return task;
        }

        public static dsn_task_t CopyRemoteFiles(
            dsn_address_t remote,
            string source_dir,
            string[] files,
            string dest_dir,
            bool overwrite, 
            TaskCode callbackCode,
            Servicelet callbackOwner,
            AioHandler callback,
            int hash = 0
            )
        {
            int idx = GlobalInterOpLookupTable.Put(callback);
            dsn_task_t task = Native.dsn_file_create_aio_task(callbackCode, _c_aio_handler_holder, (IntPtr)idx, hash);
            Native.dsn_file_copy_remote_files(remote, source_dir, files, dest_dir, overwrite, task, callbackOwner != null ? callbackOwner.tracker() : IntPtr.Zero);
            return task;
        }

        public static dsn_task_t CopyRemoteDirectory(
            dsn_address_t remote,
            string source_dir,
            string dest_dir,
            bool overwrite,
            TaskCode callbackCode,
            Servicelet callbackOwner,
            AioHandler callback,
            int hash = 0
            )
        {
            int idx = GlobalInterOpLookupTable.Put(callback);
            dsn_task_t task = Native.dsn_file_create_aio_task(callbackCode, _c_aio_handler_holder, (IntPtr)idx, hash);
            Native.dsn_file_copy_remote_directory(remote, source_dir, dest_dir, overwrite, task, callbackOwner != null ? callbackOwner.tracker() : IntPtr.Zero);
            return task;
        }            
    };
}
