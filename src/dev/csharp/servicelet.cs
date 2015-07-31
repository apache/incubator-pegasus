using System;
using System.Collections.Generic;
using System.Collections;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Runtime.InteropServices;

namespace dsn.dev.csharp
{
    using dsn_error_t = System.Int32;
    using task_code = System.Int32;
    using dsn_threadpool_code_t = System.Int32;
    using dsn_handle_t = System.UInt64;
    using dsn_task_t = System.IntPtr;
    using dsn_task_tracker_t = System.IntPtr;
    using dsn_message_t = System.IntPtr;
    #if _WIN64 // TODO: FIX ME
        using size_t = System.UInt64;
    #else
    using size_t = System.UInt64;
    #endif

    public class message : ResourceHolder
    {
        public message()
        {
            _message = IntPtr.Zero;
        }

        public message(dsn_message_t msg)
        {
            _message = msg;
            if (IntPtr.Zero != msg)
                core.dsn_msg_add_ref(msg);
        }

        protected override void ReleaseUnmanagedResources()
        {
            if (IntPtr.Zero != _message)
                core.dsn_msg_release_ref(_message);
        }

        private dsn_message_t _message;
    }
    
    public class task : ResourceHolder
    {
        public task()
        {
            _task = IntPtr.Zero;
            _rpc_response = IntPtr.Zero;
        }

        protected override void ReleaseUnmanagedResources()
        {
 	        core.dsn_task_release_ref(_task);

            if (IntPtr.Zero != _rpc_response)
                core.dsn_msg_release_ref(_rpc_response);
        }

        public void set_task_info(dsn_task_t t, servicelet svc)
        {
            _task = t;
            core.dsn_task_add_ref(t);
            core.dsn_task_set_tracker(t, svc.tracker());
        }

        public dsn_task_t native_handle() { return _task; }
                        
        public bool cancel(bool wait_until_finished, out bool finished)
        {
            return core.dsn_task_cancel2(_task, wait_until_finished, out finished);
        }

        public bool wait()
        {
            return core.dsn_task_wait(_task);
        }

        public bool wait(int timeout_millieseconds)
        {
            return core.dsn_task_wait_timeout(_task, timeout_millieseconds);
        }

        public error_code error()
        {
            return new error_code(core.dsn_task_error(_task));
        }
            
        public size_t io_size()
        {
            return core.dsn_file_get_io_size(_task);
        }
            
        public void enqueue_aio(error_code err, size_t size)
        {
            core.dsn_file_task_enqueue(_task, err, size);
        }

        public dsn_message_t response()
        {
            if (_rpc_response == IntPtr.Zero)
                _rpc_response = core.dsn_rpc_get_response(_task);
            return _rpc_response;
        }

        public void enqueue_rpc_response(error_code err, dsn_message_t resp)
        {
            core.dsn_rpc_enqueue_response(_task, err, resp);
        }

        private dsn_task_t    _task;

        private dsn_message_t _rpc_response;
    };

    public class servicelet : ResourceHolder
    {
        public servicelet(int task_bucket_count = 13)
        {
            _tracker = core.dsn_task_tracker_create(task_bucket_count);
            _access_thread_id_inited = false;
            _gch = GCHandle.Alloc(this);
        }

        protected override void ReleaseUnmanagedResources()
        {
            core.dsn_task_tracker_destroy(_tracker);
            _gch.Free();
        }

        public dsn_task_tracker_t tracker() { return _tracker; }

        public static void primary_address(out dsn_address_t addr) { core.dsn_primary_address2(out addr); }
        public static UInt32 random32(UInt32 min, UInt32 max) { return core.dsn_random32(min, max); }
        public static UInt64 random64(UInt64 min, UInt64 max) { return core.dsn_random64(min, max); }
        public static UInt64 now_ns() { return core.dsn_now_ns(); }
        public static UInt64 now_us() { return core.dsn_now_us(); }
        public static UInt64 now_ms() { return core.dsn_now_ms(); }

        //public task enqueue(
        //    task_code evt,
        //    servicelet context,
        //    task_handler callback,
        //    int hash = 0,
        //    int delay_milliseconds = 0,
        //    int timer_interval_milliseconds = 0
        //    )
        //{
        //    //var t = core.dsn_task_create()
        //}

        //template<typename T> // where T : public virtual servicelet
        //inline task enqueue(
        //    task_code evt,
        //    T* owner,
        //    void (T::*callback)(),
        //    int hash = 0,
        //    int delay_milliseconds = 0,
        //    int timer_interval_milliseconds = 0
        //    )
        //{
        //    task_handler h = std::bind(callback, owner);
        //    return enqueue(
        //        evt,
        //        owner,
        //        h,
        //        hash,
        //        delay_milliseconds,
        //        timer_interval_milliseconds
        //        );
        //}
        ////
        //// for TRequest/TResponse, we assume that the following routines are defined:
        ////    marshall(binary_writer& writer, const T& val); 
        ////    unmarshall(binary_reader& reader, __out_param T& val);
        //// either in the namespace of ::dsn::utils or T
        //// developers may write these helper functions by their own, or use tools
        //// such as protocol-buffer, thrift, or bond to generate these functions automatically
        //// for their TRequest and TResponse
        ////

        //// no callback
        //template<typename TRequest>
        //void call_one_way_typed(
        //    const dsn_address_t& server,
        //    task_code code,
        //    const TRequest& req,
        //    int hash = 0
        //    );

        //template<typename TRequest>
        //::dsn::error_code call_typed_wait(
        //    /*out*/ ::dsn::message_ptr* response,
        //    const dsn_address_t& server,
        //    task_code code,
        //    const TRequest& req,
        //    int hash = 0,
        //    int timeout_milliseconds = 0
        //    );

        //// callback type 1:
        ////  void (T::*callback)(error_code, std::shared_ptr<TRequest>&, std::shared_ptr<TResponse>&)
        //template<typename T, typename TRequest, typename TResponse>
        //task call_typed(
        //    const dsn_address_t& server,
        //    task_code code,
        //    std::shared_ptr<TRequest>& req,
        //    T* owner,
        //    void (T::*callback)(error_code, std::shared_ptr<TRequest>&, std::shared_ptr<TResponse>&),
        //    int request_hash = 0,
        //    int timeout_milliseconds = 0,
        //    int reply_hash = 0
        //    );

        //// callback type 2:
        ////  std::function<void(error_code, std::shared_ptr<TRequest>&, std::shared_ptr<TResponse>&)>
        //template<typename TRequest, typename TResponse>
        //task call_typed(
        //    const dsn_address_t& server,
        //    task_code code,
        //    std::shared_ptr<TRequest>& req,
        //    servicelet* owner,
        //    std::function<void(error_code, std::shared_ptr<TRequest>&, std::shared_ptr<TResponse>&)> callback,
        //    int request_hash = 0,
        //    int timeout_milliseconds = 0,
        //    int reply_hash = 0
        //    );

        //// callback type 5
        ////   void (T::*)(error_code, const TResponse&, void*);
        //template<typename T, typename TRequest, typename TResponse>
        //task call_typed(
        //    const dsn_address_t& server,
        //    task_code code,
        //    const TRequest& req,
        //    T* owner,
        //    void(T::*callback)(error_code, const TResponse&, void*),
        //    void* context,
        //    int request_hash = 0,
        //    int timeout_milliseconds = 0,
        //    int reply_hash = 0
        //    );

        //// callback type 3:
        ////  std::function<void(error_code, const TResponse&, void*)>
        //template<typename TRequest, typename TResponse>
        //task call_typed(
        //    const dsn_address_t& server,
        //    task_code code,
        //    const TRequest& req,
        //    servicelet* owner,
        //    std::function<void(error_code, const TResponse&, void*)> callback,
        //    void* context,
        //    int request_hash = 0,
        //    int timeout_milliseconds = 0,
        //    int reply_hash = 0
        //    );

        //// callback type 4:
        ////  std::function<void(error_code, dsn_message_t, dsn_message_t)>
        //task call(
        //    const dsn_address_t& server,
        //    dsn_message_t request,
        //    servicelet* owner,
        //    rpc_reply_handler callback,
        //    int reply_hash = 0
        //    );

        //task read(
        //    dsn_handle_t hFile,
        //    char* buffer,
        //    int count,
        //    uint64_t offset,
        //    task_code callback_code,
        //    servicelet* owner,
        //    aio_handler callback,
        //    int hash = 0
        //    );

        //task write(
        //    dsn_handle_t hFile,
        //    const char* buffer,
        //    int count,
        //    uint64_t offset,
        //    task_code callback_code,
        //    servicelet* owner,
        //    aio_handler callback,
        //    int hash = 0
        //    );

        //template<typename T>
        //inline task read(
        //    dsn_handle_t hFile,
        //    char* buffer,
        //    int count,
        //    uint64_t offset,
        //    task_code callback_code,
        //    T* owner,
        //    void(T::*callback)(error_code, uint32_t),
        //    int hash = 0
        //    )
        //{
        //    aio_handler h = std::bind(callback, owner, std::placeholders::_1, std::placeholders::_2);
        //    return read(hFile, buffer, count, offset, callback_code, owner, h, hash);
        //}

        //template<typename T>
        //inline task write(
        //    dsn_handle_t hFile,
        //    const char* buffer,
        //    int count,
        //    uint64_t offset,
        //    task_code callback_code,
        //    T* owner,
        //    void(T::*callback)(error_code, uint32_t),
        //    int hash = 0
        //    )
        //{
        //    aio_handler h = std::bind(callback, owner, std::placeholders::_1, std::placeholders::_2);
        //    return write(hFile, buffer, count, offset, callback_code, owner, h, hash);
        //}

        //task copy_remote_files(
        //    const dsn_address_t& remote,
        //    const std::string& source_dir,
        //    std::vector<std::string>& files,  // empty for all
        //    const std::string& dest_dir,
        //    bool overwrite,
        //    task_code callback_code,
        //    servicelet* owner,
        //    aio_handler callback,
        //    int hash = 0
        //    );

        //inline task copy_remote_directory(
        //    const dsn_address_t& remote,
        //    const std::string& source_dir,
        //    const std::string& dest_dir,
        //    bool overwrite,
        //    task_code callback_code,
        //    servicelet* owner,
        //    aio_handler callback,
        //    int hash = 0
        //    )
        //{
        //    std::vector<std::string> files;
        //    return copy_remote_files(
        //        remote, source_dir, files, dest_dir, overwrite,
        //        callback_code, owner, callback, hash
        //        );
        //}
            
        public void check_hashed_access()
        {
            if (_access_thread_id_inited)
            {
                Logging.dassert((core.dsn_threadpool_get_current_tid() == _access_thread_id),
                    "the service is assumed to be accessed by one thread only!"
                    );
            }
            else
            {
                _access_thread_id = core.dsn_threadpool_get_current_tid();
                _access_thread_id_inited = true;
            }
        }

        private int                            _access_thread_id;
        private bool                           _access_thread_id_inited;
        private dsn_task_tracker_t             _tracker;
        protected GCHandle                     _gch;
    };
}
