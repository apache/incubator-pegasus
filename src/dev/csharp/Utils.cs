using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.InteropServices;

namespace dsn.dev.csharp
{
    using dsn_task_t = IntPtr;
    public struct ErrorCode
    {
        public static ErrorCode ERR_OK = new ErrorCode("ERR_OK");
        public static ErrorCode ERR_TIMEOUT = new ErrorCode("ERR_TIMEOUT");
        public static ErrorCode ERR_INVALID_PARAMETERS = new ErrorCode("ERR_INVALID_PARAMETERS");
        
        public ErrorCode(int err)
        {
            _error = err;
        }

        public ErrorCode(ErrorCode err)
        {
            _error = err._error;
        }

        public ErrorCode(string err)
        {
            _error = Native.dsn_error_register(err);
        }

        public override string ToString()
        {
            var ptr = Native.dsn_error_to_string(_error);
            return Marshal.PtrToStringAnsi(ptr);
        }

        public static implicit operator int(ErrorCode ec)
        {
            return ec._error;
        }

        public override bool Equals(object obj)
        {
            return this._error == ((ErrorCode)obj)._error;
        }

        public override int GetHashCode()
        {
            return _error.GetHashCode();
        }

        private int _error;
    }


    public struct ThreadPoolCode
    {
        public static ThreadPoolCode THREAD_POOL_INVALID = new ThreadPoolCode("THREAD_POOL_INVALID");
        
        public static ThreadPoolCode THREAD_POOL_DEFAULT = new ThreadPoolCode("THREAD_POOL_DEFAULT");

        public ThreadPoolCode(int c)
        {
            _code = c;
        }

        public ThreadPoolCode(ThreadPoolCode c)
        {
            _code = c._code;
        }

        public ThreadPoolCode(string name)
        {
            _code = Native.dsn_threadpool_code_register(name);
        }

        public override string ToString()
        {
            var ptr = Native.dsn_task_code_to_string(_code);
            return Marshal.PtrToStringAnsi(ptr);
        }

        public static implicit operator int(ThreadPoolCode c)
        {
            return c._code;
        }

        public override bool Equals(object obj)
        {
            return this._code == ((ThreadPoolCode)obj)._code;
        }

        public override int GetHashCode()
        {
            return _code.GetHashCode();
        }

        private int _code;
    }

    public struct TaskCode
    {
        public static TaskCode TASK_CODE_INVALID = new TaskCode("TASK_CODE_INVALID", dsn_task_type_t.TASK_TYPE_COMPUTE, dsn_task_priority_t.TASK_PRIORITY_COMMON, ThreadPoolCode.THREAD_POOL_DEFAULT);

        public TaskCode(int c)
        {
            _code = c;
        }

        public TaskCode(TaskCode c)
        {
            _code = c._code;
        }

        public TaskCode(string name, dsn_task_type_t type, dsn_task_priority_t pri, ThreadPoolCode pool)
        {
            _code = Native.dsn_task_code_register(name, type, pri, pool);
        }

        public override string ToString()
        {
            var ptr = Native.dsn_task_code_to_string(_code);
            return Marshal.PtrToStringAnsi(ptr);
        }

        public static implicit operator int(TaskCode c)
        {
            return c._code;
        }


        public override bool Equals(object obj)
        {
            return this._code == ((TaskCode)obj)._code;
        }

        public override int GetHashCode()
        {
            return _code.GetHashCode();
        }

        private int _code;
    }

    public class SafeTaskHandle : SafeHandleZeroIsInvalid
    {
        public SafeTaskHandle(dsn_task_t nativeHandle)
            : base(nativeHandle, true)
        {
            Native.dsn_task_add_ref(nativeHandle);
        }

        protected override bool ReleaseHandle()
        {
            Native.dsn_task_release_ref(handle);
            return true;
        }

        public bool Cancel(bool waitFinished)
        {
            return Native.dsn_task_cancel(handle, waitFinished);
        }

        public bool Cancel(bool waitFinished, out bool finished)
        {
            return Native.dsn_task_cancel2(handle, waitFinished, out finished);
        }

        public void Wait()
        {
            Native.dsn_task_wait(handle);
        }

        public bool WaitTimeout(int milliseconds)
        {
            return Native.dsn_task_wait_timeout(handle, milliseconds);
        }
    }

    public class RpcAddress
    {
        public RpcAddress(dsn_address_t ad)
        {
            addr = ad;
        }

        public RpcAddress()
        {
            addr = new dsn_address_t();
            addr.ip = 0;
            addr.port = 0;
            addr.name = "invalid";
        }

        public static implicit operator dsn_address_t(RpcAddress c)
        {
            return c.addr;
        }

        public dsn_address_t addr;
    }
}
