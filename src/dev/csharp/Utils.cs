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
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.InteropServices;
using System.IO;

namespace dsn.dev.csharp
{
    using dsn_task_t = IntPtr;
    using dsn_address_t = UInt64;

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
        private int _callback_index = -1;
        
        public SafeTaskHandle(dsn_task_t nativeHandle, int callback_index)
            : base(nativeHandle, true)
        {
            _callback_index = callback_index;
            Native.dsn_task_add_ref(nativeHandle);
        }

        protected override bool ReleaseHandle()
        {
            Native.dsn_task_release_ref(handle);
            return true;
        }

        public bool Cancel(bool waitFinished)
        {
            if (Native.dsn_task_cancel(handle, waitFinished))
            {
                GlobalInterOpLookupTable.GetRelease(_callback_index);
                return true;
            }
            else
                return false;
        }

        public bool Cancel(bool waitFinished, out bool finished)
        {
            if (Native.dsn_task_cancel2(handle, waitFinished, out finished))
            {
                GlobalInterOpLookupTable.GetRelease(_callback_index);
                return true;
            }
            else
                return false;
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
            addr = 0;
        }

        public RpcAddress(string host, UInt16 port)
        {
            addr = Native.dsn_address_build(host, port);
        }

        public static implicit operator dsn_address_t(RpcAddress c)
        {
            return c.addr;
        }

        public override string ToString()
        {
            return Native.dsn_address_to_string(addr);
        }

        public dsn_address_t addr;
    }
}
