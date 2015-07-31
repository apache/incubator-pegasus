using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace dsn.dev.csharp
{
    public struct error_code
    {
        public error_code(int err)
        {
            _error = err;
        }

        public error_code(error_code err)
        {
            _error = err._error;
        }

        public string to_string()
        {
            return core.dsn_error_to_string(_error);
        }

        public static implicit operator int(error_code ec)
        {
            return ec._error;
        }

        private int _error;
    }


    public struct threadpool_code
    {
        public static threadpool_code THREAD_POOL_INVALID = new threadpool_code("THREAD_POOL_INVALID");
        
        public static threadpool_code THREAD_POOL_DEFAULT = new threadpool_code("THREAD_POOL_DEFAULT");

        public threadpool_code(int c)
        {
            _code = c;
        }

        public threadpool_code(threadpool_code c)
        {
            _code = c._code;
        }

        public threadpool_code(string name)
        {
            _code = core.dsn_threadpool_code_register(name);
        }

        public string to_string()
        {
            return core.dsn_task_code_to_string(_code);
        }

        public static implicit operator int(threadpool_code c)
        {
            return c._code;
        }

        private int _code;
    }

    public struct task_code
    {
        public static task_code TASK_CODE_INVALID = new task_code("TASK_CODE_INVALID", dsn_task_type_t.TASK_TYPE_COMPUTE, dsn_task_priority_t.TASK_PRIORITY_COMMON, threadpool_code.THREAD_POOL_DEFAULT);

        public task_code(int c)
        {
            _code = c;
        }

        public task_code(task_code c)
        {
            _code = c._code;
        }

        public task_code(string name, dsn_task_type_t type, dsn_task_priority_t pri, threadpool_code pool)
        {
            _code = core.dsn_task_code_register(name, type, pri, pool);
        }

        public string to_string()
        {
            return core.dsn_task_code_to_string(_code);
        }

        public static implicit operator int(task_code c)
        {
            return c._code;
        }

        private int _code;
    }

    public class ResourceHolder : IDisposable
    {
        private bool disposed = false;

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposed)
            {
                if (disposing)
                {
                    // Free other state (managed objects).
                    ReleaseManagedResources();
                }
                // Free your own state (unmanaged objects).
                ReleaseUnmanagedResources();
                disposed = true;
            }
        }

        protected virtual void ReleaseUnmanagedResources()
        {
        }

        protected virtual void ReleaseManagedResources()
        {
        }

        ~ResourceHolder()
        {
            // Simply call Dispose(false).
            Dispose(false);
        }
    }
}
