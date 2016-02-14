using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace dsn.dev.csharp
{
    using dsn_handle_t = IntPtr;
    public class ZLock : SafeHandleZeroIsInvalid
    {
        public ZLock(bool recursive = false) 
            : base(Native.dsn_exlock_create(recursive), true) 
        {
        }

        protected override bool ReleaseHandle()
        {
 	        Native.dsn_exlock_destroy(handle); 
            return true;
        }

        public void Lock() { Native.dsn_exlock_lock(handle); }
        public bool TryLock() { return Native.dsn_exlock_try_lock(handle); }
        public void Unlock() { Native.dsn_exlock_unlock(handle); }
    }

    /// <summary>
    /// non-recursive rwlock
    /// </summary>
    public class ZRwLockNr : SafeHandleZeroIsInvalid
    {
        public ZRwLockNr()
            : base(Native.dsn_rwlock_nr_create(), true)
        {
        }

        protected override bool ReleaseHandle()
        {
 	        Native.dsn_rwlock_nr_destroy(handle);
            return true;
        }

        public void LockRead() { Native.dsn_rwlock_nr_lock_read(handle);  }
        public void UnlockRead() { Native.dsn_rwlock_nr_unlock_read(handle); }

        public void LockWrite() { Native.dsn_rwlock_nr_lock_write(handle); }
        public void UnlockWrite() { Native.dsn_rwlock_nr_unlock_write(handle); }
    }

    public class ZSemaphore : SafeHandleZeroIsInvalid
    {
        public ZSemaphore(int initial_count = 0) 
            : base(Native.dsn_semaphore_create(initial_count), true)
        {
        }

        protected override bool ReleaseHandle()
        {
 	        Native.dsn_semaphore_destroy(handle);
            return true;
        }

        public void signal(int count = 1) 
        {
            Native.dsn_semaphore_signal(handle, count); 
        }

        public bool wait(int timeout_milliseconds = int.MaxValue) 
        {
            if (timeout_milliseconds == int.MaxValue)
            {
                Native.dsn_semaphore_wait(handle);
                return true;
            }
            else
            {
                return Native.dsn_semaphore_wait_timeout(handle, timeout_milliseconds);
            }
        }
    }

}
