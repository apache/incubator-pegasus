using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace dsn.dev.csharp
{
    public static class Logging
    {
        
        public static void dlog(dsn_log_level_t level, string fmt)
        {
            Native.dsn_log("unknown", "unknown", 0, level, fmt);
        }

        public static void dlog(dsn_log_level_t level, string fmt, object arg0)
        {
            Native.dsn_log("unknown", "unknown", 0, level, string.Format(fmt, arg0));
        }

        public static void dlog(dsn_log_level_t level, string fmt, object arg0, object arg1)
        {
            Native.dsn_log("unknown", "unknown", 0, level, string.Format(fmt, arg0, arg1));
        }

        public static void dlog(dsn_log_level_t level, string fmt, object arg0, object arg1, object arg2)
        {
            Native.dsn_log("unknown", "unknown", 0, level, string.Format(fmt, arg0, arg1, arg2));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="level"></param>
        /// <param name="title"></param>
        /// <param name="fmt"></param>

        public static void dinfo(dsn_log_level_t level, string fmt)
        {
            if (Native.dsn_log_get_start_level() >= dsn_log_level_t.LOG_LEVEL_INFORMATION)
            {
                dlog(level, fmt);
            }
        }

        public static void dinfo(dsn_log_level_t level, string fmt, object arg0)
        {
            if (Native.dsn_log_get_start_level() >= dsn_log_level_t.LOG_LEVEL_INFORMATION)
            {
                dlog(level, fmt, arg0);
            }
        }

        public static void dinfo(dsn_log_level_t level, string fmt, object arg0, object arg1)
        {
            if (Native.dsn_log_get_start_level() >= dsn_log_level_t.LOG_LEVEL_INFORMATION)
            {
                dlog(level, fmt, arg0, arg1);
            }
        }

        public static void dinfo(dsn_log_level_t level, string fmt, object arg0, object arg1, object arg2)
        {
            if (Native.dsn_log_get_start_level() >= dsn_log_level_t.LOG_LEVEL_INFORMATION)
            {
                dlog(level, fmt, arg0, arg1, arg2);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="?"></param>
        /// <param name="?"></param>
        public static void ddebug(dsn_log_level_t level, string fmt)
        {
            if (Native.dsn_log_get_start_level() >= dsn_log_level_t.LOG_LEVEL_DEBUG)
            {
                dlog(level, fmt);
            }
        }

        public static void ddebug(dsn_log_level_t level, string fmt, object arg0)
        {
            if (Native.dsn_log_get_start_level() >= dsn_log_level_t.LOG_LEVEL_DEBUG)
            {
                dlog(level, fmt, arg0);
            }
        }

        public static void ddebug(dsn_log_level_t level, string fmt, object arg0, object arg1)
        {
            if (Native.dsn_log_get_start_level() >= dsn_log_level_t.LOG_LEVEL_DEBUG)
            {
                dlog(level, fmt, arg0, arg1);
            }
        }

        public static void ddebug(dsn_log_level_t level, string fmt, object arg0, object arg1, object arg2)
        {
            if (Native.dsn_log_get_start_level() >= dsn_log_level_t.LOG_LEVEL_DEBUG)
            {
                dlog(level, fmt, arg0, arg1, arg2);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="?"></param>
        /// <param name="?"></param>
        /// 
        public static void dwarn(dsn_log_level_t level, string fmt)
        {
            if (Native.dsn_log_get_start_level() >= dsn_log_level_t.LOG_LEVEL_WARNING)
            {
                dlog(level, fmt);
            }
        }

        public static void dwarn(dsn_log_level_t level, string fmt, object arg0)
        {
            if (Native.dsn_log_get_start_level() >= dsn_log_level_t.LOG_LEVEL_WARNING)
            {
                dlog(level, fmt, arg0);
            }
        }

        public static void dwarn(dsn_log_level_t level, string fmt, object arg0, object arg1)
        {
            if (Native.dsn_log_get_start_level() >= dsn_log_level_t.LOG_LEVEL_WARNING)
            {
                dlog(level, fmt, arg0, arg1);
            }
        }

        public static void dwarn(dsn_log_level_t level, string fmt, object arg0, object arg1, object arg2)
        {
            if (Native.dsn_log_get_start_level() >= dsn_log_level_t.LOG_LEVEL_WARNING)
            {
                dlog(level, fmt, arg0, arg1, arg2);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="?"></param>
        /// <param name="?"></param>

        public static void derror(dsn_log_level_t level, string fmt)
        {
            if (Native.dsn_log_get_start_level() >= dsn_log_level_t.LOG_LEVEL_ERROR)
            {
                dlog(level, fmt);
            }
        }

        public static void derror(dsn_log_level_t level, string fmt, object arg0)
        {
            if (Native.dsn_log_get_start_level() >= dsn_log_level_t.LOG_LEVEL_ERROR)
            {
                dlog(level, fmt, arg0);
            }
        }

        public static void derror(dsn_log_level_t level, string fmt, object arg0, object arg1)
        {
            if (Native.dsn_log_get_start_level() >= dsn_log_level_t.LOG_LEVEL_ERROR)
            {
                dlog(level, fmt, arg0, arg1);
            }
        }

        public static void derror(dsn_log_level_t level, string fmt, object arg0, object arg1, object arg2)
        {
            if (Native.dsn_log_get_start_level() >= dsn_log_level_t.LOG_LEVEL_ERROR)
            {
                dlog(level, fmt, arg0, arg1, arg2);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="?"></param>
        /// <param name="?"></param>
        /// 
        public static void dfatal(dsn_log_level_t level, string fmt)
        {
            if (Native.dsn_log_get_start_level() >= dsn_log_level_t.LOG_LEVEL_FATAL)
            {
                dlog(level, fmt);
            }
        }

        public static void dfatal(dsn_log_level_t level, string fmt, object arg0)
        {
            if (Native.dsn_log_get_start_level() >= dsn_log_level_t.LOG_LEVEL_FATAL)
            {
                dlog(level, fmt, arg0);
            }
        }

        public static void dfatal(dsn_log_level_t level, string fmt, object arg0, object arg1)
        {
            if (Native.dsn_log_get_start_level() >= dsn_log_level_t.LOG_LEVEL_FATAL)
            {
                dlog(level, fmt, arg0, arg1);
            }
        }

        public static void dfatal(dsn_log_level_t level, string fmt, object arg0, object arg1, object arg2)
        {
            if (Native.dsn_log_get_start_level() >= dsn_log_level_t.LOG_LEVEL_FATAL)
            {
                dlog(level, fmt, arg0, arg1, arg2);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="?"></param>
        /// <param name="?"></param>
        public static void dassert(bool condition, string fmt)
        {
            if (!condition)
            {
                dlog(dsn_log_level_t.LOG_LEVEL_FATAL, fmt);
                Native.dsn_coredump();
            }
        }

        public static void dassert(bool condition, string fmt, object arg0)
        {
            if (!condition)
            {
                dlog(dsn_log_level_t.LOG_LEVEL_FATAL, fmt, arg0);
                Native.dsn_coredump();
            }
        }

        public static void dassert(bool condition, string fmt, object arg0, object arg1)
        {
            if (!condition)
            {
                dlog(dsn_log_level_t.LOG_LEVEL_FATAL, fmt, arg0, arg1);
                Native.dsn_coredump();
            }
        }

        public static void dassert(bool condition, string fmt, object arg0, object arg1, object arg2)
        {
            if (!condition)
            {
                dlog(dsn_log_level_t.LOG_LEVEL_FATAL, fmt, arg0, arg1, arg2);
                Native.dsn_coredump();
            }
        }
    }
}
