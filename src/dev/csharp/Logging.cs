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
