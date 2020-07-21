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
 *     service core initialization
 *
 * Revision history:
 *     July, 2015, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include <dsn/service_api_c.h>
#include <dsn/utility/ports.h>

#include <dsn/tool/simulator.h>
#include <dsn/tool/nativerun.h>
#include <dsn/toollet/tracer.h>
#include <dsn/toollet/profiler.h>
#include <dsn/toollet/fault_injector.h>
#include <dsn/toollet/explorer.h>

#include <dsn/tool/providers.common.h>
#include <dsn/utility/singleton.h>

void dsn_core_init()
{
    // register all providers
    dsn::tools::register_common_providers();

    // register all possible tools and toollets
    dsn::tools::register_tool<dsn::tools::nativerun>("nativerun");
    dsn::tools::register_tool<dsn::tools::simulator>("simulator");
    dsn::tools::register_toollet<dsn::tools::tracer>("tracer");
    dsn::tools::register_toollet<dsn::tools::profiler>("profiler");
    dsn::tools::register_toollet<dsn::tools::fault_injector>("fault_injector");
    dsn::tools::register_toollet<dsn::tools::explorer>("explorer");
}

#if defined(__linux__)
#include <dsn/git_commit.h>
#define STR_I(var) #var
#define STR(var) STR_I(var)
static char const rcsid[] =
    "$Version: rDSN (" DSN_GIT_COMMIT ")"
#if defined(DSN_BUILD_TYPE)
    " " STR(DSN_BUILD_TYPE)
#endif
        ", built by gcc " STR(__GNUC__) "." STR(__GNUC_MINOR__) "." STR(__GNUC_PATCHLEVEL__)
#if defined(DSN_BUILD_HOSTNAME)
            ", built on " STR(DSN_BUILD_HOSTNAME)
#endif
                ", built at " __DATE__ " " __TIME__ " $";
const char *dsn_rcsid() { return rcsid; }
#endif
