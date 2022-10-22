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

#include "coredump.h"
#include "runtime/tool_api.h"
#include <sys/types.h>
#include <signal.h>
#include "runtime/app_model.h"

namespace dsn {
namespace utils {

static void handle_core_dump(int);
static void handle_term(int);

void coredump::init()
{
    signal(SIGSEGV, handle_core_dump);
    signal(SIGTERM, handle_term);
}

void coredump::write()
{
    // TODO: not implemented
    //

    ::dsn::tools::sys_exit.execute(SYS_EXIT_EXCEPTION);
}

static void handle_core_dump(int signal_id)
{
    printf("got signal id: %d\n", signal_id);
    fflush(stdout);
    /*
     * firstly we must set the sig_handler to default,
     * to prevent the possible inifinite loop
     * for example: an sigsegv in the coredump::write()
     */
    if (signal_id == SIGSEGV) {
        signal(SIGSEGV, SIG_DFL);
    }
    coredump::write();
}

static void handle_term(int signal_id)
{
    printf("got signal id: %d\n", signal_id);
    fflush(stdout);
    dsn_exit(0);
}
} // namespace utils
} // namespace dsn
