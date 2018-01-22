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
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */
// apps
#include "echo.app.example.h"

static void dsn_app_registration_echo()
{
    // register all possible service apps
    dsn::service_app::register_factory<::dsn::example::echo_server_app>("server");
    dsn::service_app::register_factory<::dsn::example::echo_client_app>("client");
    dsn::service_app::register_factory<::dsn::example::echo_perf_test_client_app>(
        "client.perf.echo");
}

int main(int argc, char **argv)
{
    dsn_app_registration_echo();

    // specify what services and tools will run in config file, then run
    dsn_run(argc, argv, true);

    // TODO: external echo lib test
    dsn_mimic_app("client", 1);
    dsn::rpc_address server_addr("localhost", 27001);
    dsn::example::echo_client client(server_addr);
    std::string resp;

    client.ping_sync("hihihihihihii");

    return 0;
}
