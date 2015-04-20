/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation, Robust Distributed System Nucleus(rDSN)

 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.

 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
#include "app_client_example1_service.h"
#include "replication_common.h"

app_client_example1_service::app_client_example1_service(service_app_spec* s, configuration_ptr c)
: service_app(s, c)
{
    _client = nullptr;
    _write_timer = nullptr;
}

app_client_example1_service::~app_client_example1_service(void)
{

}

error_code app_client_example1_service::start(int argc, char** argv)
{
    replication_options opts;
    opts.initialize(config());

    _client = new app_client_example1(opts.meta_servers);
    _write_timer = tasking::enqueue(LPC_TEST, this, 
        &app_client_example1_service::on_timer, 0, 10000, 1000);

    return ERR_SUCCESS;
}

void app_client_example1_service::stop(bool cleanup)
{
    if (_write_timer != nullptr)
    {
        _write_timer->cancel(true);
        _write_timer = nullptr;
    }

    if (_client != nullptr)
    {
        delete _client;
        _client = nullptr;
    }
}

void app_client_example1_service::on_timer()
{
    for (int i = 0; i < 100; i++)
    {
        kv_pair pr;
        pr.key = "test_key";
        pr.value = "test_value.";
        _client->begin_write2(pr);
        //_client->begin_append2(pr);
    }
}

