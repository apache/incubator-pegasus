/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 * 
 * -=- Robust Distributed System Nucleus(rDSN) -=- 
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
 #pragma once

#include "replication_common.h"

using namespace dsn;
using namespace dsn::service;
using namespace dsn::replication;

class server_state;
class load_balancer;
class meta_server_failure_detector;
class meta_service : public serverlet<meta_service>
{
public:
    meta_service(server_state* state, configuration_ptr c);
    ~meta_service(void);

    void start();
    bool stop();

private:
    void on_request(message_ptr& request);

    // partition server & client => meta server
    void query_configuration_by_node(configuration_query_by_node_request& request, __out_param configuration_query_by_node_response& response);
    void query_configuration_by_index(configuration_query_by_index_request& request, __out_param configuration_query_by_index_response& response);
    void update_configuration(configuration_update_request& request, __out_param configuration_update_response& response);
      
    // load balance actions
    void on_load_balance_timer();
    void on_config_changed(global_partition_id gpid);

private:
    friend class meta_server_failure_detector;
    meta_server_failure_detector *_failure_detector;
    server_state                 *_state;
    load_balancer                *_balancer;
    task_ptr                     _balancer_timer;
    replication_options          _opts;
}; 

