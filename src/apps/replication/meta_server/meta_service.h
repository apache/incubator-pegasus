/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation

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
#pragma once

#include "replication_common.h"

using namespace rdsn;
using namespace rdsn::service;
using namespace rdsn::replication;

class server_state;
class load_balancer;
class meta_server_failure_detector;
class meta_service : public serviceletex<meta_service>
{
public:
    meta_service(server_state* state, configuration_ptr c);
    ~meta_service(void);

    void start();
    bool stop();

private:
    void OnMetaServiceRequest(message_ptr& request);

    // partition server & client => meta server
    void OnQueryConfig(ConfigurationNodeQueryRequest& request, __out ConfigurationNodeQueryResponse& response);
    void DoQueryConfigurationByIndexRequest(QueryConfigurationByIndexRequest& request, __out QueryConfigurationByIndexResponse& response);
    void update_configuration(configuration_update_request& request, __out ConfigurationUpdateResponse& response);
   
   
    // local timers
    void OnLoadBalancerTimer();

private:
    meta_server_failure_detector *_livenessMonitor;
    server_state               *_state;
    load_balancer              *_balancer;
    task_ptr                    _balancerTimer;
    replication_options         _opts;
};

