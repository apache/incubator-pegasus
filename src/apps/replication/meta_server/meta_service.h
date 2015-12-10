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
 *     meta server service for EON (rDSN layer 2)
 *
 * Revision history:
 *     2015-03-09, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

# pragma once

# include "replication_common.h"

using namespace dsn;
using namespace dsn::service;
using namespace dsn::replication;

class server_state;
class load_balancer;
class meta_server_failure_detector;
class replication_checker;
namespace test {
    class test_checker;
}

namespace dsn {
    namespace replication {
        class replication_checker;
        class test_checker;
    }
}

class meta_service : public serverlet<meta_service>
{
public:
    meta_service(const char* work_dir);
    ~meta_service();

    error_code start();
    void stop();

    const char* work_dir() const { return _work_dir.c_str(); }
    const char* cluster_root() const { return _cluster_root.c_str(); }

private:
    // partition server & client => meta server
    // query partition configuration
    void on_query_configuration_by_node(dsn_message_t req);
    void on_query_configuration_by_index(dsn_message_t req);

    // update configuration
    void on_modify_replica_config_explictly(dsn_message_t req); // for testing
    void on_update_configuration(dsn_message_t req);
    void update_configuration_on_machine_failure(std::shared_ptr<configuration_update_request>& update);

    // load balance actions
    void on_load_balance_start();
    void on_load_balance_timer();
    void on_config_changed(global_partition_id gpid);

private:
    friend class meta_server_failure_detector;
    friend class ::dsn::replication::replication_checker;
    friend class ::dsn::replication::test::test_checker;

    std::string                  _work_dir;
    std::string                  _cluster_root;
    server_state                 *_state;
    meta_server_failure_detector *_failure_detector;
    load_balancer                *_balancer;
    dsn::task_ptr                _balancer_timer;
    replication_options          _opts;
    bool                         _started;
}; 

