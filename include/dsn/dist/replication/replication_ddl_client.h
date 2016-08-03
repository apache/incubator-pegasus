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
 *     replication ddl client
 *
 * Revision history:
 *     2015-12-30, xiaotz, first version
 */

#pragma once

#include <cctype>
#include <dsn/dist/replication.h>

namespace dsn{ namespace replication{

class replication_ddl_client : public clientlet
{
public:
    replication_ddl_client(const dsn::rpc_address& meta_server);
    replication_ddl_client(const std::vector<dsn::rpc_address>& meta_servers);

    dsn::error_code create_app(const std::string& app_name, const std::string& app_type, int partition_count, int replica_count, const std::map<std::string, std::string>& envs, bool is_stateless);

    dsn::error_code drop_app(const std::string& app_name);

    dsn::error_code list_apps(const dsn::app_status::type status, const std::string& file_name);

    dsn::error_code cluster_info(const std::string& file_name);

    dsn::error_code list_nodes(const dsn::replication::node_status::type status, const std::string& file_name);

    dsn::error_code list_app(const std::string& app_name, bool detailed, const std::string& file_name);

    dsn::error_code list_app(const std::string& app_name, int32_t& app_id, int32_t& partition_count, std::vector<partition_configuration>& partitions);

    dsn::error_code control_meta_balancer_migration(bool start);

    dsn::error_code send_balancer_proposal(const configuration_balancer_request& request);
private:
    bool static valid_app_char(int c);

    void end_meta_request(task_ptr callback, int retry_times, error_code err, dsn_message_t request, dsn_message_t resp);

    template<typename TRequest>
    dsn::task_ptr request_meta(
            dsn_task_code_t code,
            std::shared_ptr<TRequest>& req,
            int timeout_milliseconds= 0,
            int reply_thread_hash = 0
            )
    {
        dsn_message_t msg = dsn_msg_create_request(code, timeout_milliseconds);
        task_ptr task = ::dsn::rpc::create_rpc_response_task(msg, nullptr, [](error_code err, dsn_message_t, dsn_message_t) { err.end_tracking(); }, reply_thread_hash);
        ::dsn::marshall(msg, *req);
        rpc::call(
            _meta_server,
            msg,
            this,
            [this, task] (error_code err, dsn_message_t request, dsn_message_t response)
            {
                end_meta_request(std::move(task), 0, err, request, response);
            }
         );
        return task;
    }

private:
    dsn::rpc_address _meta_server;
};

}} //namespace
