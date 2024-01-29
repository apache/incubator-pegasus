/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include "meta/meta_service_app.h"
#include "replica/replication_service_app.h"
#include <pegasus/version.h>
#include <pegasus/git_commit.h>
#include "utils/builtin_metrics.h"

namespace pegasus {
namespace server {

class pegasus_replication_service_app : public ::dsn::replication::replication_service_app
{
public:
    pegasus_replication_service_app(const dsn::service_app_info *info)
        : ::dsn::replication::replication_service_app::replication_service_app(info)
    {
    }

    virtual ::dsn::error_code start(const std::vector<std::string> &args) override
    {
        // args for replication http service
        std::vector<std::string> args_new(args);
        args_new.emplace_back(PEGASUS_VERSION);
        args_new.emplace_back(PEGASUS_GIT_COMMIT);

        // Actually the root caller, start_app() in service_control_task::exec() will also do
        // CHECK for ERR_OK. Do CHECK here to guarantee that all following services (such as
        // built-in metrics) are started.
        CHECK_EQ(::dsn::replication::replication_service_app::start(args_new), ::dsn::ERR_OK);

        _builtin_metrics.start();
        return ::dsn::ERR_OK;
    }

    virtual ::dsn::error_code stop(bool cleanup = false) override
    {
        ::dsn::error_code ret = ::dsn::replication::replication_service_app::stop();

        _builtin_metrics.stop();
        return ret;
    }

private:
    dsn::builtin_metrics _builtin_metrics;
};

class pegasus_meta_service_app : public ::dsn::service::meta_service_app
{
public:
    pegasus_meta_service_app(const dsn::service_app_info *info)
        : ::dsn::service::meta_service_app::meta_service_app(info)
    {
    }

    virtual ::dsn::error_code start(const std::vector<std::string> &args) override
    {
        // args for meta http service
        std::vector<std::string> args_new(args);
        args_new.emplace_back(PEGASUS_VERSION);
        args_new.emplace_back(PEGASUS_GIT_COMMIT);

        // Actually the root caller, start_app() in service_control_task::exec() will also do
        // CHECK for ERR_OK. Do CHECK here to guarantee that all following services (such as
        // built-in metrics) are started.
        CHECK_EQ(::dsn::service::meta_service_app::start(args_new), ::dsn::ERR_OK);

        _builtin_metrics.start();
        return ::dsn::ERR_OK;
    }

    virtual ::dsn::error_code stop(bool cleanup = false) override
    {
        ::dsn::error_code ret = ::dsn::service::meta_service_app::stop();

        _builtin_metrics.stop();
        return ret;
    }

private:
    dsn::builtin_metrics _builtin_metrics;
};

} // namespace server
} // namespace pegasus
