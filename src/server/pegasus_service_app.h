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
#include "reporter/pegasus_counter_reporter.h"

namespace pegasus {
namespace server {

class pegasus_replication_service_app : public replication::replication_service_app
{
public:
    pegasus_replication_service_app(const service_app_info *info)
        : replication::replication_service_app::replication_service_app(info),
          _updater_started(false)
    {
    }

    virtual error_code start(const std::vector<std::string> &args) override
    {
        // args for replication http service
        std::vector<std::string> args_new(args);
        args_new.emplace_back(PEGASUS_VERSION);
        args_new.emplace_back(PEGASUS_GIT_COMMIT);
        error_code ret = replication::replication_service_app::start(args_new);

        if (ret == ERR_OK) {
            pegasus_counter_reporter::instance().start();
            _updater_started = true;
        }
        return ret;
    }

    virtual error_code stop(bool cleanup = false) override
    {
        error_code ret = replication::replication_service_app::stop();
        if (_updater_started) {
            pegasus_counter_reporter::instance().stop();
        }
        return ret;
    }

private:
    bool _updater_started;
};

class pegasus_meta_service_app : public service::meta_service_app
{
public:
    pegasus_meta_service_app(const service_app_info *info)
        : service::meta_service_app::meta_service_app(info), _updater_started(false)
    {
    }

    virtual error_code start(const std::vector<std::string> &args) override
    {
        // args for meta http service
        std::vector<std::string> args_new(args);
        args_new.emplace_back(PEGASUS_VERSION);
        args_new.emplace_back(PEGASUS_GIT_COMMIT);
        error_code ret = service::meta_service_app::start(args_new);

        if (ret == ERR_OK) {
            pegasus_counter_reporter::instance().start();
            _updater_started = true;
        }
        return ret;
    }

    virtual error_code stop(bool cleanup = false) override
    {
        error_code ret = service::meta_service_app::stop();
        if (_updater_started) {
            pegasus_counter_reporter::instance().stop();
        }
        return ret;
    }

private:
    bool _updater_started;
};

} // namespace server
} // namespace pegasus
