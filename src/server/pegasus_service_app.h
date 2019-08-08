// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/dist/replication/meta_service_app.h>
#include <dsn/dist/replication/replication_service_app.h>
#include <pegasus/version.h>
#include <pegasus/git_commit.h>
#include "reporter/pegasus_counter_reporter.h"

namespace pegasus {
namespace server {

class pegasus_replication_service_app : public ::dsn::replication::replication_service_app
{
public:
    pegasus_replication_service_app(const dsn::service_app_info *info)
        : ::dsn::replication::replication_service_app::replication_service_app(info),
          _updater_started(false)
    {
    }

    virtual ::dsn::error_code start(const std::vector<std::string> &args) override
    {
        // args for replication http service
        std::vector<std::string> args_new(args);
        args_new.emplace_back(PEGASUS_VERSION);
        args_new.emplace_back(PEGASUS_GIT_COMMIT);
        ::dsn::error_code ret = ::dsn::replication::replication_service_app::start(args_new);

        if (ret == ::dsn::ERR_OK) {
            pegasus_counter_reporter::instance().start();
            _updater_started = true;
        }
        return ret;
    }

    virtual ::dsn::error_code stop(bool cleanup = false) override
    {
        ::dsn::error_code ret = ::dsn::replication::replication_service_app::stop();
        if (_updater_started) {
            pegasus_counter_reporter::instance().stop();
        }
        return ret;
    }

private:
    bool _updater_started;
};

class pegasus_meta_service_app : public ::dsn::service::meta_service_app
{
public:
    pegasus_meta_service_app(const dsn::service_app_info *info)
        : ::dsn::service::meta_service_app::meta_service_app(info), _updater_started(false)
    {
    }

    virtual ::dsn::error_code start(const std::vector<std::string> &args) override
    {
        // args for meta http service
        std::vector<std::string> args_new(args);
        args_new.emplace_back(PEGASUS_VERSION);
        args_new.emplace_back(PEGASUS_GIT_COMMIT);
        ::dsn::error_code ret = ::dsn::service::meta_service_app::start(args_new);

        if (ret == ::dsn::ERR_OK) {
            pegasus_counter_reporter::instance().start();
            _updater_started = true;
        }
        return ret;
    }

    virtual ::dsn::error_code stop(bool cleanup = false) override
    {
        ::dsn::error_code ret = ::dsn::service::meta_service_app::stop();
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
