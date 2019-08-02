// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/tool-api/http_server.h>

namespace dsn {

class version_http_service : public http_service
{
public:
    version_http_service()
    {
        // GET ip:port/version
        register_handler("",
                         std::bind(&version_http_service::get_version_handler,
                                   this,
                                   std::placeholders::_1,
                                   std::placeholders::_2));
    }

    std::string path() const override { return "version"; }

    void get_version_handler(const http_request &req, http_response &resp);

    void set_version(const std::string &ver) { _version = ver; }

    void set_git_commit(const std::string &git) { _git_commit = git; }

private:
    std::string _version;
    std::string _git_commit;
};

class recent_start_time_http_service : public http_service
{
public:
    recent_start_time_http_service()
    {
        // GET ip:port/recentStartTime
        register_handler("",
                         std::bind(&recent_start_time_http_service::get_recent_start_time_handler,
                                   this,
                                   std::placeholders::_1,
                                   std::placeholders::_2));
    }

    std::string path() const override { return "startTime"; }

    void get_recent_start_time_handler(const http_request &req, http_response &resp);
};

} // namespace dsn
