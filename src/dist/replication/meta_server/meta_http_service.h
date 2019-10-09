// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <algorithm>

#include <dsn/tool-api/http_server.h>

namespace dsn {
namespace replication {

class meta_service;
class meta_http_service : public http_service
{
public:
    explicit meta_http_service(meta_service *s) : _service(s)
    {
        // GET ip:port/meta/app?app_name=temp
        register_handler("app",
                         std::bind(&meta_http_service::get_app_handler,
                                   this,
                                   std::placeholders::_1,
                                   std::placeholders::_2));
        // GET ip:port/meta/apps
        register_handler("apps",
                         std::bind(&meta_http_service::list_app_handler,
                                   this,
                                   std::placeholders::_1,
                                   std::placeholders::_2));
        // GET ip:port/meta/nodes
        register_handler("nodes",
                         std::bind(&meta_http_service::list_node_handler,
                                   this,
                                   std::placeholders::_1,
                                   std::placeholders::_2));
        // GET ip:port/meta/cluster
        register_handler("cluster",
                         std::bind(&meta_http_service::get_cluster_info_handler,
                                   this,
                                   std::placeholders::_1,
                                   std::placeholders::_2));
        // GET ip:port/meta/app_envs?name=temp
        register_handler("app_envs",
                         std::bind(&meta_http_service::get_app_envs_handler,
                                   this,
                                   std::placeholders::_1,
                                   std::placeholders::_2));
    }

    std::string path() const override { return "meta"; }

    void get_app_handler(const http_request &req, http_response &resp);
    void list_app_handler(const http_request &req, http_response &resp);
    void list_node_handler(const http_request &req, http_response &resp);
    void get_cluster_info_handler(const http_request &req, http_response &resp);
    void get_app_envs_handler(const http_request &req, http_response &resp);

private:
    // set redirect location if current server is not primary
    bool redirect_if_not_primary(const http_request &req, http_response &resp);

    meta_service *_service;
};

} // namespace replication
} // namespace dsn
