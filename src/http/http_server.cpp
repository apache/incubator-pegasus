// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "http/http_server.h"

#include <stdint.h>
#include <string.h>
#include <memory>
#include <ostream>
#include <vector>

#include "fmt/core.h"
#include "gutil/map_util.h"
#include "http/builtin_http_calls.h"
#include "http/http_call_registry.h"
#include "http/http_message_parser.h"
#include "http/http_method.h"
#include "http/http_server_impl.h"
#include "http/uri_decoder.h"
#include "nodejs/http_parser.h"
#include "runtime/api_layer1.h"
#include "rpc/rpc_message.h"
#include "rpc/rpc_stream.h"
#include "runtime/serverlet.h"
#include "runtime/tool_api.h"
#include "utils/error_code.h"
#include "utils/fmt_logging.h"
#include "utils/output_utils.h"
#include "utils/strings.h"

DSN_DEFINE_bool(http, enable_http_server, true, "whether to enable the embedded HTTP server");

namespace dsn {

namespace {
error_s update_config(const http_request &req)
{
    if (req.query_args.size() != 1) {
        return error_s::make(ERR_INVALID_PARAMETERS,
                             "there should be exactly one config to be updated once");
    }

    auto iter = req.query_args.begin();
    return update_flag(iter->first, iter->second);
}

// If sub_path is 'app/duplication', the built path would be '<root_path>/app/duplication'.
std::string build_rel_path(const std::string &root_path, const std::string &sub_path)
{
    std::string rel_path(root_path);
    if (!rel_path.empty()) {
        rel_path += '/';
    }
    return rel_path += sub_path;
}

} // anonymous namespace

/*extern*/ http_call &register_http_call(std::string full_path)
{
    auto call_ptr = std::make_unique<http_call>();
    call_ptr->path = std::move(full_path);
    http_call &call = *call_ptr;
    http_call_registry::instance().add(std::move(call_ptr));
    return call;
}

/*extern*/ void deregister_http_call(const std::string &full_path)
{
    http_call_registry::instance().remove(full_path);
}

std::string http_service::get_rel_path(const std::string &sub_path) const
{
    return build_rel_path(path(), sub_path);
}

void http_service::register_handler(std::string sub_path, http_callback cb, std::string help) const
{
    register_handler(std::move(sub_path), std::move(cb), "", std::move(help));
}

void http_service::register_handler(std::string sub_path,
                                    http_callback cb,
                                    std::string parameters,
                                    std::string help) const
{
    CHECK_FALSE(sub_path.empty());
    if (!FLAGS_enable_http_server) {
        return;
    }

    auto call = std::make_unique<http_call>();
    call->path = get_rel_path(sub_path);
    call->with_callback(std::move(cb)).with_help(parameters, help);
    http_call_registry::instance().add(std::move(call));
}

void http_service::deregister_handler(std::string sub_path) const
{
    http_call_registry::instance().remove(get_rel_path(sub_path));
}

void http_server_base::update_config_handler(const http_request &req, http_response &resp)
{
    auto res = dsn::update_config(req);
    if (res.is_ok()) {
        CHECK_EQ(1, req.query_args.size());
        update_config(req.query_args.begin()->first);
    }
    utils::table_printer tp;
    tp.add_row_name_and_data("update_status", res.description());
    std::ostringstream out;
    tp.output(out, dsn::utils::table_printer::output_format::kJsonCompact);
    resp.body = out.str();
    resp.status_code = http_status_code::kOk;
}

http_server::http_server() : serverlet<http_server>("http_server")
{
    if (!FLAGS_enable_http_server) {
        return;
    }

    register_rpc_handler(RPC_HTTP_SERVICE, "http_service", &http_server::serve);

    tools::register_message_header_parser<http_message_parser>(NET_HDR_HTTP, {"GET ", "POST"});

    // add builtin services
    register_builtin_http_calls();
}

void http_server::serve(message_ex *msg)
{
    error_with<http_request> res = http_request::parse(msg);
    http_response resp;
    if (!res.is_ok()) {
        resp.status_code = http_status_code::kBadRequest;
        resp.body = fmt::format("failed to parse request: {}", res.get_error());
    } else {
        const http_request &req = res.get_value();
        auto call = http_call_registry::instance().find(req.path);
        if (call) {
            call->callback(req, resp);
        } else {
            resp.status_code = http_status_code::kNotFound;
            resp.body = fmt::format("service not found for \"{}\"", req.path);
        }
    }

    http_response_reply(resp, msg);
}

/*static*/ error_with<http_request> http_request::parse(message_ex *m)
{
    if (m->buffers.size() != HTTP_MSG_BUFFERS_NUM) {
        return error_s::make(ERR_INVALID_DATA,
                             std::string("buffer size is: ") + std::to_string(m->buffers.size()));
    }

    http_request ret;
    ret.body = m->buffers[1];
    ret.full_url = m->buffers[2];
    ret.method = static_cast<http_method>(m->header->hdr_type);

    http_parser_url u{0};
    http_parser_parse_url(ret.full_url.data(), ret.full_url.length(), false, &u);

    std::string unresolved_path;
    if (u.field_set & (1u << UF_PATH)) {
        uint16_t data_length = u.field_data[UF_PATH].len;
        unresolved_path.resize(data_length + 1);
        strncpy(&unresolved_path[0], ret.full_url.data() + u.field_data[UF_PATH].off, data_length);
        unresolved_path[data_length] = '\0';

        // decode resolved path
        auto decoded_unresolved_path = uri::decode(unresolved_path);
        if (!decoded_unresolved_path.is_ok()) {
            return decoded_unresolved_path.get_error();
        }
        unresolved_path = decoded_unresolved_path.get_value();
    }

    std::string unresolved_query;
    if (u.field_set & (1u << UF_QUERY)) {
        uint16_t data_length = u.field_data[UF_QUERY].len;
        unresolved_query.resize(data_length);
        strncpy(
            &unresolved_query[0], ret.full_url.data() + u.field_data[UF_QUERY].off, data_length);

        // decode resolved query
        auto decoded_unresolved_query = uri::decode(unresolved_query);
        if (!decoded_unresolved_query.is_ok()) {
            return decoded_unresolved_query.get_error();
        }
        unresolved_query = decoded_unresolved_query.get_value();
    }

    // remove tailing '\0'
    if (!unresolved_path.empty() && *unresolved_path.crbegin() == '\0') {
        unresolved_path.pop_back();
    }

    // parse path
    std::vector<std::string> args;
    dsn::utils::split_args(unresolved_path.c_str(), args, '/');
    std::vector<std::string> real_args;
    for (std::string &arg : args) {
        if (!arg.empty()) {
            real_args.emplace_back(std::move(arg));
        }
    }
    if (real_args.size() == 0) {
        ret.path = "";
    } else {
        std::string path = real_args[0];
        for (int i = 1; i < real_args.size(); i++) {
            path += '/';
            path += real_args[i];
        }
        ret.path = std::move(path);
    }

    // find if there are method args (<ip>:<port>/<service>/<method>?<arg>=<val>&<arg>=<val>)
    if (!unresolved_query.empty()) {
        std::vector<std::string> method_arg_val;
        dsn::utils::split_args(unresolved_query.c_str(), method_arg_val, '&');
        for (const std::string &arg_val : method_arg_val) {
            size_t sep = arg_val.find_first_of('=');
            if (sep == std::string::npos) {
                // assume this as a bool flag
                ret.query_args.emplace(arg_val, "");
                continue;
            }
            std::string name = arg_val.substr(0, sep);
            std::string value;
            if (sep + 1 < arg_val.size()) {
                value = arg_val.substr(sep + 1, arg_val.size() - sep);
            }
            if (gutil::ContainsKey(ret.query_args, name)) {
                return FMT_ERR(ERR_INVALID_PARAMETERS, "duplicate parameter: {}", name);
            }
            ret.query_args.emplace(std::move(name), std::move(value));
        }
    }

    return ret;
}

/*extern*/ void http_response_reply(const http_response &resp, message_ex *req)
{
    message_ptr resp_msg = req->create_response();

    std::ostringstream os;
    os << "HTTP/1.1 " << get_http_status_message(resp.status_code) << "\r\n";
    os << "Content-Type: " << resp.content_type << "\r\n";
    os << "Content-Length: " << resp.body.length() << "\r\n";
    if (!resp.location.empty()) {
        os << "Location: " << resp.location << "\r\n";
    }
    os << "\r\n";
    os << resp.body;

    rpc_write_stream writer(resp_msg.get());
    writer.write(os.str().data(), os.str().length());
    writer.flush();

    dsn_rpc_reply(resp_msg.get());
}

/*extern*/ void start_http_server()
{
    // starts http server as a singleton
    static http_server server;
}

/*extern*/ void register_http_service(http_service *svc)
{
    // simply hosting the memory of these http services.
    static std::vector<std::unique_ptr<http_service>> services_holder;
    static std::mutex mu;

    std::lock_guard<std::mutex> guard(mu);
    services_holder.push_back(std::unique_ptr<http_service>(svc));
}

} // namespace dsn
