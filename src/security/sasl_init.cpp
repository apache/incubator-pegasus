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

#include "sasl_init.h"

#include <sasl/sasl.h>
#include <sasl/saslplug.h>
#include <string>

#include "kinit_context.h"
#include "utils/api_utilities.h"
#include "utils/error_code.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/synchronize.h"

DSN_DEFINE_string(security, sasl_plugin_path, "/usr/lib/sasl2", "path to search sasl plugins");

namespace dsn {
namespace security {

log_level_t get_log_level(int level)
{
    switch (level) {
    case SASL_LOG_ERR:
        return LOG_LEVEL_ERROR;
    case SASL_LOG_FAIL:
    case SASL_LOG_WARN:
        return LOG_LEVEL_WARNING;
    case SASL_LOG_NOTE:
        return LOG_LEVEL_INFO;
    default:
        return LOG_LEVEL_DEBUG;
    }
}

int sasl_simple_logger(void *context, int level, const char *msg)
{
    if (SASL_LOG_NONE == level || nullptr == msg) {
        return SASL_OK;
    }

    LOG(get_log_level(level), "sasl log info: {}", msg);
    return SASL_OK;
}

int sasl_get_path(void *context, char **path)
{
    if (nullptr == path) {
        return SASL_BADPARAM;
    }
    *path = const_cast<char *>(FLAGS_sasl_plugin_path);
    return SASL_OK;
}

int sasl_get_username(void *context, int id, const char **result, unsigned *len)
{
    if (nullptr == result) {
        return SASL_BADPARAM;
    }
    static const std::string username = get_username();
    switch (id) {
    case SASL_CB_USER:
    case SASL_CB_AUTHNAME:
        *result = username.c_str();
        if (len != nullptr) {
            *len = username.length();
        }
        return SASL_OK;
    default:
        CHECK(false, "unexpected SASL callback type: {}", id);
        return SASL_BADPARAM;
    }
}

sasl_callback_t client_callbacks[] = {
    {SASL_CB_USER, (sasl_callback_ft)&sasl_get_username, nullptr},
    {SASL_CB_GETPATH, (sasl_callback_ft)&sasl_get_path, nullptr},
    {SASL_CB_AUTHNAME, (sasl_callback_ft)&sasl_get_username, nullptr},
    {SASL_CB_LOG, (sasl_callback_ft)&sasl_simple_logger, nullptr},
    {SASL_CB_LIST_END, nullptr, nullptr}};

sasl_callback_t server_callbacks[] = {{SASL_CB_LOG, (sasl_callback_ft)&sasl_simple_logger, nullptr},
                                      {SASL_CB_GETPATH, (sasl_callback_ft)&sasl_get_path, nullptr},
                                      {SASL_CB_LIST_END, nullptr, nullptr}};

// provide mutex function for sasl
void *sasl_mutex_alloc_local() { return static_cast<void *>(new utils::ex_lock_nr); }

void sasl_mutex_free_local(void *m) { delete static_cast<utils::ex_lock_nr *>(m); }

int sasl_mutex_lock_local(void *m)
{
    static_cast<utils::ex_lock_nr *>(m)->lock();
    return 0;
}

int sasl_mutex_unlock_local(void *m)
{
    static_cast<utils::ex_lock_nr *>(m)->unlock();
    return 0;
}

void sasl_set_mutex_local()
{
    // sasl_set_mutex is a function in <sasl/sasl.h>
    sasl_set_mutex(&sasl_mutex_alloc_local,
                   &sasl_mutex_lock_local,
                   &sasl_mutex_unlock_local,
                   &sasl_mutex_free_local);
}

error_s init_sasl(bool is_server)
{
    // server is also a client to other server.
    // for example: replica server is a client of meta server.
    sasl_set_mutex_local();
    int err = sasl_client_init(&client_callbacks[0]);
    error_s ret = error_s::make(ERR_OK);
    if (err != SASL_OK) {
        ret = error_s::make(ERR_SASL_INTERNAL);
        ret << "initialize sasl client failed with error: "
            << sasl_errstring(err, nullptr, nullptr);
        return ret;
    }
    if (is_server) {
        err = sasl_server_init(&server_callbacks[0], "pegasus");
        if (err != SASL_OK) {
            ret = error_s::make(ERR_SASL_INTERNAL);
            ret << "initialize sasl server failed with error: "
                << sasl_errstring(err, nullptr, nullptr);
            return ret;
        }
    }
    return ret;
}
} // namespace security
} // namespace dsn
