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

#include <stdlib.h>

#include "kinit_context.h"
#include "negotiation_manager.h"
#include "sasl_init.h"
#include "utils/errors.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/strings.h"

DSN_DECLARE_bool(enable_auth);
DSN_DECLARE_string(krb5_config);
DSN_DECLARE_string(krb5_keytab);
DSN_DECLARE_string(krb5_principal);

namespace dsn {
namespace security {

/***
 * set kerberos envs(for more details:
 * https://web.mit.edu/kerberos/krb5-1.12/doc/admin/env_variables.html)
 */
void set_krb5_env(bool is_server)
{
    setenv("KRB5CCNAME", is_server ? "MEMORY:pegasus-server" : "MEMORY:pegasus-client", 1);
    setenv("KRB5_CONFIG", FLAGS_krb5_config, 1);
    setenv("KRB5_KTNAME", FLAGS_krb5_keytab, 1);
    setenv("KRB5RCACHETYPE", "none", 1);
}

error_s init_kerberos(bool is_server)
{
    // When FLAGS_enable_auth is set but lacks of necessary parameters to execute kinit by itself,
    // then try to obtain the principal under the current Unix account for identity
    // authentication automatically.
    if (FLAGS_enable_auth && utils::is_empty(FLAGS_krb5_keytab) &&
        utils::is_empty(FLAGS_krb5_principal)) {
        return run_get_principal_without_kinit();
    }
    // set kerberos env
    set_krb5_env(is_server);

    // kinit -k -t <keytab_file> <principal>
    return run_kinit();
}

bool init(bool is_server)
{
    error_s err = init_kerberos(is_server);
    if (!err.is_ok()) {
        LOG_ERROR("initialize kerberos failed, with err = {}", err.description());
        return false;
    }
    LOG_INFO("initialize kerberos succeed");

    err = init_sasl(is_server);
    if (!err.is_ok()) {
        LOG_ERROR("initialize sasl failed, with err = {}", err.description());
        return false;
    }
    LOG_INFO("initialize sasl succeed");

    init_join_point();
    return true;
}

bool init_for_zookeeper_client()
{
    error_s err = run_kinit();
    if (!err.is_ok()) {
        LOG_ERROR("initialize kerberos failed, with err = {}", err.description());
        return false;
    }
    LOG_INFO("initialize kerberos for zookeeper client succeed");

    err = init_sasl(false);
    if (!err.is_ok()) {
        LOG_ERROR("initialize sasl failed, with err = {}", err.description());
        return false;
    }
    LOG_INFO("initialize sasl for zookeeper client succeed");
    return true;
}
} // namespace security
} // namespace dsn
