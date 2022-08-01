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

#include "kinit_context.h"
#include "sasl_init.h"
#include "negotiation_manager.h"

#include <dsn/dist/fmt_logging.h>
#include <dsn/utility/flags.h>

namespace dsn {
namespace security {
DSN_DECLARE_string(krb5_config);
DSN_DECLARE_string(krb5_keytab);

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
    // set kerberos env
    set_krb5_env(is_server);

    // kinit -k -t <keytab_file> <principal>
    return run_kinit();
}

bool init(bool is_server)
{
    error_s err = init_kerberos(is_server);
    if (!err.is_ok()) {
        derror_f("initialize kerberos failed, with err = {}", err.description());
        return false;
    }
    ddebug("initialize kerberos succeed");

    err = init_sasl(is_server);
    if (!err.is_ok()) {
        derror_f("initialize sasl failed, with err = {}", err.description());
        return false;
    }
    ddebug("initialize sasl succeed");

    init_join_point();
    return true;
}

} // namespace security
} // namespace dsn
