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

#include "negotiation.h"
#include "client_negotiation.h"
#include "server_negotiation.h"
#include "negotiation_utils.h"

#include <dsn/utility/flags.h>
#include <dsn/utility/smart_pointers.h>
#include <dsn/dist/fmt_logging.h>

namespace dsn {
namespace security {
/// TODO(zlw):we can't get string list from cflags now,
/// so we should get supported mechanisms from config in the later
const std::set<std::string> supported_mechanisms{"GSSAPI"};

DSN_DEFINE_bool("security", enable_auth, false, "whether open auth or not");
DSN_DEFINE_bool("security", mandatory_auth, false, "wheter to do authertication mandatorily");
DSN_TAG_VARIABLE(mandatory_auth, FT_MUTABLE);

negotiation::~negotiation() {}

std::unique_ptr<negotiation> create_negotiation(bool is_client, rpc_session *session)
{
    if (is_client) {
        return make_unique<client_negotiation>(session);
    } else {
        return make_unique<server_negotiation>(session);
    }
}

void negotiation::fail_negotiation()
{
    _status = negotiation_status::type::SASL_AUTH_FAIL;
    _session->on_failure(true);
}

bool negotiation::check_status(negotiation_status::type status,
                               negotiation_status::type expected_status)
{
    if (status != expected_status) {
        dwarn_f("{}: get message({}) while expect({})",
                _name,
                enum_to_string(status),
                enum_to_string(expected_status));
        return false;
    }

    return true;
}
} // namespace security
} // namespace dsn
