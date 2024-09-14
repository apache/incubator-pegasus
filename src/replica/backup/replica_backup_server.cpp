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

#include "replica_backup_server.h"

#include <string>

#include "backup_types.h"
#include "common/gpid.h"
#include "common/replication.codes.h"
#include "replica/replica.h"
#include "replica/replica_stub.h"
#include "replica_backup_manager.h"
#include "runtime/api_layer1.h"
#include "rpc/serialization.h"
#include "utils/autoref_ptr.h"
#include "utils/error_code.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/strings.h"

DSN_DECLARE_string(cold_backup_root);

namespace dsn {
class message_ex;

namespace replication {

replica_backup_server::replica_backup_server(const replica_stub *rs) : _stub(rs)
{
    dsn_rpc_register_handler(RPC_COLD_BACKUP, "cold_backup", [this](message_ex *msg) {
        on_cold_backup(backup_rpc::auto_reply(msg));
    });
    dsn_rpc_register_handler(RPC_CLEAR_COLD_BACKUP, "clear_cold_backup", [this](message_ex *msg) {
        backup_clear_request clear_req;
        unmarshall(msg, clear_req);
        on_clear_cold_backup(clear_req);
    });
}

replica_backup_server::~replica_backup_server()
{
    dsn_rpc_unregiser_handler(RPC_COLD_BACKUP);
    dsn_rpc_unregiser_handler(RPC_CLEAR_COLD_BACKUP);
}

void replica_backup_server::on_cold_backup(backup_rpc rpc)
{
    const backup_request &request = rpc.request();
    backup_response &response = rpc.response();

    LOG_INFO("received cold backup request: backup[{}.{}.{}]",
             request.pid,
             request.policy.policy_name,
             request.backup_id);
    response.pid = request.pid;
    response.policy_name = request.policy.policy_name;
    response.backup_id = request.backup_id;

    if (utils::is_empty(FLAGS_cold_backup_root)) {
        LOG_ERROR(
            "backup[{}.{}.{}]: FLAGS_cold_backup_root is empty, response ERR_OPERATION_DISABLED",
            request.pid,
            request.policy.policy_name,
            request.backup_id);
        response.err = ERR_OPERATION_DISABLED;
        return;
    }

    replica_ptr rep = _stub->get_replica(request.pid);
    if (rep != nullptr) {
        rep->on_cold_backup(request, response);
    } else {
        LOG_ERROR("backup[{}.{}.{}]: replica not found, response ERR_OBJECT_NOT_FOUND",
                  request.pid,
                  request.policy.policy_name,
                  request.backup_id);
        response.err = ERR_OBJECT_NOT_FOUND;
    }
}

void replica_backup_server::on_clear_cold_backup(const backup_clear_request &request)
{
    LOG_INFO("receive clear cold backup request: backup({}.{})",
             request.pid,
             request.policy_name.c_str());

    replica_ptr rep = _stub->get_replica(request.pid);
    if (rep != nullptr) {
        rep->get_backup_manager()->on_clear_cold_backup(request);
    }
}

} // namespace replication
} // namespace dsn
