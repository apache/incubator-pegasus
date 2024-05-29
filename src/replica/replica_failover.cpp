/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#include <atomic>
#include <string>

#include "common/fs_manager.h"
#include "common/replication_common.h"
#include "common/replication_enums.h"
#include "dsn.layer2_types.h"
#include "meta_admin_types.h"
#include "metadata_types.h"
#include "replica.h"
#include "replica/replica_context.h"
#include "replica_stub.h"
#include "runtime/rpc/rpc_address.h"
#include "runtime/rpc/rpc_host_port.h"
#include "utils/error_code.h"
#include "utils/fmt_logging.h"

namespace dsn {

namespace replication {

// The failure handling part of replica.

void replica::handle_local_failure(error_code error)
{
    LOG_INFO_PREFIX("handle local failure error {}, status = {}", error, enum_to_string(status()));

    if (error == ERR_DISK_IO_ERROR) {
        _dir_node->status = disk_status::IO_ERROR;
    } else if (error == ERR_RDB_CORRUPTION) {
        _data_corrupted = true;
    }

    if (status() == partition_status::PS_PRIMARY) {
        _stub->remove_replica_on_meta_server(_app_info, _primary_states.membership);
    }

    update_local_configuration_with_no_ballot_change(partition_status::PS_ERROR);
}

void replica::handle_remote_failure(partition_status::type st,
                                    const ::dsn::host_port &node,
                                    error_code error,
                                    const std::string &caused_by)
{
    LOG_ERROR_PREFIX("handle remote failure caused by {}, error = {}, status = {}, node = {}",
                     caused_by,
                     error,
                     enum_to_string(st),
                     node);

    CHECK_EQ(status(), partition_status::PS_PRIMARY);
    CHECK_NE(node, _stub->primary_host_port());

    switch (st) {
    case partition_status::PS_SECONDARY:
        CHECK(_primary_states.check_exist(node, partition_status::PS_SECONDARY),
              "invalid node address, address = {}, status = {}",
              node,
              enum_to_string(st));
        {
            configuration_update_request request;
            SET_IP_AND_HOST_PORT_BY_DNS(request, node, node);
            request.type = config_type::CT_DOWNGRADE_TO_INACTIVE;
            request.config = _primary_states.membership;
            downgrade_to_inactive_on_primary(request);
        }
        break;
    case partition_status::PS_POTENTIAL_SECONDARY: {
        LOG_INFO_PREFIX("remove learner {} for remote failure", node);
        // potential secondary failure does not lead to ballot change
        // therefore, it is possible to have multiple exec here
        _primary_states.learners.erase(node);
        _primary_states.statuses.erase(node);
    } break;
    case partition_status::PS_INACTIVE:
    case partition_status::PS_ERROR:
        break;
    default:
        CHECK(false, "invalid partition_status, status = {}", enum_to_string(st));
        break;
    }
}

void replica::on_meta_server_disconnected()
{
    LOG_INFO_PREFIX("meta server disconnected");

    auto old_status = status();
    update_local_configuration_with_no_ballot_change(partition_status::PS_INACTIVE);

    // make sure they can be back directly
    if (old_status == partition_status::PS_PRIMARY ||
        old_status == partition_status::PS_SECONDARY) {
        set_inactive_state_transient(true);
    }
}
}
} // namespace
