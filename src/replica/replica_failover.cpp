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

/*
 * Description:
 *     failure handling in replica
 *
 * Revision history:
 *     Mar., 2015, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include "replica.h"
#include "mutation.h"
#include "mutation_log.h"
#include "replica_stub.h"

namespace dsn {
namespace replication {

void replica::handle_local_failure(error_code error)
{
    LOG_INFO("%s: handle local failure error %s, status = %s",
             name(),
             error.to_string(),
             enum_to_string(status()));

    if (status() == partition_status::PS_PRIMARY) {
        _stub->remove_replica_on_meta_server(_app_info, _primary_states.membership);
    }

    update_local_configuration_with_no_ballot_change(partition_status::PS_ERROR);
}

void replica::handle_remote_failure(partition_status::type st,
                                    ::dsn::rpc_address node,
                                    error_code error,
                                    const std::string &caused_by)
{
    LOG_ERROR("%s: handle remote failure caused by %s, error = %s, status = %s, node = %s",
              name(),
              caused_by.c_str(),
              error.to_string(),
              enum_to_string(st),
              node.to_string());

    CHECK_EQ(status(), partition_status::PS_PRIMARY);
    CHECK_NE(node, _stub->_primary_address);

    switch (st) {
    case partition_status::PS_SECONDARY:
        CHECK(_primary_states.check_exist(node, partition_status::PS_SECONDARY),
              "invalid node address, address = {}, status = {}",
              node,
              enum_to_string(st));
        {
            configuration_update_request request;
            request.node = node;
            request.type = config_type::CT_DOWNGRADE_TO_INACTIVE;
            request.config = _primary_states.membership;
            downgrade_to_inactive_on_primary(request);
        }
        break;
    case partition_status::PS_POTENTIAL_SECONDARY: {
        LOG_INFO("%s: remove learner %s for remote failure", name(), node.to_string());
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
    LOG_INFO("%s: meta server disconnected", name());

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
