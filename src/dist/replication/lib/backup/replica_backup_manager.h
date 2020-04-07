// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dist/replication/lib/replica.h>

namespace dsn {
namespace replication {

class replica_backup_manager : replica_base
{
public:
    replica_backup_manager(replica *r) : replica_base(r), _replica(r) {}
    void on_clear_cold_backup(const backup_clear_request &request);

private:
    void clear_backup_checkpoint(const std::string &policy_name);
    void send_clear_request_to_secondaries(const gpid &pid, const std::string &policy_name);
    void background_clear_backup_checkpoint(const std::string &policy_name);

    replica *_replica;

    friend class replica;
    friend class replica_backup_manager_test;
};

} // namespace replication
} // namespace dsn
