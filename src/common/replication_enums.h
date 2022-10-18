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

#pragma once

#include "utils/enum_helper.h"

namespace dsn {
ENUM_BEGIN2(app_status::type, app_status, app_status::AS_INVALID)
ENUM_REG(app_status::AS_AVAILABLE)
ENUM_REG(app_status::AS_CREATING)
ENUM_REG(app_status::AS_CREATE_FAILED)
ENUM_REG(app_status::AS_DROPPING)
ENUM_REG(app_status::AS_DROP_FAILED)
ENUM_REG(app_status::AS_DROPPED)
ENUM_REG(app_status::AS_RECALLING)
ENUM_END2(app_status::type, app_status)

ENUM_BEGIN2(replication::partition_status::type,
            partition_status,
            replication::partition_status::PS_INVALID)
ENUM_REG(replication::partition_status::PS_INACTIVE)
ENUM_REG(replication::partition_status::PS_ERROR)
ENUM_REG(replication::partition_status::PS_PRIMARY)
ENUM_REG(replication::partition_status::PS_SECONDARY)
ENUM_REG(replication::partition_status::PS_POTENTIAL_SECONDARY)
ENUM_REG(replication::partition_status::PS_PARTITION_SPLIT)
ENUM_END2(replication::partition_status::type, partition_status)

ENUM_BEGIN2(replication::read_semantic::type,
            read_semantic,
            replication::read_semantic::ReadInvalid)
ENUM_REG(replication::read_semantic::ReadLastUpdate)
ENUM_REG(replication::read_semantic::ReadOutdated)
ENUM_REG(replication::read_semantic::ReadSnapshot)
ENUM_END2(replication::read_semantic::type, read_semantic)

ENUM_BEGIN2(replication::learn_type::type, learn_type, replication::learn_type::LT_INVALID)
ENUM_REG(replication::learn_type::LT_CACHE)
ENUM_REG(replication::learn_type::LT_APP)
ENUM_REG(replication::learn_type::LT_LOG)
ENUM_END2(replication::learn_type::type, learn_type)

ENUM_BEGIN2(replication::learner_status::type,
            learner_status,
            replication::learner_status::LearningInvalid)
ENUM_REG(replication::learner_status::LearningWithoutPrepare)
ENUM_REG(replication::learner_status::LearningWithPrepareTransient)
ENUM_REG(replication::learner_status::LearningWithPrepare)
ENUM_REG(replication::learner_status::LearningSucceeded)
ENUM_REG(replication::learner_status::LearningFailed)
ENUM_END2(replication::learner_status::type, learner_status)

ENUM_BEGIN2(replication::config_type::type, config_type, replication::config_type::CT_INVALID)
ENUM_REG(replication::config_type::CT_ASSIGN_PRIMARY)
ENUM_REG(replication::config_type::CT_UPGRADE_TO_PRIMARY)
ENUM_REG(replication::config_type::CT_ADD_SECONDARY)
ENUM_REG(replication::config_type::CT_UPGRADE_TO_SECONDARY)
ENUM_REG(replication::config_type::CT_DOWNGRADE_TO_SECONDARY)
ENUM_REG(replication::config_type::CT_DOWNGRADE_TO_INACTIVE)
ENUM_REG(replication::config_type::CT_REMOVE)
ENUM_REG(replication::config_type::CT_ADD_SECONDARY_FOR_LB)
ENUM_REG(replication::config_type::CT_PRIMARY_FORCE_UPDATE_BALLOT)
ENUM_REG(replication::config_type::CT_DROP_PARTITION)
ENUM_REG(replication::config_type::CT_REGISTER_CHILD)
ENUM_END2(replication::config_type::type, config_type)

ENUM_BEGIN2(replication::node_status::type, node_status, replication::node_status::NS_INVALID)
ENUM_REG(replication::node_status::NS_ALIVE)
ENUM_REG(replication::node_status::NS_UNALIVE)
ENUM_END2(replication::node_status::type, node_status)

ENUM_BEGIN2(replication::bulk_load_status::type,
            bulk_load_status,
            replication::bulk_load_status::BLS_INVALID)
ENUM_REG(replication::bulk_load_status::BLS_INVALID)
ENUM_REG(replication::bulk_load_status::BLS_DOWNLOADING)
ENUM_REG(replication::bulk_load_status::BLS_DOWNLOADED)
ENUM_REG(replication::bulk_load_status::BLS_INGESTING)
ENUM_REG(replication::bulk_load_status::BLS_SUCCEED)
ENUM_REG(replication::bulk_load_status::BLS_FAILED)
ENUM_REG(replication::bulk_load_status::BLS_PAUSING)
ENUM_REG(replication::bulk_load_status::BLS_PAUSED)
ENUM_REG(replication::bulk_load_status::BLS_CANCELED)
ENUM_END2(replication::bulk_load_status::type, bulk_load_status)

ENUM_BEGIN2(replication::ingestion_status::type,
            ingestion_status,
            replication::ingestion_status::IS_INVALID)
ENUM_REG(replication::ingestion_status::IS_INVALID)
ENUM_REG(replication::ingestion_status::IS_RUNNING)
ENUM_REG(replication::ingestion_status::IS_SUCCEED)
ENUM_REG(replication::ingestion_status::IS_FAILED)
ENUM_END2(replication::ingestion_status::type, ingestion_status)

ENUM_BEGIN2(replication::hotkey_type::type, hotkey_type, replication::hotkey_type::READ)
ENUM_REG(replication::hotkey_type::READ)
ENUM_REG(replication::hotkey_type::WRITE)
ENUM_END2(replication::hotkey_type::type, hotkey_type)

ENUM_BEGIN2(replication::detect_action::type, detect_action, replication::detect_action::START)
ENUM_REG(replication::detect_action::START)
ENUM_REG(replication::detect_action::STOP)
ENUM_REG(replication::detect_action::QUERY)
ENUM_END2(replication::detect_action::type, detect_action)

ENUM_BEGIN2(replication::split_status::type, split_status, replication::split_status::NOT_SPLIT)
ENUM_REG(replication::split_status::NOT_SPLIT)
ENUM_REG(replication::split_status::SPLITTING)
ENUM_REG(replication::split_status::PAUSING)
ENUM_REG(replication::split_status::PAUSED)
ENUM_REG(replication::split_status::CANCELING)
ENUM_END2(replication::split_status::type, split_status)

ENUM_BEGIN2(replication::disk_migration_status::type,
            disk_migration_status,
            replication::disk_migration_status::IDLE)
ENUM_REG(replication::disk_migration_status::IDLE)
ENUM_REG(replication::disk_migration_status::MOVING)
ENUM_REG(replication::disk_migration_status::MOVED)
ENUM_REG(replication::disk_migration_status::CLOSED)
ENUM_END2(replication::disk_migration_status::type, disk_migration_status)

ENUM_BEGIN2(replication::disk_status::type, disk_status, replication::disk_status::NORMAL)
ENUM_REG(replication::disk_status::NORMAL)
ENUM_REG(replication::disk_status::SPACE_INSUFFICIENT)
ENUM_END2(replication::disk_status::type, disk_status)

ENUM_BEGIN2(replication::manual_compaction_status::type,
            manual_compaction_status,
            replication::manual_compaction_status::IDLE)
ENUM_REG(replication::manual_compaction_status::IDLE)
ENUM_REG(replication::manual_compaction_status::QUEUING)
ENUM_REG(replication::manual_compaction_status::RUNNING)
ENUM_REG(replication::manual_compaction_status::FINISHED)
ENUM_END2(replication::manual_compaction_status::type, manual_compaction_status)
} // namespace dsn
