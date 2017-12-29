#pragma once

#include <dsn/utility/enum_helper.h>

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
ENUM_END2(replication::config_type::type, config_type)

ENUM_BEGIN2(replication::node_status::type, node_status, replication::node_status::NS_INVALID)
ENUM_REG(replication::node_status::NS_ALIVE)
ENUM_REG(replication::node_status::NS_UNALIVE)
ENUM_END2(replication::node_status::type, node_status)
}
