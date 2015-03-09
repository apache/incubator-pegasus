/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation

 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.

 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
#pragma once

# include <rdsn/internal/serialization.h>
# include <rdsn/internal/enum_helper.h>

namespace rdsn {
    namespace replication {

        struct ReplicationMsgHeader
        {
            uint64_t id;
        };

        struct CdtMsgHeader
        {
            int32_t RpcTag;
        };

        struct CdtMsgResponseHeader
        {
            int Err;
            end_point PrimaryAddress;
        };

        struct global_partition_id
        {
            int32_t  tableId = -1;
            int32_t  pidx = -1;
        };

        inline bool operator == (const global_partition_id& l, const global_partition_id& r)
        {
            return l.tableId == r.tableId && l.pidx == r.pidx;
        }

        struct MutationHeader
        {
            global_partition_id gpid;
            int64_t             ballot;
            int64_t             decree;
            int64_t             logOffset;
            int64_t             lastCommittedDecree;
        };

        struct mutation_data
        {
            MutationHeader    header;
            std::list<utils::blob>  updates;
        };

        enum partition_status
        {
            PS_INACTIVE,
            PS_ERROR,
            PS_PRIMARY,
            PS_SECONDARY,
            PS_POTENTIAL_SECONDARY,
            PS_INVALID,
        };

        ENUM_BEGIN(partition_status, PS_INVALID)
            ENUM_REG(PS_INACTIVE)
            ENUM_REG(PS_ERROR)
            ENUM_REG(PS_PRIMARY)
            ENUM_REG(PS_SECONDARY)
            ENUM_REG(PS_POTENTIAL_SECONDARY)
            ENUM_END(partition_status)

        struct partition_configuration
        {
            std::string            app_type;
            global_partition_id    gpid;
            int64_t                ballot;
            end_point              primary;
            std::vector<end_point> secondaries;
            std::vector<end_point> dropOuts;
            std::vector<end_point> expects;
            int64_t                lastCommittedDecree;
        };

        struct replica_configuration
        {
            global_partition_id            gpid;
            int64_t                        ballot;
            end_point        primary;
            partition_status              status = PS_INACTIVE;
        };

        enum read_semantic
        {
            ReadLastUpdate,
            ReadOutdated,
            ReadSnapshot
        };

        struct client_read_request
        {
            global_partition_id gpid;
            read_semantic      semantic = ReadLastUpdate;
            int64_t             versionDecree = -1;
        };

        struct ClientResponse
        {
            int err = 0;
            int32_t pendingRequestCount = 0;
            int64_t lastCommittedDecree = 0;
        };

        struct PrepareAck
        {
            global_partition_id gpid;
            int          err;
            int64_t             ballot;
            int64_t             decree;
            int64_t             lastCommittedDecreeInApp;
            int64_t             lastCommittedDecreeInPrepareList;
        };

        struct learn_state
        {
            utils::blob                       meta;
            std::vector<std::string>          files;
        };

        enum LearnerState
        {
            LearningWithoutPrepare,
            LearningWithPrepare,
            LearningSucceeded,
            LearningFailed,
            Learning_INVALID
        };

        ENUM_BEGIN(LearnerState, Learning_INVALID)
            ENUM_REG(LearningWithoutPrepare)
            ENUM_REG(LearningWithPrepare)
            ENUM_REG(LearningSucceeded)
            ENUM_REG(LearningFailed)
            ENUM_END(LearnerState)

        struct learn_request
        {
            global_partition_id    gpid;
            end_point learner;
            uint64_t               signature;
            int64_t                lastCommittedDecreeInApp;
            int64_t                lastCommittedDecreeInPrepareList;
            utils::blob                 appSpecificLearnRequest;
        };

        struct learn_response
        {
            int               err;
            replica_configuration    config;
            int64_t                   commitDecree;
            int64_t                   prepareStartDecree;
            learn_state              state;
            std::string                  baseLocalDir;
        };

        struct group_check_request
        {
            std::string                  app_type;
            end_point    node;
            replica_configuration    config;
            int64_t                   lastCommittedDecree;
            uint64_t                  learnerSignature;
        };

        struct group_check_response
        {
            global_partition_id       gpid;
            int                err;
            int64_t                   lastCommittedDecreeInApp;
            int64_t                   lastCommittedDecreeInPrepareList;
            LearnerState            learnerState = LearningFailed;
            uint64_t                  learnerSignature;
            end_point    node;
        };


        ///////////// demo ///////////////

        enum SimpleKvOperation
        {
            SKV_NOP,
            SKV_UPDATE,
            SKV_READ,
            SKV_APPEND,
        };

        struct SimpleKvRequest
        {
            SimpleKvOperation op = SKV_NOP;
            std::string key;
            std::string value;
        };

        struct SimpleKvResponse
        {
            int32_t  err;
            std::string key;
            std::string value;
        };


        /////////////////// coordinator messages ////////////////////
        enum config_type
        {
            CT_NONE,
            CT_ASSIGN_PRIMARY,
            CT_ADD_SECONDARY,
            CT_DOWNGRADE_TO_SECONDARY,
            CT_DOWNGRADE_TO_INACTIVE,
            CT_REMOVE,

            // not used by coordinator
            CT_UPGRADE_TO_SECONDARY,
        };

        ENUM_BEGIN(config_type, CT_NONE)
            ENUM_REG(CT_ASSIGN_PRIMARY)
            ENUM_REG(CT_ADD_SECONDARY)
            ENUM_REG(CT_DOWNGRADE_TO_SECONDARY)
            ENUM_REG(CT_DOWNGRADE_TO_INACTIVE)
            ENUM_REG(CT_REMOVE)
            ENUM_REG(CT_UPGRADE_TO_SECONDARY)
            ENUM_END(config_type)

            // primary | secondary(upgrading) (w/ new config) => coordinator
        struct configuration_update_request
        {
            partition_configuration  config;
            config_type              type = CT_NONE;
            end_point    node;
        };

        // coordinator (config mgr) => primary | secondary (downgrade) (w/ new config)
        struct ConfigurationUpdateResponse
        {
            int                err;
            partition_configuration  config;
        };

        // proposal:  coordinator(LBM) => primary  (w/ current config)
        struct ConfigurationProposalRequest
        {
            partition_configuration  config;
            config_type              type = CT_NONE;
            end_point   node;
            bool                    isCleanData = false;
            bool                    isUpgrade = false;
        };

        // client => coordinator
        struct ConfigurationNodeQueryRequest
        {
            end_point    node;
        };

        // coordinator => client
        struct ConfigurationNodeQueryResponse
        {
            int                        err;
            std::list<partition_configuration> partitions;
        };


        struct QueryPNDecreeRequest
        {
            global_partition_id                partitionId;
            end_point    node;
        };

        struct QueryPNDecreeResponse
        {
            int                 err;
            int64_t                    lastDecree;
        };

        struct QueryConfigurationByIndexRequest
        {
            std::string           app_name;
            std::vector<uint32_t>   parIdxes;
        };

        struct QueryConfigurationByIndexResponse
        {
            int                           err;
            std::vector<partition_configuration> partitions;
        };



        DEFINE_POD_SERIALIZATION(replication::ReplicationMsgHeader)
            DEFINE_POD_SERIALIZATION(replication::CdtMsgHeader)

            inline void marshall(::rdsn::utils::binary_writer& writer, const CdtMsgResponseHeader& val, uint16_t pos = 0xffff)
        {
            marshall(writer, val.Err, pos);
            marshall(writer, val.PrimaryAddress, pos);
        }

        inline void unmarshall(::rdsn::utils::binary_reader& reader, __out CdtMsgResponseHeader& val)
        {
            unmarshall(reader, val.Err);
            unmarshall(reader, val.PrimaryAddress);
        }

        DEFINE_POD_SERIALIZATION(replication::global_partition_id)

            DEFINE_POD_SERIALIZATION(replication::MutationHeader)

            inline void marshall(::rdsn::utils::binary_writer& writer, const mutation_data& val, uint16_t pos = 0xffff)
        {
            marshall(writer, val.header, pos);
            marshall(writer, val.updates, pos);
        }

        inline void unmarshall(::rdsn::utils::binary_reader& reader, __out mutation_data& val)
        {
            unmarshall(reader, val.header);
            unmarshall(reader, val.updates);
        }

        DEFINE_POD_SERIALIZATION(replication::partition_status)

            inline void marshall(::rdsn::utils::binary_writer& writer, const partition_configuration& val, uint16_t pos = 0xffff)
        {
            marshall(writer, val.app_type, pos);
            marshall(writer, val.gpid, pos);
            marshall(writer, val.ballot, pos);
            marshall(writer, val.primary, pos);
            marshall(writer, val.secondaries, pos);
            marshall(writer, val.dropOuts, pos);
            marshall(writer, val.expects, pos);
            marshall(writer, val.lastCommittedDecree, pos);
        }

        inline void unmarshall(::rdsn::utils::binary_reader& reader, __out partition_configuration& val)
        {
            unmarshall(reader, val.app_type);
            unmarshall(reader, val.gpid);
            unmarshall(reader, val.ballot);
            unmarshall(reader, val.primary);
            unmarshall(reader, val.secondaries);
            unmarshall(reader, val.dropOuts);
            unmarshall(reader, val.expects);
            unmarshall(reader, val.lastCommittedDecree);
        }

        inline void marshall(::rdsn::utils::binary_writer& writer, const replica_configuration& val, uint16_t pos = 0xffff)
        {
            marshall(writer, val.gpid, pos);
            marshall(writer, val.ballot, pos);
            marshall(writer, val.primary, pos);
            marshall(writer, val.status, pos);
        }

        inline void unmarshall(::rdsn::utils::binary_reader& reader, __out replica_configuration& val)
        {
            unmarshall(reader, val.gpid);
            unmarshall(reader, val.ballot);
            unmarshall(reader, val.primary);
            unmarshall(reader, val.status);
        }

        DEFINE_POD_SERIALIZATION(replication::read_semantic)

            DEFINE_POD_SERIALIZATION(replication::client_read_request)

            DEFINE_POD_SERIALIZATION(replication::ClientResponse)

            DEFINE_POD_SERIALIZATION(replication::PrepareAck)

            inline void marshall(::rdsn::utils::binary_writer& writer, const learn_state& val, uint16_t pos = 0xffff)
        {
            marshall(writer, val.meta, pos);
            marshall(writer, val.files, pos);
        }

        inline void unmarshall(::rdsn::utils::binary_reader& reader, __out learn_state& val)
        {
            unmarshall(reader, val.meta);
            unmarshall(reader, val.files);
        }

        DEFINE_POD_SERIALIZATION(replication::LearnerState)

            inline void marshall(::rdsn::utils::binary_writer& writer, const QueryConfigurationByIndexRequest& val, uint16_t pos = 0xffff)
        {
            marshall(writer, val.app_name, pos);
            marshall(writer, val.parIdxes, pos);
        }

        inline void unmarshall(::rdsn::utils::binary_reader& reader, __out QueryConfigurationByIndexRequest& val)
        {
            unmarshall(reader, val.app_name);
            unmarshall(reader, val.parIdxes);
        }

        inline void marshall(::rdsn::utils::binary_writer& writer, const QueryConfigurationByIndexResponse& val, uint16_t pos = 0xffff)
        {
            marshall(writer, val.err, pos);
            marshall(writer, val.partitions, pos);
        }

        inline void unmarshall(::rdsn::utils::binary_reader& reader, __out QueryConfigurationByIndexResponse& val)
        {
            unmarshall(reader, val.err);
            unmarshall(reader, val.partitions);
        }


        DEFINE_POD_SERIALIZATION(replication::SimpleKvOperation)


            inline void marshall(::rdsn::utils::binary_writer& writer, const SimpleKvRequest& val, uint16_t pos = 0xffff)
        {
            marshall(writer, val.op, pos);
            marshall(writer, val.key, pos);
            marshall(writer, val.value, pos);
        }

        inline void unmarshall(::rdsn::utils::binary_reader& reader, __out SimpleKvRequest& val)
        {
            unmarshall(reader, val.op);
            unmarshall(reader, val.key);
            unmarshall(reader, val.value);
        }



        inline void marshall(::rdsn::utils::binary_writer& writer, const SimpleKvResponse& val, uint16_t pos = 0xffff)
        {
            marshall(writer, val.err, pos);
            marshall(writer, val.key, pos);
            marshall(writer, val.value, pos);
        }

        inline void unmarshall(::rdsn::utils::binary_reader& reader, __out SimpleKvResponse& val)
        {
            unmarshall(reader, val.err);
            unmarshall(reader, val.key);
            unmarshall(reader, val.value);
        }

        inline void marshall(::rdsn::utils::binary_writer& writer, const learn_request& val, uint16_t pos = 0xffff)
        {
            marshall(writer, val.gpid, pos);
            marshall(writer, val.learner, pos);
            marshall(writer, val.signature, pos);
            marshall(writer, val.lastCommittedDecreeInApp, pos);
            marshall(writer, val.lastCommittedDecreeInPrepareList, pos);
            marshall(writer, val.appSpecificLearnRequest, pos);
        }

        inline void unmarshall(::rdsn::utils::binary_reader& reader, __out learn_request& val)
        {
            unmarshall(reader, val.gpid);
            unmarshall(reader, val.learner);
            unmarshall(reader, val.signature);
            unmarshall(reader, val.lastCommittedDecreeInApp);
            unmarshall(reader, val.lastCommittedDecreeInPrepareList);
            unmarshall(reader, val.appSpecificLearnRequest);
        }

        inline void marshall(::rdsn::utils::binary_writer& writer, const learn_response& val, uint16_t pos = 0xffff)
        {
            marshall(writer, val.err, pos);
            marshall(writer, val.config, pos);
            marshall(writer, val.commitDecree, pos);
            marshall(writer, val.prepareStartDecree, pos);
            marshall(writer, val.state, pos);
            marshall(writer, val.baseLocalDir, pos);
        }

        inline void unmarshall(::rdsn::utils::binary_reader& reader, __out learn_response& val)
        {
            unmarshall(reader, val.err);
            unmarshall(reader, val.config);
            unmarshall(reader, val.commitDecree);
            unmarshall(reader, val.prepareStartDecree);
            unmarshall(reader, val.state);
            unmarshall(reader, val.baseLocalDir);
        }

        inline void marshall(::rdsn::utils::binary_writer& writer, const group_check_request& val, uint16_t pos = 0xffff)
        {
            marshall(writer, val.app_type, pos);
            marshall(writer, val.node, pos);
            marshall(writer, val.config, pos);
            marshall(writer, val.lastCommittedDecree, pos);
            marshall(writer, val.learnerSignature, pos);
        }

        inline void unmarshall(::rdsn::utils::binary_reader& reader, __out group_check_request& val)
        {
            unmarshall(reader, val.app_type);
            unmarshall(reader, val.node);
            unmarshall(reader, val.config);
            unmarshall(reader, val.lastCommittedDecree);
            unmarshall(reader, val.learnerSignature);
        }

        inline void marshall(::rdsn::utils::binary_writer& writer, const group_check_response& val, uint16_t pos = 0xffff)
        {
            marshall(writer, val.gpid, pos);
            marshall(writer, val.err, pos);
            marshall(writer, val.lastCommittedDecreeInApp, pos);
            marshall(writer, val.lastCommittedDecreeInPrepareList, pos);
            marshall(writer, val.learnerState, pos);
            marshall(writer, val.learnerSignature, pos);
            marshall(writer, val.node, pos);
        }

        inline void unmarshall(::rdsn::utils::binary_reader& reader, __out group_check_response& val)
        {
            unmarshall(reader, val.gpid);
            unmarshall(reader, val.err);
            unmarshall(reader, val.lastCommittedDecreeInApp);
            unmarshall(reader, val.lastCommittedDecreeInPrepareList);
            unmarshall(reader, val.learnerState);
            unmarshall(reader, val.learnerSignature);
            unmarshall(reader, val.node);
        }

        DEFINE_POD_SERIALIZATION(replication::config_type)

            inline void marshall(::rdsn::utils::binary_writer& writer, const configuration_update_request& val, uint16_t pos = 0xffff)
        {
            marshall(writer, val.config, pos);
            marshall(writer, val.type, pos);
            marshall(writer, val.node, pos);
        }

        inline void unmarshall(::rdsn::utils::binary_reader& reader, __out configuration_update_request& val)
        {
            unmarshall(reader, val.config);
            unmarshall(reader, val.type);
            unmarshall(reader, val.node);
        }

        inline void marshall(::rdsn::utils::binary_writer& writer, const ConfigurationUpdateResponse& val, uint16_t pos = 0xffff)
        {
            marshall(writer, val.err, pos);
            marshall(writer, val.config, pos);
        }

        inline void unmarshall(::rdsn::utils::binary_reader& reader, __out ConfigurationUpdateResponse& val)
        {
            unmarshall(reader, val.err);
            unmarshall(reader, val.config);
        }

        inline void marshall(::rdsn::utils::binary_writer& writer, const QueryPNDecreeRequest& val, uint16_t pos = 0xffff)
        {
            marshall(writer, val.partitionId, pos);
            marshall(writer, val.node, pos);
        }

        inline void unmarshall(::rdsn::utils::binary_reader& reader, __out QueryPNDecreeRequest& val)
        {
            unmarshall(reader, val.partitionId);
            unmarshall(reader, val.node);
        }

        DEFINE_POD_SERIALIZATION(replication::QueryPNDecreeResponse)

            inline void marshall(::rdsn::utils::binary_writer& writer, const ConfigurationNodeQueryRequest& val, uint16_t pos = 0xffff)
        {
            marshall(writer, val.node, pos);
        }

        inline void unmarshall(::rdsn::utils::binary_reader& reader, __out ConfigurationNodeQueryRequest& val)
        {
            unmarshall(reader, val.node);
        }

        inline void marshall(::rdsn::utils::binary_writer& writer, const ConfigurationNodeQueryResponse& val, uint16_t pos = 0xffff)
        {
            marshall(writer, val.err, pos);
            marshall(writer, val.partitions, pos);
        }

        inline void unmarshall(::rdsn::utils::binary_reader& reader, __out ConfigurationNodeQueryResponse& val)
        {
            unmarshall(reader, val.err);
            unmarshall(reader, val.partitions);
        }
    }
} // end namespace rdsn::replication
