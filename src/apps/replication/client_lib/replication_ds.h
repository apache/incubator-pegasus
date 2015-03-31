/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation, Robust Distributed System Nucleus(rDSN)

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

# include <dsn/internal/serialization.h>
# include <dsn/internal/enum_helper.h>

namespace dsn {
    namespace replication {

        struct ReplicationMsgHeader
        {
            uint64_t id;
        };

        struct meta_msg_header
        {
            int32_t rpc_tag;
        };

        struct meta_response_header
        {
            int err;
            end_point primary_address;
        };

        struct global_partition_id
        {
            int32_t  app_id = -1;
            int32_t  pidx = -1;
        };

        inline bool operator == (const global_partition_id& l, const global_partition_id& r)
        {
            return l.app_id == r.app_id && l.pidx == r.pidx;
        }

        struct mutation_header
        {
            global_partition_id gpid;
            int64_t             ballot;
            int64_t             decree;
            int64_t             log_offset;
            int64_t             last_committed_decree;
        };

        struct mutation_data
        {
            mutation_header    header;
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
            int32_t                max_replica_count;
            end_point              primary;
            std::vector<end_point> secondaries;
            std::vector<end_point> drop_outs;
            int64_t                last_committed_decree;
        };

        struct replica_configuration
        {
            global_partition_id            gpid;
            int64_t                        ballot;
            end_point        primary;
            partition_status              status = PS_INACTIVE;
        };

        enum read_semantic_t
        {
            ReadLastUpdate,
            ReadOutdated,
            ReadSnapshot
        };

        struct client_read_request
        {
            global_partition_id gpid;
            read_semantic_t      semantic = ReadLastUpdate;
            int64_t             version_decree = -1;
        };

        struct client_response
        {
            int err = 0;
            int32_t pending_request_count = 0;
            int64_t last_committed_decree = 0;
        };

        struct PrepareAck
        {
            global_partition_id gpid;
            int          err;
            int64_t             ballot;
            int64_t             decree;
            int64_t             last_committed_decree_in_app;
            int64_t             last_committed_decree_in_prepare_list;
        };

        struct learn_state
        {
            utils::blob                       meta;
            std::vector<std::string>          files;
        };

        enum learner_status
        {
            LearningWithoutPrepare,
            LearningWithPrepare,
            LearningSucceeded,
            LearningFailed,
            Learning_INVALID
        };

        ENUM_BEGIN(learner_status, Learning_INVALID)
            ENUM_REG(LearningWithoutPrepare)
            ENUM_REG(LearningWithPrepare)
            ENUM_REG(LearningSucceeded)
            ENUM_REG(LearningFailed)
            ENUM_END(learner_status)

        struct learn_request
        {
            global_partition_id    gpid;
            end_point learner;
            uint64_t               signature;
            int64_t                last_committed_decree_in_app;
            int64_t                last_committed_decree_in_prepare_list;
            utils::blob                 app_specific_learn_request;
        };

        struct learn_response
        {
            int               err;
            replica_configuration    config;
            int64_t                   commit_decree;
            int64_t                   prepare_start_decree;
            learn_state              state;
            std::string                  base_local_dir;
        };

        struct group_check_request
        {
            std::string                  app_type;
            end_point    node;
            replica_configuration    config;
            int64_t                   last_committed_decree;
            uint64_t                  learner_signature;
        };

        struct group_check_response
        {
            global_partition_id       gpid;
            int                err;
            int64_t                   last_committed_decree_in_app;
            int64_t                   last_committed_decree_in_prepare_list;
            learner_status            learner_status_ = LearningFailed;
            uint64_t                  learner_signature;
            end_point    node;
        };


        ///////////// demo ///////////////

        enum simple_kv_operation
        {
            SKV_NOP,
            SKV_UPDATE,
            SKV_READ,
            SKV_APPEND,
        };

        struct simple_kv_request
        {
            simple_kv_operation op = SKV_NOP;
            std::string key;
            std::string value;
        };

        struct simple_kv_response
        {
            int32_t  err;
            std::string key;
            std::string value;
        };


        /////////////////// meta server messages ////////////////////
        enum config_type
        {
            CT_NONE,
            CT_ASSIGN_PRIMARY,
            CT_ADD_SECONDARY,
            CT_DOWNGRADE_TO_SECONDARY,
            CT_DOWNGRADE_TO_INACTIVE,
            CT_REMOVE,

            // not used by meta server
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

            // primary | secondary(upgrading) (w/ new config) => meta server
        struct configuration_update_request
        {
            partition_configuration  config;
            config_type              type = CT_NONE;
            end_point    node;
        };

        // meta server (config mgr) => primary | secondary (downgrade) (w/ new config)
        struct configuration_update_response
        {
            int                err;
            partition_configuration  config;
        };

        // proposal:  meta server(LBM) => primary  (w/ current config)
        struct configuration_proposal_request
        {
            partition_configuration  config;
            config_type              type = CT_NONE;
            end_point   node;
            bool                    is_clean_data = false;
            bool                    is_upgrade = false;
        };

        // client => meta server
        struct configuration_node_query_request
        {
            end_point    node;
        };

        // meta server => client
        struct configuration_node_query_response
        {
            int                        err;
            std::list<partition_configuration> partitions;
        };


        struct query_replica_decree_request
        {
            global_partition_id                partition_id;
            end_point    node;
        };

        struct query_replica_decree_response
        {
            int                 err;
            int64_t                    last_decree;
        };

        struct query_configuration_by_index_request
        {
            std::string           app_name;
            std::vector<uint32_t>   partition_indices;
        };

        struct query_configuration_by_index_response
        {
            int                           err;
            std::vector<partition_configuration> partitions;
        };



        DEFINE_POD_SERIALIZATION(replication::ReplicationMsgHeader)
            DEFINE_POD_SERIALIZATION(replication::meta_msg_header)

            inline void marshall(::dsn::utils::binary_writer& writer, const meta_response_header& val, uint16_t pos = 0xffff)
        {
            marshall(writer, val.err, pos);
            marshall(writer, val.primary_address, pos);
        }

        inline void unmarshall(::dsn::utils::binary_reader& reader, __out_param meta_response_header& val)
        {
            unmarshall(reader, val.err);
            unmarshall(reader, val.primary_address);
        }

        DEFINE_POD_SERIALIZATION(replication::global_partition_id)

            DEFINE_POD_SERIALIZATION(replication::mutation_header)

            inline void marshall(::dsn::utils::binary_writer& writer, const mutation_data& val, uint16_t pos = 0xffff)
        {
            marshall(writer, val.header, pos);
            marshall(writer, val.updates, pos);
        }

        inline void unmarshall(::dsn::utils::binary_reader& reader, __out_param mutation_data& val)
        {
            unmarshall(reader, val.header);
            unmarshall(reader, val.updates);
        }

        DEFINE_POD_SERIALIZATION(replication::partition_status)

            inline void marshall(::dsn::utils::binary_writer& writer, const partition_configuration& val, uint16_t pos = 0xffff)
        {
            marshall(writer, val.app_type, pos);
            marshall(writer, val.gpid, pos);
            marshall(writer, val.ballot, pos);
            marshall(writer, val.max_replica_count, pos);
            marshall(writer, val.primary, pos);
            marshall(writer, val.secondaries, pos);
            marshall(writer, val.drop_outs, pos);
            marshall(writer, val.last_committed_decree, pos);
        }

        inline void unmarshall(::dsn::utils::binary_reader& reader, __out_param partition_configuration& val)
        {
            unmarshall(reader, val.app_type);
            unmarshall(reader, val.gpid);
            unmarshall(reader, val.ballot);
            unmarshall(reader, val.max_replica_count);
            unmarshall(reader, val.primary);
            unmarshall(reader, val.secondaries);
            unmarshall(reader, val.drop_outs);
            unmarshall(reader, val.last_committed_decree);
        }

        inline void marshall(::dsn::utils::binary_writer& writer, const replica_configuration& val, uint16_t pos = 0xffff)
        {
            marshall(writer, val.gpid, pos);
            marshall(writer, val.ballot, pos);
            marshall(writer, val.primary, pos);
            marshall(writer, val.status, pos);
        }

        inline void unmarshall(::dsn::utils::binary_reader& reader, __out_param replica_configuration& val)
        {
            unmarshall(reader, val.gpid);
            unmarshall(reader, val.ballot);
            unmarshall(reader, val.primary);
            unmarshall(reader, val.status);
        }

        DEFINE_POD_SERIALIZATION(replication::read_semantic_t)

            DEFINE_POD_SERIALIZATION(replication::client_read_request)

            DEFINE_POD_SERIALIZATION(replication::client_response)

            DEFINE_POD_SERIALIZATION(replication::PrepareAck)

            inline void marshall(::dsn::utils::binary_writer& writer, const learn_state& val, uint16_t pos = 0xffff)
        {
            marshall(writer, val.meta, pos);
            marshall(writer, val.files, pos);
        }

        inline void unmarshall(::dsn::utils::binary_reader& reader, __out_param learn_state& val)
        {
            unmarshall(reader, val.meta);
            unmarshall(reader, val.files);
        }

        DEFINE_POD_SERIALIZATION(replication::learner_status)

            inline void marshall(::dsn::utils::binary_writer& writer, const query_configuration_by_index_request& val, uint16_t pos = 0xffff)
        {
            marshall(writer, val.app_name, pos);
            marshall(writer, val.partition_indices, pos);
        }

        inline void unmarshall(::dsn::utils::binary_reader& reader, __out_param query_configuration_by_index_request& val)
        {
            unmarshall(reader, val.app_name);
            unmarshall(reader, val.partition_indices);
        }

        inline void marshall(::dsn::utils::binary_writer& writer, const query_configuration_by_index_response& val, uint16_t pos = 0xffff)
        {
            marshall(writer, val.err, pos);
            marshall(writer, val.partitions, pos);
        }

        inline void unmarshall(::dsn::utils::binary_reader& reader, __out_param query_configuration_by_index_response& val)
        {
            unmarshall(reader, val.err);
            unmarshall(reader, val.partitions);
        }


        DEFINE_POD_SERIALIZATION(replication::simple_kv_operation)


            inline void marshall(::dsn::utils::binary_writer& writer, const simple_kv_request& val, uint16_t pos = 0xffff)
        {
            marshall(writer, val.op, pos);
            marshall(writer, val.key, pos);
            marshall(writer, val.value, pos);
        }

        inline void unmarshall(::dsn::utils::binary_reader& reader, __out_param simple_kv_request& val)
        {
            unmarshall(reader, val.op);
            unmarshall(reader, val.key);
            unmarshall(reader, val.value);
        }



        inline void marshall(::dsn::utils::binary_writer& writer, const simple_kv_response& val, uint16_t pos = 0xffff)
        {
            marshall(writer, val.err, pos);
            marshall(writer, val.key, pos);
            marshall(writer, val.value, pos);
        }

        inline void unmarshall(::dsn::utils::binary_reader& reader, __out_param simple_kv_response& val)
        {
            unmarshall(reader, val.err);
            unmarshall(reader, val.key);
            unmarshall(reader, val.value);
        }

        inline void marshall(::dsn::utils::binary_writer& writer, const learn_request& val, uint16_t pos = 0xffff)
        {
            marshall(writer, val.gpid, pos);
            marshall(writer, val.learner, pos);
            marshall(writer, val.signature, pos);
            marshall(writer, val.last_committed_decree_in_app, pos);
            marshall(writer, val.last_committed_decree_in_prepare_list, pos);
            marshall(writer, val.app_specific_learn_request, pos);
        }

        inline void unmarshall(::dsn::utils::binary_reader& reader, __out_param learn_request& val)
        {
            unmarshall(reader, val.gpid);
            unmarshall(reader, val.learner);
            unmarshall(reader, val.signature);
            unmarshall(reader, val.last_committed_decree_in_app);
            unmarshall(reader, val.last_committed_decree_in_prepare_list);
            unmarshall(reader, val.app_specific_learn_request);
        }

        inline void marshall(::dsn::utils::binary_writer& writer, const learn_response& val, uint16_t pos = 0xffff)
        {
            marshall(writer, val.err, pos);
            marshall(writer, val.config, pos);
            marshall(writer, val.commit_decree, pos);
            marshall(writer, val.prepare_start_decree, pos);
            marshall(writer, val.state, pos);
            marshall(writer, val.base_local_dir, pos);
        }

        inline void unmarshall(::dsn::utils::binary_reader& reader, __out_param learn_response& val)
        {
            unmarshall(reader, val.err);
            unmarshall(reader, val.config);
            unmarshall(reader, val.commit_decree);
            unmarshall(reader, val.prepare_start_decree);
            unmarshall(reader, val.state);
            unmarshall(reader, val.base_local_dir);
        }

        inline void marshall(::dsn::utils::binary_writer& writer, const group_check_request& val, uint16_t pos = 0xffff)
        {
            marshall(writer, val.app_type, pos);
            marshall(writer, val.node, pos);
            marshall(writer, val.config, pos);
            marshall(writer, val.last_committed_decree, pos);
            marshall(writer, val.learner_signature, pos);
        }

        inline void unmarshall(::dsn::utils::binary_reader& reader, __out_param group_check_request& val)
        {
            unmarshall(reader, val.app_type);
            unmarshall(reader, val.node);
            unmarshall(reader, val.config);
            unmarshall(reader, val.last_committed_decree);
            unmarshall(reader, val.learner_signature);
        }

        inline void marshall(::dsn::utils::binary_writer& writer, const group_check_response& val, uint16_t pos = 0xffff)
        {
            marshall(writer, val.gpid, pos);
            marshall(writer, val.err, pos);
            marshall(writer, val.last_committed_decree_in_app, pos);
            marshall(writer, val.last_committed_decree_in_prepare_list, pos);
            marshall(writer, val.learner_status_, pos);
            marshall(writer, val.learner_signature, pos);
            marshall(writer, val.node, pos);
        }

        inline void unmarshall(::dsn::utils::binary_reader& reader, __out_param group_check_response& val)
        {
            unmarshall(reader, val.gpid);
            unmarshall(reader, val.err);
            unmarshall(reader, val.last_committed_decree_in_app);
            unmarshall(reader, val.last_committed_decree_in_prepare_list);
            unmarshall(reader, val.learner_status_);
            unmarshall(reader, val.learner_signature);
            unmarshall(reader, val.node);
        }

        DEFINE_POD_SERIALIZATION(replication::config_type)

            inline void marshall(::dsn::utils::binary_writer& writer, const configuration_update_request& val, uint16_t pos = 0xffff)
        {
            marshall(writer, val.config, pos);
            marshall(writer, val.type, pos);
            marshall(writer, val.node, pos);
        }

        inline void unmarshall(::dsn::utils::binary_reader& reader, __out_param configuration_update_request& val)
        {
            unmarshall(reader, val.config);
            unmarshall(reader, val.type);
            unmarshall(reader, val.node);
        }

        inline void marshall(::dsn::utils::binary_writer& writer, const configuration_update_response& val, uint16_t pos = 0xffff)
        {
            marshall(writer, val.err, pos);
            marshall(writer, val.config, pos);
        }

        inline void unmarshall(::dsn::utils::binary_reader& reader, __out_param configuration_update_response& val)
        {
            unmarshall(reader, val.err);
            unmarshall(reader, val.config);
        }

        inline void marshall(::dsn::utils::binary_writer& writer, const query_replica_decree_request& val, uint16_t pos = 0xffff)
        {
            marshall(writer, val.partition_id, pos);
            marshall(writer, val.node, pos);
        }

        inline void unmarshall(::dsn::utils::binary_reader& reader, __out_param query_replica_decree_request& val)
        {
            unmarshall(reader, val.partition_id);
            unmarshall(reader, val.node);
        }

        DEFINE_POD_SERIALIZATION(replication::query_replica_decree_response)

            inline void marshall(::dsn::utils::binary_writer& writer, const configuration_node_query_request& val, uint16_t pos = 0xffff)
        {
            marshall(writer, val.node, pos);
        }

        inline void unmarshall(::dsn::utils::binary_reader& reader, __out_param configuration_node_query_request& val)
        {
            unmarshall(reader, val.node);
        }

        inline void marshall(::dsn::utils::binary_writer& writer, const configuration_node_query_response& val, uint16_t pos = 0xffff)
        {
            marshall(writer, val.err, pos);
            marshall(writer, val.partitions, pos);
        }

        inline void unmarshall(::dsn::utils::binary_reader& reader, __out_param configuration_node_query_response& val)
        {
            unmarshall(reader, val.err);
            unmarshall(reader, val.partitions);
        }
    }
} // end namespace dsn::replication
