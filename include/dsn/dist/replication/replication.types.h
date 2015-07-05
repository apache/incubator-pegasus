# pragma once

//
// uncomment the following line if you want to use 
// data encoding/decoding from the original tool instead of rDSN
// in this case, you need to use these tools to generate
// type files with --gen=cpp etc. options
//
// !!! WARNING: not feasible for replicated service yet!!! 
//
// # define DSN_NOT_USE_DEFAULT_SERIALIZATION

# include <dsn/internal/serialization.h>

# ifdef DSN_NOT_USE_DEFAULT_SERIALIZATION

# include <dsn/thrift_helper.h>
# include "replication_types.h" 

namespace dsn { namespace replication { 
    // ---------- global_partition_id -------------
    inline void marshall(::dsn::binary_writer& writer, const global_partition_id& val, uint16_t pos = 0xffff)
    {
        boost::shared_ptr<::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::marshall_rpc_args<global_partition_id>(&proto, val, &global_partition_id::write);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param global_partition_id& val)
    {
        boost::shared_ptr<::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::unmarshall_rpc_args<global_partition_id>(&proto, val, &global_partition_id::read);
    };

    // ---------- mutation_header -------------
    inline void marshall(::dsn::binary_writer& writer, const mutation_header& val, uint16_t pos = 0xffff)
    {
        boost::shared_ptr<::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::marshall_rpc_args<mutation_header>(&proto, val, &mutation_header::write);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param mutation_header& val)
    {
        boost::shared_ptr<::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::unmarshall_rpc_args<mutation_header>(&proto, val, &mutation_header::read);
    };

    // ---------- mutation_data -------------
    inline void marshall(::dsn::binary_writer& writer, const mutation_data& val, uint16_t pos = 0xffff)
    {
        boost::shared_ptr<::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::marshall_rpc_args<mutation_data>(&proto, val, &mutation_data::write);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param mutation_data& val)
    {
        boost::shared_ptr<::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::unmarshall_rpc_args<mutation_data>(&proto, val, &mutation_data::read);
    };

    // ---------- partition_configuration -------------
    inline void marshall(::dsn::binary_writer& writer, const partition_configuration& val, uint16_t pos = 0xffff)
    {
        boost::shared_ptr<::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::marshall_rpc_args<partition_configuration>(&proto, val, &partition_configuration::write);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param partition_configuration& val)
    {
        boost::shared_ptr<::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::unmarshall_rpc_args<partition_configuration>(&proto, val, &partition_configuration::read);
    };

    // ---------- replica_configuration -------------
    inline void marshall(::dsn::binary_writer& writer, const replica_configuration& val, uint16_t pos = 0xffff)
    {
        boost::shared_ptr<::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::marshall_rpc_args<replica_configuration>(&proto, val, &replica_configuration::write);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param replica_configuration& val)
    {
        boost::shared_ptr<::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::unmarshall_rpc_args<replica_configuration>(&proto, val, &replica_configuration::read);
    };

    // ---------- prepare_msg -------------
    inline void marshall(::dsn::binary_writer& writer, const prepare_msg& val, uint16_t pos = 0xffff)
    {
        boost::shared_ptr<::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::marshall_rpc_args<prepare_msg>(&proto, val, &prepare_msg::write);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param prepare_msg& val)
    {
        boost::shared_ptr<::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::unmarshall_rpc_args<prepare_msg>(&proto, val, &prepare_msg::read);
    };

    // ---------- read_request_header -------------
    inline void marshall(::dsn::binary_writer& writer, const read_request_header& val, uint16_t pos = 0xffff)
    {
        boost::shared_ptr<::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::marshall_rpc_args<read_request_header>(&proto, val, &read_request_header::write);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param read_request_header& val)
    {
        boost::shared_ptr<::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::unmarshall_rpc_args<read_request_header>(&proto, val, &read_request_header::read);
    };

    // ---------- write_request_header -------------
    inline void marshall(::dsn::binary_writer& writer, const write_request_header& val, uint16_t pos = 0xffff)
    {
        boost::shared_ptr<::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::marshall_rpc_args<write_request_header>(&proto, val, &write_request_header::write);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param write_request_header& val)
    {
        boost::shared_ptr<::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::unmarshall_rpc_args<write_request_header>(&proto, val, &write_request_header::read);
    };

    // ---------- rw_response_header -------------
    inline void marshall(::dsn::binary_writer& writer, const rw_response_header& val, uint16_t pos = 0xffff)
    {
        boost::shared_ptr<::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::marshall_rpc_args<rw_response_header>(&proto, val, &rw_response_header::write);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param rw_response_header& val)
    {
        boost::shared_ptr<::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::unmarshall_rpc_args<rw_response_header>(&proto, val, &rw_response_header::read);
    };

    // ---------- prepare_ack -------------
    inline void marshall(::dsn::binary_writer& writer, const prepare_ack& val, uint16_t pos = 0xffff)
    {
        boost::shared_ptr<::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::marshall_rpc_args<prepare_ack>(&proto, val, &prepare_ack::write);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param prepare_ack& val)
    {
        boost::shared_ptr<::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::unmarshall_rpc_args<prepare_ack>(&proto, val, &prepare_ack::read);
    };

    // ---------- learn_state -------------
    inline void marshall(::dsn::binary_writer& writer, const learn_state& val, uint16_t pos = 0xffff)
    {
        boost::shared_ptr<::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::marshall_rpc_args<learn_state>(&proto, val, &learn_state::write);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param learn_state& val)
    {
        boost::shared_ptr<::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::unmarshall_rpc_args<learn_state>(&proto, val, &learn_state::read);
    };

    // ---------- learn_request -------------
    inline void marshall(::dsn::binary_writer& writer, const learn_request& val, uint16_t pos = 0xffff)
    {
        boost::shared_ptr<::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::marshall_rpc_args<learn_request>(&proto, val, &learn_request::write);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param learn_request& val)
    {
        boost::shared_ptr<::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::unmarshall_rpc_args<learn_request>(&proto, val, &learn_request::read);
    };

    // ---------- learn_response -------------
    inline void marshall(::dsn::binary_writer& writer, const learn_response& val, uint16_t pos = 0xffff)
    {
        boost::shared_ptr<::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::marshall_rpc_args<learn_response>(&proto, val, &learn_response::write);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param learn_response& val)
    {
        boost::shared_ptr<::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::unmarshall_rpc_args<learn_response>(&proto, val, &learn_response::read);
    };

    // ---------- group_check_request -------------
    inline void marshall(::dsn::binary_writer& writer, const group_check_request& val, uint16_t pos = 0xffff)
    {
        boost::shared_ptr<::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::marshall_rpc_args<group_check_request>(&proto, val, &group_check_request::write);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param group_check_request& val)
    {
        boost::shared_ptr<::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::unmarshall_rpc_args<group_check_request>(&proto, val, &group_check_request::read);
    };

    // ---------- group_check_response -------------
    inline void marshall(::dsn::binary_writer& writer, const group_check_response& val, uint16_t pos = 0xffff)
    {
        boost::shared_ptr<::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::marshall_rpc_args<group_check_response>(&proto, val, &group_check_response::write);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param group_check_response& val)
    {
        boost::shared_ptr<::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::unmarshall_rpc_args<group_check_response>(&proto, val, &group_check_response::read);
    };

    // ---------- meta_request_header -------------
    inline void marshall(::dsn::binary_writer& writer, const meta_request_header& val, uint16_t pos = 0xffff)
    {
        boost::shared_ptr<::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::marshall_rpc_args<meta_request_header>(&proto, val, &meta_request_header::write);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param meta_request_header& val)
    {
        boost::shared_ptr<::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::unmarshall_rpc_args<meta_request_header>(&proto, val, &meta_request_header::read);
    };

    // ---------- meta_response_header -------------
    inline void marshall(::dsn::binary_writer& writer, const meta_response_header& val, uint16_t pos = 0xffff)
    {
        boost::shared_ptr<::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::marshall_rpc_args<meta_response_header>(&proto, val, &meta_response_header::write);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param meta_response_header& val)
    {
        boost::shared_ptr<::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::unmarshall_rpc_args<meta_response_header>(&proto, val, &meta_response_header::read);
    };

    // ---------- configuration_update_request -------------
    inline void marshall(::dsn::binary_writer& writer, const configuration_update_request& val, uint16_t pos = 0xffff)
    {
        boost::shared_ptr<::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::marshall_rpc_args<configuration_update_request>(&proto, val, &configuration_update_request::write);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param configuration_update_request& val)
    {
        boost::shared_ptr<::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::unmarshall_rpc_args<configuration_update_request>(&proto, val, &configuration_update_request::read);
    };

    // ---------- configuration_update_response -------------
    inline void marshall(::dsn::binary_writer& writer, const configuration_update_response& val, uint16_t pos = 0xffff)
    {
        boost::shared_ptr<::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::marshall_rpc_args<configuration_update_response>(&proto, val, &configuration_update_response::write);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param configuration_update_response& val)
    {
        boost::shared_ptr<::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::unmarshall_rpc_args<configuration_update_response>(&proto, val, &configuration_update_response::read);
    };

    // ---------- configuration_proposal_request -------------
    inline void marshall(::dsn::binary_writer& writer, const configuration_proposal_request& val, uint16_t pos = 0xffff)
    {
        boost::shared_ptr<::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::marshall_rpc_args<configuration_proposal_request>(&proto, val, &configuration_proposal_request::write);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param configuration_proposal_request& val)
    {
        boost::shared_ptr<::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::unmarshall_rpc_args<configuration_proposal_request>(&proto, val, &configuration_proposal_request::read);
    };

    // ---------- configuration_query_by_node_request -------------
    inline void marshall(::dsn::binary_writer& writer, const configuration_query_by_node_request& val, uint16_t pos = 0xffff)
    {
        boost::shared_ptr<::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::marshall_rpc_args<configuration_query_by_node_request>(&proto, val, &configuration_query_by_node_request::write);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param configuration_query_by_node_request& val)
    {
        boost::shared_ptr<::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::unmarshall_rpc_args<configuration_query_by_node_request>(&proto, val, &configuration_query_by_node_request::read);
    };

    // ---------- configuration_query_by_node_response -------------
    inline void marshall(::dsn::binary_writer& writer, const configuration_query_by_node_response& val, uint16_t pos = 0xffff)
    {
        boost::shared_ptr<::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::marshall_rpc_args<configuration_query_by_node_response>(&proto, val, &configuration_query_by_node_response::write);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param configuration_query_by_node_response& val)
    {
        boost::shared_ptr<::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::unmarshall_rpc_args<configuration_query_by_node_response>(&proto, val, &configuration_query_by_node_response::read);
    };

    // ---------- configuration_query_by_index_request -------------
    inline void marshall(::dsn::binary_writer& writer, const configuration_query_by_index_request& val, uint16_t pos = 0xffff)
    {
        boost::shared_ptr<::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::marshall_rpc_args<configuration_query_by_index_request>(&proto, val, &configuration_query_by_index_request::write);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param configuration_query_by_index_request& val)
    {
        boost::shared_ptr<::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::unmarshall_rpc_args<configuration_query_by_index_request>(&proto, val, &configuration_query_by_index_request::read);
    };

    // ---------- configuration_query_by_index_response -------------
    inline void marshall(::dsn::binary_writer& writer, const configuration_query_by_index_response& val, uint16_t pos = 0xffff)
    {
        boost::shared_ptr<::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::marshall_rpc_args<configuration_query_by_index_response>(&proto, val, &configuration_query_by_index_response::write);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param configuration_query_by_index_response& val)
    {
        boost::shared_ptr<::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::unmarshall_rpc_args<configuration_query_by_index_response>(&proto, val, &configuration_query_by_index_response::read);
    };

    // ---------- query_replica_decree_request -------------
    inline void marshall(::dsn::binary_writer& writer, const query_replica_decree_request& val, uint16_t pos = 0xffff)
    {
        boost::shared_ptr<::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::marshall_rpc_args<query_replica_decree_request>(&proto, val, &query_replica_decree_request::write);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param query_replica_decree_request& val)
    {
        boost::shared_ptr<::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::unmarshall_rpc_args<query_replica_decree_request>(&proto, val, &query_replica_decree_request::read);
    };

    // ---------- query_replica_decree_response -------------
    inline void marshall(::dsn::binary_writer& writer, const query_replica_decree_response& val, uint16_t pos = 0xffff)
    {
        boost::shared_ptr<::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::marshall_rpc_args<query_replica_decree_response>(&proto, val, &query_replica_decree_response::write);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param query_replica_decree_response& val)
    {
        boost::shared_ptr<::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
        ::apache::thrift::protocol::TBinaryProtocol proto(transport);
        ::dsn::unmarshall_rpc_args<query_replica_decree_response>(&proto, val, &query_replica_decree_response::read);
    };

} } 


# else // use rDSN's data encoding/decoding

namespace dsn { namespace replication { 
    // ---------- partition_status -------------
    enum partition_status
    {
        PS_INACTIVE = 0,
        PS_ERROR = 1,
        PS_PRIMARY = 2,
        PS_SECONDARY = 3,
        PS_POTENTIAL_SECONDARY = 4,
        PS_INVALID = 5,
    };

    DEFINE_POD_SERIALIZATION(partition_status);

    // ---------- read_semantic_t -------------
    enum read_semantic_t
    {
        ReadLastUpdate = 0,
        ReadOutdated = 1,
        ReadSnapshot = 2,
    };

    DEFINE_POD_SERIALIZATION(read_semantic_t);

    // ---------- learner_status -------------
    enum learner_status
    {
        LearningWithoutPrepare = 0,
        LearningWithPrepare = 1,
        LearningSucceeded = 2,
        LearningFailed = 3,
        Learning_INVALID = 4,
    };

    DEFINE_POD_SERIALIZATION(learner_status);

    // ---------- config_type -------------
    enum config_type
    {
        CT_NONE = 0,
        CT_ASSIGN_PRIMARY = 1,
        CT_UPGRADE_TO_PRIMARY = 2,
        CT_ADD_SECONDARY = 3,
        CT_DOWNGRADE_TO_SECONDARY = 4,
        CT_DOWNGRADE_TO_INACTIVE = 5,
        CT_REMOVE = 6,
        CT_UPGRADE_TO_SECONDARY = 7,
    };

    DEFINE_POD_SERIALIZATION(config_type);

    // ---------- global_partition_id -------------
    struct global_partition_id
    {
        int32_t app_id;
        int32_t pidx;

        bool operator < (const global_partition_id& r) const
        {
            return app_id < r.app_id || (app_id == r.app_id && pidx < r.pidx);
        }

        bool operator == (const global_partition_id& r) const
        {
            return app_id == r.app_id && pidx == r.pidx;
        }
    };

    inline void marshall(::dsn::binary_writer& writer, const global_partition_id& val, uint16_t pos = 0xffff)
    {
        marshall(writer, val.app_id, pos);
        marshall(writer, val.pidx, pos);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param global_partition_id& val)
    {
        unmarshall(reader, val.app_id);
        unmarshall(reader, val.pidx);
    };

    // ---------- mutation_header -------------
    struct mutation_header
    {
        global_partition_id gpid;
        int64_t ballot;
        int64_t decree;
        int64_t log_offset;
        int64_t last_committed_decree;
    };

    inline void marshall(::dsn::binary_writer& writer, const mutation_header& val, uint16_t pos = 0xffff)
    {
        marshall(writer, val.gpid, pos);
        marshall(writer, val.ballot, pos);
        marshall(writer, val.decree, pos);
        marshall(writer, val.log_offset, pos);
        marshall(writer, val.last_committed_decree, pos);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param mutation_header& val)
    {
        unmarshall(reader, val.gpid);
        unmarshall(reader, val.ballot);
        unmarshall(reader, val.decree);
        unmarshall(reader, val.log_offset);
        unmarshall(reader, val.last_committed_decree);
    };

    // ---------- mutation_data -------------
    struct mutation_data
    {
        mutation_header header;
        std::vector< ::dsn::blob> updates;
    };

    inline void marshall(::dsn::binary_writer& writer, const mutation_data& val, uint16_t pos = 0xffff)
    {
        marshall(writer, val.header, pos);
        marshall(writer, val.updates, pos);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param mutation_data& val)
    {
        unmarshall(reader, val.header);
        unmarshall(reader, val.updates);
    };

    // ---------- partition_configuration -------------
    struct partition_configuration
    {
        std::string app_type;
        global_partition_id gpid;
        int64_t ballot;
        int32_t max_replica_count;
        ::dsn::end_point primary;
        std::vector< ::dsn::end_point> secondaries;
        std::vector< ::dsn::end_point> drop_outs;
        int64_t last_committed_decree;
    };

    inline void marshall(::dsn::binary_writer& writer, const partition_configuration& val, uint16_t pos = 0xffff)
    {
        marshall(writer, val.app_type, pos);
        marshall(writer, val.gpid, pos);
        marshall(writer, val.ballot, pos);
        marshall(writer, val.max_replica_count, pos);
        marshall(writer, val.primary, pos);
        marshall(writer, val.secondaries, pos);
        marshall(writer, val.drop_outs, pos);
        marshall(writer, val.last_committed_decree, pos);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param partition_configuration& val)
    {
        unmarshall(reader, val.app_type);
        unmarshall(reader, val.gpid);
        unmarshall(reader, val.ballot);
        unmarshall(reader, val.max_replica_count);
        unmarshall(reader, val.primary);
        unmarshall(reader, val.secondaries);
        unmarshall(reader, val.drop_outs);
        unmarshall(reader, val.last_committed_decree);
    };

    // ---------- replica_configuration -------------
    struct replica_configuration
    {
        global_partition_id gpid;
        int64_t ballot;
        ::dsn::end_point primary;
        partition_status status;
    };

    inline void marshall(::dsn::binary_writer& writer, const replica_configuration& val, uint16_t pos = 0xffff)
    {
        marshall(writer, val.gpid, pos);
        marshall(writer, val.ballot, pos);
        marshall(writer, val.primary, pos);
        marshall(writer, val.status, pos);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param replica_configuration& val)
    {
        unmarshall(reader, val.gpid);
        unmarshall(reader, val.ballot);
        unmarshall(reader, val.primary);
        unmarshall(reader, val.status);
    };

    // ---------- prepare_msg -------------
    struct prepare_msg
    {
        replica_configuration config;
        mutation_data mu;
    };

    inline void marshall(::dsn::binary_writer& writer, const prepare_msg& val, uint16_t pos = 0xffff)
    {
        marshall(writer, val.config, pos);
        marshall(writer, val.mu, pos);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param prepare_msg& val)
    {
        unmarshall(reader, val.config);
        unmarshall(reader, val.mu);
    };

    // ---------- read_request_header -------------
    struct read_request_header
    {
        global_partition_id gpid;
        int32_t code;
        read_semantic_t semantic;
        int64_t version_decree;
    };

    inline void marshall(::dsn::binary_writer& writer, const read_request_header& val, uint16_t pos = 0xffff)
    {
        marshall(writer, val.gpid, pos);
        marshall(writer, val.code, pos);
        marshall(writer, val.semantic, pos);
        marshall(writer, val.version_decree, pos);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param read_request_header& val)
    {
        unmarshall(reader, val.gpid);
        unmarshall(reader, val.code);
        unmarshall(reader, val.semantic);
        unmarshall(reader, val.version_decree);
    };

    // ---------- write_request_header -------------
    struct write_request_header
    {
        global_partition_id gpid;
        int32_t code;
    };

    inline void marshall(::dsn::binary_writer& writer, const write_request_header& val, uint16_t pos = 0xffff)
    {
        marshall(writer, val.gpid, pos);
        marshall(writer, val.code, pos);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param write_request_header& val)
    {
        unmarshall(reader, val.gpid);
        unmarshall(reader, val.code);
    };

    // ---------- rw_response_header -------------
    struct rw_response_header
    {
        int32_t err;
    };

    inline void marshall(::dsn::binary_writer& writer, const rw_response_header& val, uint16_t pos = 0xffff)
    {
        marshall(writer, val.err, pos);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param rw_response_header& val)
    {
        unmarshall(reader, val.err);
    };

    // ---------- prepare_ack -------------
    struct prepare_ack
    {
        global_partition_id gpid;
        int32_t err;
        int64_t ballot;
        int64_t decree;
        int64_t last_committed_decree_in_app;
        int64_t last_committed_decree_in_prepare_list;
    };

    inline void marshall(::dsn::binary_writer& writer, const prepare_ack& val, uint16_t pos = 0xffff)
    {
        marshall(writer, val.gpid, pos);
        marshall(writer, val.err, pos);
        marshall(writer, val.ballot, pos);
        marshall(writer, val.decree, pos);
        marshall(writer, val.last_committed_decree_in_app, pos);
        marshall(writer, val.last_committed_decree_in_prepare_list, pos);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param prepare_ack& val)
    {
        unmarshall(reader, val.gpid);
        unmarshall(reader, val.err);
        unmarshall(reader, val.ballot);
        unmarshall(reader, val.decree);
        unmarshall(reader, val.last_committed_decree_in_app);
        unmarshall(reader, val.last_committed_decree_in_prepare_list);
    };

    // ---------- learn_state -------------
    struct learn_state
    {
        std::vector< ::dsn::blob> meta;
        std::vector< std::string> files;
    };

    inline void marshall(::dsn::binary_writer& writer, const learn_state& val, uint16_t pos = 0xffff)
    {
        marshall(writer, val.meta, pos);
        marshall(writer, val.files, pos);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param learn_state& val)
    {
        unmarshall(reader, val.meta);
        unmarshall(reader, val.files);
    };

    // ---------- learn_request -------------
    struct learn_request
    {
        global_partition_id gpid;
        ::dsn::end_point learner;
        int64_t signature;
        int64_t last_committed_decree_in_app;
        int64_t last_committed_decree_in_prepare_list;
        ::dsn::blob app_specific_learn_request;
    };

    inline void marshall(::dsn::binary_writer& writer, const learn_request& val, uint16_t pos = 0xffff)
    {
        marshall(writer, val.gpid, pos);
        marshall(writer, val.learner, pos);
        marshall(writer, val.signature, pos);
        marshall(writer, val.last_committed_decree_in_app, pos);
        marshall(writer, val.last_committed_decree_in_prepare_list, pos);
        marshall(writer, val.app_specific_learn_request, pos);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param learn_request& val)
    {
        unmarshall(reader, val.gpid);
        unmarshall(reader, val.learner);
        unmarshall(reader, val.signature);
        unmarshall(reader, val.last_committed_decree_in_app);
        unmarshall(reader, val.last_committed_decree_in_prepare_list);
        unmarshall(reader, val.app_specific_learn_request);
    };

    // ---------- learn_response -------------
    struct learn_response
    {
        int32_t err;
        replica_configuration config;
        int64_t commit_decree;
        int64_t prepare_start_decree;
        learn_state state;
        std::string base_local_dir;
    };

    inline void marshall(::dsn::binary_writer& writer, const learn_response& val, uint16_t pos = 0xffff)
    {
        marshall(writer, val.err, pos);
        marshall(writer, val.config, pos);
        marshall(writer, val.commit_decree, pos);
        marshall(writer, val.prepare_start_decree, pos);
        marshall(writer, val.state, pos);
        marshall(writer, val.base_local_dir, pos);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param learn_response& val)
    {
        unmarshall(reader, val.err);
        unmarshall(reader, val.config);
        unmarshall(reader, val.commit_decree);
        unmarshall(reader, val.prepare_start_decree);
        unmarshall(reader, val.state);
        unmarshall(reader, val.base_local_dir);
    };

    // ---------- group_check_request -------------
    struct group_check_request
    {
        std::string app_type;
        ::dsn::end_point node;
        replica_configuration config;
        int64_t last_committed_decree;
        int64_t learner_signature;
    };

    inline void marshall(::dsn::binary_writer& writer, const group_check_request& val, uint16_t pos = 0xffff)
    {
        marshall(writer, val.app_type, pos);
        marshall(writer, val.node, pos);
        marshall(writer, val.config, pos);
        marshall(writer, val.last_committed_decree, pos);
        marshall(writer, val.learner_signature, pos);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param group_check_request& val)
    {
        unmarshall(reader, val.app_type);
        unmarshall(reader, val.node);
        unmarshall(reader, val.config);
        unmarshall(reader, val.last_committed_decree);
        unmarshall(reader, val.learner_signature);
    };

    // ---------- group_check_response -------------
    struct group_check_response
    {
        global_partition_id gpid;
        int32_t err;
        int64_t last_committed_decree_in_app;
        int64_t last_committed_decree_in_prepare_list;
        learner_status learner_status_;
        int64_t learner_signature;
        ::dsn::end_point node;
    };

    inline void marshall(::dsn::binary_writer& writer, const group_check_response& val, uint16_t pos = 0xffff)
    {
        marshall(writer, val.gpid, pos);
        marshall(writer, val.err, pos);
        marshall(writer, val.last_committed_decree_in_app, pos);
        marshall(writer, val.last_committed_decree_in_prepare_list, pos);
        marshall(writer, val.learner_status_, pos);
        marshall(writer, val.learner_signature, pos);
        marshall(writer, val.node, pos);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param group_check_response& val)
    {
        unmarshall(reader, val.gpid);
        unmarshall(reader, val.err);
        unmarshall(reader, val.last_committed_decree_in_app);
        unmarshall(reader, val.last_committed_decree_in_prepare_list);
        unmarshall(reader, val.learner_status_);
        unmarshall(reader, val.learner_signature);
        unmarshall(reader, val.node);
    };

    // ---------- meta_request_header -------------
    struct meta_request_header
    {
        int32_t rpc_tag;
    };

    inline void marshall(::dsn::binary_writer& writer, const meta_request_header& val, uint16_t pos = 0xffff)
    {
        marshall(writer, val.rpc_tag, pos);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param meta_request_header& val)
    {
        unmarshall(reader, val.rpc_tag);
    };

    // ---------- meta_response_header -------------
    struct meta_response_header
    {
        int32_t err;
        ::dsn::end_point primary_address;
    };

    inline void marshall(::dsn::binary_writer& writer, const meta_response_header& val, uint16_t pos = 0xffff)
    {
        marshall(writer, val.err, pos);
        marshall(writer, val.primary_address, pos);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param meta_response_header& val)
    {
        unmarshall(reader, val.err);
        unmarshall(reader, val.primary_address);
    };

    // ---------- configuration_update_request -------------
    struct configuration_update_request
    {
        partition_configuration config;
        config_type type;
        ::dsn::end_point node;
    };

    inline void marshall(::dsn::binary_writer& writer, const configuration_update_request& val, uint16_t pos = 0xffff)
    {
        marshall(writer, val.config, pos);
        marshall(writer, val.type, pos);
        marshall(writer, val.node, pos);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param configuration_update_request& val)
    {
        unmarshall(reader, val.config);
        unmarshall(reader, val.type);
        unmarshall(reader, val.node);
    };

    // ---------- configuration_update_response -------------
    struct configuration_update_response
    {
        int32_t err;
        partition_configuration config;
    };

    inline void marshall(::dsn::binary_writer& writer, const configuration_update_response& val, uint16_t pos = 0xffff)
    {
        marshall(writer, val.err, pos);
        marshall(writer, val.config, pos);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param configuration_update_response& val)
    {
        unmarshall(reader, val.err);
        unmarshall(reader, val.config);
    };

    // ---------- configuration_proposal_request -------------
    struct configuration_proposal_request
    {
        partition_configuration config;
        config_type type;
        ::dsn::end_point node;
        bool is_clean_data;
        bool is_upgrade;
    };

    inline void marshall(::dsn::binary_writer& writer, const configuration_proposal_request& val, uint16_t pos = 0xffff)
    {
        marshall(writer, val.config, pos);
        marshall(writer, val.type, pos);
        marshall(writer, val.node, pos);
        marshall(writer, val.is_clean_data, pos);
        marshall(writer, val.is_upgrade, pos);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param configuration_proposal_request& val)
    {
        unmarshall(reader, val.config);
        unmarshall(reader, val.type);
        unmarshall(reader, val.node);
        unmarshall(reader, val.is_clean_data);
        unmarshall(reader, val.is_upgrade);
    };

    // ---------- configuration_query_by_node_request -------------
    struct configuration_query_by_node_request
    {
        ::dsn::end_point node;
    };

    inline void marshall(::dsn::binary_writer& writer, const configuration_query_by_node_request& val, uint16_t pos = 0xffff)
    {
        marshall(writer, val.node, pos);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param configuration_query_by_node_request& val)
    {
        unmarshall(reader, val.node);
    };

    // ---------- configuration_query_by_node_response -------------
    struct configuration_query_by_node_response
    {
        int32_t err;
        std::vector< partition_configuration> partitions;
    };

    inline void marshall(::dsn::binary_writer& writer, const configuration_query_by_node_response& val, uint16_t pos = 0xffff)
    {
        marshall(writer, val.err, pos);
        marshall(writer, val.partitions, pos);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param configuration_query_by_node_response& val)
    {
        unmarshall(reader, val.err);
        unmarshall(reader, val.partitions);
    };

    // ---------- configuration_query_by_index_request -------------
    struct configuration_query_by_index_request
    {
        std::string app_name;
        std::vector< int32_t> partition_indices;
    };

    inline void marshall(::dsn::binary_writer& writer, const configuration_query_by_index_request& val, uint16_t pos = 0xffff)
    {
        marshall(writer, val.app_name, pos);
        marshall(writer, val.partition_indices, pos);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param configuration_query_by_index_request& val)
    {
        unmarshall(reader, val.app_name);
        unmarshall(reader, val.partition_indices);
    };

    // ---------- configuration_query_by_index_response -------------
    struct configuration_query_by_index_response
    {
        int32_t err;
        int32_t app_id;
        int32_t partition_count;
        std::vector< partition_configuration> partitions;
    };

    inline void marshall(::dsn::binary_writer& writer, const configuration_query_by_index_response& val, uint16_t pos = 0xffff)
    {
        marshall(writer, val.err, pos);
        marshall(writer, val.app_id, pos);
        marshall(writer, val.partition_count, pos);
        marshall(writer, val.partitions, pos);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param configuration_query_by_index_response& val)
    {
        unmarshall(reader, val.err);
        unmarshall(reader, val.app_id);
        unmarshall(reader, val.partition_count);
        unmarshall(reader, val.partitions);
    };

    // ---------- query_replica_decree_request -------------
    struct query_replica_decree_request
    {
        global_partition_id gpid;
        ::dsn::end_point node;
    };

    inline void marshall(::dsn::binary_writer& writer, const query_replica_decree_request& val, uint16_t pos = 0xffff)
    {
        marshall(writer, val.gpid, pos);
        marshall(writer, val.node, pos);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param query_replica_decree_request& val)
    {
        unmarshall(reader, val.gpid);
        unmarshall(reader, val.node);
    };

    // ---------- query_replica_decree_response -------------
    struct query_replica_decree_response
    {
        int32_t err;
        int64_t last_decree;
    };

    inline void marshall(::dsn::binary_writer& writer, const query_replica_decree_response& val, uint16_t pos = 0xffff)
    {
        marshall(writer, val.err, pos);
        marshall(writer, val.last_decree, pos);
    };

    inline void unmarshall(::dsn::binary_reader& reader, __out_param query_replica_decree_response& val)
    {
        unmarshall(reader, val.err);
        unmarshall(reader, val.last_decree);
    };

} } 

#endif 
