#include <dsn/idl/thrift_rpc.h>
#include <dsn/idl/thrift_helper.h>

namespace dsn{

void thrift_header_parser::read_thrift_header_from_buffer(/*out*/dsn_thrift_header& result, const char* buffer)
{
    result.hdr_type = be32toh( *(int32_t*)(buffer) );
    buffer += sizeof(int32_t);
    result.hdr_crc32 = be32toh( *(int32_t*)(buffer) );
    buffer += sizeof(int32_t);
    result.total_length = be32toh( *(int32_t*)(buffer) );
    buffer += sizeof(int32_t);
    result.body_offset = be32toh( *(int32_t*)(buffer) );
    buffer += sizeof(int32_t);
    result.request_hash = be32toh( *(int32_t*)(buffer) );
    buffer += sizeof(int32_t);
    result.opt.o = be64toh( *(int64_t*)(buffer) );
}

dsn::message_ex* thrift_header_parser::parse_dsn_message(dsn_thrift_header* header, dsn::blob& message_data)
{
    dsn::blob message_content = message_data.range(header->body_offset);
    dsn::message_ex* msg = message_ex::create_receive_message_with_standalone_header(message_content);
    dsn::message_header* dsn_hdr = msg->header;

    dsn::rpc_read_stream stream(msg);
    boost::shared_ptr< ::dsn::binary_reader_transport > input_trans(new ::dsn::binary_reader_transport( stream ));
    ::apache::thrift::protocol::TBinaryProtocol iprot(input_trans);

    std::string fname;
    ::apache::thrift::protocol::TMessageType mtype;
    int32_t seqid;
    iprot.readMessageBegin(fname, mtype, seqid);

    dinfo("rpc name: %s, type: %d, seqid: %d", fname.c_str(), mtype, seqid);
    memset(dsn_hdr, 0, sizeof(*dsn_hdr));
    dsn_hdr->hdr_type = hdr_dsn_thrift;
    dsn_hdr->body_length = header->total_length - header->body_offset;
    strncpy(dsn_hdr->rpc_name, fname.c_str(), DSN_MAX_TASK_CODE_NAME_LENGTH);

    if (mtype == ::apache::thrift::protocol::T_CALL || mtype == ::apache::thrift::protocol::T_ONEWAY)
        dsn_hdr->context.u.is_request = 1;

    dsn_hdr->id = seqid;
    dsn_hdr->context.u.is_replication_needed = header->opt.u.is_replication_needed;
    dsn_hdr->context.u.is_forward_disabled = header->opt.u.is_forward_msg_disabled;
    dsn_hdr->client.hash = header->request_hash;

    iprot.readStructBegin(fname);
    return msg;
}

void thrift_header_parser::add_prefix_for_thrift_response(message_ex* msg)
{
    dsn::rpc_write_stream write_stream(msg);
    ::dsn::binary_writer_transport trans(write_stream);
    boost::shared_ptr< ::dsn::binary_writer_transport> trans_ptr(&trans, [](::dsn::binary_writer_transport*) {});
    ::apache::thrift::protocol::TBinaryProtocol msg_proto(trans_ptr);

    msg_proto.writeMessageBegin(msg->header->rpc_name, ::apache::thrift::protocol::T_REPLY, msg->header->id);
    msg_proto.writeStructBegin(""); //resp args
}

void thrift_header_parser::add_postfix_for_thrift_response(message_ex* msg)
{
    dsn::rpc_write_stream write_stream(msg);
    ::dsn::binary_writer_transport trans(write_stream);
    boost::shared_ptr< ::dsn::binary_writer_transport> trans_ptr(&trans, [](::dsn::binary_writer_transport*) {});
    ::apache::thrift::protocol::TBinaryProtocol msg_proto(trans_ptr);

    //after all fields, write a field stop
    msg_proto.writeFieldStop();
    //writestruct end, this is because all thirft returning values are in a struct
    msg_proto.writeStructEnd();
    //write message end, which indicate the end of a thrift message
    msg_proto.writeMessageEnd();
}

void thrift_header_parser::adjust_thrift_response(message_ex* msg)
{
    dassert(msg->is_response_adjusted_for_custom_rpc==false, "we have adjusted this");

    msg->is_response_adjusted_for_custom_rpc = true;
    add_postfix_for_thrift_response(msg);
}

int thrift_header_parser::prepare_buffers_on_send(message_ex* msg, int offset, /*out*/message_parser::send_buf* buffers)
{
    if ( !msg->is_response_adjusted_for_custom_rpc )
        adjust_thrift_response(msg);

    dassert(!msg->buffers.empty(), "buffers is not possible to be empty");

    // we ignore header for thrift resp
    offset += sizeof(message_header);

    int count=0;
    //ignore the msg header
    for (unsigned int i=0; i!=msg->buffers.size(); ++i)
    {
        blob& bb = msg->buffers[i];
        if (offset >= bb.length())
        {
            offset -= bb.length();
            continue;
        }

        buffers[count].buf = (void*)(bb.data() + offset);
        buffers[count].sz = (uint32_t)(bb.length() - offset);
        offset = 0;
        ++count;
    }
    return count;
}

int thrift_header_parser::get_send_buffers_count_and_total_length(message_ex* msg, /*out*/int* total_length)
{
    if ( !msg->is_response_adjusted_for_custom_rpc )
        adjust_thrift_response(msg);

    // when send thrift message, we ignore the header
    *total_length = msg->header->body_length;
    return msg->buffers.size();
}
}
