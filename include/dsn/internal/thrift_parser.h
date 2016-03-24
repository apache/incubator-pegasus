#ifndef THRIFT_RPC_H
#define THRIFT_RPC_H

# include <dsn/internal/message_parser.h>

namespace dsn
{
typedef union
{
    struct {
        uint64_t is_forward_msg_disabled: 1;
        uint64_t is_replication_needed: 1;
        uint64_t unused: 63;
    }u;
    uint64_t o;
}dsn_thrift_header_options;

struct dsn_thrift_header
{
    int32_t hdr_type;
    int32_t hdr_crc32;
    int32_t total_length;
    int32_t body_offset;
    int32_t request_hash;
    dsn_thrift_header_options opt;
};

class thrift_header_parser
{
private:
    static void adjust_thrift_response(message_ex* msg);
    static void add_postfix_for_thrift_response(message_ex* msg);

public:
    static void read_thrift_header_from_buffer(/*out*/dsn_thrift_header& result, const char* buffer);
    static dsn::message_ex* parse_dsn_message(dsn_thrift_header* header, dsn::blob& message_data);
    static void add_prefix_for_thrift_response(message_ex* msg);
    static int prepare_buffers_on_send(message_ex* msg, int offest, /*out*/message_parser::send_buf* buffers);
    static int get_send_buffers_count_and_total_length(message_ex* msg, /*out*/int* total_length);
};

}
#endif // THRIFT_RPC_H
