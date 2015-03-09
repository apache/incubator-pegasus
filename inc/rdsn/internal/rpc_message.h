# pragma once

# include <atomic>
# include <rdsn/internal/rdsn_types.h>
# include <rdsn/internal/end_point.h>
# include <rdsn/internal/extensible_object.h>
# include <rdsn/internal/task_code.h>
# include <rdsn/internal/error_code.h>

namespace rdsn {

struct message_header
{    
    int32_t       hdr_crc32;
    int32_t       body_crc32;
    int32_t       body_length;
    int32_t       version;
    uint64_t      id;
    uint64_t      rpc_id;
    char          rpc_name[MAX_TASK_CODE_NAME_LENGTH + 1];

    // info from client => server
    union
    {
        struct 
        {
            int32_t  hash;
            int32_t  timeout_milliseconds;
            uint16_t port;
        } client;

        struct 
        {
            int32_t  error;
        } server;
    };

    // local fields - no need to be transmitted
    end_point     from_address;
    end_point     to_address;
    uint16_t      local_rpc_code;
    
    static int serialized_size()
    {
        return (int)(FIELD_OFFSET(message_header, from_address));
    }

    void marshall(utils::binary_writer& writer);
    void unmarshall(utils::binary_reader& reader);
    void new_rpc_id();
    
    static bool is_right_header(char* hdr);
    static int get_body_length(char* hdr);
};

class rpc_server_session;
class message : public ref_object, public extensible_object<message, 4>
{
public:
    message(); // write             
    message(utils::blob bb, bool parse_hdr = true); // read 
    virtual ~message();

    //
    // routines for request and response
    //
    static message_ptr create_request(task_code rpc_code, int timeout_milliseconds = 0, int hash = 0);
    message_ptr create_response();

    //
    // routines for reader & writer
    //
    utils::binary_reader& reader() { return *_reader; }
    utils::binary_writer& writer() { return *_writer; }

    //
    // quick read
    //
    template<typename T> void read(__out T& val) { _reader->read(val); }
    void read(__out std::string& s) { _reader->read(s); }
    void read(char* buffer, int sz) { _reader->read(buffer, sz); }
    void read(utils::blob& blob) { _reader->read(blob); }
    utils::blob get_input_buffer() const { return _reader->get_buffer(); }
    utils::blob get_input_remaining_buffer() const { return _reader->get_remaining_buffer(); }
    bool is_eof() const { return _reader->is_eof(); }
    int get_remaining_size() const { return _reader->get_remaining_size(); }
    
    //
    // quick write
    //
    uint16_t write_placeholder() { return _writer->write_placeholder(); }
    template<typename T> void write(const T& val, uint16_t pos = 0xffff) { return _writer->write(val, pos); }
    void write(const std::string& val, uint16_t pos = 0xffff) { return _writer->write(val, pos); }
    void write(const char* buffer, int sz, uint16_t pos = 0xffff) { return _writer->write(buffer, sz, pos); }
    void write(const utils::blob& val, uint16_t pos = 0xffff) { return _writer->write(val, pos); }
    void get_output_buffers(__out std::vector<utils::blob>& buffers) const { return _writer->get_buffers(buffers); }
    int  get_output_buffer_count() const { return _writer->get_buffer_count(); }
    utils::blob get_output_buffer() const { return _writer->get_buffer();}
    
    //
    // other routines
    //
    void seal(bool fillCrc, bool is_placeholder = false);
    message_header& header() { return _msg_header; }
    int  total_size() const { return is_read() ? _reader->total_size() : _writer->total_size(); }
    bool is_read() const { return _reader != nullptr; }
    error_code error() const { error_code ec; ec.set(_msg_header.server.error); return ec; }
    int elapsed_timeout_milliseconds() const { return _elapsed_timeout_milliseconds; }
    void add_elapsed_timeout_milliseconds(int timeout_milliseconds) { _elapsed_timeout_milliseconds += timeout_milliseconds; }
    bool is_right_header() const;
    bool is_right_body() const;
    static uint64_t new_id() { return ++_id; }
    rpc_server_session_ptr& server_session() { return _server_session; }

private:            
    void read_header();

private:
    message_header       _msg_header;
    int                  _elapsed_timeout_milliseconds;
    rpc_server_session_ptr _server_session;

    utils::binary_reader *_reader;
    utils::binary_writer *_writer;

protected:
    static std::atomic<uint64_t> _id;
};

DEFINE_REF_OBJECT(message)

extern void marshall(utils::binary_writer& writer, const end_point& val, uint16_t pos = 0xffff);
extern void unmarshall(utils::binary_reader& reader, __out end_point& val);
} // end namespace
