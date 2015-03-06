# include <rdsn/internal/rpc_message.h>
# include <rdsn/internal/logging.h>
# include "task_engine.h"
# include <rdsn/service_api.h>
# include "crc.h"

using namespace rdsn::utils;

#define __TITLE__ "message"

namespace rdsn {

void marshall(binary_writer& writer, const end_point& val, uint16_t pos /*= 0xffff*/)
{
    writer.write(val.ip, pos);
    writer.write(val.port, pos);
    writer.write(val.name, pos);
}

void unmarshall(binary_reader& reader, __out end_point& val)
{
    reader.read(val.ip);
    reader.read(val.port);
    reader.read(val.name);
}

void message_header::marshall(binary_writer& writer)
{
    writer.write((const char*)this, serialized_size());
}

void message_header::unmarshall(binary_reader& reader)
{
    reader.read((char*)this, serialized_size());
}

void message_header::new_rpc_id()
{
    rpc_id = get_random64();
}

/*static*/ bool message_header::is_right_header(char* hdr)
{
    int32_t crc32 = *(int32_t*)hdr;
    if (crc32)
    {
        //rdsn_assert  (*(int32_t*)data == hdr_crc32, "HeaderCrc must be put at the beginning of the buffer");
        *(int32_t*)hdr = 0;
        bool r = ((uint32_t)crc32 == crc32::compute(hdr, message_header::serialized_size(), 0));
        *(int32_t*)hdr = crc32;
        return r;
    }

    // crc is not enabled
    else
    {
        return true;
    }
}

/*static*/ int message_header::get_body_length(char* hdr)
{
    return ((message_header*)hdr)->body_length;
}

std::atomic<uint64_t> message::_id(0);

message::message()
{
    _elapsed_timeout_milliseconds = 0;            

    _reader = nullptr;
    _writer = new binary_writer();

    memset(&_msg_header, 0, FIELD_OFFSET(message_header, from_address));
    seal(false, true);
}
        
message::message(utils::blob bb, bool parse_hdr)
{
    _elapsed_timeout_milliseconds = 0;

    _reader = new binary_reader(bb);
    _writer = nullptr;

    if (parse_hdr)
    {
        read_header();
    }
    else
    {
        memset(&_msg_header, 0, message_header::serialized_size());
    }
}
                
message::~message()
{
    if (_reader != nullptr)
    {
        delete _reader;
        _reader = nullptr;
    }

    if (_writer != nullptr)
    {
        delete _writer;
        _writer = nullptr;
    }
}
                
message_ptr message::create_request(task_code rpc_code, int timeout_milliseconds, int hash)
{
    message_ptr msg(new message());
    msg->header().local_rpc_code = (uint16_t)rpc_code;
    msg->header().hash = hash;
    if (timeout_milliseconds == 0)
    {
        msg->header().timeout_milliseconds = task_spec::get(rpc_code)->rpc_default_timeout_milliseconds;
    }
    else
    {
        msg->header().timeout_milliseconds = timeout_milliseconds;
    }    

    const char* rpcName = rpc_code.to_string();
    strcpy(msg->header().rpc_name, rpcName);

    msg->header().is_request = true;            
    msg->header().id = message::new_id();
    return msg;
}
        
message_ptr message::create_response()
{
    message_ptr msg(new message());

    msg->header().id = _msg_header.id;
    msg->header().rpc_id = _msg_header.rpc_id;
        
    msg->header().error = ERR_SUCCESS;    
    strcpy(msg->header().rpc_name, _msg_header.rpc_name);

    msg->header().hash = _msg_header.hash;
    msg->header().timeout_milliseconds = _msg_header.timeout_milliseconds;

    msg->header().is_request = false;
    msg->header().is_response_expected = true;
     
    msg->header().local_rpc_code = _msg_header.local_rpc_code;
    msg->header().from_address = _msg_header.to_address;
    msg->header().to_address = _msg_header.from_address;
	msg->header().client_port = _msg_header.from_address.port;

    msg->_server_session = _server_session;

    return msg;
}

void message::seal(bool fillCrc, bool is_placeholder /*= false*/)
{
    rdsn_assert  (!is_read(), "seal can only be applied to write mode messages");
    if (is_placeholder)
    {
        std::string dummy;
        dummy.resize(_msg_header.serialized_size(), '\0');
        _writer->write((const char*)&dummy[0], _msg_header.serialized_size());
    }
    else
    {
        header().body_length = total_size() - message_header::serialized_size();

        if (fillCrc)
        {
            // compute data crc if necessary
            if (header().body_crc32 == 0)
            {
                std::vector<utils::blob> buffers;
                _writer->get_buffers(buffers);

                buffers[0] = buffers[0].range(0, buffers[0].length() - message_header::serialized_size());

                uint32_t crc32 = 0;
                uint32_t len = 0;
                for (auto it = buffers.begin(); it != buffers.end(); it++)
                {
                    uint32_t lcrc = crc32::compute(it->data(), it->length(), crc32);

                    /*uintxx_t uInitialCrcAB,
                    uintxx_t uInitialCrcA,
                    uintxx_t uFinalCrcA,
                    uint64_t uSizeA,
                    uintxx_t uInitialCrcB,
                    uintxx_t uFinalCrcB,
                    uint64_t uSizeB*/
                    crc32 = crc32::concatenate(
                        0, 
                        0, crc32, len, 
                        crc32, lcrc, it->length()
                        );

                    len += it->length();
                }

                rdsn_assert  (len == (uint32_t)header().body_length, "data length is wrong");
                header().body_crc32 = crc32;
            }

            utils::blob bb = _writer->get_first_buffer();
            rdsn_assert  (bb.length() >= _msg_header.serialized_size(), "the reserved blob size for message must be greater than the header size to ensure header is contiguous");
            header().hdr_crc32 = 0;
            binary_writer writer(bb);
            _msg_header.marshall(writer);

            header().hdr_crc32 = crc32::compute(bb.data(), message_header::serialized_size(), 0);
            *(uint32_t*)bb.data() = header().hdr_crc32;
        }

        // crc is not enabled
        else
        {
            utils::blob bb = _writer->get_first_buffer();
            rdsn_assert  (bb.length() >= _msg_header.serialized_size(), "the reserved blob size for message must be greater than the header size to ensure header is contiguous");
            binary_writer writer(bb);
            _msg_header.marshall(writer);
        }
    }
}

bool message::is_right_header() const
{
    rdsn_assert  (is_read(), "message must be of read mode");
    if (_msg_header.hdr_crc32)
    {
        utils::blob bb = _reader->get_buffer();
        return _msg_header.is_right_header((char*)bb.data());
    }

    // crc is not enabled
    else
    {
        return true;
    }
}

bool message::is_right_body() const
{
    rdsn_assert  (is_read(), "message must be of read mode");
    if (_msg_header.body_crc32)
    {
        utils::blob bb = _reader->get_buffer();
        return (uint32_t)_msg_header.body_crc32 == crc32::compute((char*)bb.data() + message_header::serialized_size(), _msg_header.body_length, 0);
    }

    // crc is not enabled
    else
    {
        return true;
    }
}

void message::read_header()
{
    rdsn_assert  (is_read(), "message must be of read mode");
    _msg_header.unmarshall(*_reader);
}

} // end namespace rdsn
