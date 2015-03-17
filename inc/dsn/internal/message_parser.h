# pragma once

# include <dsn/internal/dsn_types.h>
# include <dsn/internal/rpc_message.h>

namespace dsn 
{
    class message_parser
    {
    public:
        message_parser(int buffer_block_size);

        // before read
        void* read_buffer_ptr(int read_next);
        int read_buffer_capacity() const;

        // afer read, see if we can compose a message
        virtual message_ptr on_read(int read_length, __out_param int& read_next) = 0;

    protected:
        void create_new_buffer(int sz);
        void mark_read(int read_length);

    protected:        
        utils::blob            _read_buffer;
        int                    _read_buffer_occupied;
        int                 _buffer_block_size;
    };

    class dsn_message_parser : public message_parser
    {
    public:
        dsn_message_parser(int buffer_block_size);

        virtual message_ptr on_read(int read_length, __out_param int& read_next);

    private:

    };
}