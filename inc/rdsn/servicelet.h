#pragma once

# include <rdsn/internal/service_base.h>

namespace rdsn { namespace service {

template<typename T>
class servicelet : public service_base
{        
public:
    servicelet(const char* name);
    ~servicelet();
    
protected:
    task_ptr enqueue_task(
        task_code evt,
        task_handler callback,
        int hash = 0,
        int delay_milliseconds = 0,
        int timer_interval_milliseconds = 0
        )
    {
        return service_base::enqueue_task(evt, callback, hash, delay_milliseconds, timer_interval_milliseconds);
    }

    task_ptr enqueue_task(
        task_code evt,
        void (T::*handler)(),
        int hash = 0,
        int delay_milliseconds = 0,
        int timer_interval_milliseconds = 0
        )
    {
        return service_base::enqueue_task(evt, 
            std::bind(handler, static_cast<T*>(this)), 
            hash, delay_milliseconds, timer_interval_milliseconds);
    }

    void register_rpc_handler(task_code rpc_code, const char* name_, void (T::*handler)(message_ptr&))
    {
        service_base::register_rpc_handler(rpc_code, name_, 
            std::bind(handler, static_cast<T*>(this), std::placeholders::_1));
    }

    rpc_response_task_ptr rpc_call(
        const end_point& server_addr,
        message_ptr& request,
        void (T::*callback)(error_code, message_ptr&, message_ptr&),
        int reply_hash = 0
        )
    {
        return service_base::rpc_call(server_addr, request,
            this,
            std::bind(callback, static_cast<T*>(this), std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
            reply_hash
            );
    }

    rpc_response_task_ptr rpc_call(
        const end_point& server_addr,
        message_ptr& request,
        rpc_reply_handler callback,
        int reply_hash = 0
        )
    {
        return service_base::rpc_call(server_addr, request, this, callback, reply_hash);
    }

    aio_task_ptr file_read(
        handle_t hFile,
        char* buffer,
        int count,
        uint64_t offset,
        task_code callback_code,
        void(T::*callback)(error_code, uint32_t),
        int hash = 0
        )
    {
        return service_base::file_read(hFile, buffer, count, offset, callback_code,
            std::bind(callback, static_cast<T*>(this), std::placeholders::_1, std::placeholders::_2),
            hash
            );
    }

    aio_task_ptr file_write(
        handle_t hFile,
        const char* buffer,
        int count,
        uint64_t offset,
        task_code callback_code,
        void(T::*callback)(error_code, uint32_t),
        int hash = 0
        )
    {
        return service_base::file_write(hFile, buffer, count, offset, callback_code,
            std::bind(callback, static_cast<T*>(this), std::placeholders::_1, std::placeholders::_2),
            hash
            );
    }
};

// ------------- inline implementation ----------------
template<typename T>
servicelet<T>::servicelet(const char* name)
    : service_base(name)
{
}

template<typename T>
servicelet<T>::~servicelet()
{
}

}} // end namespace
