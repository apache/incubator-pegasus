# pragma once

# include <rdsn/internal/task.h>
# include <rdsn/internal/service_app.h>
# include <rdsn/internal/zlocks.h>

namespace rdsn { namespace service {

namespace tasking
{
    inline void enqueue(task_ptr& task, int delay_milliseconds = 0, service_app* app = nullptr) 
    { 
        task->enqueue(delay_milliseconds, app); 
    }

    inline bool cancel(task_ptr& task, bool wait_until_finished)
    {
        return task->cancel(wait_until_finished);
    }

    inline bool wait(task_ptr& task, int timeout_milliseconds = INFINITE)
    {
        return task->wait(timeout_milliseconds);
    }
}

namespace rpc
{
    extern const end_point& get_local_address();

    extern bool register_rpc_handler(task_code code, const char* name, rpc_server_handler* handler);

    extern bool unregister_rpc_handler(task_code code);

    extern rpc_response_task_ptr call(const end_point& server, message_ptr& request, rpc_response_task_ptr callback = nullptr);
    
    extern void reply(message_ptr& response);
}

namespace file
{
    extern handle_t open(const char* file_name, int flag, int pmode);

    extern void read(handle_t hFile, char* buffer, int count, uint64_t offset, aio_task_ptr& callback);

    extern void write(handle_t hFile, const char* buffer, int count, uint64_t offset, aio_task_ptr& callback); 

    extern error_code close(handle_t hFile);
}

namespace env
{
    // since Epoch (1970-01-01 00:00:00 +0000 (UTC))
    extern uint64_t now_ns();

    // generate random number [min, max]
    extern uint64_t random64(uint64_t min, uint64_t max);

    inline uint64_t now_us() { return now_ns() / 1000; }
    inline uint64_t now_ms() { return now_ns() / 1000000; }
    inline uint32_t random32(uint32_t min, uint32_t max) { return static_cast<uint32_t>(random64(min, max)); }
    inline double   probability() { return (double)random32(0, 1000000000) / 1000000000.0; }
}

namespace system
{
	extern bool run(const char* config);

    namespace internal_use_only
    {
        extern bool register_service(const char* name, service_app_factory factory);
    }

    template<typename T> bool register_service(const char* name)
    {
        return internal_use_only::register_service(name, service_app::create<T>);
    }    
}
}} // end namespace rdsn::service
