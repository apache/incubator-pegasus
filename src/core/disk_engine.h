# pragma once

# include "service_engine.h"
# include <rdsn/internal/synchronize.h>
# include <rdsn/internal/aio_provider.h>

namespace rdsn {

class disk_engine
{
public:
    disk_engine(service_node* node);
    ~disk_engine();

    void start(aio_provider* provider);

    // asynchonous file read/write
    handle_t        open(const char* fileName, int flag, int pmode);
    error_code      close(handle_t hFile);
    void            read(aio_task_ptr& aio);
    void            write(aio_task_ptr& aio);  

    disk_aio_ptr    prepare_aio_context(aio_task* tsk) { return _provider->prepare_aio_context(tsk); }
    
private:
    friend class aio_provider;
    void start_io(aio_task_ptr& aio);
    void complete_io(aio_task_ptr& aio, error_code err, uint32_t bytes, int delay_milliseconds = 0);

private:
    bool           _is_running;
    aio_provider    *_provider;
    service_node    *_node;

    std::recursive_mutex   _lock;    
    int            _request_count;
};

} // end namespace
