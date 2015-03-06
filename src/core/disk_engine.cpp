# include "disk_engine.h"
# include <rdsn/internal/perf_counters.h>
# include <rdsn/internal/logging.h>
# include <rdsn/internal/aio_provider.h>
# include <rdsn/internal/utils.h>
# include <rdsn/service_api.h>

#define __TITLE__ "disk_engine"

using namespace rdsn::utils;

namespace rdsn {

//----------------- disk_engine ------------------------
disk_engine::disk_engine(service_node* node)
{
    _request_count = 0;
    _is_running = false;    
    _provider = nullptr;
    _node = node;        
}

disk_engine::~disk_engine()
{
}

void disk_engine::start(aio_provider* provider)
{
    auto_lock l(_lock);
    if (_is_running)
        return;  

    _provider = provider;
    _is_running = true;
}

handle_t disk_engine::open(const char* file_name, int flag, int pmode)
{            
    return _provider->open(file_name, flag, pmode);
}

error_code disk_engine::close(handle_t hFile)
{
    return _provider->close(hFile);
}

void disk_engine::read(aio_task_ptr& aio)
{
    aio->aio()->type = AIO_Read;    
    return start_io(aio);
}

void disk_engine::write(aio_task_ptr& aio)
{
    aio->aio()->type = AIO_Write;
    return start_io(aio);
}

void disk_engine::start_io(aio_task_ptr& aio_tsk)
{
    auto aio = aio_tsk->aio();
    aio->engine = this;
    
    {
        auto_lock l(_lock);
        if (!_is_running)
        {
            aio_tsk->enqueue(ERR_SERVICE_NOT_FOUND, 0, 0, _node);
            return;
        }
       
        _request_count++;
    }

    aio_tsk->add_ref();

    // TODO: profiling, throttling here 

    if (aio_tsk->spec().on_aio_call.execute(task::get_current_task(), aio_tsk.get(), true))
    {
        return _provider->aio(aio_tsk); 
    }
    else
    {
        aio_tsk->enqueue(ERR_FILE_OPERATION_FAILED, 0, 0, _node);
    }
}

void disk_engine::complete_io(aio_task_ptr& aio, error_code err, uint32_t bytes, int delay_milliseconds)
{
    // TODO: failure injection, profiling, throttling

    if (err != ERR_SUCCESS)
    {
        rwarn(
                    "disk operation failure with code %s, err = 0x%x, aio task id = %llx",
                    aio->spec().name,
                    err.get(),
                    aio->id()
                    );
    }
    
    {
        auto_lock l(_lock);
        _request_count--;
    }
    
    aio->enqueue(err, bytes, delay_milliseconds, _node);
    aio->release_ref();
}


} // end namespace
