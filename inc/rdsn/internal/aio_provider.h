# pragma once

# include <rdsn/internal/task.h>

namespace rdsn {

class disk_engine;
class aio_provider
{
public:
    template <typename T> static aio_provider* create(disk_engine* disk, aio_provider* inner_provider)
    {
        return new T(disk, inner_provider);
    }

public:
    aio_provider(disk_engine* disk, aio_provider* inner_provider);

    virtual handle_t  open(const char* file_name, int flag, int pmode) = 0;
    virtual error_code close(handle_t hFile) = 0;
    virtual void    aio(aio_task_ptr& aio) = 0;
    virtual disk_aio_ptr prepare_aio_context(aio_task*) = 0;

protected:
    void complete_io(aio_task_ptr& aio, error_code err, uint32_t bytes, int delay_milliseconds = 0);

private:
    disk_engine *_engine;
};


} // end namespace 


