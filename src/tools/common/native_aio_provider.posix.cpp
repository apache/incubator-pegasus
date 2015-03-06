
#include "native_aio_provider.posix.h"

#if defined(__MACH__) || defined(__linux__)

#include <aio.h>
#include <fcntl.h>
#include <cstdlib>

#define __TITLE__ "aio.provider.posix"

namespace rdsn {
    namespace tools {

        native_posix_aio_provider::native_posix_aio_provider(disk_engine* disk, aio_provider* inner_provider)
            : aio_provider(disk, inner_provider)
        {
        }

        native_posix_aio_provider::~native_posix_aio_provider()
        {
        }

        handle_t native_posix_aio_provider::open(const char* file_name, int flag, int pmode)
        {
            return (handle_t)::open(file_name, flag, pmode);
        }

        error_code native_posix_aio_provider::close(handle_t hFile)
        {
			// TODO: handle failure
            ::close((int)hFile);
			return ERR_SUCCESS;
        }

        struct posix_disk_aio_context : public disk_aio
        {
            struct aiocb cb;
            aio_task*  tsk;
            utils::notify_event*  evt;
            error_code err;
            uint32_t bytes;
        };

        disk_aio_ptr native_posix_aio_provider::prepare_aio_context(aio_task* tsk)
        {
            auto r = new posix_disk_aio_context;
            bzero((char*)&r->cb, sizeof(r->cb));
            r->tsk = tsk;
            r->evt = nullptr;
            return disk_aio_ptr(r);
        }

        void native_posix_aio_provider::aio(aio_task_ptr& aio_tsk)
        {
            aio_internal(aio_tsk, true);
        }
		
		static void aio_completed(sigval sigval)
		{
		}

        error_code native_posix_aio_provider::aio_internal(aio_task_ptr& aio_tsk, bool async, __out uint32_t* pbytes /*= nullptr*/)
        {
            auto aio = (posix_disk_aio_context*)aio_tsk->aio().get();
            int r;

            aio->cb.aio_fildes = (int)(long)aio->file;
            aio->cb.aio_buf = aio->buffer;
            aio->cb.aio_nbytes = aio->buffer_size;
            aio->cb.aio_offset = aio->file_offset;

            // set up callback
            aio->cb.aio_sigevent.sigev_notify = SIGEV_THREAD;
            aio->cb.aio_sigevent.sigev_notify_function = aio_completed;
            aio->cb.aio_sigevent.sigev_notify_attributes = NULL;
            aio->cb.aio_sigevent.sigev_value.sival_ptr = aio;

            if (!async)
            {
                aio->evt = new utils::notify_event();
                aio->err = ERR_SUCCESS;
                aio->bytes = 0;
            }

            switch (aio->type)
            {
            case AIO_Read:
                r = aio_read(&aio->cb);
                break;
            case AIO_Write:
                r = aio_write(&aio->cb);
                break;
            default:
                rdsn_assert(false, "unknown aio type %u", (int)aio->type);
                break;
            }

            if (r < 0)
            {
                rdsn_error("file op faile, err = %d", r);
            }

            if (async)
            {
                return 0;
            }
            else
            {
                aio->evt->wait();
                delete aio->evt;
                aio->evt = nullptr;
                *pbytes = aio->bytes;
                return aio->err;
            }
        }

    }
} // end namespace rdsn::tools
#endif
