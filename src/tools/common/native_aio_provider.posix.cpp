/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 * 
 * -=- Robust Distributed System Nucleus (rDSN) -=- 
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

# ifndef _WIN32

# include "native_aio_provider.posix.h"

# include <aio.h>
# include <fcntl.h>
# include <cstdlib>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "aio.provider.posix"

namespace dsn {
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
            ::close(static_cast<int>(hFile));
            return ERR_OK;
        }

        struct posix_disk_aio_context : public disk_aio
        {
            struct aiocb cb;
            aio_task*  tsk;
            native_posix_aio_provider* this_;
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
        
        void aio_completed(sigval sigval)
        {
            auto ctx = (posix_disk_aio_context *)sigval.sival_ptr;
            
            int err = aio_error(&ctx->cb);
            if (err != EINPROGRESS)
            {
                if (err != 0)
                {
                    derror("file operation failed, errno = %d", errno);
                }

                size_t bytes = aio_return(&ctx->cb); // from e.g., read or write
                if (!ctx->evt)
                {
                    aio_task_ptr aio(ctx->tsk);
                    ctx->this_->complete_io(aio, err == 0 ? ERR_OK : ERR_FILE_OPERATION_FAILED, bytes);
                }
                else
                {
                    ctx->err = err == 0 ? ERR_OK : ERR_FILE_OPERATION_FAILED;
                    ctx->bytes = bytes;
                    ctx->evt->notify();
                }
            }
        }

        error_code native_posix_aio_provider::aio_internal(aio_task_ptr& aio_tsk, bool async, __out_param uint32_t* pbytes /*= nullptr*/)
        {
            auto aio = (posix_disk_aio_context *)aio_tsk->aio().get();
            int r;

            aio->this_ = this;
            aio->cb.aio_fildes = static_cast<int>((ssize_t)aio->file);
            aio->cb.aio_buf = aio->buffer;
            aio->cb.aio_nbytes = aio->buffer_size;
            aio->cb.aio_offset = aio->file_offset;

            // set up callback
            aio->cb.aio_sigevent.sigev_notify = SIGEV_THREAD;
            aio->cb.aio_sigevent.sigev_notify_function = aio_completed;
            aio->cb.aio_sigevent.sigev_notify_attributes = nullptr;
            aio->cb.aio_sigevent.sigev_value.sival_ptr = aio;

            if (!async)
            {
                aio->evt = new utils::notify_event();
                aio->err = ERR_OK;
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
                dassert (false, "unknown aio type %u", static_cast<int>(aio->type));
                break;
            }

            if (r != 0)
            {
                derror("file op failed, err = %d. On FreeBSD, you may need to load aio kernel module by running 'sudo kldload aio'.", errno);

                if (async)
                {
                    complete_io(aio_tsk, ERR_FILE_OPERATION_FAILED, 0);
                }
                else
                {
                    delete aio->evt;
                    aio->evt = nullptr;
                }
                return ERR_FILE_OPERATION_FAILED;
            }
            else 
            {
                if (async)
                {
                    return ERR_IO_PENDING;
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

    }
} // end namespace dsn::tools

#endif
