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

# ifdef __linux__

# include "native_aio_provider.linux.h"

# include <fcntl.h>
# include <cstdlib>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "aio.provider.native"

namespace dsn {
    namespace tools {

        native_linux_aio_provider::native_linux_aio_provider(disk_engine* disk, aio_provider* inner_provider)
            : aio_provider(disk, inner_provider)
        {

            memset(&_ctx, 0, sizeof(_ctx));
            auto ret = io_setup(128, &_ctx); // 128 concurrent events
            dassert(ret == 0, "io_setup error, ret = %d", ret);

            new std::thread(std::bind(&native_linux_aio_provider::get_event, this));
        }

        native_linux_aio_provider::~native_linux_aio_provider()
        {
            auto ret = io_destroy(_ctx);
            dassert(ret == 0, "io_destroy error, ret = %d", ret);
        }

        handle_t native_linux_aio_provider::open(const char* file_name, int flag, int pmode)
        {
            return (handle_t)::open(file_name, flag, pmode);
        }

        error_code native_linux_aio_provider::close(handle_t hFile)
        {
            // TODO: handle failure
            ::close(static_cast<int>(hFile));
            return ERR_OK;
        }

        disk_aio_ptr native_linux_aio_provider::prepare_aio_context(aio_task* tsk)
        {
            auto r = new linux_disk_aio_context;
            bzero((char*)&r->cb, sizeof(r->cb));
            r->tsk = tsk;
            r->evt = nullptr;
            return disk_aio_ptr(r);
        }

        void native_linux_aio_provider::aio(aio_task_ptr& aio_tsk)
        {
            aio_internal(aio_tsk, true);
        }

        void native_linux_aio_provider::get_event()
        {
            struct io_event events[1];
            int ret;
            linux_disk_aio_context * aio;

            while (true)
            {
                ret = io_getevents(_ctx, 1, 1, events, NULL);
                if (ret > 0) // should be 1
                {
                    dassert(ret == 1, "");
                    struct iocb *io = events[0].obj;
                    complete_aio(io, static_cast<int>(events[0].res), static_cast<int>(events[0].res2));
                }
            }
        }

        void native_linux_aio_provider::complete_aio(struct iocb* io, int bytes, int err)
        {
            linux_disk_aio_context* aio = CONTAINING_RECORD(io, linux_disk_aio_context, cb);
            if (err != 0)
            {
                derror("aio error, err = %d", err);
            }

            if (!aio->evt)
            {
                aio_task_ptr aio_ptr(aio->tsk);
                aio->this_->complete_io(aio_ptr, (err == 0) ? ERR_OK : ERR_FILE_OPERATION_FAILED, bytes);
            }
            else
            {
                aio->err = (err == 0) ? ERR_OK : ERR_FILE_OPERATION_FAILED;
                aio->bytes = bytes;
                aio->evt->notify();
            }
        }

        error_code native_linux_aio_provider::aio_internal(aio_task_ptr& aio_tsk, bool async, __out_param uint32_t* pbytes /*= nullptr*/)
        {
            struct iocb *cbs[1];
            linux_disk_aio_context * aio;
            int ret;

            aio = (linux_disk_aio_context *)aio_tsk->aio().get();

            memset(&aio->cb, 0, sizeof(aio->cb));

            aio->this_ = this;

            switch (aio->type)
            {
            case AIO_Read:
                io_prep_pread(&aio->cb, static_cast<int>((ssize_t)aio->file), aio->buffer, aio->buffer_size, aio->file_offset);
                break;
            case AIO_Write:
                io_prep_pwrite(&aio->cb, static_cast<int>((ssize_t)aio->file), aio->buffer, aio->buffer_size, aio->file_offset);
                break;
            default:
                derror("unknown aio type %u", static_cast<int>(aio->type));
            }

            if (!async)
            {
                aio->evt = new utils::notify_event();
                aio->err = ERR_OK;
                aio->bytes = 0;
            }

            cbs[0] = &aio->cb;
            ret = io_submit(_ctx, 1, cbs);

            if (ret != 1)
            {
                if (ret < 0)
                    derror("io_submit error, ret = %d", ret);
                else
                    derror("could not sumbit IOs, ret = %d", ret);

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
