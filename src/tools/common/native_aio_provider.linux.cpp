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

#include "native_aio_provider.linux.h"

#if defined(__MACH__) || defined(__linux__)

#include <fcntl.h>
#include <cstdlib>

#define __TITLE__ "aio.provider.native"

namespace dsn {
    namespace tools {

        native_linux_aio_provider::native_linux_aio_provider(disk_engine* disk, aio_provider* inner_provider)
            : aio_provider(disk, inner_provider)
        {
			new std::thread(std::bind(&native_linux_aio_provider::get_event, this));
        }

        native_linux_aio_provider::~native_linux_aio_provider()
        {
        }

        handle_t native_linux_aio_provider::open(const char* file_name, int flag, int pmode)
        {
            return (handle_t)::open(file_name, flag, pmode);
        }

        error_code native_linux_aio_provider::close(handle_t hFile)
        {
            // TODO: handle failure
            ::close(static_cast<int>(hFile));
            return ERR_SUCCESS;
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
			io_context_t ctx;
			linux_disk_aio_context * aio;
			
			while(true)
			{
				{
					::dsn::service::zauto_lock l(_lock);
					if(_ctx_q.empty())
						continue;
					ctx = _ctx_q.front();
					aio = _aio_q.front();

					_ctx_q.pop();
					_aio_q.pop();
				}
				ret = io_getevents(ctx, 1, 1, events, NULL);
				if(ret > 0) // should be 1
				{
					for(int i = 0; i < ret; i++)
					{
						io_callback_t cb = (io_callback_t) events[i].data;
						cb(ctx, &aio->cb, events[i].res, events[i].res2);
					}
				}
				memset(&ctx, 0, sizeof(ctx));
			}
		}

		void native_linux_aio_provider::aio_complete(io_context_t ctx, struct iocb *this_cb, long res, long res2)
		{
			linux_disk_aio_context * aio = container_of(this_cb, linux_disk_aio_context, cb);

			size_t bytes = size_t(this_cb->u.c.nbytes); // from e.g., read or write
			if(res2 != 0)
			{
				derror("aio error");
			}
			if(res != bytes)
			{
				derror("aio bytes miss");
			}

			if(!aio->evt)
			{
				aio_task_ptr aio_ptr(aio->tsk);
				aio->this_->complete_io(aio_ptr, (res2 == 0 && res == bytes) ? ERR_SUCCESS : ERR_FILE_OPERATION_FAILED, bytes);
			}
			else
			{
				aio->err = (res2 == 0 && res == bytes) ? ERR_SUCCESS : ERR_FILE_OPERATION_FAILED;
				aio->bytes = bytes;
				aio->evt->notify();
			}
		}

        error_code native_linux_aio_provider::aio_internal(aio_task_ptr& aio_tsk, bool async, __out_param uint32_t* pbytes /*= nullptr*/)
        {
			struct iocb *cbs[1];
			io_context_t ctx;
			linux_disk_aio_context * aio;
			int ret;

			memset (&ctx, 0, sizeof(ctx));
			
			ret = io_setup(128, &ctx);
			
			if (ret < 0)
			{
				derror("io_setep error!");
			}

			aio = (linux_disk_aio_context *)aio_tsk->aio().get();
			
			memset (&aio->cb, 0, sizeof(aio->cb));

			aio->this_ = this;
			aio->cb.aio_fildes = static_cast<int>((ssize_t)aio->file);
			
			switch(aio->type)
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
				aio->err = ERR_SUCCESS;
				aio->bytes = 0;
			}

			io_set_callback(&aio->cb, aio_complete);
			
			cbs[0] = &aio->cb;

			{
				::dsn::service::zauto_lock l(_lock);
				_ctx_q.push(ctx);
				_aio_q.push(aio);
			}
			ret = io_submit(ctx, 1, cbs);
			if (ret != 1) 
			{
				if (ret < 0)
					derror("io_submit error");
				else
					derror("could not sumbit IOs");
			}

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
} // end namespace dsn::tools
#endif
