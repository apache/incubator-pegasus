#pragma once

# include <rdsn/tool_api.h>
# include <rdsn/internal/synchronize.h>

namespace rdsn {
    namespace tools {
        class native_win_aio_provider : public aio_provider
        {
        public:
            native_win_aio_provider(disk_engine* disk, aio_provider* inner_provider);
            ~native_win_aio_provider();

            virtual handle_t open(const char* file_name, int flag, int pmode);
            virtual error_code close(handle_t hFile);
            virtual void    aio(aio_task_ptr& aio);            
            virtual disk_aio_ptr prepare_aio_context(aio_task* tsk);

        protected:
            error_code aio_internal(aio_task_ptr& aio, bool async, __out uint32_t* pbytes = nullptr);

        private:
            void worker();
            std::thread *_worker_thr;
            handle_t    _iocp;
        };
    }
}


