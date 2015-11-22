# include <dsn/internal/aio_provider.h>
# include <gtest/gtest.h>
# include <dsn/service_api_cpp.h>
# include <dsn/service_api_c.h>
# include <dsn/internal/task.h>
# include "../../core/disk_engine.h"
# include "test_utils.h"
# include "../../tools/hpc/hpc_aio_provider.h"

namespace dsn{
    namespace test{
        class hpc_aio_provider_for_test : public dsn::tools::hpc_aio_provider
        {
        public:
            hpc_aio_provider_for_test(disk_engine* disk, aio_provider* inner_provider)
                :dsn::tools::hpc_aio_provider(disk, inner_provider), f(false)
            {
            }

            virtual void aio(aio_task* aio_tsk)
            {
                uint32_t b;
                error_code err;
                if(f)
                {
                    dsn::tools::hpc_aio_provider::aio(aio_tsk);
                }
                else
                {
                    auto err = aio_internal(aio_tsk, f, &b);
                    complete_io(aio_tsk, err, b);
                    err.end_tracking();
                }
                f = !f;
            }
        private:
            bool f;
        };
    }
}
