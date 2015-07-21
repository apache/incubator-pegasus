
# include <dsn/service_api_c.h>

# include <dsn/internal/enum_helper.h>

ENUM_BEGIN(dsn_task_type_t, TASK_TYPE_INVALID)
    ENUM_REG(TASK_TYPE_RPC_REQUEST)
    ENUM_REG(TASK_TYPE_RPC_RESPONSE)
    ENUM_REG(TASK_TYPE_COMPUTE)
    ENUM_REG(TASK_TYPE_AIO)
    ENUM_REG(TASK_TYPE_CONTINUATION)
ENUM_END(dsn_task_type_t)

