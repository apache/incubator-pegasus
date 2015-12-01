#pragma once

#include <dsn/cpp/auto_codes.h>

namespace dsn { namespace dist {

DEFINE_ERR_CODE(ERR_ZOOKEEPER_OPERATION)
error_code from_zerror(int zerr);

}}
