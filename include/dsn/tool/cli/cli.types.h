# pragma once
# include <dsn/service_api_cpp.h>

# include <dsn/tool/cli/cli_constants.h>
# include <dsn/tool/cli/cli_types.h>
# include <dsn/tool/cli/cli.code.definition.h>

# include <dsn/cpp/serialization.h>

namespace dsn {
    GENERATED_TYPE_SERIALIZATION(command, THRIFT)
} 