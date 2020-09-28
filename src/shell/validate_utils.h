#include "shell/argh.h"
#include <dsn/dist/fmt_logging.h>
#include "command_executor.h"

bool validate_cmd(const argh::parser &cmd,
                  const std::set<std::string> &params,
                  const std::set<std::string> &flags);

bool validate_ip(shell_context *sc,
                 const std::string &ip_str,
                 /*out*/ dsn::rpc_address &target_address,
                 /*out*/ std::string &err_info);