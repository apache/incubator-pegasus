#include <zookeeper.h>
#include <dsn/dist/error_code.h>
#include <dsn/cpp/auto_codes.h>

#include "zookeeper_error.h"
namespace dsn { namespace dist {

error_code from_zerror(int zerr)
{
    if (ZOK == zerr)
        return ERR_OK;
    if (ZBADARGUMENTS == zerr)
        return ERR_INVALID_PARAMETERS;
    if (ZCONNECTIONLOSS == zerr || ZOPERATIONTIMEOUT == zerr)
        return ERR_TIMEOUT;
    if (ZNONODE == zerr)
        return ERR_OBJECT_NOT_FOUND;
    if (ZNODEEXISTS == zerr)
        return ERR_NODE_ALREADY_EXIST;

    return ERR_ZOOKEEPER_OPERATION;
}

}}
