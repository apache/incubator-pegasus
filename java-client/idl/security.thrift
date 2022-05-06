include "base.thrift"

namespace cpp dsn.apps
namespace java com.xiaomi.infra.pegasus.apps
namespace py pypegasus.rrdb

// negotiation process:
//
//                       client                              server
//                          | ---    SASL_LIST_MECHANISMS     --> |
//                          | <--  SASL_LIST_MECHANISMS_RESP  --- |
//                          | --     SASL_SELECT_MECHANISMS   --> |
//                          | <-- SASL_SELECT_MECHANISMS_RESP --- |
//                          |                                     |
//                          | ---       SASL_INITIATE         --> |
//                          |                                     |
//                          | <--       SASL_CHALLENGE        --- |
//                          | ---     SASL_CHALLENGE_RESP     --> |
//                          |                                     |
//                          |               .....                 |
//                          |                                     |
//                          | <--       SASL_CHALLENGE        --- |
//                          | ---     SASL_CHALLENGE_RESP     --> |
//                          |                                     | (authentication will succeed
//                          |                                     |  if all challenges passed)
//                          | <--         SASL_SUCC           --- |
// (client won't response   |                                     |
// if servers says ok)      |                                     |
//                          | ---         RPC_CALL           ---> |
//                          | <--         RPC_RESP           ---- |

enum negotiation_status {
    INVALID
    SASL_LIST_MECHANISMS
    SASL_LIST_MECHANISMS_RESP
    SASL_SELECT_MECHANISMS
    SASL_SELECT_MECHANISMS_RESP
    SASL_INITIATE
    SASL_CHALLENGE
    SASL_CHALLENGE_RESP
    SASL_SUCC
    SASL_AUTH_DISABLE
    SASL_AUTH_FAIL
}

struct negotiation_request
{
    1: negotiation_status status;
    2: base.blob msg;
}

struct negotiation_response
{
    1: negotiation_status status;
    2: base.blob msg;
}

service security
{
    negotiation_response negotiate(1:negotiation_request request);
}
