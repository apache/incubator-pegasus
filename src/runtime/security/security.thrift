// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

include "../../../idl/dsn.thrift"

namespace cpp dsn.security

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

struct negotiation_request {
    1: negotiation_status status;
    2: dsn.blob msg;
}

struct negotiation_response {
    1: negotiation_status status;
    2: dsn.blob msg;
}
