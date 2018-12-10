// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

PEGASUS_ERR_CODE(PERR_OK, 0, "success");
PEGASUS_ERR_CODE(PERR_UNKNOWN, -1, "unknown error");
PEGASUS_ERR_CODE(PERR_TIMEOUT, -2, "timeout");
PEGASUS_ERR_CODE(PERR_OBJECT_NOT_FOUND, -3, "object not found");
PEGASUS_ERR_CODE(PERR_NETWORK_FAILURE, -4, "network failure");
PEGASUS_ERR_CODE(PERR_HANDLER_NOT_FOUND, -5, "handler not found");
PEGASUS_ERR_CODE(PERR_OPERATION_DISABLED, -6, "operation disabled");
PEGASUS_ERR_CODE(PERR_NOT_ENOUGH_MEMBER, -7, "no enough member");
PEGASUS_ERR_CODE(PERR_SCAN_COMPLETE, 1, "scan complete");

// SERVER ERROR
PEGASUS_ERR_CODE(PERR_APP_NOT_EXIST, -101, "app not exist");
PEGASUS_ERR_CODE(PERR_APP_EXIST, -102, "app already exist");
PEGASUS_ERR_CODE(PERR_SERVER_INTERNAL_ERROR, -103, "server internal error");
PEGASUS_ERR_CODE(PERR_SERVER_CHANGED, -104, "server changed");
PEGASUS_ERR_CODE(PERR_APP_BUSY, -105, "app busy");

// CLIENT ERROR
PEGASUS_ERR_CODE(PERR_INVALID_APP_NAME,
                 -201,
                 "app name is invalid, only letters, digits or underscore is valid");
PEGASUS_ERR_CODE(PERR_INVALID_HASH_KEY, -202, "hash key can't be empty");
PEGASUS_ERR_CODE(PERR_INVALID_VALUE, -203, "value can't be empty");
PEGASUS_ERR_CODE(PERR_INVALID_PAR_COUNT, -204, "partition count must be a power of 2");
PEGASUS_ERR_CODE(PERR_INVALID_REP_COUNT, -205, "replication count must be 3");
PEGASUS_ERR_CODE(PERR_INVALID_SPLIT_COUNT, -206, "split count must be greater than 0");
PEGASUS_ERR_CODE(PERR_GEO_DECODE_VALUE_ERROR,
                 -207,
                 "decode latitude and longitude from value error");
PEGASUS_ERR_CODE(PERR_GEO_INVALID_LATLNG_ERROR, -208, "latitude or longitude is invalid");

// SERVER ERROR
// start from -301

// ROCKSDB SERVER ERROR
PEGASUS_ERR_CODE(PERR_NOT_FOUND, -1001, "not found");
PEGASUS_ERR_CODE(PERR_CORRUPTION, -1002, "corruption");
PEGASUS_ERR_CODE(PERR_NOT_SUPPORTED, -1003, "not supported");
PEGASUS_ERR_CODE(PERR_INVALID_ARGUMENT, -1004, "invalid argument");
PEGASUS_ERR_CODE(PERR_IO_ERROR, -1005, "io error");
PEGASUS_ERR_CODE(PERR_MERGE_IN_PROGRESS, -1006, "merge in progress");
PEGASUS_ERR_CODE(PERR_INCOMPLETE, -1007, "incomplete");
PEGASUS_ERR_CODE(PERR_SHUTDOWN_IN_PROGRESS, -1008, "shutdown in progress");
PEGASUS_ERR_CODE(PERR_ROCKSDB_TIME_OUT, -1009, "rocksdb time out");
PEGASUS_ERR_CODE(PERR_ABORTED, -1010, "aborted");
PEGASUS_ERR_CODE(PERR_BUSY, -1011, "busy");
PEGASUS_ERR_CODE(PERR_EXPIRED, -1012, "expired");
PEGASUS_ERR_CODE(PERR_TRY_AGAIN, -1013, "try again");
