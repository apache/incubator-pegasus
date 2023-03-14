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

#pragma once

#include <stdint.h>
#include <string>

#include "runtime/rpc/rpc_holder.h"

namespace dsn {
class gpid;

namespace replication {
class backup_request;
class backup_response;

class cold_backup_constant
{
public:
    static const std::string APP_METADATA;
    static const std::string APP_BACKUP_STATUS;
    static const std::string CURRENT_CHECKPOINT;
    static const std::string BACKUP_METADATA;
    static const std::string BACKUP_INFO;
    static const int32_t PROGRESS_FINISHED;
};

typedef rpc_holder<backup_request, backup_response> backup_rpc;

class backup_restore_constant
{
public:
    static const std::string FORCE_RESTORE;
    static const std::string BLOCK_SERVICE_PROVIDER;
    static const std::string CLUSTER_NAME;
    static const std::string POLICY_NAME;
    static const std::string APP_NAME;
    static const std::string APP_ID;
    static const std::string BACKUP_ID;
    static const std::string SKIP_BAD_PARTITION;
    static const std::string RESTORE_PATH;
};

namespace cold_backup {

//
//  Attention: when compose the path on block service, we use appname_appid, because appname_appid
//              can identify the case below:
//     -- case: you create one app with name A and it's appid is 1, then after backup a time later,
//              you drop the table, then create a new app with name A and with appid 3
//              using appname_appid, can idenfity the backup data is belong to which app

// The directory structure on block service
//
//      <root>/<backup_id>/<appname_appid>/meta/app_metadata
//                                        /meta/app_backup_status
//                                        /partition_1/checkpoint@ip:port/***.sst
//                                        /partition_1/checkpoint@ip:port/CURRENT
//                                        /partition_1/checkpoint@ip:port/backup_metadata
//                                        /partition_1/current_checkpoint
//      <root>/<backup_id>/<appname_appid>/meta/app_metadata
//                                        /meta/app_backup_status
//                                        /partition_1/checkpoint@ip:port/***.sst
//                                        /partition_1/checkpoint@ip:port/CURRENT
//                                        /partition_1/checkpoint@ip:port/backup_metadata
//                                        /partition_1/current_checkpoint
//      <root>/<backup_id>/backup_info
//

//
// the purpose of some file:
//      1, app_metadata : the metadata of the app, the same with the app's app_info
//      2, app_backup_status: the flag file, represent whether the app have finish backup, if this
//         file exist on block filesystem, backup is finished, otherwise, app haven't finished
//         backup, we ignore its context
//      3, backup_metadata : the file to statistic the information of a checkpoint, include all the
//         file's name, size and md5
//      4, current_checkpoint : specifing which checkpoint directory is valid
//      5, backup_info : recording the information of this backup
//

// compose the path for app on block service
// input:
//  -- root:  the prefix of path
// return:
//      the path: <root>/<backup_id>
std::string get_backup_path(const std::string &root, int64_t backup_id);

// return: <root>/<backup_id>/backup_info
std::string get_backup_info_file(const std::string &root, int64_t backup_id);

// compose the path for replica on block service
// input:
//  -- root:  the prefix of the path
// return:
//      the path: <root>/<backup_id>/<appname_appid>/<partition_index>
std::string get_replica_backup_path(const std::string &root,
                                    const std::string &app_name,
                                    gpid pid,
                                    int64_t backup_id);

// compose the path for meta on block service
// input:
//  -- root:  the prefix of the path
// return:
//      the path: <root>/<backup_id>/<appname_appid>/meta
std::string get_app_meta_backup_path(const std::string &root,
                                     const std::string &app_name,
                                     int32_t app_id,
                                     int64_t backup_id);

// compose the absolute path(AP) of app_metadata_file on block service
// input:
//  -- prefix:      the prefix of AP
// return:
//      the AP of app meta data file:
//      <root>/<backup_id>/<appname_appid>/meta/app_metadata
std::string get_app_metadata_file(const std::string &root,
                                  const std::string &app_name,
                                  int32_t app_id,
                                  int64_t backup_id);

// compose the absolute path(AP) of app_backup_status file on block service
// input:
//  -- prefix:      the prefix of AP
// return:
//      the AP of flag-file, which represent whether the app have finished backup:
//      <root>/<backup_id>/<appname_appid>/meta/app_backup_status
std::string get_app_backup_status_file(const std::string &root,
                                       const std::string &app_name,
                                       int32_t app_id,
                                       int64_t backup_id);

// compose the absolute path(AP) of current chekpoint file on block service
// input:
//  -- root:      the prefix of AP on block service
//  -- pid:         gpid of replica
// return:
//      the AP of current checkpoint file:
//      <root>/<backup_id>/<appname_appid>/<partition_index>/current_checkpoint
std::string get_current_chkpt_file(const std::string &root,
                                   const std::string &app_name,
                                   gpid pid,
                                   int64_t backup_id);

// compose the checkpoint directory name on block service
// return:
//      checkpoint directory name: checkpoint@<ip:port>
std::string get_remote_chkpt_dirname();

// compose the absolute path(AP) of checkpoint dir for replica on block service
// input:
//  -- root:       the prefix of the AP
//  -- pid:          gpid of replcia
// return:
//      the AP of the checkpoint dir:
//      <root>/<backup_id>/<appname_appid>/<partition_index>/checkpoint@<ip:port>
std::string get_remote_chkpt_dir(const std::string &root,
                                 const std::string &app_name,
                                 gpid pid,
                                 int64_t backup_id);

// compose the absolute path(AP) of checkpoint meta for replica on block service
// input:
//  -- root:       the prefix of the AP
//  -- pid:          gpid of replcia
// return:
//      the AP of the checkpoint file metadata:
//      <root>/<backup_id>/<appname_appid>/<partition_index>/checkpoint@<ip:port>/backup_metadata
std::string get_remote_chkpt_meta_file(const std::string &root,
                                       const std::string &app_name,
                                       gpid pid,
                                       int64_t backup_id);

} // namespace cold_backup
} // namespace replication
} // namespace dsn
