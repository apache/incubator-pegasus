/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.xiaomi.infra.pegasus.spark.analyser.examples.basic

import com.xiaomi.infra.pegasus.spark.analyser.{ColdBackupConfig, DataV0}
import org.apache.spark.{SparkConf, SparkContext}
import com.xiaomi.infra.pegasus.spark.analyser.CustomImplicits._
import com.xiaomi.infra.pegasus.spark.common.FDSConfig

object CountData {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("count pegasus data stored in XiaoMi's FDS")
      .setIfMissing("spark.master", "local[1]")

    val coldBackupConfig =
      new ColdBackupConfig(
        new FDSConfig( // if data is in HDFS, pass HDFSConfig()
          "accessKey",
          "accessSecret",
          "bucketName",
          "endPoint"
        ),
        "clusterName",
        "tableName"
        // `initDataVersion` means auto set data version
        // from pegasus-gateway(https://git.n.xiaomi.com/pegasus/pegasus-gateway),
        // if your cluster not support gateway, you need use setDataVersion(), default is dataV1
      ).initDataVersion() // or setDataVersion(args)

    var count = 0
    val sc = new SparkContext(conf)
      .pegasusSnapshotRDD(coldBackupConfig)
      .map(_ => {
        count = count + 1
        if (count % 10000 == 0) {
          println("count=" + count)
        }
      })
      .count()
  }
}
