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

package com.xiaomi.infra.pegasus.spark.bulkloader.examples

import com.xiaomi.infra.pegasus.spark.FDSConfig
import com.xiaomi.infra.pegasus.spark.bulkloader.{
  BulkLoaderConfig,
  PegasusRecord
}
import org.apache.spark.{SparkConf, SparkContext}
import com.xiaomi.infra.pegasus.spark.bulkloader.CustomImplicits._

object CSVBulkLoader {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("pegasus data bulkloader")
      .setIfMissing("spark.master", "local[1]")

    val sc = new SparkContext(conf)

    val config = new BulkLoaderConfig(
      new FDSConfig(
        "accessKey",
        "accessSecret",
        "bucketName",
        "endPoint"
      ),
      "clusterName",
      "tableName"
    ).setTableId(20)
      .setTablePartitionCount(32)

    // This example only shows how to convert CSV file to PegasusFile, actually any data source that
    // can be converted RDD can be also converted to PegasusFile.
    // Note: if the partition size > 2G before "saveAsPegasusFile", you need
    // sc.textFile("data.csv").repartition(n), and let the partition size < 2G
    sc.textFile("data.csv")
      .map(i => {
        val lines = i.split(",")
        PegasusRecord.createV1(
          lines(0).getBytes(),
          lines(1).getBytes(),
          lines(2).getBytes()
        )
      })
      .saveAsPegasusFile(config)
  }

}
