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

package com.xiaomi.infra.pegasus.spark.analyser.examples.parquet

import com.xiaomi.infra.pegasus.spark.FDSConfig
import com.xiaomi.infra.pegasus.spark.analyser.ColdBackupConfig
import com.xiaomi.infra.pegasus.spark.analyser.CustomImplicits._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

object ConvertParquet {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("convertParquet")
      .master(
        "local[1]"
      ) // this config only for test at local, remove it before deploy in cluster
      .getOrCreate()

    // if data in HDFS, pass HDFSConfig()
    val coldBackupConfig =
      new ColdBackupConfig(new FDSConfig("", "", "", "", ""), "onebox", "temp")

    val rdd = spark.sparkContext.pegasusSnapshotRDD(coldBackupConfig)

    // please make sure data can be converted valid string value
    val dataFrame = spark.createDataFrame(
      rdd.map(i =>
        Row(new String(i.hashKey), new String(i.sortKey), new String(i.value))
      ),
      Schema.struct
    )

    dataFrame
      .coalesce(1)
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      // in online, need save on hdfs
      .save("/tmp")

    //after convert success(only test)
    val dataFrameResult = spark.read.parquet("/tmp")
    dataFrameResult.show(10)
  }
}
