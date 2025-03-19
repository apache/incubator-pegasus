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

package com.xiaomi.infra.pegasus.spark.analyser

import org.apache.spark.SparkContext

/**
  * custom implicits object, you can:
  *
  * import com.xiaomi.infra.pegasus.spark.analyser.CustomImplicits._
  *
  * to use it.
  */
object CustomImplicits {

  /**
    * The implicit method of converting SparkContext to PegasusContext
    * @param context
    * @return PegasusContext
    */
  implicit def convert2PegasusContext(
      context: SparkContext
  ): PegasusContext =
    new PegasusContext(context)
}
