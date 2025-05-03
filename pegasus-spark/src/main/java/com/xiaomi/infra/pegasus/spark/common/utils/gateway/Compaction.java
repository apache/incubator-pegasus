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

package com.xiaomi.infra.pegasus.spark.common.utils.gateway;

import com.xiaomi.infra.pegasus.spark.common.PegasusSparkException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Compaction {

  public static class Error {
    public String Errno;

    public Error(String errno) {
      Errno = errno;
    }
  }

  public static class Response {
    public Compaction.Error err;
    public String hint_msg;
    public int progress;
  }

  private static final Log LOG = LogFactory.getLog(Compaction.class);

  public String periodicTriggerTime;
  public boolean triggerAfterLoaded;
  public String concurrent;

  public Compaction(String periodicTriggerTime, int concurrent, boolean triggerAfterLoaded)
      throws PegasusSparkException {
    String[] times = periodicTriggerTime.split(":");

    if (times.length != 2) {
      throw new PegasusSparkException(
          "compaction trigger time is invalid! time=" + periodicTriggerTime);
    }

    int hour = Integer.parseInt(times[0]);
    int minute = Integer.parseInt(times[1]);
    if (hour < 0 || hour >= 24 || minute < 0 || minute >= 60) {
      throw new PegasusSparkException(
          "compaction trigger time is invalid! time=" + periodicTriggerTime);
    }

    if (minute == 0 && !times[1].equals("00")) {
      throw new PegasusSparkException(
          "compaction trigger time is invalid! time=" + periodicTriggerTime);
    }

    if (minute < 10 && !times[1].contains("0")) {
      throw new PegasusSparkException(
          "compaction trigger time is invalid! time=" + periodicTriggerTime);
    }

    if (concurrent <= 0) {
      throw new PegasusSparkException(
          "compaction concurrent(must > 0) is invalid! concurrent=" + concurrent);
    }

    this.periodicTriggerTime = periodicTriggerTime;
    this.concurrent = Integer.toString(concurrent);
    this.triggerAfterLoaded = triggerAfterLoaded;

    LOG.info("init compaction task successfully: " + toString());
  }

  @Override
  public String toString() {
    return "Compaction{"
        + "periodicTriggerTime='"
        + periodicTriggerTime
        + '\''
        + ", triggerAfterLoading="
        + triggerAfterLoaded
        + ", concurrent='"
        + concurrent
        + '\''
        + '}';
  }
}
