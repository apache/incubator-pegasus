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
package org.apache.pegasus.metrics;

import org.apache.pegasus.tools.Tools;
import org.slf4j.Logger;

/** Created by weijiesun on 18-3-8. */
public final class MetricsManager {
  public static void updateCount(String counterName, long count) {
    metrics.setMeter(counterName, count);
  }

  public static void setHistogramValue(String counterName, long value) {
    metrics.setHistorgram(counterName, value);
  }

  public static final void initFromHost(String host, String tag, int reportIntervalSec) {
    synchronized (logger) {
      if (started) {
        logger.warn(
            "perf counter system has started with host({}), tag({}), interval({}), "
                + "skip this init with host({}), tag({}), interval(){}",
            MetricsManager.host,
            MetricsManager.tag,
            MetricsManager.reportIntervalSecs,
            host,
            tag,
            reportIntervalSec);
        return;
      }

      logger.info(
          "init metrics with host({}), tag({}), interval({})", host, tag, reportIntervalSec);

      MetricsManager.host = host;
      MetricsManager.tag = tag;
      MetricsManager.reportIntervalSecs = reportIntervalSec;
      metrics = new MetricsPool(host, tag, reportIntervalSec);
      reporter = new MetricsReporter(reportIntervalSec, metrics);
      reporter.start();
      started = true;
    }
  }

  public static final void detectHostAndInit(String tag, int reportIntervalSec) {
    initFromHost(Tools.getLocalHostAddress().getHostName(), tag, reportIntervalSec);
  }

  public static final void finish() {
    synchronized (logger) {
      if (started) {
        reporter.stop();
        started = false;
      }
    }
  }

  private static boolean started = false;
  private static String host;
  private static String tag;
  private static int reportIntervalSecs;

  private static MetricsPool metrics;
  private static MetricsReporter reporter;
  private static final Logger logger = org.slf4j.LoggerFactory.getLogger(MetricsManager.class);
}
