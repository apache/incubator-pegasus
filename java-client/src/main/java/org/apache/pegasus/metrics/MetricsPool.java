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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import java.util.Map;
import java.util.SortedMap;
import org.apache.pegasus.tools.Tools;
import org.json.JSONException;
import org.json.JSONObject;

/** Created by weijiesun on 18-3-9. */
public final class MetricsPool {

  private final String defaultTags;
  private final FalconMetric theMetric;
  private final MetricRegistry registry = new MetricRegistry();

  public MetricsPool(String host, String tags, int reportStepSec) {
    theMetric = new FalconMetric();
    theMetric.endpoint = host;
    theMetric.step = reportStepSec;
    theMetric.tags = tags;
    defaultTags = tags;
  }

  public void setMeter(String counterName, long count) {
    registry.meter(counterName).mark(count);
  }

  public void setHistogram(String counterName, long value) {
    registry.histogram(counterName).update(value);
  }

  public void genJsonsFromMeter(String name, Meter meter, StringBuilder output)
      throws JSONException {
    theMetric.counterType = "GAUGE";
    theMetric.metric = name + ".cps-1sec";
    theMetric.tags = getTableTag(name);
    theMetric.value = meter.getMeanRate();
    oneMetricToJson(theMetric, output);
  }

  public void genJsonsFromHistogram(String name, Histogram hist, StringBuilder output)
      throws JSONException {
    theMetric.counterType = "GAUGE";
    Snapshot s = hist.getSnapshot();

    theMetric.metric = name + ".p99";
    theMetric.tags = getTableTag(name);
    theMetric.value = s.get99thPercentile();
    oneMetricToJson(theMetric, output);
    output.append(',');

    theMetric.metric = name + ".p999";
    theMetric.tags = getTableTag(name);
    theMetric.value = s.get999thPercentile();
    oneMetricToJson(theMetric, output);
  }

  public static void oneMetricToJson(FalconMetric metric, StringBuilder output)
      throws JSONException {
    JSONObject obj = new JSONObject();
    obj.put("endpoint", metric.endpoint);
    obj.put("metric", metric.metric);
    obj.put("timestamp", metric.timestamp);
    obj.put("step", metric.step);
    obj.put("value", metric.value);
    obj.put("counterType", metric.counterType);
    obj.put("tags", metric.tags);
    output.append(obj.toString());
  }

  public String metricsToJson() throws JSONException {
    theMetric.timestamp = Tools.unixEpochMills() / 1000;

    StringBuilder builder = new StringBuilder();
    builder.append('[');
    SortedMap<String, Meter> meters = registry.getMeters();
    for (Map.Entry<String, Meter> entry : meters.entrySet()) {
      genJsonsFromMeter(entry.getKey(), entry.getValue(), builder);
      builder.append(',');
    }

    for (Map.Entry<String, Histogram> entry : registry.getHistograms().entrySet()) {
      genJsonsFromHistogram(entry.getKey(), entry.getValue(), builder);
      builder.append(',');
    }

    if (builder.charAt(builder.length() - 1) == ',') {
      builder.deleteCharAt(builder.length() - 1);
    }

    builder.append("]");

    return builder.toString();
  }

  private String getTableTag(String counterName) {
    if (defaultTags.contains("table=")) {
      return defaultTags;
    }
    String[] result = counterName.split("@");
    if (result.length >= 2) {
      return defaultTags.equals("")
          ? ("table=" + result[1])
          : (defaultTags + ",table=" + result[1]);
    }
    return defaultTags;
  }

  static final class FalconMetric {
    public String endpoint; // metric host
    public String metric; // metric name
    public long timestamp; // report time in unix seconds
    public int step; // report interval in seconds;
    public double value; // metric value
    public String counterType; // GAUGE or COUNTER
    public String tags; // metrics description
  }
}
