// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.metrics;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.xiaomi.infra.pegasus.tools.Tools;
import java.util.Map;
import java.util.SortedMap;
import org.json.JSONException;
import org.json.JSONObject;

/** Created by weijiesun on 18-3-9. */
public final class MetricsPool {
  public MetricsPool(String host, String tags, int reportStepSec) {
    theMetric = new FalconMetric();
    theMetric.endpoint = host;
    theMetric.step = reportStepSec;
    theMetric.tags = tags;
  }

  public void setMeter(String counterName, long count) {
    registry.meter(counterName).mark(count);
  }

  public void setHistorgram(String counterName, long value) {
    registry.histogram(counterName).update(value);
  }

  public void genJsonsFromMeter(String name, Meter meter, StringBuilder output)
      throws JSONException {
    theMetric.counterType = "GAUGE";

    theMetric.metric = name + ".cps-1sec";
    theMetric.value = meter.getMeanRate();
    oneMetricToJson(theMetric, output);
    output.append(',');

    theMetric.metric = name + ".cps-1min";
    theMetric.value = meter.getOneMinuteRate();
    oneMetricToJson(theMetric, output);
    output.append(',');

    theMetric.metric = name + ".cps-5min";
    theMetric.value = meter.getFiveMinuteRate();
    oneMetricToJson(theMetric, output);
    output.append(',');

    theMetric.metric = name + ".cps-15min";
    theMetric.value = meter.getFifteenMinuteRate();
    oneMetricToJson(theMetric, output);
  }

  public void genJsonsFromHistogram(String name, Histogram hist, StringBuilder output)
      throws JSONException {
    theMetric.counterType = "GAUGE";
    Snapshot s = hist.getSnapshot();

    theMetric.metric = name + ".p50";
    theMetric.value = s.getMedian();
    oneMetricToJson(theMetric, output);
    output.append(',');

    theMetric.metric = name + ".p99";
    theMetric.value = s.get99thPercentile();
    oneMetricToJson(theMetric, output);
    output.append(',');

    theMetric.metric = name + ".p999";
    theMetric.value = s.get999thPercentile();
    oneMetricToJson(theMetric, output);
    output.append(',');

    theMetric.metric = name + ".max";
    theMetric.value = s.getMax();
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
    boolean start = true;
    for (Map.Entry<String, Meter> entry : meters.entrySet()) {
      if (!start) {
        builder.append(',');
      }
      genJsonsFromMeter(entry.getKey(), entry.getValue(), builder);
      start = false;
    }

    for (Map.Entry<String, Histogram> entry : registry.getHistograms().entrySet()) {
      if (!start) {
        builder.append(',');
      }
      genJsonsFromHistogram(entry.getKey(), entry.getValue(), builder);
      start = false;
    }
    builder.append("]");

    return builder.toString();
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

  private FalconMetric theMetric;
  private final MetricRegistry registry = new MetricRegistry();
}
