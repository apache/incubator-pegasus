// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.metrics;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import junit.framework.Assert;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

/** Created by weijiesun on 18-3-9. */
public class MetricsPoolTest {
  @Before
  public void before() {
    r = new MetricRegistry();
  }

  @Test
  public void genJsonsFromMeter() throws Exception {
    String host = "simple-test-host.bj";
    String tags = "what=you,like=another";
    MetricsPool pool = new MetricsPool(host, tags, 20);
    Meter m = r.meter("TestName");

    m.mark(1);
    m.mark(1);

    StringBuilder builder = new StringBuilder();
    pool.genJsonsFromMeter("TestName", m, builder);

    JSONArray array = new JSONArray("[" + builder.toString() + "]");
    Assert.assertEquals(1, array.length());

    String[] metrics = {
      "TestName.cps-1sec", "TestName.cps-1min", "TestName.cps-5min", "TestName.cps-15min"
    };

    for (int i = 0; i < array.length(); ++i) {
      JSONObject j = array.getJSONObject(i);

      Assert.assertEquals(tags, j.getString("tags"));
      Assert.assertEquals(metrics[i], j.getString("metric"));
      Assert.assertEquals("GAUGE", j.getString("counterType"));
      Assert.assertEquals(20, j.getInt("step"));
      Assert.assertEquals(host, j.getString("endpoint"));
    }
  }

  @Test
  public void genJsonFromHistogram() throws Exception {
    String host = "simple-test-host.bj";
    String tags = "what=you,like=another";
    MetricsPool pool = new MetricsPool(host, tags, 20);
    Histogram h = r.histogram("TestHist");
    for (int i = 0; i < 1000000; ++i) h.update((long) i);

    StringBuilder builder = new StringBuilder();
    pool.genJsonsFromHistogram("TestHist", h, builder);

    JSONArray array = new JSONArray("[" + builder.toString() + "]");
    Assert.assertEquals(2, array.length());

    String[] metrics = {"TestHist.p99", "TestHist.p999"};

    for (int i = 0; i < array.length(); ++i) {
      JSONObject j = array.getJSONObject(i);

      Assert.assertEquals(tags, j.getString("tags"));
      Assert.assertEquals(metrics[i], j.getString("metric"));
      Assert.assertEquals("GAUGE", j.getString("counterType"));
      Assert.assertEquals(20, j.getInt("step"));
      Assert.assertEquals(host, j.getString("endpoint"));
    }
  }

  @Test
  public void oneMetricToJson() throws Exception {
    MetricsPool.FalconMetric metric = new MetricsPool.FalconMetric();
    metric.endpoint = "1.2.3.4";
    metric.metric = "simple_metric";
    metric.timestamp = 12343455L;
    metric.step = 30;
    metric.value = 50;
    metric.counterType = "GAUGE";
    metric.tags = "cluster=onebox,app=new";

    StringBuilder builder = new StringBuilder();
    MetricsPool.oneMetricToJson(metric, builder);

    JSONObject obj = new JSONObject(builder.toString());
    Assert.assertEquals(metric.endpoint, obj.getString("endpoint"));
    Assert.assertEquals(metric.metric, obj.getString("metric"));
    Assert.assertEquals(metric.timestamp, obj.getLong("timestamp"));
    Assert.assertEquals(metric.step, obj.getInt("step"));
    Assert.assertEquals(metric.value, obj.getDouble("value"));
    Assert.assertEquals(metric.counterType, obj.getString("counterType"));
    Assert.assertEquals(metric.tags, obj.getString("tags"));

    builder.setLength(0);
    metric.tags = "";
    MetricsPool.oneMetricToJson(metric, builder);
    obj = new JSONObject(builder.toString());
    Assert.assertEquals(metric.endpoint, obj.getString("endpoint"));
    Assert.assertEquals(metric.metric, obj.getString("metric"));
    Assert.assertEquals(metric.timestamp, obj.getLong("timestamp"));
    Assert.assertEquals(metric.step, obj.getInt("step"));
    Assert.assertEquals(metric.value, obj.getDouble("value"));
    Assert.assertEquals(metric.counterType, obj.getString("counterType"));
    Assert.assertEquals(metric.tags, obj.getString("tags"));
  }

  @Test
  public void metricsToJson() throws Exception {
    String host = "simple-test-host.bj";
    String tags = "what=you,like=another";
    MetricsPool pool = new MetricsPool(host, tags, 20);

    pool.setMeter("aaa@temp", 1);
    pool.setMeter("aaa", 2);

    for (int i = 0; i < 10000; ++i) {
      pool.setHistorgram("ccc", i);
      pool.setHistorgram("ccc@temp", i);
    }

    JSONArray array = new JSONArray(pool.metricsToJson());
    Assert.assertEquals(6, array.length());
    for (int i = 0; i < array.length(); ++i) {
      JSONObject j = array.getJSONObject(i);

      if (j.getString("metric").contains("@")) {
        Assert.assertEquals(tags + ",table=temp", j.getString("tags"));
      } else {
        Assert.assertEquals(tags, j.getString("tags"));
      }
      Assert.assertEquals("GAUGE", j.getString("counterType"));
      Assert.assertEquals(20, j.getInt("step"));
      Assert.assertEquals(host, j.getString("endpoint"));
    }
  }

  MetricRegistry r;
}
