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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Created by weijiesun on 18-3-9. */
public class MetricsPoolTest {
  @BeforeEach
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
    assertEquals(1, array.length());

    String[] metrics = {
      "TestName.cps-1sec", "TestName.cps-1min", "TestName.cps-5min", "TestName.cps-15min"
    };

    for (int i = 0; i < array.length(); ++i) {
      JSONObject j = array.getJSONObject(i);

      assertEquals(tags, j.getString("tags"));
      assertEquals(metrics[i], j.getString("metric"));
      assertEquals("GAUGE", j.getString("counterType"));
      assertEquals(20, j.getInt("step"));
      assertEquals(host, j.getString("endpoint"));
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
    assertEquals(2, array.length());

    String[] metrics = {"TestHist.p99", "TestHist.p999"};

    for (int i = 0; i < array.length(); ++i) {
      JSONObject j = array.getJSONObject(i);

      assertEquals(tags, j.getString("tags"));
      assertEquals(metrics[i], j.getString("metric"));
      assertEquals("GAUGE", j.getString("counterType"));
      assertEquals(20, j.getInt("step"));
      assertEquals(host, j.getString("endpoint"));
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
    assertEquals(metric.endpoint, obj.getString("endpoint"));
    assertEquals(metric.metric, obj.getString("metric"));
    assertEquals(metric.timestamp, obj.getLong("timestamp"));
    assertEquals(metric.step, obj.getInt("step"));
    assertEquals(metric.value, obj.getDouble("value"));
    assertEquals(metric.counterType, obj.getString("counterType"));
    assertEquals(metric.tags, obj.getString("tags"));

    builder.setLength(0);
    metric.tags = "";
    MetricsPool.oneMetricToJson(metric, builder);
    obj = new JSONObject(builder.toString());
    assertEquals(metric.endpoint, obj.getString("endpoint"));
    assertEquals(metric.metric, obj.getString("metric"));
    assertEquals(metric.timestamp, obj.getLong("timestamp"));
    assertEquals(metric.step, obj.getInt("step"));
    assertEquals(metric.value, obj.getDouble("value"));
    assertEquals(metric.counterType, obj.getString("counterType"));
    assertEquals(metric.tags, obj.getString("tags"));
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
    assertEquals(6, array.length());
    for (int i = 0; i < array.length(); ++i) {
      JSONObject j = array.getJSONObject(i);

      if (j.getString("metric").contains("@")) {
        assertEquals(tags + ",table=temp", j.getString("tags"));
      } else {
        assertEquals(tags, j.getString("tags"));
      }
      assertEquals("GAUGE", j.getString("counterType"));
      assertEquals(20, j.getInt("step"));
      assertEquals(host, j.getString("endpoint"));
    }
  }

  MetricRegistry r;
}
