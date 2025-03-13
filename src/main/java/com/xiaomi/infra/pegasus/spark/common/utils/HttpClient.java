package com.xiaomi.infra.pegasus.spark.common.utils;

import com.xiaomi.infra.pegasus.spark.common.PegasusSparkException;
import com.xiaomi.infra.pegasus.spark.common.utils.gateway.Cluster;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.ParseException;
import org.apache.http.client.ServiceUnavailableRetryStrategy;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HttpContext;

public class HttpClient {
  private static final Log LOG = LogFactory.getLog(Cluster.class);

  private static final org.apache.http.client.HttpClient httpClient;
  private static final RequestConfig config;

  static {
    httpClient =
        HttpClientBuilder.create()
            .setRetryHandler(
                (e, i, httpContext) -> {
                  if (i >= 1) {
                    LOG.warn(
                        String.format(
                            "request failed = %s, try retry it. count = %d", e.getMessage(), i));
                    try {
                      Thread.sleep(30000);
                    } catch (InterruptedException ie) {
                      LOG.error("sleep 10s for retry failed" + ie.getMessage());
                    }
                  }
                  return i <= 5 && e.getMessage().contains("timed out");
                })
            .setServiceUnavailableRetryStrategy(
                new ServiceUnavailableRetryStrategy() {
                  public boolean retryRequest(
                      HttpResponse response, int executionCount, HttpContext context) {
                    if (executionCount > 1) {
                      LOG.warn(
                          String.format(
                              "request failed = %s[%d], try retry it. count = %d",
                              response.getStatusLine().getReasonPhrase(),
                              response.getStatusLine().getStatusCode(),
                              executionCount));
                    }
                    return executionCount <= 5
                            && (response.getStatusLine().getStatusCode()
                                    == HttpStatus.SC_SERVICE_UNAVAILABLE
                                || response.getStatusLine().getStatusCode()
                                    == HttpStatus.SC_INTERNAL_SERVER_ERROR)
                        || response.getStatusLine().getStatusCode() == HttpStatus.SC_BAD_GATEWAY;
                  }

                  public long getRetryInterval() {
                    return 30000;
                  }
                })
            .build();

    config =
        RequestConfig.custom()
            .setConnectTimeout(30000)
            .setConnectionRequestTimeout(30000)
            .setSocketTimeout(30000)
            .build();
  }

  public static HttpResponse get(String path, Map<String, String> params)
      throws PegasusSparkException {
    HttpGet request = new HttpGet(path);
    request.setConfig(config);
    List<NameValuePair> uriParams = parseParams(params);
    try {
      URI uri = new URIBuilder(request.getURI()).addParameters(uriParams).build();
      request.setURI(uri);
      return httpClient.execute(request);
    } catch (URISyntaxException e) {
      throw new PegasusSparkException(
          String.format("build get url failed, reason: %s", e.getMessage()));
    } catch (ParseException | IOException e) {
      throw new PegasusSparkException(
          String.format("get from %s failed, reason: %s", request.getURI(), e.getMessage()));
    } catch (Exception e) {
      throw new PegasusSparkException(String.format("get failed, reason: %s", e.getMessage()));
    }
  }

  public static HttpResponse post(String path, String jsonStr) throws PegasusSparkException {
    HttpPost request = new HttpPost(path);
    try {
      request.setHeader("Accept", "application/json");
      request.setHeader("Content-Type", "application/json");
      request.setEntity(new StringEntity(jsonStr, "UTF-8"));
      return httpClient.execute(request);
    } catch (ParseException | IOException e) {
      throw new PegasusSparkException(
          String.format("post to %s failed, reason: %s", request.getURI(), e.getMessage()));
    } catch (Exception e) {
      throw new PegasusSparkException(String.format("post failed, reason: %s", e.getMessage()));
    }
  }

  private static List<NameValuePair> parseParams(Map<String, String> params) {
    List<NameValuePair> uriParams = new ArrayList<>();
    if (params != null) {
      for (Map.Entry<String, String> entry : params.entrySet()) {
        NameValuePair nvp = new BasicNameValuePair(entry.getKey(), entry.getValue());
        uriParams.add(nvp);
      }
    }
    return uriParams;
  }
}
