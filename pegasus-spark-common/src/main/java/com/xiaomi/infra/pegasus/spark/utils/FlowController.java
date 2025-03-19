package com.xiaomi.infra.pegasus.spark.utils;

import com.revinate.guava.util.concurrent.RateLimiter;
import java.io.Serializable;

public class FlowController {

  public static class RateLimiterConfig implements Serializable {
    private long megabytes;
    private long qps;
    private double burstFactor;

    public RateLimiterConfig() {
      this.megabytes = 0;
      this.qps = 0;
      this.burstFactor = 1.5;
    }

    /**
     * set the throughput MB/s, exceed the value will be blocked, default 0 means no limit
     *
     * @param megabytes MB/s
     * @return this
     */
    public RateLimiterConfig setMegabytes(long megabytes) {
      assert megabytes >= 0 : "megabytes >= 0";
      this.megabytes = megabytes;
      return this;
    }

    /**
     * set the qps, exceed the value will be blocked, default 0 means no limit
     *
     * @param qps
     * @return
     */
    public RateLimiterConfig setQps(long qps) {
      assert qps >= 0 : "qps >= 0";
      this.qps = qps;
      return this;
    }

    /**
     * set the burst factor, the burstRate = baseRate * burstFactor, default is 1.5
     *
     * @param burstFactor
     * @return
     */
    public RateLimiterConfig setBurstFactor(double burstFactor) {
      assert burstFactor >= 1 : "burstFactor >=1";
      this.burstFactor = burstFactor;
      return this;
    }

    public long getMegabytes() {
      return megabytes;
    }

    public long getQps() {
      return qps;
    }

    public double getBurstFactor() {
      return burstFactor;
    }
  }

  private RateLimiter bytesLimiter;
  private RateLimiter qpsLimiter;

  /**
   * FlowController constructor using partitionCount and RateLimiterConfig
   *
   * @param partitionCount spark partition count, generally is task parallelism, if
   *     RateLimiterConfig rate limit is `rate`, the one partition rate is `rate / partitionCount`
   * @param config RateLimiterConfig include `qps` and `byte` limit, if their value is 0, won't
   *     create RateLimiter
   */
  public FlowController(int partitionCount, RateLimiterConfig config) {
    if (config.megabytes > 0) {
      this.bytesLimiter =
          RateLimiter.create(
              1.0 * (config.megabytes << 20) / partitionCount,
              (config.megabytes << 20) * config.burstFactor / partitionCount);
    }

    if (config.qps > 0) {
      this.qpsLimiter =
          RateLimiter.create(
              1.0 * config.qps / partitionCount, config.qps * config.burstFactor / partitionCount);
    }
  }

  /**
   * Acquires the given bytes number of permits from {@link #bytesLimiter}, blocking until the
   * request can be granted. Tells the amount of time slept, if any.
   *
   * @param bytes the number of permits to acquire
   * @return time spent sleeping to enforce rate, in seconds; The follow case means no limiter and
   *     return 0:
   *     <p>case1: bytesLimiter = null means no bytes limiter
   *     <p>case2: bytes acquire is 0
   *     <p>case3: token is enough
   * @throws IllegalArgumentException if the bytes is negative
   */
  public double acquireBytes(int bytes) {
    if (bytesLimiter == null || bytes == 0) {
      return 0;
    }

    return bytesLimiter.acquire(bytes);
  }

  /**
   * Acquires a single permits from {@link #qpsLimiter}, blocking until the request can be granted.
   * Tells the amount of time slept, if any.
   *
   * @return time spent sleeping to enforce rate, in seconds; The follow case means no limiter and
   *     return 0:
   *     <p>case1: qpsLimiter = null means no bytes limiter
   *     <p>case2: token is enough
   */
  public double acquireQPS() {
    if (qpsLimiter == null) {
      return 0;
    }
    return qpsLimiter.acquire();
  }

  /**
   * Acquires the given request number of permits from {@link #qpsLimiter}, blocking until the
   * request can be granted. Tells the amount of time slept, if any.
   *
   * @return time spent sleeping to enforce rate, in seconds; The follow case means no limiter and
   *     return 0:
   *     <p>case1: qpsLimiter = null means no bytes limiter
   *     <p>case2: request acquire is 0
   *     <p>case3: token is enough
   * @throws IllegalArgumentException if the request is negative
   */
  public double acquireQPS(int request) {
    if (qpsLimiter == null || request == 0) {
      return 0;
    }
    return qpsLimiter.acquire(request);
  }
}
