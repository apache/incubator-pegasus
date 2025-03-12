package com.xiaomi.infra.pegasus.spark.common.utils;

import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import java.util.concurrent.TimeUnit;

public class AutoRetryer {

  public static <T> Retryer<T> getDefaultRetryer() {
    return getRetryer(3, 3);
  }

  public static <T> Retryer<T> getRetryer(int sleepTimeSec, int attemptNumber) {
    return RetryerBuilder.<T>newBuilder()
        .retryIfException()
        .withWaitStrategy(WaitStrategies.fixedWait(sleepTimeSec, TimeUnit.SECONDS))
        .withStopStrategy(StopStrategies.stopAfterAttempt(attemptNumber))
        .build();
  }
}
