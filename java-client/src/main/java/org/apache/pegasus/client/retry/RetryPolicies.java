package org.apache.pegasus.client.retry;

public enum RetryPolicies {
  DEFAULT(DefaultRetryPolicy.class),
  EXPONENTIAL(ExponentialBackoffRetryPolicy.class);

  private final Class<? extends RetryPolicy> clazz;

  RetryPolicies(Class<? extends RetryPolicy> clazz) {
    this.clazz = clazz;
  }

  public Class<? extends RetryPolicy> getImplementationClass() {
    return clazz;
  }
}
