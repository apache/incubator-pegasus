// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.client;

/**
 * @author shenyuannan
 *     <p>Scan options.
 */
public class ScanOptions {
  public int timeoutMillis = 5000; // operation timeout in milli-seconds.
  // if timeoutMillis > 0, it is a timeout value for current op,
  // else the timeout value in the configuration file will be used.
  public int batchSize = 100; // internal buffer batch size
  public boolean startInclusive = true; // if the startSortKey is included
  public boolean stopInclusive = false; // if the stopSortKey is included
  public FilterType hashKeyFilterType = FilterType.FT_NO_FILTER; // filter type for hash key
  public byte[] hashKeyFilterPattern = null; // filter pattern for hash key
  public FilterType sortKeyFilterType = FilterType.FT_NO_FILTER; // filter type for sort key
  public byte[] sortKeyFilterPattern = null; // filter pattern for sort key
  public boolean noValue = false; // only fetch hash_key and sort_key, but not fetch value

  public ScanOptions() {}

  public ScanOptions(ScanOptions o) {
    timeoutMillis = o.timeoutMillis;
    batchSize = o.batchSize;
    startInclusive = o.startInclusive;
    stopInclusive = o.stopInclusive;
    hashKeyFilterType = o.hashKeyFilterType;
    hashKeyFilterPattern = o.hashKeyFilterPattern;
    sortKeyFilterType = o.sortKeyFilterType;
    sortKeyFilterPattern = o.sortKeyFilterPattern;
    noValue = o.noValue;
  }
}
