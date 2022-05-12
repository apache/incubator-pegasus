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
package org.apache.pegasus.client;

/**
 * @author qinzuoyan
 *     <p>Multi-get options.
 */
public class MultiGetOptions {
  public boolean startInclusive = true; // if the startSortKey is included
  public boolean stopInclusive = false; // if the stopSortKey is included
  public FilterType sortKeyFilterType = FilterType.FT_NO_FILTER; // filter type for sort key
  public byte[] sortKeyFilterPattern = null; // filter pattern for sort key
  public boolean noValue = false; // only fetch hash_key and sort_key, but not fetch value
  public boolean reverse = false; // if search in reverse direction

  public MultiGetOptions() {}

  public MultiGetOptions(MultiGetOptions o) {
    startInclusive = o.startInclusive;
    stopInclusive = o.stopInclusive;
    sortKeyFilterType = o.sortKeyFilterType;
    sortKeyFilterPattern = o.sortKeyFilterPattern;
    noValue = o.noValue;
    reverse = o.reverse;
  }
}
