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
package org.apache.pegasus.tools;

import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pegasus.apps.mutate;
import org.apache.pegasus.client.Mutations;

public class WriteLimiter {
  private static final int SINGLE_KEY_SIZE = 1024;
  private static final int SINGLE_VALUE_SIZE = 400 * 1024;
  private static final int MULTI_VALUE_COUNT = 1000;
  private static final int MULTI_VALUE_SIZE = 1024 * 1024;

  private boolean enableWriteLimit;

  public WriteLimiter(boolean enableWriteLimit) {
    this.enableWriteLimit = enableWriteLimit;
  }

  public void validateSingleSet(byte[] hashKey, byte[] sortKey, byte[] value)
      throws IllegalArgumentException {
    if (!enableWriteLimit) {
      return;
    }

    checkSingleHashKey(hashKey);
    checkSingleSortKey(hashKey, sortKey);
    checkSingleValue(hashKey, sortKey, value);
  }

  public void validateCheckAndSet(byte[] hashKey, byte[] setSortKey, byte[] setValue)
      throws IllegalArgumentException {
    validateSingleSet(hashKey, setSortKey, setValue);
  }

  public void validateMultiSet(byte[] hashKey, List<Pair<byte[], byte[]>> values)
      throws IllegalArgumentException {
    if (!enableWriteLimit) {
      return;
    }

    checkSingleHashKey(hashKey);
    checkMultiValueCount(hashKey, values.size());

    int valuesLength = 0;
    for (Pair<byte[], byte[]> value : values) {
      byte[] sortKey = value.getLeft() == null ? "".getBytes() : value.getLeft();
      byte[] multiValue = value.getRight() == null ? "".getBytes() : value.getRight();
      checkSingleSortKey(hashKey, sortKey);
      checkSingleValue(hashKey, sortKey, multiValue);
      valuesLength += multiValue.length;
      checkMultiValueSize(hashKey, valuesLength);
    }
  }

  public void validateCheckAndMutate(byte[] hashKey, Mutations mutations)
      throws IllegalArgumentException {
    if (!enableWriteLimit) {
      return;
    }

    checkSingleHashKey(hashKey);
    checkMultiValueCount(hashKey, mutations.getMutations().size());

    int valuesLength = 0;
    for (mutate mu : mutations.getMutations()) {
      byte[] sortKey = mu.sort_key == null ? "".getBytes() : mu.sort_key.data;
      byte[] MutateValue = mu.value == null ? "".getBytes() : mu.value.data;
      checkSingleSortKey(hashKey, sortKey);
      checkSingleValue(hashKey, sortKey, MutateValue);
      valuesLength += MutateValue.length;
      checkMultiValueSize(hashKey, valuesLength);
    }
  }

  private void checkSingleHashKey(byte[] hashKey) throws IllegalArgumentException {
    if (hashKey == null) {
      hashKey = "".getBytes();
    }

    if (hashKey.length > SINGLE_KEY_SIZE) {
      throw new IllegalArgumentException(
          "Exceed the hashKey length threshold = "
              + SINGLE_KEY_SIZE
              + ",hashKeyLength = "
              + hashKey.length
              + ",hashKey(head 100) = "
              + subString(new String(hashKey)));
    }
  }

  private void checkSingleSortKey(byte[] hashKey, byte[] sortKey) throws IllegalArgumentException {
    if (hashKey == null) {
      hashKey = "".getBytes();
    }

    if (sortKey == null) {
      sortKey = "".getBytes();
    }

    if (sortKey.length > SINGLE_KEY_SIZE) {
      throw new IllegalArgumentException(
          "Exceed the sort key length threshold = "
              + SINGLE_KEY_SIZE
              + ",sortKeyLength = "
              + sortKey.length
              + ",hashKey(head 100) = "
              + subString(new String(hashKey))
              + ",sortKey(head 100) = "
              + subString(new String(sortKey)));
    }
  }

  private void checkSingleValue(byte[] hashKey, byte[] sortKey, byte[] value)
      throws IllegalArgumentException {
    if (hashKey == null) {
      hashKey = "".getBytes();
    }

    if (sortKey == null) {
      sortKey = "".getBytes();
    }

    if (value == null) {
      value = "".getBytes();
    }

    if (value.length > SINGLE_VALUE_SIZE) {
      throw new IllegalArgumentException(
          "Exceed the value length threshold = "
              + SINGLE_VALUE_SIZE
              + ",valueLength = "
              + value.length
              + ",hashKey(head 100) = "
              + subString(new String(hashKey))
              + ",sortKey(head 100) = "
              + subString(new String(sortKey)));
    }
  }

  private void checkMultiValueCount(byte[] hashKey, int count) throws IllegalArgumentException {
    if (hashKey == null) {
      hashKey = "".getBytes();
    }

    if (count > MULTI_VALUE_COUNT) {
      throw new IllegalArgumentException(
          "Exceed the value count threshold = "
              + MULTI_VALUE_COUNT
              + ",valueCount = "
              + count
              + ",hashKey(head 100) = "
              + subString(new String(hashKey)));
    }
  }

  private void checkMultiValueSize(byte[] hashKey, int length) throws IllegalArgumentException {
    if (hashKey == null) {
      hashKey = "".getBytes();
    }

    if (length > MULTI_VALUE_SIZE) {
      throw new IllegalArgumentException(
          "Exceed the multi value length threshold = "
              + MULTI_VALUE_SIZE
              + ",hashKey(head 100) = "
              + subString(new String(hashKey)));
    }
  }

  private String subString(String str) {
    return str.length() < 100 ? str : str.substring(0, 100);
  }
}
