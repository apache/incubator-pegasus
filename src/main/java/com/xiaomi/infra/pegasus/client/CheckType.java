// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.client;

/**
 * @author qinzuoyan
 *     <p>Check type.
 */
public enum CheckType {
  CT_NO_CHECK(0),

  // appearance
  CT_VALUE_NOT_EXIST(1), // value is not exist
  CT_VALUE_NOT_EXIST_OR_EMPTY(2), // value is not exist or value is empty
  CT_VALUE_EXIST(3), // value is exist
  CT_VALUE_NOT_EMPTY(4), // value is exist and not empty

  // match
  CT_VALUE_MATCH_ANYWHERE(5), // operand matches anywhere in value
  CT_VALUE_MATCH_PREFIX(6), // operand matches prefix in value
  CT_VALUE_MATCH_POSTFIX(7), // operand matches postfix in value

  // bytes compare
  CT_VALUE_BYTES_LESS(8), // bytes compare: value < operand
  CT_VALUE_BYTES_LESS_OR_EQUAL(9), // bytes compare: value <= operand
  CT_VALUE_BYTES_EQUAL(10), // bytes compare: value == operand
  CT_VALUE_BYTES_GREATER_OR_EQUAL(11), // bytes compare: value >= operand
  CT_VALUE_BYTES_GREATER(12), // bytes compare: value > operand

  // int compare: first transfer bytes to int64; then compare by int value
  CT_VALUE_INT_LESS(13), // int compare: value < operand
  CT_VALUE_INT_LESS_OR_EQUAL(14), // int compare: value <= operand
  CT_VALUE_INT_EQUAL(15), // int compare: value == operand
  CT_VALUE_INT_GREATER_OR_EQUAL(16), // int compare: value >= operand
  CT_VALUE_INT_GREATER(17); // int compare: value > operand

  private final int value;

  private CheckType(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }
}
