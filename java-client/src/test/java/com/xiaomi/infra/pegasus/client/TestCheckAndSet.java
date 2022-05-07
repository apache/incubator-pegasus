// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.client;

/** @author qinzuoyan */
import org.junit.Assert;
import org.junit.Test;

/** Created by mi on 18-7-17. */
public class TestCheckAndSet {
  @Test
  public void testValueNotExist() throws PException {
    PegasusClientInterface client = PegasusClientFactory.getSingletonClient();
    String tableName = "temp";
    byte[] hashKey = "check_and_set_java_test_value_not_exist".getBytes();

    try {
      client.del(tableName, hashKey, "k1".getBytes());

      CheckAndSetOptions options = new CheckAndSetOptions();
      PegasusTableInterface.CheckAndSetResult result;
      byte[] value;

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k1".getBytes(),
              CheckType.CT_VALUE_NOT_EXIST,
              null,
              "k1".getBytes(),
              "v1".getBytes(),
              options);
      Assert.assertTrue(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertFalse(result.checkValueExist);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertArrayEquals("v1".getBytes(), value);

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k1".getBytes(),
              CheckType.CT_VALUE_NOT_EXIST,
              null,
              "k1".getBytes(),
              "v2".getBytes(),
              options);
      Assert.assertFalse(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("v1".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertArrayEquals("v1".getBytes(), value);

      options.returnCheckValue = false;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k1".getBytes(),
              CheckType.CT_VALUE_NOT_EXIST,
              null,
              "k1".getBytes(),
              "v1".getBytes(),
              options);
      Assert.assertFalse(result.setSucceed);
      Assert.assertFalse(result.checkValueReturned);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertArrayEquals("v1".getBytes(), value);

      client.del(tableName, hashKey, "k1".getBytes());
    } catch (PException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }

    try {
      client.del(tableName, hashKey, "k2".getBytes());

      CheckAndSetOptions options = new CheckAndSetOptions();
      PegasusTableInterface.CheckAndSetResult result;
      byte[] value;

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k2".getBytes(),
              CheckType.CT_VALUE_NOT_EXIST,
              null,
              "k2".getBytes(),
              "".getBytes(),
              options);
      Assert.assertTrue(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertFalse(result.checkValueExist);
      value = client.get(tableName, hashKey, "k2".getBytes());
      Assert.assertArrayEquals("".getBytes(), value);

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k2".getBytes(),
              CheckType.CT_VALUE_NOT_EXIST,
              null,
              "k2".getBytes(),
              "v2".getBytes(),
              options);
      Assert.assertFalse(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k2".getBytes());
      Assert.assertArrayEquals("".getBytes(), value);

      client.del(tableName, hashKey, "k1".getBytes());
    } catch (PException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }

    try {
      client.del(tableName, hashKey, "k3".getBytes());
      client.del(tableName, hashKey, "k4".getBytes());

      CheckAndSetOptions options = new CheckAndSetOptions();
      PegasusTableInterface.CheckAndSetResult result;
      byte[] value;

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k3".getBytes(),
              CheckType.CT_VALUE_NOT_EXIST,
              null,
              "k4".getBytes(),
              "v4".getBytes(),
              options);
      Assert.assertTrue(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertFalse(result.checkValueExist);
      value = client.get(tableName, hashKey, "k4".getBytes());
      Assert.assertArrayEquals("v4".getBytes(), value);

      client.del(tableName, hashKey, "k3".getBytes());
      client.del(tableName, hashKey, "k4".getBytes());
    } catch (PException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }

    PegasusClientFactory.closeSingletonClient();
  }

  @Test
  public void testValueExist() throws PException {
    PegasusClientInterface client = PegasusClientFactory.getSingletonClient();
    String tableName = "temp";
    byte[] hashKey = "check_and_set_java_test_value_exist".getBytes();

    try {
      client.del(tableName, hashKey, "k1".getBytes());

      CheckAndSetOptions options = new CheckAndSetOptions();
      PegasusTableInterface.CheckAndSetResult result;
      byte[] value;

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k1".getBytes(),
              CheckType.CT_VALUE_EXIST,
              null,
              "k1".getBytes(),
              "v1".getBytes(),
              options);
      Assert.assertFalse(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertFalse(result.checkValueExist);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertNull(value);

      client.set(tableName, hashKey, "k1".getBytes(), "".getBytes());

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k1".getBytes(),
              CheckType.CT_VALUE_EXIST,
              null,
              "k1".getBytes(),
              "v1".getBytes(),
              options);
      Assert.assertTrue(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertArrayEquals("v1".getBytes(), value);

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k1".getBytes(),
              CheckType.CT_VALUE_EXIST,
              null,
              "k1".getBytes(),
              "v2".getBytes(),
              options);
      Assert.assertTrue(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("v1".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertArrayEquals("v2".getBytes(), value);

      client.del(tableName, hashKey, "k1".getBytes());
    } catch (PException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }

    try {
      client.set(tableName, hashKey, "k3".getBytes(), "v3".getBytes());
      client.del(tableName, hashKey, "k4".getBytes());

      CheckAndSetOptions options = new CheckAndSetOptions();
      PegasusTableInterface.CheckAndSetResult result;
      byte[] value;

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k3".getBytes(),
              CheckType.CT_VALUE_EXIST,
              null,
              "k4".getBytes(),
              "v4".getBytes(),
              options);
      Assert.assertTrue(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("v3".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k4".getBytes());
      Assert.assertArrayEquals("v4".getBytes(), value);

      client.del(tableName, hashKey, "k3".getBytes());
      client.del(tableName, hashKey, "k4".getBytes());
    } catch (PException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }

    PegasusClientFactory.closeSingletonClient();
  }

  @Test
  public void testValueNotEmpty() throws PException {
    PegasusClientInterface client = PegasusClientFactory.getSingletonClient();
    String tableName = "temp";
    byte[] hashKey = "check_and_set_java_test_value_not_empty".getBytes();

    try {
      client.del(tableName, hashKey, "k1".getBytes());

      CheckAndSetOptions options = new CheckAndSetOptions();
      PegasusTableInterface.CheckAndSetResult result;
      byte[] value;

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k1".getBytes(),
              CheckType.CT_VALUE_NOT_EMPTY,
              null,
              "k1".getBytes(),
              "v1".getBytes(),
              options);
      Assert.assertFalse(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertFalse(result.checkValueExist);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertNull(value);

      client.set(tableName, hashKey, "k1".getBytes(), "".getBytes());

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k1".getBytes(),
              CheckType.CT_VALUE_NOT_EMPTY,
              null,
              "k1".getBytes(),
              "v1".getBytes(),
              options);
      Assert.assertFalse(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertArrayEquals("".getBytes(), value);

      client.set(tableName, hashKey, "k1".getBytes(), "v1".getBytes());

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k1".getBytes(),
              CheckType.CT_VALUE_NOT_EMPTY,
              null,
              "k1".getBytes(),
              "v2".getBytes(),
              options);
      Assert.assertTrue(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("v1".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertArrayEquals("v2".getBytes(), value);

      client.del(tableName, hashKey, "k1".getBytes());
    } catch (PException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }

    try {
      client.set(tableName, hashKey, "k3".getBytes(), "v3".getBytes());
      client.del(tableName, hashKey, "k4".getBytes());

      CheckAndSetOptions options = new CheckAndSetOptions();
      PegasusTableInterface.CheckAndSetResult result;
      byte[] value;

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k3".getBytes(),
              CheckType.CT_VALUE_NOT_EMPTY,
              null,
              "k4".getBytes(),
              "v4".getBytes(),
              options);
      Assert.assertTrue(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("v3".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k4".getBytes());
      Assert.assertArrayEquals("v4".getBytes(), value);

      client.del(tableName, hashKey, "k3".getBytes());
      client.del(tableName, hashKey, "k4".getBytes());
    } catch (PException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }

    PegasusClientFactory.closeSingletonClient();
  }

  @Test
  public void testMatchAnywhere() throws PException {
    PegasusClientInterface client = PegasusClientFactory.getSingletonClient();
    String tableName = "temp";
    byte[] hashKey = "check_and_set_java_test_value_match_anywhere".getBytes();

    try {
      client.del(tableName, hashKey, "k1".getBytes());

      CheckAndSetOptions options = new CheckAndSetOptions();
      PegasusTableInterface.CheckAndSetResult result;
      byte[] value;

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k1".getBytes(),
              CheckType.CT_VALUE_MATCH_ANYWHERE,
              "v".getBytes(),
              "k1".getBytes(),
              "v1".getBytes(),
              options);
      Assert.assertFalse(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertFalse(result.checkValueExist);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertNull(value);

      client.set(tableName, hashKey, "k1".getBytes(), "".getBytes());

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k1".getBytes(),
              CheckType.CT_VALUE_MATCH_ANYWHERE,
              "v".getBytes(),
              "k1".getBytes(),
              "v1".getBytes(),
              options);
      Assert.assertFalse(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertArrayEquals("".getBytes(), value);

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k1".getBytes(),
              CheckType.CT_VALUE_MATCH_ANYWHERE,
              "".getBytes(),
              "k1".getBytes(),
              "v1".getBytes(),
              options);
      Assert.assertTrue(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertArrayEquals("v1".getBytes(), value);

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k1".getBytes(),
              CheckType.CT_VALUE_MATCH_ANYWHERE,
              "".getBytes(),
              "k1".getBytes(),
              "v2".getBytes(),
              options);
      Assert.assertTrue(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("v1".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertArrayEquals("v2".getBytes(), value);

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k1".getBytes(),
              CheckType.CT_VALUE_MATCH_ANYWHERE,
              "2".getBytes(),
              "k1".getBytes(),
              "v111v".getBytes(),
              options);
      Assert.assertTrue(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("v2".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertArrayEquals("v111v".getBytes(), value);

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k1".getBytes(),
              CheckType.CT_VALUE_MATCH_ANYWHERE,
              "111".getBytes(),
              "k1".getBytes(),
              "v2".getBytes(),
              options);
      Assert.assertTrue(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("v111v".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertArrayEquals("v2".getBytes(), value);

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k1".getBytes(),
              CheckType.CT_VALUE_MATCH_ANYWHERE,
              "y".getBytes(),
              "k1".getBytes(),
              "v3".getBytes(),
              options);
      Assert.assertFalse(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("v2".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertArrayEquals("v2".getBytes(), value);

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k1".getBytes(),
              CheckType.CT_VALUE_MATCH_ANYWHERE,
              "v2v".getBytes(),
              "k1".getBytes(),
              "v3".getBytes(),
              options);
      Assert.assertFalse(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("v2".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertArrayEquals("v2".getBytes(), value);

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k1".getBytes(),
              CheckType.CT_VALUE_MATCH_ANYWHERE,
              "v2".getBytes(),
              "k1".getBytes(),
              "v3".getBytes(),
              options);
      Assert.assertTrue(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("v2".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertArrayEquals("v3".getBytes(), value);

      client.del(tableName, hashKey, "k1".getBytes());
    } catch (PException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }

    try {
      client.set(tableName, hashKey, "k3".getBytes(), "v333v".getBytes());
      client.del(tableName, hashKey, "k4".getBytes());

      CheckAndSetOptions options = new CheckAndSetOptions();
      PegasusTableInterface.CheckAndSetResult result;
      byte[] value;

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k3".getBytes(),
              CheckType.CT_VALUE_MATCH_ANYWHERE,
              "333".getBytes(),
              "k4".getBytes(),
              "v4".getBytes(),
              options);
      Assert.assertTrue(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("v333v".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k4".getBytes());
      Assert.assertArrayEquals("v4".getBytes(), value);

      client.del(tableName, hashKey, "k3".getBytes());
      client.del(tableName, hashKey, "k4".getBytes());
    } catch (PException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }

    PegasusClientFactory.closeSingletonClient();
  }

  @Test
  public void testMatchPrefix() throws PException {
    PegasusClientInterface client = PegasusClientFactory.getSingletonClient();
    String tableName = "temp";
    byte[] hashKey = "check_and_set_java_test_value_match_prefix".getBytes();

    try {
      client.del(tableName, hashKey, "k1".getBytes());

      CheckAndSetOptions options = new CheckAndSetOptions();
      PegasusTableInterface.CheckAndSetResult result;
      byte[] value;

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k1".getBytes(),
              CheckType.CT_VALUE_MATCH_PREFIX,
              "v".getBytes(),
              "k1".getBytes(),
              "v1".getBytes(),
              options);
      Assert.assertFalse(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertFalse(result.checkValueExist);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertNull(value);

      client.set(tableName, hashKey, "k1".getBytes(), "".getBytes());

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k1".getBytes(),
              CheckType.CT_VALUE_MATCH_PREFIX,
              "v".getBytes(),
              "k1".getBytes(),
              "v1".getBytes(),
              options);
      Assert.assertFalse(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertArrayEquals("".getBytes(), value);

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k1".getBytes(),
              CheckType.CT_VALUE_MATCH_PREFIX,
              "".getBytes(),
              "k1".getBytes(),
              "v1".getBytes(),
              options);
      Assert.assertTrue(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertArrayEquals("v1".getBytes(), value);

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k1".getBytes(),
              CheckType.CT_VALUE_MATCH_PREFIX,
              "".getBytes(),
              "k1".getBytes(),
              "v2".getBytes(),
              options);
      Assert.assertTrue(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("v1".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertArrayEquals("v2".getBytes(), value);

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k1".getBytes(),
              CheckType.CT_VALUE_MATCH_PREFIX,
              "v".getBytes(),
              "k1".getBytes(),
              "v111v".getBytes(),
              options);
      Assert.assertTrue(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("v2".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertArrayEquals("v111v".getBytes(), value);

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k1".getBytes(),
              CheckType.CT_VALUE_MATCH_PREFIX,
              "111".getBytes(),
              "k1".getBytes(),
              "v2".getBytes(),
              options);
      Assert.assertFalse(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("v111v".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertArrayEquals("v111v".getBytes(), value);

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k1".getBytes(),
              CheckType.CT_VALUE_MATCH_PREFIX,
              "v111".getBytes(),
              "k1".getBytes(),
              "v2".getBytes(),
              options);
      Assert.assertTrue(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("v111v".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertArrayEquals("v2".getBytes(), value);

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k1".getBytes(),
              CheckType.CT_VALUE_MATCH_PREFIX,
              "y".getBytes(),
              "k1".getBytes(),
              "v3".getBytes(),
              options);
      Assert.assertFalse(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("v2".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertArrayEquals("v2".getBytes(), value);

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k1".getBytes(),
              CheckType.CT_VALUE_MATCH_PREFIX,
              "v2v".getBytes(),
              "k1".getBytes(),
              "v3".getBytes(),
              options);
      Assert.assertFalse(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("v2".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertArrayEquals("v2".getBytes(), value);

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k1".getBytes(),
              CheckType.CT_VALUE_MATCH_PREFIX,
              "v2".getBytes(),
              "k1".getBytes(),
              "v3".getBytes(),
              options);
      Assert.assertTrue(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("v2".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertArrayEquals("v3".getBytes(), value);

      client.del(tableName, hashKey, "k1".getBytes());
    } catch (PException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }

    try {
      client.set(tableName, hashKey, "k3".getBytes(), "v333v".getBytes());
      client.del(tableName, hashKey, "k4".getBytes());

      CheckAndSetOptions options = new CheckAndSetOptions();
      PegasusTableInterface.CheckAndSetResult result;
      byte[] value;

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k3".getBytes(),
              CheckType.CT_VALUE_MATCH_PREFIX,
              "v333".getBytes(),
              "k4".getBytes(),
              "v4".getBytes(),
              options);
      Assert.assertTrue(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("v333v".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k4".getBytes());
      Assert.assertArrayEquals("v4".getBytes(), value);

      client.del(tableName, hashKey, "k3".getBytes());
      client.del(tableName, hashKey, "k4".getBytes());
    } catch (PException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }

    PegasusClientFactory.closeSingletonClient();
  }

  @Test
  public void testMatchPostfix() throws PException {
    PegasusClientInterface client = PegasusClientFactory.getSingletonClient();
    String tableName = "temp";
    byte[] hashKey = "check_and_set_java_test_value_match_postfix".getBytes();

    try {
      client.del(tableName, hashKey, "k1".getBytes());

      CheckAndSetOptions options = new CheckAndSetOptions();
      PegasusTableInterface.CheckAndSetResult result;
      byte[] value;

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k1".getBytes(),
              CheckType.CT_VALUE_MATCH_POSTFIX,
              "v".getBytes(),
              "k1".getBytes(),
              "v1".getBytes(),
              options);
      Assert.assertFalse(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertFalse(result.checkValueExist);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertNull(value);

      client.set(tableName, hashKey, "k1".getBytes(), "".getBytes());

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k1".getBytes(),
              CheckType.CT_VALUE_MATCH_POSTFIX,
              "v".getBytes(),
              "k1".getBytes(),
              "v1".getBytes(),
              options);
      Assert.assertFalse(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertArrayEquals("".getBytes(), value);

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k1".getBytes(),
              CheckType.CT_VALUE_MATCH_POSTFIX,
              "".getBytes(),
              "k1".getBytes(),
              "v1".getBytes(),
              options);
      Assert.assertTrue(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertArrayEquals("v1".getBytes(), value);

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k1".getBytes(),
              CheckType.CT_VALUE_MATCH_POSTFIX,
              "".getBytes(),
              "k1".getBytes(),
              "v2".getBytes(),
              options);
      Assert.assertTrue(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("v1".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertArrayEquals("v2".getBytes(), value);

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k1".getBytes(),
              CheckType.CT_VALUE_MATCH_POSTFIX,
              "2".getBytes(),
              "k1".getBytes(),
              "v111v".getBytes(),
              options);
      Assert.assertTrue(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("v2".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertArrayEquals("v111v".getBytes(), value);

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k1".getBytes(),
              CheckType.CT_VALUE_MATCH_POSTFIX,
              "111".getBytes(),
              "k1".getBytes(),
              "v2".getBytes(),
              options);
      Assert.assertFalse(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("v111v".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertArrayEquals("v111v".getBytes(), value);

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k1".getBytes(),
              CheckType.CT_VALUE_MATCH_POSTFIX,
              "111v".getBytes(),
              "k1".getBytes(),
              "v2".getBytes(),
              options);
      Assert.assertTrue(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("v111v".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertArrayEquals("v2".getBytes(), value);

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k1".getBytes(),
              CheckType.CT_VALUE_MATCH_POSTFIX,
              "y".getBytes(),
              "k1".getBytes(),
              "v3".getBytes(),
              options);
      Assert.assertFalse(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("v2".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertArrayEquals("v2".getBytes(), value);

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k1".getBytes(),
              CheckType.CT_VALUE_MATCH_POSTFIX,
              "2v2".getBytes(),
              "k1".getBytes(),
              "v3".getBytes(),
              options);
      Assert.assertFalse(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("v2".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertArrayEquals("v2".getBytes(), value);

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k1".getBytes(),
              CheckType.CT_VALUE_MATCH_POSTFIX,
              "v2".getBytes(),
              "k1".getBytes(),
              "v3".getBytes(),
              options);
      Assert.assertTrue(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("v2".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertArrayEquals("v3".getBytes(), value);

      client.del(tableName, hashKey, "k1".getBytes());
    } catch (PException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }

    try {
      client.set(tableName, hashKey, "k3".getBytes(), "v333v".getBytes());
      client.del(tableName, hashKey, "k4".getBytes());

      CheckAndSetOptions options = new CheckAndSetOptions();
      PegasusTableInterface.CheckAndSetResult result;
      byte[] value;

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k3".getBytes(),
              CheckType.CT_VALUE_MATCH_POSTFIX,
              "333v".getBytes(),
              "k4".getBytes(),
              "v4".getBytes(),
              options);
      Assert.assertTrue(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("v333v".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k4".getBytes());
      Assert.assertArrayEquals("v4".getBytes(), value);

      client.del(tableName, hashKey, "k3".getBytes());
      client.del(tableName, hashKey, "k4".getBytes());
    } catch (PException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }

    PegasusClientFactory.closeSingletonClient();
  }

  @Test
  public void testBytesCompare() throws PException {
    PegasusClientInterface client = PegasusClientFactory.getSingletonClient();
    String tableName = "temp";
    byte[] hashKey = "check_and_set_java_test_value_bytes_compare".getBytes();

    try {
      client.del(tableName, hashKey, "k1".getBytes());

      CheckAndSetOptions options = new CheckAndSetOptions();
      PegasusTableInterface.CheckAndSetResult result;
      byte[] value;

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k1".getBytes(),
              CheckType.CT_VALUE_BYTES_EQUAL,
              "".getBytes(),
              "k1".getBytes(),
              "v1".getBytes(),
              options);
      Assert.assertFalse(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertFalse(result.checkValueExist);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertNull(value);

      client.set(tableName, hashKey, "k1".getBytes(), "".getBytes());

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k1".getBytes(),
              CheckType.CT_VALUE_BYTES_EQUAL,
              "".getBytes(),
              "k1".getBytes(),
              "v1".getBytes(),
              options);
      Assert.assertTrue(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertArrayEquals("v1".getBytes(), value);

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k1".getBytes(),
              CheckType.CT_VALUE_BYTES_EQUAL,
              "".getBytes(),
              "k1".getBytes(),
              "v1".getBytes(),
              options);
      Assert.assertFalse(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("v1".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertArrayEquals("v1".getBytes(), value);

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k1".getBytes(),
              CheckType.CT_VALUE_BYTES_EQUAL,
              "v1".getBytes(),
              "k1".getBytes(),
              "v2".getBytes(),
              options);
      Assert.assertTrue(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("v1".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertArrayEquals("v2".getBytes(), value);

      client.del(tableName, hashKey, "k1".getBytes());
    } catch (PException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }

    try {
      client.set(tableName, hashKey, "k3".getBytes(), "v3".getBytes());
      client.del(tableName, hashKey, "k4".getBytes());

      CheckAndSetOptions options = new CheckAndSetOptions();
      PegasusTableInterface.CheckAndSetResult result;
      byte[] value;

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k3".getBytes(),
              CheckType.CT_VALUE_BYTES_EQUAL,
              "v3".getBytes(),
              "k4".getBytes(),
              "v4".getBytes(),
              options);
      Assert.assertTrue(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("v3".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k4".getBytes());
      Assert.assertArrayEquals("v4".getBytes(), value);

      client.del(tableName, hashKey, "k3".getBytes());
      client.del(tableName, hashKey, "k4".getBytes());
    } catch (PException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }

    try {
      client.set(tableName, hashKey, "k5".getBytes(), "v1".getBytes());

      CheckAndSetOptions options = new CheckAndSetOptions();
      PegasusTableInterface.CheckAndSetResult result;
      byte[] value;

      // v1 < v2
      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k5".getBytes(),
              CheckType.CT_VALUE_BYTES_LESS,
              "v2".getBytes(),
              "k5".getBytes(),
              "v2".getBytes(),
              options);
      Assert.assertTrue(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("v1".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k5".getBytes());
      Assert.assertArrayEquals("v2".getBytes(), value);

      // v2 <= v2
      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k5".getBytes(),
              CheckType.CT_VALUE_BYTES_LESS_OR_EQUAL,
              "v2".getBytes(),
              "k5".getBytes(),
              "v3".getBytes(),
              options);
      Assert.assertTrue(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("v2".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k5".getBytes());
      Assert.assertArrayEquals("v3".getBytes(), value);

      // v3 <= v4
      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k5".getBytes(),
              CheckType.CT_VALUE_BYTES_LESS_OR_EQUAL,
              "v4".getBytes(),
              "k5".getBytes(),
              "v4".getBytes(),
              options);
      Assert.assertTrue(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("v3".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k5".getBytes());
      Assert.assertArrayEquals("v4".getBytes(), value);

      // v4 >= v4
      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k5".getBytes(),
              CheckType.CT_VALUE_BYTES_GREATER_OR_EQUAL,
              "v4".getBytes(),
              "k5".getBytes(),
              "v5".getBytes(),
              options);
      Assert.assertTrue(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("v4".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k5".getBytes());
      Assert.assertArrayEquals("v5".getBytes(), value);

      // v5 >= v4
      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k5".getBytes(),
              CheckType.CT_VALUE_BYTES_GREATER_OR_EQUAL,
              "v4".getBytes(),
              "k5".getBytes(),
              "v6".getBytes(),
              options);
      Assert.assertTrue(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("v5".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k5".getBytes());
      Assert.assertArrayEquals("v6".getBytes(), value);

      // v6 > v5
      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k5".getBytes(),
              CheckType.CT_VALUE_BYTES_GREATER,
              "v5".getBytes(),
              "k5".getBytes(),
              "v7".getBytes(),
              options);
      Assert.assertTrue(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("v6".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k5".getBytes());
      Assert.assertArrayEquals("v7".getBytes(), value);

      client.del(tableName, hashKey, "k5".getBytes());
    } catch (PException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }

    PegasusClientFactory.closeSingletonClient();
  }

  @Test
  public void testIntCompare() throws PException {
    PegasusClientInterface client = PegasusClientFactory.getSingletonClient();
    String tableName = "temp";
    byte[] hashKey = "check_and_set_java_test_value_int_compare".getBytes();

    try {
      client.del(tableName, hashKey, "k1".getBytes());

      CheckAndSetOptions options = new CheckAndSetOptions();
      PegasusTableInterface.CheckAndSetResult result;
      byte[] value;

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k1".getBytes(),
              CheckType.CT_VALUE_INT_EQUAL,
              "1".getBytes(),
              "k1".getBytes(),
              "2".getBytes(),
              options);
      Assert.assertFalse(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertFalse(result.checkValueExist);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertNull(value);

      client.set(tableName, hashKey, "k1".getBytes(), "".getBytes());

      options.returnCheckValue = true;
      try {
        client.checkAndSet(
            tableName,
            hashKey,
            "k1".getBytes(),
            CheckType.CT_VALUE_INT_EQUAL,
            "1".getBytes(),
            "k1".getBytes(),
            "2".getBytes(),
            options);
        Assert.assertTrue(false);
      } catch (PException ex) {
        Assert.assertTrue(ex.getMessage(), ex.getMessage().endsWith("rocksdb error: 4"));

      } catch (Exception ex) {
        Assert.assertTrue(false);
      }
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertArrayEquals("".getBytes(), value);

      client.set(tableName, hashKey, "k1".getBytes(), "1".getBytes());

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k1".getBytes(),
              CheckType.CT_VALUE_INT_EQUAL,
              "1".getBytes(),
              "k1".getBytes(),
              "2".getBytes(),
              options);
      Assert.assertTrue(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("1".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertArrayEquals("2".getBytes(), value);

      options.returnCheckValue = true;
      try {
        client.checkAndSet(
            tableName,
            hashKey,
            "k1".getBytes(),
            CheckType.CT_VALUE_INT_EQUAL,
            "".getBytes(),
            "k1".getBytes(),
            "3".getBytes(),
            options);
        Assert.assertTrue(false);
      } catch (PException ex) {
        Assert.assertTrue(ex.getMessage(), ex.getMessage().endsWith("rocksdb error: 4"));

      } catch (Exception ex) {
        Assert.assertTrue(false);
      }
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertArrayEquals("2".getBytes(), value);

      options.returnCheckValue = true;
      try {
        client.checkAndSet(
            tableName,
            hashKey,
            "k1".getBytes(),
            CheckType.CT_VALUE_INT_EQUAL,
            "v1".getBytes(),
            "k1".getBytes(),
            "3".getBytes(),
            options);
        Assert.assertTrue(false);
      } catch (PException ex) {
        Assert.assertTrue(ex.getMessage(), ex.getMessage().endsWith("rocksdb error: 4"));

      } catch (Exception ex) {
        Assert.assertTrue(false);
      }
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertArrayEquals("2".getBytes(), value);

      options.returnCheckValue = true;
      try {
        client.checkAndSet(
            tableName,
            hashKey,
            "k1".getBytes(),
            CheckType.CT_VALUE_INT_EQUAL,
            "88888888888888888888888888888888888888888888888".getBytes(),
            "k1".getBytes(),
            "3".getBytes(),
            options);
        Assert.assertTrue(false);
      } catch (PException ex) {
        Assert.assertTrue(ex.getMessage(), ex.getMessage().endsWith("rocksdb error: 4"));

      } catch (Exception ex) {
        Assert.assertTrue(false);
      }
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertArrayEquals("2".getBytes(), value);

      client.set(tableName, hashKey, "k1".getBytes(), "0".getBytes());

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k1".getBytes(),
              CheckType.CT_VALUE_INT_EQUAL,
              "0".getBytes(),
              "k1".getBytes(),
              "-1".getBytes(),
              options);
      Assert.assertTrue(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("0".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertArrayEquals("-1".getBytes(), value);

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k1".getBytes(),
              CheckType.CT_VALUE_INT_EQUAL,
              "-1".getBytes(),
              "k1".getBytes(),
              "-2".getBytes(),
              options);
      Assert.assertTrue(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("-1".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertArrayEquals("-2".getBytes(), value);

      client.del(tableName, hashKey, "k1".getBytes());
    } catch (PException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }

    try {
      client.set(tableName, hashKey, "k3".getBytes(), "3".getBytes());
      client.del(tableName, hashKey, "k4".getBytes());

      CheckAndSetOptions options = new CheckAndSetOptions();
      PegasusTableInterface.CheckAndSetResult result;
      byte[] value;

      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k3".getBytes(),
              CheckType.CT_VALUE_INT_EQUAL,
              "3".getBytes(),
              "k4".getBytes(),
              "4".getBytes(),
              options);
      Assert.assertTrue(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("3".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k4".getBytes());
      Assert.assertArrayEquals("4".getBytes(), value);

      client.del(tableName, hashKey, "k3".getBytes());
      client.del(tableName, hashKey, "k4".getBytes());
    } catch (PException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }

    try {
      client.set(tableName, hashKey, "k5".getBytes(), "1".getBytes());

      CheckAndSetOptions options = new CheckAndSetOptions();
      PegasusTableInterface.CheckAndSetResult result;
      byte[] value;

      // 1 < 2
      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k5".getBytes(),
              CheckType.CT_VALUE_INT_LESS,
              "2".getBytes(),
              "k5".getBytes(),
              "2".getBytes(),
              options);
      Assert.assertTrue(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("1".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k5".getBytes());
      Assert.assertArrayEquals("2".getBytes(), value);

      // 2 <= 2
      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k5".getBytes(),
              CheckType.CT_VALUE_INT_LESS_OR_EQUAL,
              "2".getBytes(),
              "k5".getBytes(),
              "3".getBytes(),
              options);
      Assert.assertTrue(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("2".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k5".getBytes());
      Assert.assertArrayEquals("3".getBytes(), value);

      // 3 <= 4
      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k5".getBytes(),
              CheckType.CT_VALUE_INT_LESS_OR_EQUAL,
              "4".getBytes(),
              "k5".getBytes(),
              "4".getBytes(),
              options);
      Assert.assertTrue(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("3".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k5".getBytes());
      Assert.assertArrayEquals("4".getBytes(), value);

      // 4 >= 4
      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k5".getBytes(),
              CheckType.CT_VALUE_INT_GREATER_OR_EQUAL,
              "4".getBytes(),
              "k5".getBytes(),
              "5".getBytes(),
              options);
      Assert.assertTrue(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("4".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k5".getBytes());
      Assert.assertArrayEquals("5".getBytes(), value);

      // 5 >= 4
      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k5".getBytes(),
              CheckType.CT_VALUE_INT_GREATER_OR_EQUAL,
              "4".getBytes(),
              "k5".getBytes(),
              "6".getBytes(),
              options);
      Assert.assertTrue(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("5".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k5".getBytes());
      Assert.assertArrayEquals("6".getBytes(), value);

      // 6 > 5
      options.returnCheckValue = true;
      result =
          client.checkAndSet(
              tableName,
              hashKey,
              "k5".getBytes(),
              CheckType.CT_VALUE_INT_GREATER,
              "5".getBytes(),
              "k5".getBytes(),
              "7".getBytes(),
              options);
      Assert.assertTrue(result.setSucceed);
      Assert.assertTrue(result.checkValueReturned);
      Assert.assertTrue(result.checkValueExist);
      Assert.assertArrayEquals("6".getBytes(), result.checkValue);
      value = client.get(tableName, hashKey, "k5".getBytes());
      Assert.assertArrayEquals("7".getBytes(), value);

      client.del(tableName, hashKey, "k5".getBytes());
    } catch (PException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }

    PegasusClientFactory.closeSingletonClient();
  }

  @Test
  public void testCompareExchange() throws PException {
    PegasusClientInterface client = PegasusClientFactory.getSingletonClient();
    String tableName = "temp";
    byte[] hashKey = "check_and_set_java_test_compare_exchange".getBytes();

    try {
      client.del(tableName, hashKey, "k1".getBytes());

      PegasusTableInterface.CompareExchangeResult result;
      byte[] value;

      result =
          client.compareExchange(
              tableName, hashKey, "k1".getBytes(), "".getBytes(), "v1".getBytes(), 0);
      Assert.assertFalse(result.setSucceed);
      Assert.assertNull(result.actualValue);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertNull(value);

      client.set(tableName, hashKey, "k1".getBytes(), "".getBytes());

      result =
          client.compareExchange(
              tableName, hashKey, "k1".getBytes(), "".getBytes(), "v1".getBytes(), 0);
      Assert.assertTrue(result.setSucceed);
      Assert.assertNull(result.actualValue);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertArrayEquals("v1".getBytes(), value);

      result =
          client.compareExchange(
              tableName, hashKey, "k1".getBytes(), "".getBytes(), "v2".getBytes(), 0);
      Assert.assertFalse(result.setSucceed);
      Assert.assertArrayEquals("v1".getBytes(), result.actualValue);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertArrayEquals("v1".getBytes(), value);

      result =
          client.compareExchange(
              tableName, hashKey, "k1".getBytes(), "v1".getBytes(), "v2".getBytes(), 0);
      Assert.assertTrue(result.setSucceed);
      Assert.assertNull(result.actualValue);
      value = client.get(tableName, hashKey, "k1".getBytes());
      Assert.assertArrayEquals("v2".getBytes(), value);

      client.del(tableName, hashKey, "k1".getBytes());
    } catch (PException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }

    PegasusClientFactory.closeSingletonClient();
  }
}
