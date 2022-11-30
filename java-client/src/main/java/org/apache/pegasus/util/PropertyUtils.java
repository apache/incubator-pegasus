/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.pegasus.util;

import com.google.common.base.Strings;
import java.util.Properties;

/** Utility class for dealing with getting configurations from a {@link Properties}. */
public final class PropertyUtils {

  private PropertyUtils() {}

  public static boolean getBoolean(Properties config, String key, boolean defaultValue) {
    String value = config.getProperty(key);
    if (Strings.isNullOrEmpty(value)) {
      return defaultValue;
    } else {
      return Boolean.parseBoolean(value);
    }
  }

  public static int getInt(Properties config, String key, int defaultValue) {
    String value = config.getProperty(key);
    if (Strings.isNullOrEmpty(value)) {
      return defaultValue;
    } else {
      return Integer.parseInt(value);
    }
  }

  public static long getLong(Properties config, String key, long defaultValue) {
    String value = config.getProperty(key);
    if (Strings.isNullOrEmpty(value)) {
      return defaultValue;
    } else {
      return Long.parseLong(value);
    }
  }
}
