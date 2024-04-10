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
package org.apache.pegasus.base;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TStruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class host_port
    implements TBase<host_port, host_port._Fields>, java.io.Serializable, Cloneable {
  private static final TStruct STRUCT_DESC = new TStruct("host_port");

  private static final Logger LOG = LoggerFactory.getLogger(host_port.class);

  public String hostAddress;

  public short port;

  /**
   * The set of fields this struct contains, along with convenience methods for finding and
   * manipulating them.
   */
  public enum _Fields implements TFieldIdEnum {
    ;

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /** Find the _Fields constant that matches fieldId, or null if its not found. */
    public static _Fields findByThriftId(int fieldId) {
      switch (fieldId) {
        default:
          return null;
      }
    }

    /** Find the _Fields constant that matches fieldId, throwing an exception if it is not found. */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null)
        throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /** Find the _Fields constant that matches name, or null if its not found. */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  public static final Map<_Fields, FieldMetaData> metaDataMap;

  static {
    Map<_Fields, FieldMetaData> tmpMap = new EnumMap<_Fields, FieldMetaData>(_Fields.class);
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    FieldMetaData.addStructMetaDataMap(host_port.class, metaDataMap);
  }

  public host_port() {
    this.hostAddress = null;
    this.port = 0;
  }

  public boolean isInvalid() {
    return this.hostAddress == null || this.port == 0;
  }

  public String get_host() {
    return hostAddress;
  }

  public short get_port() {
    return port;
  }

  public static InetAddress getInetAddress(final String host) {
    InetAddress[] addrs = getAllInetAddresses(host);
    if (addrs != null && addrs.length > 0) {
      return addrs[0];
    }
    return null;
  }

  public static InetAddress[] getAllInetAddresses(final String host) {
    final long start = System.nanoTime();
    try {
      InetAddress[] ipAddrs = InetAddress.getAllByName(host);
      long latency = System.nanoTime() - start;
      if (latency > 500000 /*ns*/ && LOG.isDebugEnabled()) {
        LOG.debug("Resolved IP of `{}' to {} in {}ns", host, ipAddrs, latency);
      } else if (latency >= 3000000 /*ns*/) {
        LOG.warn("Slow DNS lookup! Resolved IP of `{}' to {} in {}ns", host, ipAddrs, latency);
      }
      return ipAddrs;
    } catch (UnknownHostException e) {
      LOG.error("Failed to resolve the IP of `{}' in {}ns", host, (System.nanoTime() - start));
      return null;
    }
  }

  /** Performs a deep copy on <i>other</i>. */
  public host_port(host_port other) {
    this.hostAddress = other.hostAddress;
    this.port = other.port;
  }

  public host_port deepCopy() {
    return new host_port(this);
  }

  @Override
  public void clear() {
    this.hostAddress = null;
    this.port = 0;
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    }
    throw new IllegalStateException();
  }

  /**
   * Returns true if field corresponding to fieldID is set (has been asigned a value) and false
   * otherwise
   */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null) return false;
    if (that instanceof host_port) return this.equals((host_port) that);
    return false;
  }

  public boolean equals(host_port that) {
    if (that == null) return false;
    return this.hostAddress.equals(that.hostAddress) && this.port == that.port;
  }

  @Override
  public int hashCode() {
    int result = 17;
    result = 31 * result + (hostAddress != null ? hostAddress.hashCode() : 0);
    result = 31 * result + port;
    return result;
  }

  public int compareTo(host_port other) {
    if (other == null) {
      return 1;
    }

    int comparsion = this.hostAddress.compareTo(other.hostAddress);
    if (comparsion == 0) {
      return comparsion;
    }

    return Integer.compare(this.port, other.port);
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(TProtocol iprot) throws TException {
    port = iprot.readI16();
    hostAddress = iprot.readString();
    // check for required fields of primitive type, which can't be checked in the validate method
    validate();
  }

  public void write(TProtocol oprot) throws TException {
    validate();
    oprot.writeString(hostAddress);
    oprot.writeI16(port);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("host_port(");
    sb.append(get_host());
    sb.append(":");
    sb.append(get_port());
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws TException {
    // check for required fields
  }
}
