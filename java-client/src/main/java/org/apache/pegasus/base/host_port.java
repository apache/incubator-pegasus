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

import org.apache.thrift.TBase;

public class host_port
    implements TBase<host_port, host_port._Fields>,
        java.io.Serializable,
        Cloneable,
        Comparable<host_port> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC =
      new org.apache.thrift.protocol.TStruct("host_port");

  public String host;

  public short port;

  public byte hostPortType;

  /**
   * The set of fields this struct contains, along with convenience methods for finding and
   * manipulating them.
   */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    ;

    private static final java.util.Map<java.lang.String, _Fields> byName =
        new java.util.HashMap<java.lang.String, _Fields>();

    static {
      for (_Fields field : java.util.EnumSet.allOf(_Fields.class)) {
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
        throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /** Find the _Fields constant that matches name, or null if its not found. */
    public static _Fields findByName(java.lang.String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final java.lang.String _fieldName;

    _Fields(short thriftId, java.lang.String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public java.lang.String getFieldName() {
      return _fieldName;
    }
  }

  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;

  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap =
        new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(host_port.class, metaDataMap);
  }

  public host_port() {
    this.host = null;
    this.port = 0;
    this.hostPortType = 0;
  }

  /** Performs a deep copy on <i>other</i>. */
  public host_port(host_port other) {
    this.host = other.host;
    this.port = other.port;
    this.hostPortType = other.hostPortType;
  }

  public host_port deepCopy() {
    return new host_port(this);
  }

  public String getHost() {
    return this.host;
  }

  public int getPort() {
    return (int) (this.port & 0xffff);
  }

  public byte getHostPortType() {
    return this.hostPortType;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public void setPort(int port) {
    this.port = (short) port;
  }

  public void setHostPortType(byte hostPortType) {
    this.hostPortType = hostPortType;
  }

  @Override
  public void clear() {
    this.host = null;
    this.port = 0;
    this.hostPortType = 0;
  }

  public void setFieldValue(_Fields field, java.lang.Object value) {
    switch (field) {
    }
  }

  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    }
    throw new java.lang.IllegalStateException();
  }

  /**
   * Returns true if field corresponding to fieldID is set (has been assigned a value) and false
   * otherwise
   */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that == null) return false;
    if (that instanceof host_port) return this.equals((host_port) that);
    return false;
  }

  public boolean equals(host_port that) {
    if (that == null) return false;

    return this.host.equals(that.host)
        && this.port == that.port
        && this.hostPortType == that.hostPortType;
  }

  @Override
  public int hashCode() {
    int result = 17;
    result = 31 * result + (host != null ? host.hashCode() : 0);
    result = 31 * result + port;
    result = 31 * result + hostPortType;
    return result;
  }

  @Override
  public int compareTo(host_port other) {
    if (other == null) {
      throw new NullPointerException();
    }

    int lastComparison = 0;

    lastComparison = this.host.compareTo(other.host);
    if (lastComparison != 0) {
      return lastComparison;
    }

    lastComparison = Integer.compare(this.port, other.port);
    if (lastComparison != 0) {
      return lastComparison;
    }

    return Byte.compare(this.hostPortType, other.hostPortType);
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    host = iprot.readString();
    port = iprot.readI16();
    hostPortType = iprot.readByte();
    validate();
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot)
      throws org.apache.thrift.TException {
    validate();
    oprot.writeString(host);
    oprot.writeI16(port);
    oprot.writeByte(hostPortType);
  }

  @Override
  public java.lang.String toString() {
    java.lang.StringBuilder sb = new java.lang.StringBuilder("host_port(");
    sb.append(getHost());
    sb.append(":");
    sb.append(getPort());
    sb.append(", HostPortType = ");
    sb.append(getHostPortType());
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
  }
}
