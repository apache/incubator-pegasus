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
/**
 * Autogenerated by Thrift
 *
 * <p>DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package org.apache.pegasus.base;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TStruct;

public final class rpc_address
    implements TBase<rpc_address, rpc_address._Fields>, java.io.Serializable, Cloneable {
  private static final TStruct STRUCT_DESC = new TStruct("rpc_address");

  public long address;

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
    FieldMetaData.addStructMetaDataMap(rpc_address.class, metaDataMap);
  }

  public rpc_address() {
    this.address = 0;
  }

  public boolean isInvalid() {
    return this.address == 0;
  }

  public String get_ip() throws UnknownHostException {
    byte[] byte_array =
        new byte[] {
          (byte) (0xff & (address >> 56)),
          (byte) (0xff & (address >> 48)),
          (byte) (0xff & (address >> 40)),
          (byte) (0xff & (address >> 32))
        };
    return InetAddress.getByAddress(byte_array).getHostAddress();
  }

  public int get_port() {
    return (int) (0xffff & (address >> 16));
  }

  // get rpc_address from the format xx.xx.xx.xx:abcd
  public boolean fromString(String ipPort) {
    String[] pairs = ipPort.split(":");

    int ip;
    if (pairs.length != 2) {
      return false;
    }

    try {
      // TODO(wutao1): getByName will query DNS if the given address is not valid ip:port.
      byte[] byteArray = InetAddress.getByName(pairs[0]).getAddress();
      ip = ByteBuffer.wrap(byteArray).order(ByteOrder.BIG_ENDIAN).getInt();
    } catch (UnknownHostException e) {
      return false;
    }

    Integer port = Integer.valueOf(pairs[1]);
    address = ((long) ip << 32) + ((long) port << 16) + 1;
    return true;
  }

  public static rpc_address fromIpPort(String ipPort) {
    rpc_address addr = new rpc_address();
    return addr.fromString(ipPort) ? addr : null;
  }

  /** Performs a deep copy on <i>other</i>. */
  public rpc_address(rpc_address other) {
    this.address = other.address;
  }

  public rpc_address deepCopy() {
    return new rpc_address(this);
  }

  @Override
  public void clear() {
    this.address = 0;
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
    if (that instanceof rpc_address) return this.equals((rpc_address) that);
    return false;
  }

  public boolean equals(rpc_address that) {
    if (that == null) return false;
    return this.address == that.address;
  }

  @Override
  public int hashCode() {
    return (int) (address ^ (address >>> 32));
  }

  public int compareTo(rpc_address other) {
    if (address < other.address) return -1;
    if (address > other.address) return 1;
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(TProtocol iprot) throws TException {
    address = iprot.readI64();
    // check for required fields of primitive type, which can't be checked in the validate method
    validate();
  }

  public void write(TProtocol oprot) throws TException {
    validate();
    oprot.writeI64(address);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("rpc_address(");
    try {
      sb.append(get_ip());
    } catch (UnknownHostException e) {
      sb.append("invalid_addr");
    }
    sb.append(":");
    sb.append(String.valueOf(get_port()));
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws TException {
    // check for required fields
  }
}
