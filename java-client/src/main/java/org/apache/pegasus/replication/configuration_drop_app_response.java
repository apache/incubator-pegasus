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
 * Autogenerated by Thrift Compiler (0.11.0)
 *
 * <p>DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *
 * @generated
 */
package org.apache.pegasus.replication;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(
    value = "Autogenerated by Thrift Compiler (0.11.0)",
    date = "2022-05-17")
public class configuration_drop_app_response
    implements org.apache.thrift.TBase<
            configuration_drop_app_response, configuration_drop_app_response._Fields>,
        java.io.Serializable,
        Cloneable,
        Comparable<configuration_drop_app_response> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC =
      new org.apache.thrift.protocol.TStruct("configuration_drop_app_response");

  private static final org.apache.thrift.protocol.TField ERR_FIELD_DESC =
      new org.apache.thrift.protocol.TField(
          "err", org.apache.thrift.protocol.TType.STRUCT, (short) 1);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY =
      new configuration_drop_app_responseStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY =
      new configuration_drop_app_responseTupleSchemeFactory();

  public org.apache.pegasus.base.error_code err; // required

  /**
   * The set of fields this struct contains, along with convenience methods for finding and
   * manipulating them.
   */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    ERR((short) 1, "err");

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
        case 1: // ERR
          return ERR;
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

  // isset id assignments
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;

  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap =
        new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(
        _Fields.ERR,
        new org.apache.thrift.meta_data.FieldMetaData(
            "err",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.StructMetaData(
                org.apache.thrift.protocol.TType.STRUCT,
                org.apache.pegasus.base.error_code.class)));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(
        configuration_drop_app_response.class, metaDataMap);
  }

  public configuration_drop_app_response() {}

  public configuration_drop_app_response(org.apache.pegasus.base.error_code err) {
    this();
    this.err = err;
  }

  /** Performs a deep copy on <i>other</i>. */
  public configuration_drop_app_response(configuration_drop_app_response other) {
    if (other.isSetErr()) {
      this.err = new org.apache.pegasus.base.error_code(other.err);
    }
  }

  public configuration_drop_app_response deepCopy() {
    return new configuration_drop_app_response(this);
  }

  @Override
  public void clear() {
    this.err = null;
  }

  public org.apache.pegasus.base.error_code getErr() {
    return this.err;
  }

  public configuration_drop_app_response setErr(org.apache.pegasus.base.error_code err) {
    this.err = err;
    return this;
  }

  public void unsetErr() {
    this.err = null;
  }

  /** Returns true if field err is set (has been assigned a value) and false otherwise */
  public boolean isSetErr() {
    return this.err != null;
  }

  public void setErrIsSet(boolean value) {
    if (!value) {
      this.err = null;
    }
  }

  public void setFieldValue(_Fields field, java.lang.Object value) {
    switch (field) {
      case ERR:
        if (value == null) {
          unsetErr();
        } else {
          setErr((org.apache.pegasus.base.error_code) value);
        }
        break;
    }
  }

  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
      case ERR:
        return getErr();
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
      case ERR:
        return isSetErr();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that == null) return false;
    if (that instanceof configuration_drop_app_response)
      return this.equals((configuration_drop_app_response) that);
    return false;
  }

  public boolean equals(configuration_drop_app_response that) {
    if (that == null) return false;
    if (this == that) return true;

    boolean this_present_err = true && this.isSetErr();
    boolean that_present_err = true && that.isSetErr();
    if (this_present_err || that_present_err) {
      if (!(this_present_err && that_present_err)) return false;
      if (!this.err.equals(that.err)) return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((isSetErr()) ? 131071 : 524287);
    if (isSetErr()) hashCode = hashCode * 8191 + err.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(configuration_drop_app_response other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.valueOf(isSetErr()).compareTo(other.isSetErr());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetErr()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.err, other.err);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    scheme(iprot).read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot)
      throws org.apache.thrift.TException {
    scheme(oprot).write(oprot, this);
  }

  @Override
  public java.lang.String toString() {
    java.lang.StringBuilder sb = new java.lang.StringBuilder("configuration_drop_app_response(");
    boolean first = true;

    sb.append("err:");
    if (this.err == null) {
      sb.append("null");
    } else {
      sb.append(this.err);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
    if (err != null) {
      err.validate();
    }
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(
          new org.apache.thrift.protocol.TCompactProtocol(
              new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in)
      throws java.io.IOException, java.lang.ClassNotFoundException {
    try {
      read(
          new org.apache.thrift.protocol.TCompactProtocol(
              new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class configuration_drop_app_responseStandardSchemeFactory
      implements org.apache.thrift.scheme.SchemeFactory {
    public configuration_drop_app_responseStandardScheme getScheme() {
      return new configuration_drop_app_responseStandardScheme();
    }
  }

  private static class configuration_drop_app_responseStandardScheme
      extends org.apache.thrift.scheme.StandardScheme<configuration_drop_app_response> {

    public void read(
        org.apache.thrift.protocol.TProtocol iprot, configuration_drop_app_response struct)
        throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true) {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) {
          break;
        }
        switch (schemeField.id) {
          case 1: // ERR
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.err = new org.apache.pegasus.base.error_code();
              struct.err.read(iprot);
              struct.setErrIsSet(true);
            } else {
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      struct.validate();
    }

    public void write(
        org.apache.thrift.protocol.TProtocol oprot, configuration_drop_app_response struct)
        throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.err != null) {
        oprot.writeFieldBegin(ERR_FIELD_DESC);
        struct.err.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }
  }

  private static class configuration_drop_app_responseTupleSchemeFactory
      implements org.apache.thrift.scheme.SchemeFactory {
    public configuration_drop_app_responseTupleScheme getScheme() {
      return new configuration_drop_app_responseTupleScheme();
    }
  }

  private static class configuration_drop_app_responseTupleScheme
      extends org.apache.thrift.scheme.TupleScheme<configuration_drop_app_response> {

    @Override
    public void write(
        org.apache.thrift.protocol.TProtocol prot, configuration_drop_app_response struct)
        throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot =
          (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetErr()) {
        optionals.set(0);
      }
      oprot.writeBitSet(optionals, 1);
      if (struct.isSetErr()) {
        struct.err.write(oprot);
      }
    }

    @Override
    public void read(
        org.apache.thrift.protocol.TProtocol prot, configuration_drop_app_response struct)
        throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot =
          (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet incoming = iprot.readBitSet(1);
      if (incoming.get(0)) {
        struct.err = new org.apache.pegasus.base.error_code();
        struct.err.read(iprot);
        struct.setErrIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(
      org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme())
            ? STANDARD_SCHEME_FACTORY
            : TUPLE_SCHEME_FACTORY)
        .getScheme();
  }
}
