#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

from aenum import Enum
import six

import socket
import struct
import logging
from thrift.Thrift import TType

from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
try:
  from thrift.protocol import fastbinary
except:
  fastbinary = None


class blob:

  thrift_spec = (
  )

  def read(self, iprot):
    self.data = iprot.readBinary()

  def write(self, oprot):
    oprot.writeBinary(self.data)

  def validate(self):
    return

  def __init__(self, data=None):
    if isinstance(data,str):
        data = data.encode('UTF-8')
    self.data = data

  def __hash__(self):
    value = 17
    return value

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.items()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

  def __len__(self):
      return len(self.data)


class rocksdb_error_types(Enum):
    kOk = 0
    kNotFound = 1
    kCorruption = 2
    kNotSupported = 3
    kInvalidArgument = 4
    kIOError = 5
    kMergeInProgress = 6
    kIncomplete = 7
    kShutdownInProgress = 8
    kTimedOut = 9
    kAborted = 10
    kBusy = 11
    kExpired = 12
    kTryAgain = 13
    kNoNeedOperate = 101


class error_types(Enum):
    ERR_OK = 0
    ERR_UNKNOWN = 1
    ERR_SERVICE_NOT_FOUND = 2
    ERR_SERVICE_ALREADY_RUNNING = 3
    ERR_IO_PENDING = 4
    ERR_TIMEOUT = 5
    ERR_SERVICE_NOT_ACTIVE = 6
    ERR_BUSY = 7
    ERR_NETWORK_INIT_FAILED = 8
    ERR_FORWARD_TO_OTHERS = 9
    ERR_OBJECT_NOT_FOUND = 10
    ERR_HANDLER_NOT_FOUND = 11
    ERR_LEARN_FILE_FAILED = 12
    ERR_GET_LEARN_STATE_FAILED = 13
    ERR_INVALID_VERSION = 14
    ERR_INVALID_PARAMETERS = 15
    ERR_CAPACITY_EXCEEDED = 16
    ERR_INVALID_STATE = 17
    ERR_INACTIVE_STATE = 18
    ERR_NOT_ENOUGH_MEMBER = 19
    ERR_FILE_OPERATION_FAILED = 20
    ERR_HANDLE_EOF = 21
    ERR_WRONG_CHECKSUM = 22
    ERR_INVALID_DATA = 23
    ERR_INVALID_HANDLE = 24
    ERR_INCOMPLETE_DATA = 25
    ERR_VERSION_OUTDATED = 26
    ERR_PATH_NOT_FOUND = 27
    ERR_PATH_ALREADY_EXIST = 28
    ERR_ADDRESS_ALREADY_USED = 29
    ERR_STATE_FREEZED = 30
    ERR_LOCAL_APP_FAILURE = 31
    ERR_BIND_IOCP_FAILED = 32
    ERR_NETWORK_START_FAILED = 33
    ERR_NOT_IMPLEMENTED = 34
    ERR_CHECKPOINT_FAILED = 35
    ERR_WRONG_TIMING = 36
    ERR_NO_NEED_OPERATE = 37
    ERR_CORRUPTION = 38
    ERR_TRY_AGAIN = 39
    ERR_CLUSTER_NOT_FOUND = 40
    ERR_CLUSTER_ALREADY_EXIST = 41
    ERR_SERVICE_ALREADY_EXIST = 42
    ERR_INJECTED = 43
    ERR_REPLICATION_FAILURE = 44
    ERR_APP_EXIST = 45
    ERR_APP_NOT_EXIST = 46
    ERR_BUSY_CREATING = 47
    ERR_BUSY_DROPPING = 48
    ERR_NETWORK_FAILURE = 49
    ERR_UNDER_RECOVERY = 50
    ERR_LEARNER_NOT_FOUND = 51
    ERR_OPERATION_DISABLED = 52
    ERR_EXPIRED = 53
    ERR_LOCK_ALREADY_EXIST = 54
    ERR_HOLD_BY_OTHERS = 55
    ERR_RECURSIVE_LOCK = 56
    ERR_NO_OWNER = 57
    ERR_NODE_ALREADY_EXIST = 58
    ERR_INCONSISTENT_STATE = 59
    ERR_ARRAY_INDEX_OUT_OF_RANGE = 60
    ERR_DIR_NOT_EMPTY = 61
    ERR_FS_INTERNAL = 62
    ERR_IGNORE_BAD_DATA = 63
    ERR_APP_DROPPED = 64
    ERR_MOCK_INTERNAL = 65
    ERR_ZOOKEEPER_OPERATION = 66
    ERR_CHILD_REGISTERED = 67
    ERR_INGESTION_FAILED = 68
    ERR_UNAUTHENTICATED = 69
    ERR_KRB5_INTERNAL = 70
    ERR_SASL_INTERNAL = 71
    ERR_SASL_INCOMPLETE = 72
    ERR_ACL_DENY = 73
    ERR_SPLITTING = 74
    ERR_PARENT_PARTITION_MISUSED = 75
    ERR_CHILD_NOT_READY = 76
    ERR_DISK_INSUFFICIENT = 77
    # ERROR_CODE defined by client
    ERR_SESSION_RESET = 78
    ERR_THREAD_INTERRUPTED = 79

class error_code:

  thrift_spec = (
  )

  def __init__(self, ):
    self.errno = error_types.ERR_UNKNOWN

  @staticmethod
  def value_of(error_name):
    return error_types[error_name]

  def read(self, iprot):
    self.errno = iprot.readString()

  def write(self, oprot):
    oprot.writeString()

  def validate(self):
    return


  def __hash__(self):
    value = 17
    return value

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.items()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class task_code:

  thrift_spec = (
  )

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('task_code')
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __hash__(self):
    value = 17
    return value

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.items()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class rpc_address:

  thrift_spec = (
    (1, TType.I64, 'address', None, None, ), # 1
  )

  def __init__(self):
    self.address = 0

  def is_valid(self):
    return self.address == 0

  def from_string(self, ip_port):
    ip, port = ip_port.split(':')
    self.address = socket.ntohl(struct.unpack("I", socket.inet_aton(ip))[0])
    self.address = (self.address << 32) + (int(port) << 16) + 1     # TODO why + 1?
    return True

  def to_ip_port(self):
    s = []
    address = self.address
    port = (address >> 16) & 0xFFFF
    address = address >> 32
    for i in range(4):
        s.append(str(address & 0xFF))
        address = address >> 8
    host = '.'.join(s[::-1])
    return host, port

  def read(self, iprot):
    self.address = iprot.readI64() & 0xFFFFFFFFFFFFFFFF

  def write(self, oprot):
    oprot.writeI64(self.address)

  def validate(self):
    return

  def __hash__(self):
    return self.address ^ (self.address >> 32)

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.items()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return other.__class__.__name__ == "rpc_address" and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)


# TODO(yingchun): host_port is now just a place holder and not well implemented, need improve it
class host_port_types(Enum):
    kHostTypeInvalid = 0
    kHostTypeIpv4 = 1
    kHostTypeGroup = 2


class host_port:

  thrift_spec = (
    (1, TType.STRING, 'host', None, None, ), # 1
    (2, TType.I16, 'port', None, None, ), # 2
    (3, TType.I08, 'type', None, None, ), # 3
  )

  def __init__(self):
    self.host = ""
    self.port = 0
    self.type = host_port_types.kHostTypeInvalid

  def is_valid(self):
    return self.type != host_port_types.kHostTypeInvalid

  def from_string(self, host_port_str):
    host_and_port = host_port_str.split(':')
    if len(host_and_port) != 2:
        return False
    self.host = host_and_port[0]
    self.port = int(host_and_port[1])
    # TODO(yingchun): Maybe it's not true, improve it
    self.type = host_port_types.kHostTypeIpv4
    return True

  def to_host_port(self):
    if not self.is_valid():
      return None, None
    return self.host, self.port

  def read(self, iprot):
    self.host = iprot.readString()
    self.port = iprot.readI16()
    self.type = iprot.readByte()

  def write(self, oprot):
    oprot.writeString(self.host)
    oprot.writeI16(self.port)
    oprot.writeByte(self.type)

  def validate(self):
    return

  def __hash__(self):
    return hash(self.host) ^ self.port ^ self.type

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.items()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return other.__class__.__name__ == "host_port" and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)


class gpid:

  thrift_spec = (
    (1, TType.I64, 'value', None, None, ), # 1
  )

  def __init__(self, app_id=0, pidx=0):
    self.value = (pidx << 32) + app_id

  def read(self, iprot):
    self.value = iprot.readI64()

  def write(self, oprot):
    oprot.writeI64(self.value)

  def validate(self):
    return

  def __hash__(self):
    return self.value >> 32 ^ self.value & 0x00000000ffffffff

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.items()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

  def get_app_id(self):
    return self.value & 0x00000000ffffffff

  def get_pidx(self):
    return self.value >> 32
