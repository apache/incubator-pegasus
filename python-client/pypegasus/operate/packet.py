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

import struct
import ctypes

from thrift.Thrift import TMessageType

from pypegasus import utils
from pypegasus.rrdb import (
        meta,
        rrdb)
from pypegasus.base.ttypes import (
    rocksdb_error_types,
    error_code,
    gpid)
from pypegasus.utils import tools


class ThriftHeader(object):
    HEADER_LENGTH = 48
    HEADER_TYPE = b'THFT'

    def __init__(self, gpid, partition_hash=0):
        self.hdr_version = 0
        self.header_length = self.HEADER_LENGTH
        self.header_crc32 = 0
        self.body_length = 0
        self.body_crc32 = 0
        self.app_id = gpid.get_app_id()
        self.partition_index = gpid.get_pidx()
        self.client_timeout = 0
        self.thread_hash = 0
        self.partition_hash = partition_hash

    def to_bytes(self):
        v = (self.HEADER_TYPE,
             self.hdr_version,
             self.header_length,
             self.header_crc32,
             self.body_length,
             self.body_crc32,
             self.app_id,
             self.partition_index,
             self.client_timeout,
             self.thread_hash,
             self.partition_hash)
        s = struct.Struct('>4siiiiiiiiiq')
        buff = ctypes.create_string_buffer(s.size)
        s.pack_into(buff, 0, *v)

        return buff


class ClientOperator(object):
    def __init__(self, gpid=gpid(), request=None, partition_hash=0):
        self.pid = gpid
        self.header = ThriftHeader(gpid, partition_hash)
        self.error_code = error_code()
        self.request = request
        self.response = None

    def prepare_thrift_header(self, body_length):
        self.header.body_length = body_length
        self.header.thread_hash = tools.dsn_gpid_to_thread_hash(self.header.app_id, self.header.partition_index)
        return self.header.to_bytes()

    @staticmethod
    def parse_result(resp):
        return resp.error


class QueryCfgOperator(ClientOperator):
    def __init__(self, gpid, request, partition_hash):
        ClientOperator.__init__(self, gpid, request, partition_hash)

    def send_data(self, oprot, seqid):
        oprot.writeMessageBegin("RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX", TMessageType.CALL, seqid)
        args = meta.query_cfg_args(self.request)
        args.write(oprot)
        oprot.writeMessageEnd()

    @staticmethod
    def parse_result(resp):
        return resp


class RrdbTtlOperator(ClientOperator):
    def __init__(self, gpid, request, partition_hash):
        ClientOperator.__init__(self, gpid, request, partition_hash)

    def send_data(self, oprot, seqid):
        oprot.writeMessageBegin("RPC_RRDB_RRDB_TTL", TMessageType.CALL, seqid)
        args = rrdb.get_args(self.request)
        args.write(oprot)
        oprot.writeMessageEnd()

    @staticmethod
    def parse_result(resp):
        resp.error = utils.tools.convert_error_type(resp.error)
        return resp.error, resp.ttl_seconds


class RrdbGetOperator(ClientOperator):
    def __init__(self, gpid, request, partition_hash):
        ClientOperator.__init__(self, gpid, request, partition_hash)

    def send_data(self, oprot, seqid):
        oprot.writeMessageBegin("RPC_RRDB_RRDB_GET", TMessageType.CALL, seqid)
        args = rrdb.get_args(self.request)
        args.write(oprot)
        oprot.writeMessageEnd()

    @staticmethod
    def parse_result(resp):
        resp.error = utils.tools.convert_error_type(resp.error)
        return resp.error, resp.value.data


class RrdbMultiGetOperator(ClientOperator):
    def __init__(self, gpid, request, partition_hash):
        ClientOperator.__init__(self, gpid, request, partition_hash)

    def send_data(self, oprot, seqid):
        oprot.writeMessageBegin("RPC_RRDB_RRDB_MULTI_GET", TMessageType.CALL, seqid)
        args = rrdb.multi_get_args(self.request)
        args.write(oprot)
        oprot.writeMessageEnd()

    @staticmethod
    def parse_result(resp):
        data = {}
        if resp.error == rocksdb_error_types.kOk.value\
           or resp.error == rocksdb_error_types.kIncomplete.value:
            for kv in resp.kvs:
                data[kv.key.data] = kv.value.data

        resp.error = utils.tools.convert_error_type(resp.error)

        return resp.error, data


class RrdbPutOperator(ClientOperator):
    def __init__(self, gpid, request, partition_hash):
        ClientOperator.__init__(self, gpid, request, partition_hash)

    def send_data(self, oprot, seqid):
        oprot.writeMessageBegin("RPC_RRDB_RRDB_PUT", TMessageType.CALL, seqid)
        args = rrdb.put_args(self.request)
        args.write(oprot)
        oprot.writeMessageEnd()

    @staticmethod
    def parse_result(resp):
        return resp.error, None


class RrdbMultiPutOperator(ClientOperator):
    def __init__(self, gpid, request, partition_hash):
        ClientOperator.__init__(self, gpid, request, partition_hash)

    def send_data(self, oprot, seqid):
        oprot.writeMessageBegin("RPC_RRDB_RRDB_MULTI_PUT", TMessageType.CALL, seqid)
        args = rrdb.multi_put_args(self.request)
        args.write(oprot)
        oprot.writeMessageEnd()

    @staticmethod
    def parse_result(resp):
        return resp.error, None


class RrdbRemoveOperator(ClientOperator):
    def __init__(self, gpid, request, partition_hash):
        ClientOperator.__init__(self, gpid, request, partition_hash)

    def send_data(self, oprot, seqid):
        oprot.writeMessageBegin("RPC_RRDB_RRDB_REMOVE", TMessageType.CALL, seqid)
        args = rrdb.remove_args(self.request)
        args.write(oprot)
        oprot.writeMessageEnd()

    @staticmethod
    def parse_result(resp):
        return resp.error, None


class RrdbMultiRemoveOperator(ClientOperator):
    def __init__(self, gpid, request, partition_hash):
        ClientOperator.__init__(self, gpid, request, partition_hash)

    def send_data(self, oprot, seqid):
        oprot.writeMessageBegin("RPC_RRDB_RRDB_MULTI_REMOVE", TMessageType.CALL, seqid)
        args = rrdb.multi_remove_args(self.request)
        args.write(oprot)
        oprot.writeMessageEnd()

    @staticmethod
    def parse_result(resp):
        return resp.error, resp.count


class RrdbSortkeyCountOperator(ClientOperator):
    def __init__(self, gpid, request, partition_hash):
        ClientOperator.__init__(self, gpid, request, partition_hash)

    def send_data(self, oprot, seqid):
        oprot.writeMessageBegin("RPC_RRDB_RRDB_SORTKEY_COUNT", TMessageType.CALL, seqid)
        args = rrdb.sortkey_count_args(self.request)
        args.write(oprot)
        oprot.writeMessageEnd()

    @staticmethod
    def parse_result(resp):
        return resp.error, resp.count


class RrdbGetScannerOperator(ClientOperator):
    def __init__(self, gpid, request, partition_hash):
        ClientOperator.__init__(self, gpid, request, partition_hash)

    def send_data(self, oprot, seqid):
        oprot.writeMessageBegin("RPC_RRDB_RRDB_GET_SCANNER", TMessageType.CALL, seqid)
        args = rrdb.get_scanner_args(self.request)
        args.write(oprot)
        oprot.writeMessageEnd()

    @staticmethod
    def parse_result(resp):
        return {'error': resp.error,
                'context_id': resp.context_id,
                'kvs': resp.kvs}


class RrdbScanOperator(ClientOperator):
    def __init__(self, gpid, request, partition_hash):
        ClientOperator.__init__(self, gpid, request, partition_hash)

    def send_data(self, oprot, seqid):
        oprot.writeMessageBegin("RPC_RRDB_RRDB_SCAN", TMessageType.CALL, seqid)
        args = rrdb.scan_args(self.request)
        args.write(oprot)
        oprot.writeMessageEnd()

    @staticmethod
    def parse_result(resp):
        return {'error': resp.error,
                'context_id': resp.context_id,
                'kvs': resp.kvs}


class RrdbClearScannerOperator(ClientOperator):
    def __init__(self, gpid, request, partition_hash):
        ClientOperator.__init__(self, gpid, request, partition_hash)

    def send_data(self, oprot, seqid):
        oprot.writeMessageBegin("RPC_RRDB_RRDB_CLEAR_SCANNER", TMessageType.CALL, seqid)
        args = rrdb.clear_scanner_args(self.request)
        args.write(oprot)
        oprot.writeMessageEnd()
