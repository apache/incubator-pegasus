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

from __future__ import print_function
from __future__ import with_statement

import os
import logging.config
import six

from thrift.Thrift import TMessageType, TApplicationException
from twisted.internet import defer
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, succeed, fail
from twisted.internet.protocol import ClientCreator

from pypegasus import rrdb, replication
from pypegasus.base.ttypes import *
from pypegasus.operate.packet import *
from pypegasus.replication.ttypes import query_cfg_request
from pypegasus.rrdb import *
from pypegasus.rrdb.ttypes import scan_request, get_scanner_request, update_request, key_value, multi_put_request, \
    multi_get_request, multi_remove_request
from pypegasus.transport.protocol import *
from pypegasus.utils.tools import restore_key, get_ttl, bytes_cmp, ScanOptions

try:
    from thrift.protocol import fastbinary
except:
    fastbinary = None


logging.config.fileConfig(os.path.dirname(__file__)+"/logger.conf")
logger = logging.getLogger("pgclient")

DEFAULT_TIMEOUT = 2000               # ms
META_CHECK_INTERVAL = 2              # s
MAX_TIMEOUT_THRESHOLD = 5            # times


class BaseSession(object):

    def __init__(self, transport, oprot_factory, container, timeout):
        self._transport = transport                     # TPegasusTransport
        self._oprot_factory = oprot_factory
        self._container = container
        self._seqid = 0
        self._requests = {}
        self._default_timeout = timeout

    def __del__(self):
       self.close()

    def get_peer_addr(self):
        return self._transport.get_peer_addr()

    def cb_send(self, _, seqid):
        return self._requests[seqid]

    def eb_send(self, f, seqid):
        d = self._requests.pop(seqid)
        d.errback(f)
        logger.warning('peer: %s, failure: %s',
                       self.get_peer_addr(), f)
        return d

    def eb_recv(self, f):
        logger.warning('peer: %s, failure: %s',
                       self.get_peer_addr(), f)

    def on_timeout(self, _, seconds):
        ec = error_types.ERR_TIMEOUT
        self._container.update_state(ec)
        logger.warning('peer: %s, time: %s s, timeout',
                       self.get_peer_addr(), seconds)
        return ec.value, None

    def operate(self, op, timeout=None):
        if not isinstance(timeout, int) or timeout <= 0:
            timeout = self._default_timeout

        seqid = self._seqid = self._seqid + 1           # TODO should keep atomic
        dr = defer.Deferred()
        dr.addErrback(self.eb_recv)
        self._requests[seqid] = dr

        # ds(deferred send) will wait dr(deferred receive)
        ds = defer.maybeDeferred(self.send_req, op, seqid)
        ds.addCallbacks(
            callback=self.cb_send,
            callbackArgs=(seqid,),
            errback=self.eb_send,
            errbackArgs=(seqid,))
        ds.addTimeout(timeout/1000.0, reactor, self.on_timeout)
        return ds

    def send_req(self, op, seqid):
        oprot = self._oprot_factory.getProtocol(self._transport)
        oprot.trans.seek(ThriftHeader.HEADER_LENGTH)                    # skip header
        op.send_data(oprot, seqid)
        body_length = oprot.trans.tell() - ThriftHeader.HEADER_LENGTH
        oprot.trans.seek(0)                                             # back to header
        oprot.trans.write(op.prepare_thrift_header(body_length))
        oprot.trans.flush()

    def recv_ACK(self, iprot, mtype, rseqid, errno, result_type, parser):
        if rseqid not in self._requests:
            logger.warning('peer: %s rseqid: %s not found',
                           self.get_peer_addr(), rseqid)
            return
        d = self._requests.pop(rseqid)
        if errno != 'ERR_OK':
            rc = error_code.value_of(errno)
            self._container.update_state(rc)
            return d.callback(rc)
        else:
            if mtype == TMessageType.EXCEPTION:
                x = TApplicationException()
                x.read(iprot)
                iprot.readMessageEnd()
                return d.errback(x)

            result = result_type()
            result.read(iprot)
            if result.success:
                return d.callback(parser(result.success))

            return d.errback(TApplicationException(TApplicationException.MISSING_RESULT,
                                                   "%s failed: unknown result" %
                                                   getattr(result_type, '__name__')))

    def close(self):
        self._transport.close()


class MetaSession(BaseSession):

    def __init__(self, transport, oprot_factory, container, timeout):
        BaseSession.__init__(self, transport, oprot_factory, container, timeout)

    def recv_RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX_ACK(self, iprot, mtype, rseqid, errno):
        self.recv_ACK(iprot, mtype, rseqid, errno,
                      meta.query_cfg_result,
                      QueryCfgOperator.parse_result)


class ReplicaSession(BaseSession):

    def __init__(self, transport, oprot_factory, container, timeout):
        BaseSession.__init__(self, transport, oprot_factory, container, timeout)

    def recv_RPC_RRDB_RRDB_PUT_ACK(self, iprot, mtype, rseqid, errno):
        self.recv_ACK(iprot, mtype, rseqid, errno,
                      rrdb.put_result, RrdbPutOperator.parse_result)

    def recv_RPC_RRDB_RRDB_TTL_ACK(self, iprot, mtype, rseqid, errno):
        self.recv_ACK(iprot, mtype, rseqid, errno,
                      rrdb.ttl_result,
                      RrdbTtlOperator.parse_result)

    def recv_RPC_RRDB_RRDB_GET_ACK(self, iprot, mtype, rseqid, errno):
        self.recv_ACK(iprot, mtype, rseqid, errno,
                      rrdb.get_result,
                      RrdbGetOperator.parse_result)

    def recv_RPC_RRDB_RRDB_REMOVE_ACK(self, iprot, mtype, rseqid, errno):
        self.recv_ACK(iprot, mtype, rseqid, errno,
                      rrdb.put_result,
                      RrdbRemoveOperator.parse_result)

    def recv_RPC_RRDB_RRDB_SORTKEY_COUNT_ACK(self, iprot, mtype, rseqid, errno):
        self.recv_ACK(iprot, mtype, rseqid, errno,
                      rrdb.sortkey_count_result,
                      RrdbSortkeyCountOperator.parse_result)

    def recv_RPC_RRDB_RRDB_MULTI_PUT_ACK(self, iprot, mtype, rseqid, errno):
        self.recv_ACK(iprot, mtype, rseqid, errno,
                      rrdb.put_result,
                      RrdbMultiPutOperator.parse_result)

    def recv_RPC_RRDB_RRDB_MULTI_GET_ACK(self, iprot, mtype, rseqid, errno):
        self.recv_ACK(iprot, mtype, rseqid, errno,
                      rrdb.multi_get_result,
                      RrdbMultiGetOperator.parse_result)

    def recv_RPC_RRDB_RRDB_MULTI_REMOVE_ACK(self, iprot, mtype, rseqid, errno):
        self.recv_ACK(iprot, mtype, rseqid, errno,
                      rrdb.multi_remove_result,
                      RrdbMultiRemoveOperator.parse_result)

    def recv_RPC_RRDB_RRDB_GET_SCANNER_ACK(self, iprot, mtype, rseqid, errno):
        self.recv_ACK(iprot, mtype, rseqid, errno,
                      rrdb.get_scanner_result,
                      RrdbGetScannerOperator.parse_result)

    def recv_RPC_RRDB_RRDB_SCAN_ACK(self, iprot, mtype, rseqid, errno):
        self.recv_ACK(iprot, mtype, rseqid, errno,
                      rrdb.scan_result,
                      RrdbScanOperator.parse_result)


class SessionManager(object):
    def __init__(self, name, timeout):
        self.name = name
        self.session_dict = {}                                      # rpc_addr => session
        self.timeout = timeout

    def __del__(self):
        self.close()

    def got_conn(self, conn):
        addr = rpc_address()
        addr.from_string(conn.transport.addr[0] + ':' + str(conn.transport.addr[1]))
        self.session_dict[addr] = conn.client
        return conn.client

    def got_err(self, err):
        logger.error('table: %s, connect err: %s',
                     self.name, err)

    def close(self):
        for session in self.session_dict.values():
            session.close()

    def update_state(self, ec):
        pass


class MetaSessionManager(SessionManager):

    def __init__(self, table_name, timeout):
        SessionManager.__init__(self, table_name, timeout)
        self.addr_list = []

    def add_meta_server(self, meta_addr):
        rpc_addr = rpc_address()
        if rpc_addr.from_string(meta_addr):
            ip_port = meta_addr.split(':')
            if not len(ip_port) == 2:
                return False

            ip, port = ip_port[0], int(ip_port[1])
            self.addr_list.append((ip, port))

            return True
        else:
            return False

    @inlineCallbacks
    def query_one(self, session):
        req = query_cfg_request(self.name, [])
        op = QueryCfgOperator(gpid(0, 0), req, 0)
        ret = yield session.operate(op)
        defer.returnValue(ret)

    def got_results(self, res):
        for (suc, result) in res:
            if suc                                                    \
               and result.__class__.__name__ == "query_cfg_response"  \
               and result.is_stateful:
                logger.info('table: %s, partition result: %s',
                            self.name, result)
                return result

        logger.error('query partition info err. table: %s err: %s',
                     self.name, res)

    def query(self):
        ds = []
        for (ip, port) in self.addr_list:
            rpc_addr = rpc_address()
            rpc_addr.from_string(ip + ':' + str(port))
            if rpc_addr in self.session_dict:
                self.session_dict[rpc_addr].close()

            d = ClientCreator(reactor,
                              TPegasusThriftClientProtocol,
                              MetaSession,
                              TBinaryProtocol.TBinaryProtocolFactory(),
                              None,
                              self,
                              self.timeout
                              ).connectTCP(ip, port, self.timeout)
            d.addCallbacks(self.got_conn, self.got_err)
            d.addCallbacks(self.query_one, self.got_err)
            ds.append(d)

        dlist = defer.DeferredList(ds, consumeErrors=True)
        dlist.addCallback(self.got_results)
        return dlist


class Table(SessionManager):
    def __init__(self, name, container, timeout):
        SessionManager.__init__(self, name, timeout)
        self.app_id = 0
        self.partition_count = 0
        self.query_cfg_response = None
        self.partition_dict = {}        # partition_index => rpc_addr
        self.partition_ballot = {}      # partition_index => ballot
        self.container = container

    def got_results(self, res):
        logger.info('table: %s, replica session: %s',
                    self.name, res)
        return True

    def update_cfg(self, resp):
        if resp.__class__.__name__ != "query_cfg_response":
            logger.error('table: %s, query_cfg_response is error',
                         self.name)
            return None

        self.query_cfg_response = resp
        self.app_id = self.query_cfg_response.app_id
        self.partition_count = self.query_cfg_response.partition_count

        ds = []
        connected_rpc_addrs = {}
        for partition in self.query_cfg_response.partitions:
            rpc_addr = partition.primary
            self.partition_dict[partition.pid.get_pidx()] = rpc_addr
            self.partition_ballot[partition.pid.get_pidx()] = partition.ballot

            # table is partition split, and child partition is not ready
            # child requests should be redirected to its parent partition
            # this will be happened when query meta is called during partition split
            if partition.ballot < 0:
                continue

            if rpc_addr in connected_rpc_addrs or rpc_addr.address == 0:
                continue

            ip, port = rpc_addr.to_ip_port()
            if rpc_addr in self.session_dict:
                self.session_dict[rpc_addr].close()

            d = ClientCreator(reactor,
                              TPegasusThriftClientProtocol,
                              ReplicaSession,
                              TBinaryProtocol.TBinaryProtocolFactory(),
                              None,
                              self.container,
                              self.timeout
                              ).connectTCP(ip, port, self.timeout)
            connected_rpc_addrs[rpc_addr] = 1
            d.addCallbacks(self.got_conn, self.got_err)
            ds.append(d)

        dlist = defer.DeferredList(ds, consumeErrors=True)
        dlist.addCallback(self.got_results)
        return dlist

    def get_hashkey_hash(self, hash_key):
        if six.PY3 and isinstance(hash_key, six.string_types):
            hash_key = hash_key.encode("UTF-8")
        return PegasusHash.default_hash(hash_key)

    def get_blob_hash(self, blob_key):
        return PegasusHash.hash(blob_key)

    def get_gpid_by_hash(self, partition_hash):
        pidx = partition_hash % self.get_partition_count()
        if self.partition_ballot[pidx] < 0:
            logger.warn("table[%s] partition[%d] is not ready, requests will send to parent partition[%d]", 
                self.name, 
                pidx, 
                pidx - int(self.partition_count / 2))
            pidx -= int(self.partition_count / 2)
        return gpid(self.app_id, pidx)

    def get_all_gpid(self):
        return [gpid(self.app_id, pidx)
                for pidx in range(self.get_partition_count())]

    def get_partition_count(self):
        return self.query_cfg_response.partition_count

    def get_session(self, peer_gpid):
        pidx = peer_gpid.get_pidx()
        if pidx in self.partition_dict.keys():
            replica_addr = self.partition_dict[pidx]
            if replica_addr in self.session_dict.keys():
                return self.session_dict[replica_addr]

        self.container.update_state(error_types.ERR_OBJECT_NOT_FOUND)
        return None


class PegasusScanner(object):
    """
    Pegasus scanner class, used for scanning data in pegasus table.
    """

    CONTEXT_ID_VALID_MIN = 0
    CONTEXT_ID_COMPLETED = -1
    CONTEXT_ID_NOT_EXIST = -2

    def __init__(self, table, gpid_list, scan_options, partition_hash_list, check_hash,
                 start_key=blob(b'\x00\x00'), stop_key=blob(b'\xFF\xFF')):
        self._table = table
        self._gpid = gpid(0)
        self._gpid_list = gpid_list
        self._scan_options = scan_options
        self._start_key = start_key
        self._stop_key = stop_key
        self._p = -1
        self._context_id = self.CONTEXT_ID_COMPLETED
        self._kvs = []
        self._partition_hash = 0
        self._partition_hash_list = partition_hash_list
        self._check_hash = check_hash

    def __repr__(self):
        lst = ['%s=%r' % (key, value)
               for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(lst))

    @inlineCallbacks
    def get_next(self):
        """
        scan the next k-v pair for the scanner.
        :return: (tuple<tuple<hash_key, sort_key>, value> or None)
                 all the sort_keys returned by this API are in ascend order.
        """
        self._p += 1
        while self._p >= len(self._kvs):
            if self._context_id == self.CONTEXT_ID_COMPLETED:
                # reach the end of one partition
                if len(self._gpid_list) == 0:
                    defer.returnValue(None)
                else:
                    self._gpid = self._gpid_list.pop()
                    self._partition_hash = self._partition_hash_list.pop()
                    self.split_reset()
            elif self._context_id == self.CONTEXT_ID_NOT_EXIST:
                # no valid context_id found
                yield self.start_scan()
            else:
                ret = yield self.next_batch()
                if ret == 1:
                    # context not found
                    self._context_id = self.CONTEXT_ID_NOT_EXIST
                elif ret != 0:
                    raise Exception("Rocksdb error: " + ret)

            self._p += 1

        defer.returnValue((restore_key(self._kvs[self._p].key.data),
                          self._kvs[self._p].value.data))

    def split_reset(self):
        self._kvs = []
        self._p = -1
        self._context_id = self.CONTEXT_ID_NOT_EXIST

    def scan_cb(self, ctx):
        if isinstance(ctx, dict) and ctx['error'] == 0:
            self._kvs = ctx['kvs']
            self._p = -1
            self._context_id = ctx['context_id']

            return ctx['error']
        else:
            raise Exception('operate error!')

    def scan_err_cb(self, err):
        logger.error('scan table: %s start_key: %s stop_key: %s is error: %s',
                     self._table.name, self._start_key, self._stop_key, err)
        return err

    def start_scan(self):
        request = get_scanner_request()
        if len(self._kvs) == 0:
            request.start_key = self._start_key
            request.start_inclusive = self._scan_options.start_inclusive
        else:
            request.start_key = self._kvs[-1].key
            request.start_inclusive = False

        request.stop_key = self._stop_key
        request.stop_inclusive = self._scan_options.stop_inclusive
        request.batch_size = self._scan_options.batch_size
        request.need_check_hash = self._check_hash

        op = RrdbGetScannerOperator(self._gpid, request, self._partition_hash)
        session = self._table.get_session(self._gpid)
        if not session or not op:
            raise Exception('session or packet error!')

        ret = session.operate(op, self._scan_options.timeout_millis)
        ret.addCallbacks(self.scan_cb, self.scan_err_cb)
        return ret

    def next_batch(self):
        request = scan_request(self._context_id)
        op = RrdbScanOperator(self._gpid, request, self._partition_hash)
        session = self._table.get_session(self._gpid)
        if not session or not op:
            raise Exception('session or packet error!')

        ret = session.operate(op, self._scan_options.timeout_millis)
        ret.addCallbacks(self.scan_cb, self.scan_err_cb)
        return ret

    def close(self):
        if self._context_id >= self.CONTEXT_ID_VALID_MIN:
            op = RrdbClearScannerOperator(self._gpid, self._context_id, self._partition_hash)
            session = self._table.get_session(self._gpid)
            self._context_id = self.CONTEXT_ID_COMPLETED
            if not session or not op:
                raise Exception('session or packet error!')

            session.operate(op, self._scan_options.timeout_millis)


class PegasusHash(object):
    polynomial, = struct.unpack('<q', struct.pack('<Q', 0x9a6c9329ac4bc9b5))
    table_forward = [0] * 256

    @classmethod
    def unsigned_right_shift(cls, val, n):
        if val >= 0:
            val >>= n
        else:
            val = ((val + 0x10000000000000000) >> n)
        return val

    @classmethod
    def populate_table(cls):
        for i in range(256):
            crc = i
            for j in range(8):
                if crc & 1:
                    crc = cls.unsigned_right_shift(crc, 1)
                    crc ^= cls.polynomial
                else:
                    crc = cls.unsigned_right_shift(crc, 1)
            cls.table_forward[i] = crc

    @classmethod
    def crc64(cls, data, offset, length):
        crc = 0xffffffffffffffff
        end = offset + length

        for c in data[offset:end:1]:
                crc = cls.table_forward[(c ^ crc) & 0xFF] ^ cls.unsigned_right_shift(crc, 8)
        return ~crc

    @classmethod
    def default_hash(cls, hash_key):
        return cls.crc64(hash_key, 0, len(hash_key))

    @classmethod
    def hash(cls, blob_key):
        assert blob_key is not None and len(blob_key) >= 2, 'blob_key is invalid!'

        # hash_key_len is in big endian
        s = struct.Struct('>H')
        hash_key_len = s.unpack(blob_key.data[:2])[0]

        assert hash_key_len != 0xFFFF and (2 + hash_key_len <= len(blob_key)), 'blob_key hash_key_len is invalid!'
        if hash_key_len == 0:
            return cls.crc64(blob_key.data, 2, len(blob_key) - 2)
        else:
            return cls.crc64(blob_key.data, 2, hash_key_len)


class Pegasus(object):
    """
    Pegasus client class.
    """

    @classmethod
    def generate_key(cls, hash_key, sort_key):
        # assert(len(hash_key) < sys.maxsize > 1)
        if six.PY3 and isinstance(hash_key, six.string_types):
            hash_key = hash_key.encode("UTF-8")
        if six.PY3 and isinstance(sort_key, six.string_types):
            sort_key = sort_key.encode("UTF-8")

        # hash_key_len is in big endian
        hash_key_len = len(hash_key)
        sort_key_len = len(sort_key)

        if sort_key_len > 0:
            values = (hash_key_len, hash_key, sort_key)
            s = struct.Struct('>H'+str(hash_key_len)+'s'+str(sort_key_len)+'s')
        else:
            values = (hash_key_len, hash_key)
            s = struct.Struct('>H'+str(hash_key_len)+'s')

        buff = ctypes.create_string_buffer(s.size)
        s.pack_into(buff, 0, *values)

        return blob(buff)

    @classmethod
    def generate_next_bytes(cls, buff):
        pos = len(buff) - 1
        found = False
        while pos >= 0:
            if ord(buff[pos]) != 0xFF:
                buff[pos] += 1
                found = True
                break
        if found:
            return buff
        else:
            return buff + chr(0)

    @classmethod
    def generate_stop_key(cls, hash_key, stop_sort_key):
        if stop_sort_key:
            return cls.generate_key(hash_key, stop_sort_key), True
        else:
            return cls.generate_next_bytes(hash_key), False

    def __init__(self, meta_addrs=None, table_name='',
                 timeout=DEFAULT_TIMEOUT):
        """
        :param meta_addrs: (list) pagasus meta servers list.
                           example: ['127.0.0.1:34601', '127.0.0.1:34602', '127.0.0.1:34603']
        :param table_name: (str) table name/app name used in pegasus.
        :param timeout: (int) default timeout in milliseconds when communicate with meta sever and replica server.
        """
        self.name = table_name
        self.table = Table(table_name, self, timeout)
        self.meta_session_manager = MetaSessionManager(table_name, timeout)
        if isinstance(meta_addrs, list):
            for meta_addr in meta_addrs:
                self.meta_session_manager.add_meta_server(meta_addr)
        PegasusHash.populate_table()
        self.timeout_times = 0
        self.update_partition = False
        self.timer = reactor.callLater(META_CHECK_INTERVAL, self.check_state)

    def init(self):
        """
        Initialize the client before you can use it.

        :return: (DeferredList) True when initialized succeed, others when failed.
        """
        dlist = self.meta_session_manager.query()
        dlist.addCallback(self.table.update_cfg)
        return dlist

    def close(self):
        """
        Close the client. The client can not be used again after closed.
        """
        self.timer.cancel()
        self.table.close()
        self.meta_session_manager.close()

    @inlineCallbacks
    def check_state(self):
        logger.info('table: %s, checking meta ...',
                    self.name)
        if self.update_partition:
            self.update_partition = False
            yield self.init()
        self.timer = reactor.callLater(META_CHECK_INTERVAL, self.check_state)

    def update_state(self, ec):
        if ec == error_types.ERR_TIMEOUT:
            self.timeout_times += 1
            if self.timeout_times >= MAX_TIMEOUT_THRESHOLD:
                self.update_partition = True
                self.timeout_times = 0
        elif ec == error_types.ERR_INVALID_DATA:
            logger.error('table: %s, ignore ec: %s:%s',
                         self.name, ec.name, ec.value)                     # TODO when it happen?
        elif ec == error_types.ERR_SESSION_RESET:
            pass
        elif (ec == error_types.ERR_OBJECT_NOT_FOUND
              or ec == error_types.ERR_INACTIVE_STATE
              or ec == error_types.ERR_INVALID_STATE
              or ec == error_types.ERR_NOT_ENOUGH_MEMBER
              or ec == error_types.ERR_PARENT_PARTITION_MISUSED):
            self.update_partition = True
        else:
            logger.error('table: %s, ignore ec: %s:%s',
                         self.name, ec.name, ec.value)

    def ttl(self, hash_key, sort_key, timeout=0):
        """
        Get ttl(time to live) of the data.

        :param hash_key: (bytes) which hash key used for this API.
        :param sort_key: (bytes) which sort key used for this API.
        :param timeout: (int) how long will the operation timeout in milliseconds.
                        if timeout > 0, it is a timeout value for current operation,
                        else the timeout value specified to create the instance will be used.
        :return: (tuple<error_types.code.value, int>) (code, ttl)
                 code: error_types.ERR_OK.value when data exist, error_types.ERR_OBJECT_NOT_FOUND.value when data not found.
                 ttl: in seconds, -1 means forever.
        """
        blob_key = self.generate_key(hash_key, sort_key)
        partition_hash = self.table.get_blob_hash(blob_key)
        peer_gpid = self.table.get_gpid_by_hash(partition_hash)
        session = self.table.get_session(peer_gpid)
        op = RrdbTtlOperator(peer_gpid, blob_key, partition_hash)
        if not session or not op:
            return error_types.ERR_INVALID_STATE.value, 0

        return session.operate(op, timeout)

    def exist(self, hash_key, sort_key, timeout=0):
        """
        Check value exist.

        :param hash_key: (bytes) which hash key used for this API.
        :param sort_key: (bytes) which sort key used for this API.
        :param timeout: (int) how long will the operation timeout in milliseconds.
                        if timeout > 0, it is a timeout value for current operation,
                        else the timeout value specified to create the instance will be used.
        :return: (tuple<error_types.code.value, None>) (code, ign)
                 code: error_types.ERR_OK.value when data exist, error_types.ERR_OBJECT_NOT_FOUND.value when data not found.
                 ign: useless, should be ignored.
        """
        return self.ttl(hash_key, sort_key, timeout)

    def get(self, hash_key, sort_key, timeout=0):
        """
        Get value stored in <hash_key, sort_key>.

        :param hash_key: (bytes) which hash key used for this API.
        :param sort_key: (bytes) which sort key used for this API.
        :param timeout: (int) how long will the operation timeout in milliseconds.
                        if timeout > 0, it is a timeout value for current operation,
                        else the timeout value specified to create the instance will be used.
        :return: (tuple<error_types.code.value, bytes>) (code, value).
                 code: error_types.ERR_OK.value when data got succeed, error_types.ERR_OBJECT_NOT_FOUND.value when data not found.
                 value: data stored in this <hash_key, sort_key>
        """
        blob_key = self.generate_key(hash_key, sort_key)
        partition_hash = self.table.get_blob_hash(blob_key)
        peer_gpid = self.table.get_gpid_by_hash(partition_hash)
        session = self.table.get_session(peer_gpid)
        op = RrdbGetOperator(peer_gpid, blob_key, partition_hash)
        if not session or not op:
            return error_types.ERR_INVALID_STATE.value, 0

        return session.operate(op, timeout)

    def set(self, hash_key, sort_key, value, ttl=0, timeout=0):
        """
        Set value to be stored in <hash_key, sort_key>.

        :param hash_key: (bytes) which hash key used for this API.
        :param sort_key: (bytes) which sort key used for this API.
        :param value: (bytes) value to be stored under <hash_key, sort_key>.
        :param ttl: (int) ttl(time to live) in seconds of this data.
        :param timeout: (int) how long will the operation timeout in milliseconds.
                        if timeout > 0, it is a timeout value for current operation,
                        else the timeout value specified to create the instance will be used.
        :return: (tuple<error_types.code.value, None>) (code, ign)
                 code: error_types.ERR_OK.value when data stored succeed.
                 ign: useless, should be ignored.
        """
        blob_key = self.generate_key(hash_key, sort_key)
        partition_hash = self.table.get_blob_hash(blob_key)
        peer_gpid = self.table.get_gpid_by_hash(partition_hash)
        session = self.table.get_session(peer_gpid)
        op = RrdbPutOperator(peer_gpid, update_request(blob_key, blob(value), get_ttl(ttl)), partition_hash)
        if not session or not op:
            return error_types.ERR_INVALID_STATE.value, 0

        return session.operate(op, timeout)

    def remove(self, hash_key, sort_key, timeout=0):
        """
        Remove the entire <hash_key, sort_key>-value in pegasus.

        :param hash_key: (bytes) which hash key used for this API.
        :param sort_key: (bytes) which sort key used for this API.
        :param timeout: (int) how long will the operation timeout in milliseconds.
                        if timeout > 0, it is a timeout value for current operation,
                        else the timeout value specified to create the instance will be used.
        :return: (tuple<error_types.code.value, None>) (code, ign)
                 code: error_types.ERR_OK.value when data stored succeed.
                 ign: useless, should be ignored.
        """
        blob_key = self.generate_key(hash_key, sort_key)
        partition_hash = self.table.get_blob_hash(blob_key)
        peer_gpid = self.table.get_gpid_by_hash(partition_hash)
        session = self.table.get_session(peer_gpid)
        op = RrdbRemoveOperator(peer_gpid, blob_key, partition_hash)
        if not session or not op:
            return error_types.ERR_INVALID_STATE.value, 0

        return session.operate(op, timeout)

    def sort_key_count(self, hash_key, timeout=0):
        """
        Get the total sort key count under the hash_key.

        :param hash_key: (bytes) which hash key used for this API.
        :param timeout: (int) how long will the operation timeout in milliseconds.
                        if timeout > 0, it is a timeout value for current operation,
                        else the timeout value specified to create the instance will be used.
        :return: (tuple<error_types.code.value, count>) (code, count)
                 code: error_types.ERR_OK.value when data got succeed, error_types.ERR_OBJECT_NOT_FOUND.value when data not found.
                 value: total sort key count under the hash_key.
        """
        partition_hash = self.table.get_hashkey_hash(hash_key)
        peer_gpid = self.table.get_gpid_by_hash(partition_hash)
        session = self.table.get_session(peer_gpid)
        op = RrdbSortkeyCountOperator(peer_gpid, blob(hash_key), partition_hash)
        if not session or not op:
            return error_types.ERR_INVALID_STATE.value, 0

        return session.operate(op, timeout)

    def multi_set(self, hash_key, sortkey_value_dict, ttl=0, timeout=0):
        """
        Set multiple sort_keys-values under hash_key to be stored.

        :param hash_key: (bytes) which hash key used for this API.
        :param sortkey_value_dict: (dict) <sort_key, value> pairs in dict.
        :param ttl: (int) ttl(time to live) in seconds of these data.
        :param timeout: (int) how long will the operation timeout in milliseconds.
                        if timeout > 0, it is a timeout value for current operation,
                        else the timeout value specified to create the instance will be used.
        :return: (tuple<error_types.code.value, _>) (code, ign)
                 code: error_types.ERR_OK.value when data stored succeed.
                 ign: useless, should be ignored.
        """
        partition_hash = self.table.get_hashkey_hash(hash_key)
        peer_gpid = self.table.get_gpid_by_hash(partition_hash)
        session = self.table.get_session(peer_gpid)
        kvs = [key_value(blob(k), blob(v)) for k, v in sortkey_value_dict.items()]
        ttl = get_ttl(ttl)
        req = multi_put_request(blob(hash_key), kvs, ttl)
        op = RrdbMultiPutOperator(peer_gpid, req, partition_hash)
        if not session or not op:
            return error_types.ERR_INVALID_STATE.value, 0

        return session.operate(op, timeout)

    def multi_get(self, hash_key,
                  sortkey_set,
                  max_kv_count=100,
                  max_kv_size=1000000,
                  no_value=False,
                  timeout=0):
        """
        Get multiple values stored in <hash_key, sortkey> pairs.

        :param hash_key: (bytes) which hash key used for this API.
        :param sortkey_set: (set) sort keys in set.
        :param max_kv_count: (int) max count of k-v pairs to be fetched. max_fetch_count <= 0 means no limit.
        :param max_kv_size: (int) max total data size of k-v pairs to be fetched. max_fetch_size <= 0 means no limit.
        :param no_value: (bool) whether to fetch value of these keys.
        :param timeout: (int) how long will the operation timeout in milliseconds.
                        if timeout > 0, it is a timeout value for current operation,
                        else the timeout value specified to create the instance will be used.
        :return: (tuple<error_types.code.value, dict>) (code, kvs)
                 code: error_types.ERR_OK.value when data got succeed.
                 kvs: <sort_key, value> pairs in dict.
        """
        partition_hash = self.table.get_hashkey_hash(hash_key)
        peer_gpid = self.table.get_gpid_by_hash(partition_hash)
        session = self.table.get_session(peer_gpid)
        ks = []
        if sortkey_set is None:
            pass
        elif isinstance(sortkey_set, set):
            ks = [blob(k) for k in sortkey_set]
        else:
            return error_types.ERR_INVALID_PARAMETERS.value, 0

        req = multi_get_request(blob(hash_key), ks,
                                max_kv_count, max_kv_size,
                                no_value)
        op = RrdbMultiGetOperator(peer_gpid, req, partition_hash)
        if not session or not op:
            return error_types.ERR_INVALID_STATE.value, 0

        return session.operate(op, timeout)

    def multi_get_opt(self, hash_key,
                      start_sort_key, stop_sort_key,
                      multi_get_options,
                      max_kv_count=100,
                      max_kv_size=1000000,
                      timeout=0):
        """
        Get multiple values stored in hash_key, and sort key range in [start_sort_key, stop_sort_key) as default.

        :param hash_key: (bytes) which hash key used for this API.
        :param start_sort_key: (bytes) returned k-v pairs is start from start_sort_key.
        :param stop_sort_key: (bytes) returned k-v pairs is stop at stop_sort_key.
        :param multi_get_options: (MultiGetOptions) configurable multi_get options.
        :param max_kv_count: (int) max count of k-v pairs to be fetched. max_fetch_count <= 0 means no limit.
        :param max_kv_size: (int) max total data size of k-v pairs to be fetched. max_fetch_size <= 0 means no limit.
        :param timeout: (int) how long will the operation timeout in milliseconds.
                        if timeout > 0, it is a timeout value for current operation,
                        else the timeout value specified to create the instance will be used.
        :return: (tuple<error_types.code.value, dict>) (code, kvs)
                 code: error_types.ERR_OK.value when data got succeed.
                 kvs: <sort_key, value> pairs in dict.
        """
        partition_hash = self.table.get_hashkey_hash(hash_key)
        peer_gpid = self.table.get_gpid_by_hash(partition_hash)
        session = self.table.get_session(peer_gpid)
        req = multi_get_request(blob(hash_key),
                                None,
                                max_kv_count,
                                max_kv_size,
                                multi_get_options.no_value,
                                blob(start_sort_key),
                                blob(stop_sort_key),
                                multi_get_options.start_inclusive,
                                multi_get_options.stop_inclusive,
                                multi_get_options.sortkey_filter_type,
                                blob(multi_get_options.sortkey_filter_pattern),
                                multi_get_options.reverse)
        op = RrdbMultiGetOperator(peer_gpid, req, partition_hash)
        if not session or not op:
            return error_types.ERR_INVALID_STATE.value, 0

        return session.operate(op, timeout)

    def get_sort_keys(self, hash_key,
                      max_kv_count=100,
                      max_kv_size=1000000,
                      timeout=0):
        """
        Get multiple sort keys under hash_key.

        :param hash_key: (bytes) which hash key used for this API.
        :param max_kv_count: (int) max count of k-v pairs to be fetched. max_fetch_count <= 0 means no limit.
        :param max_kv_size: (int) max total data size of k-v pairs to be fetched. max_fetch_size <= 0 means no limit.
        :param timeout: (int) how long will the operation timeout in milliseconds.
                        if timeout > 0, it is a timeout value for current operation,
                        else the timeout value specified to create the instance will be used.
        :return: (tuple<error_types.code.value, set>) (code, ks)
                 code: error_types.ERR_OK.value when data got succeed.
                 ks: <sort_key, ign> pairs in dict, ign will always be empty str.
        """
        return self.multi_get(hash_key, None,
                              max_kv_count, max_kv_size,
                              True, timeout)

    def multi_del(self, hash_key, sortkey_set, timeout=0):
        """
        Remove multiple entire <hash_key, sort_key>-values in pegasus.

        :param hash_key: (bytes) which hash key used for this API.
        :param sortkey_set: (set) sort keys in set.
        :param timeout: (int) how long will the operation timeout in milliseconds.
                        if timeout > 0, it is a timeout value for current operation,
                        else the timeout value specified to create the instance will be used.
        :return: (tuple<error_types.code.value, int>) (code, count).
                 code: error_types.ERR_OK.value when data got succeed.
                 count: count of deleted k-v pairs.
        """
        partition_hash = self.table.get_hashkey_hash(hash_key)
        peer_gpid = self.table.get_gpid_by_hash(partition_hash)
        session = self.table.get_session(peer_gpid)
        ks = []
        if isinstance(sortkey_set, set):
            ks = [blob(k) for k in sortkey_set]
        else:
            return error_types.ERR_INVALID_PARAMETERS.value, 0

        req = multi_remove_request(blob(hash_key), ks)     # 100 limit?
        op = RrdbMultiRemoveOperator(peer_gpid, req, partition_hash)
        if not session or not op:
            return error_types.ERR_INVALID_STATE.value, 0

        return session.operate(op, timeout)

    def get_scanner(self, hash_key,
                    start_sort_key, stop_sort_key,
                    scan_options):
        """
        Get scanner for hash_key, start from start_sort_key, and stop at stop_sort_key.
        Whether the scanner include the start_sort_key and stop_sort_key is configurable by scan_options

        :param hash_key: (bytes) which hash key used for this API.
        :param start_sort_key: (bytes) returned scanner is start from start_sort_key.
        :param stop_sort_key: (bytes) returned scanner is stop at stop_sort_key.
        :param scan_options: (ScanOptions) configurable scan options.
        :return: (PegasusScanner) scanner, instance of PegasusScanner.
        """
        start_key = self.generate_key(hash_key, start_sort_key)
        stop_key, stop_inclusive = self.generate_stop_key(hash_key, stop_sort_key)
        if not stop_inclusive:
            scan_options.stop_inclusive = stop_inclusive
        gpid_list = []
        hash_list = []
        r = bytes_cmp(start_key.data, stop_key.data)
        if r < 0 or                                                                     \
           (r == 0 and scan_options.start_inclusive and scan_options.stop_inclusive):
            partition_hash = self.table.get_blob_hash(start_key)
            gpid_list.append(self.table.get_gpid_by_hash(partition_hash))
            hash_list.append(partition_hash)

        return PegasusScanner(self.table, gpid_list, scan_options, hash_list, False, start_key, stop_key)

    def get_unordered_scanners(self, max_split_count, scan_options):
        """
        Get scanners for the whole pegasus table.

        :param max_split_count: (int) max count of scanners will be returned.
        :param scan_options: (ScanOptions) configurable scan options.
        :return: (list) instance of PegasusScanner list.
                 each scanner in this list can scan separate part of the whole pegasus table.
        """
        if max_split_count <= 0:
            return None

        all_gpid_list = self.table.get_all_gpid()
        split = min(len(all_gpid_list), max_split_count)
        count = len(all_gpid_list)
        size = count // split
        more = count % split

        opt = ScanOptions()
        opt.timeout_millis = scan_options.timeout_millis
        opt.batch_size = scan_options.batch_size
        opt.snapshot = scan_options.snapshot
        scanner_list = []
        for i in range(split):
            gpid_list = []
            hash_list = []
            s = i < more and size + 1 or size
            for j in range(s):
                if all_gpid_list:
                    count -= 1
                    gpid_list.append(all_gpid_list[count])
                    hash_list.append(int(count))

            scanner_list.append(PegasusScanner(self.table, gpid_list, opt, hash_list, True))

        return scanner_list
