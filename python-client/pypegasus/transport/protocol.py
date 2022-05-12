#! /usr/bin/env python
# coding=utf-8

from __future__ import print_function

from struct import unpack

from .compat import BufferIO
try:
    from cStringIO import StringIO
except ImportError:
    from io import BytesIO as StringIO

from thrift.transport import TTwisted
from thrift.transport import TTransport

from twisted.internet.protocol import connectionDone
from pypegasus.base import ttypes


class TPegasusTransport(TTwisted.TCallbackTransport):
    """Class that wraps another transport and buffers its I/O.
    """
    DEFAULT_BUFFER = 4096

    def __init__(self, trans, func, rbuf_size=DEFAULT_BUFFER):
        TTwisted.TCallbackTransport.__init__(self, func)
        self.__trans = trans
        self.__wbuf = BufferIO()
        # Pass string argument to initialize read buffer as cStringIO.InputType
        self.__rbuf = BufferIO(b'')
        self.__rbuf_size = rbuf_size

    def get_peer_addr(self):
        return self.__trans.getPeer()

    def isOpen(self):
        return self.__trans.isOpen()

    def open(self):
        return self.__trans.open()

    def close(self):
        return self.__trans.loseConnection()

    def read(self, sz):
        ret = self.__rbuf.read(sz)
        if len(ret) != 0:
            return ret
        self.__rbuf = BufferIO(self.__trans.read(max(sz, self.__rbuf_size)))
        return self.__rbuf.read(sz)

    def seek(self, offset):
        try:
            self.__wbuf.seek(offset)
        except Exception as e:
            # on exception reset wbuf so it doesn't contain a partial function call
            self.__wbuf = BufferIO()
            raise e

    def tell(self):
        try:
            return self.__wbuf.tell()
        except Exception as e:
            # on exception reset wbuf so it doesn't contain a partial function call
            self.__wbuf = BufferIO()
            raise e

    def write(self, buf):
        try:
            self.__wbuf.write(buf)
        except Exception as e:
            # on exception reset wbuf so it doesn't contain a partial function call
            self.__wbuf = BufferIO()
            raise e

    def flush(self):
        msg = self.__wbuf.getvalue()
        self.__wbuf = StringIO()
        return self.sendMessage(msg)


class TPegasusThriftClientProtocol(TTwisted.ThriftClientProtocol):

    def __init__(self, client_class, iprot_factory, oprot_factory=None, container=None, timeout=2000):
        TTwisted.ThriftClientProtocol.__init__(self, client_class, iprot_factory, oprot_factory)
        self.client = None
        self.container = container
        self.timeout = timeout

    def connectionMade(self):
        tmo = TPegasusTransport(self.transport, self.dispatch)
        self.client = self._client_class(tmo, self._oprot_factory, self.container, self.timeout)
        self.started.callback(self.client)

    def connectionLost(self, reason=connectionDone):
        try:
            TTwisted.ThriftClientProtocol.connectionLost(self, reason)
        except Exception as e:
            pass

    def sendString(self, string):
        """
        overwrite IntNStringReceiver.sendString(...)
        because we don't need a length N ahead of data

        @param string: The string to send.
        @type string: C{bytes}
        """

        self.transport.write(string)

    def stringReceived(self, frame):
        tr = TTransport.TMemoryBuffer(frame)
        iprot = self._iprot_factory.getProtocol(tr)
        ec = ttypes.error_code()
        ec.read(iprot)
        (fname, mtype, rseqid) = iprot.readMessageBegin()
        try:
            method = self.recv_map[fname]
        except KeyError:
            method = getattr(self.client, 'recv_' + fname)
            self.recv_map[fname] = method

        method(iprot, mtype, rseqid, ec.errno)

    def dataReceived(self, data):
        """
        overwrite IntNStringReceiver.dataReceived
        Convert int prefixed strings into calls to stringReceived.
        """
        # Try to minimize string copying (via slices) by keeping one buffer
        # containing all the data we have so far and a separate offset into that
        # buffer.
        alldata = self._unprocessed + data
        currentOffset = 0
        prefixLength = self.prefixLength
        fmt = self.structFormat     # "!I" 32bit
        self._unprocessed = alldata

        while len(alldata) >= (currentOffset + prefixLength) and not self.paused:
            messageStart = currentOffset + prefixLength
            length, = unpack(fmt, alldata[currentOffset:messageStart])
            if length > self.MAX_LENGTH:
                self._unprocessed = alldata
                self._compatibilityOffset = currentOffset
                self.lengthLimitExceeded(length)
                return
            messageEnd = currentOffset + length     # different with super function
            if len(alldata) < messageEnd:
                break

            # Here we have to slice the working buffer so we can send just the
            # netstring into the stringReceived callback.
            packet = alldata[messageStart:messageEnd]
            currentOffset = messageEnd
            self._compatibilityOffset = currentOffset
            self.stringReceived(packet)

            # Check to see if the backwards compat "recvd" attribute got written
            # to by application code.  If so, drop the current data buffer and
            # switch to the new buffer given by that attribute's value.
            if 'recvd' in self.__dict__:
                alldata = self.__dict__.pop('recvd')
                self._unprocessed = alldata
                self._compatibilityOffset = currentOffset = 0
                if alldata:
                    continue
                return

        # Slice off all the data that has been processed, avoiding holding onto
        # memory to store it, and update the compatibility attributes to reflect
        # that change.
        self._unprocessed = alldata[currentOffset:]
        self._compatibilityOffset = 0
