/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 * 
 * -=- Robust Distributed System Nucleus (rDSN) -=- 
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

/*
 * Description:
 *     What is this file about?
 *
 * Revision history:
 *     Feb., 2016, @imzhenyu (Zhenyu Guo), done in rDSN.CSharp project and copied here
 *     xxxx-xx-xx, author, fix bug about xxx
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;

namespace dsn.dev.csharp
{ 
    //public interface ISerializable
    //{
    //    void Marshall(Stream writeStream);
    //    void UnMarshall(Stream readStream);
    //}

    public abstract class SafeHandleZeroIsInvalid : SafeHandle
    {
        public SafeHandleZeroIsInvalid(IntPtr handle, bool isOwner)
            : base(handle, isOwner)
        {
        }

        public override bool IsInvalid => handle == IntPtr.Zero;
    }

    public class Message : SafeHandleZeroIsInvalid
    {
        public Message(IntPtr msg, bool owner)
            : base(msg, owner)
        {
        }

        protected override bool ReleaseHandle()
        {
            if (!IsInvalid)
            {
                Native.dsn_msg_release_ref(handle);
                return true;
            }
            return false;
        }
    }

    public class UriAddress : SafeHandleZeroIsInvalid
    {
        public UriAddress(string url)
            : base(Native.dsn_uri_build(url), true)
        {
        }

        protected override bool ReleaseHandle()
        {
            if (!IsInvalid)
            {
                Native.dsn_uri_destroy(handle);
                return true;
            }
            return false;
        }
    }

    public abstract class RpcStream : Stream
    {
        public RpcStream(IntPtr msg, bool owner, bool isRead)
        { 
            _msg = new Message(msg, owner);
            _isRead = isRead;
        }

        public IntPtr DangerousGetHandle()
        {
            return _msg.DangerousGetHandle();
        }

        public override bool CanRead => _isRead;

        public override bool CanSeek => false;

        [ComVisible(false)]
        public override bool CanTimeout => false;

        public override bool CanWrite => !_isRead;

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        protected Message _msg;
        private bool _isRead;
    }

    public class RpcWriteStream : RpcStream
    {
        public RpcWriteStream(TaskCode code, int timeoutMilliseconds, int thread_hash, ulong partition_hash)
            : base(Native.dsn_msg_create_request(code, timeoutMilliseconds, thread_hash, partition_hash), false, false)
        {
            _currentWriteOffset = 0;
            _currentBufferLength = IntPtr.Zero;
            _length = 0;
        }

        public RpcWriteStream(IntPtr msg, int minSize = 256)
            : base(msg, false, false)
        {
            _currentWriteOffset = 0;
            _currentBufferLength = IntPtr.Zero;
            _length = 0;
        }

        public override long Length => _length;

        public override long Position
        {
            get 
            {
                return _length; 
            }
            set
            {
                throw new NotSupportedException();
            }
        }

        private void PrepareWriteBuffer(int minSize)
        {
            Native.dsn_msg_write_next(_msg.DangerousGetHandle(),
                out _currentBuffer, out _currentBufferLength, (IntPtr)minSize);

            _currentWriteOffset = 0;
        }

        public override void Flush()
        {
            if (_currentWriteOffset > 0)
            {
                Native.dsn_msg_write_commit(_msg.DangerousGetHandle(), (IntPtr)_currentWriteOffset);
            }
            _currentWriteOffset = 0;
            _currentBufferLength = IntPtr.Zero;
        }

        public bool IsFlushed()
        {
            return _currentWriteOffset == 0;
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            while (count > 0)
            {
                if (count + _currentWriteOffset > (int)_currentBufferLength)
                {
                    Flush();
                    PrepareWriteBuffer(count);
                }

                var cp = count > ((int)_currentBufferLength - _currentWriteOffset) ?
                    ((int)_currentBufferLength - _currentWriteOffset) : count;

                Marshal.Copy(buffer, offset, _currentBuffer + _currentWriteOffset, cp);

                offset += cp;
                count -= cp;
                _currentWriteOffset += cp;
            }
        }

        private IntPtr _currentBuffer;
        private int _currentWriteOffset;
        private IntPtr _currentBufferLength;
        private long _length;
    }

    public class RpcReadStream : RpcStream
    {
        public RpcReadStream(IntPtr msg, bool owner)
            : base(msg, owner, true)
        {
            Native.dsn_msg_read_next(msg, out _buffer, out _length);
            Native.dsn_msg_read_commit(msg, _length);

            _pos = 0;
        }

        public override long Length => (long)_length;

        public override long Position
        { 
            get { return _pos; } 
            set
            {
                Logging.dassert(value >= 0 && value <= (long)_length, "given position is too large");
                _pos = value;
            }
        }
        
        public override void Flush()
        {
            // nothing to do
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            count = (_pos + count > (int)_length) ? (int)((long)_length - _pos) : count;

            // TODO: whole buffer copy to managed memory first
            Marshal.Copy((IntPtr)(_buffer.ToInt64() + _pos), buffer, offset, count);
            _pos += count;
            return count;
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        private IntPtr _buffer;
        private long _pos;
        private IntPtr _length;
    }

    public static class RpcStreamIoHelper
    {
        public static void Read(this Stream rs, out ulong val)
        {
            using (var reader = new BinaryReader(rs))
            {
                val = reader.ReadUInt64();
            }
        }

        public static void Write(this Stream ws, ulong val)
        {
            using (var writer = new BinaryWriter(ws))
            {
                writer.Write(val);
            }
        }

        public static void Read(this Stream rs, out uint val)
        {
            using (var reader = new BinaryReader(rs))
            {
                val = reader.ReadUInt32();
            }
        }

        public static void Write(this Stream ws, uint val)
        {
            using (var writer = new BinaryWriter(ws))
            {
                writer.Write(val);
            }
        }

        public static void Read(this Stream rs, out ushort val)
        {
            using (var reader = new BinaryReader(rs))
            {
                val = reader.ReadUInt16();
            }
        }

        public static void Write(this Stream ws, ushort val)
        {
            using (var writer = new BinaryWriter(ws))
            {
                writer.Write(val);
            }
        }

        public static void Read(this Stream rs, out byte val)
        {
            using (var reader = new BinaryReader(rs))
            {
                val = reader.ReadByte();
            }
        }

        public static void Write(this Stream ws, byte val)
        {
            using (var writer = new BinaryWriter(ws))
            {
                writer.Write(val);
            }
        }

        public static void Read(this Stream rs, out long val)
        {
            using (var reader = new BinaryReader(rs))
            {
                val = reader.ReadInt64();
            }
        }

        public static void Write(this Stream ws, long val)
        {
            using (var writer = new BinaryWriter(ws))
            {
                writer.Write(val);
            }
        }

        public static void Read(this Stream rs, out int val)
        {
            using (var reader = new BinaryReader(rs))
            {
                val = reader.ReadInt32();
            }
        }

        public static void Write(this Stream ws, int val)
        {
            using (var writer = new BinaryWriter(ws))
            {
                writer.Write(val);
            }
        }

        public static void Read(this Stream rs, out short val)
        {
            using (var reader = new BinaryReader(rs))
            {
                val = reader.ReadInt16();
            }
        }

        public static void Write(this Stream ws, short val)
        {
            using (var writer = new BinaryWriter(ws))
            {
                writer.Write(val);
            }
        }

        public static void Read(this Stream rs, out sbyte val)
        {
            using (var reader = new BinaryReader(rs))
            {
                val = reader.ReadSByte();
            }
        }

        public static void Write(this Stream ws, sbyte val)
        {
            using (var writer = new BinaryWriter(ws))
            {
                writer.Write(val);
            }
        }

        public static void Read(this Stream rs, out bool val)
        {
            using (var reader = new BinaryReader(rs))
            {
                val = reader.ReadBoolean();
            }
        }

        public static void Write(this Stream ws, bool val)
        {
            using (var writer = new BinaryWriter(ws))
            {
                writer.Write(val);
            }
        }

        public static void Read(this Stream rs, out double val)
        {
            using (var reader = new BinaryReader(rs))
            {
                val = reader.ReadDouble();
            }
        }

        public static void Write(this Stream ws, double val)
        {
            using (var writer = new BinaryWriter(ws))
            {
                writer.Write(val);
            }
        }

        public static void Read(this Stream rs, out float val)
        {
            using (var reader = new BinaryReader(rs))
            {
                val = reader.ReadSingle();
            }
        }

        public static void Write(this Stream ws, float val)
        {
            using (var writer = new BinaryWriter(ws))
            {
                writer.Write(val);
            }
        }

        public static void Read(this Stream rs, out string val)
        {
            using (var reader = new BinaryReader(rs))
            {
                val = reader.ReadString();
            }
        }

        public static void Write(this Stream ws, string val)
        {
            using (var writer = new BinaryWriter(ws))
            {
                writer.Write(val);
            }
        }

        public static void Read(this Stream rs, out TaskCode val)
        {
            using (var reader = new BinaryReader(rs))
            {
                val = new TaskCode(reader.ReadInt32());
            }
        }

        public static void Write(this Stream ws, TaskCode val)
        {
            using (var writer = new BinaryWriter(ws))
            {
                writer.Write(val);
            }
        }

        public static void Read(this Stream rs, out RpcAddress val)
        {
            using (var reader = new BinaryReader(rs))
            {
                val = new RpcAddress(reader.ReadUInt64());
            }
        }

        public static void Write(this Stream ws, RpcAddress val)
        {
            using (var writer = new BinaryWriter(ws))
            {
                writer.Write(val);
            }
        }

        public static void Read(this Stream rs, out List<RpcAddress> val)
        {
            using (var reader = new BinaryReader(rs))
            {
                var c = reader.ReadInt32();
                val = new List<RpcAddress>();
                for (var i = 0; i < c; i++)
                {
                    val.Add(new RpcAddress(reader.ReadUInt64()));
                }
            }
        }

        public static void Write(this Stream ws, List<RpcAddress> val)
        {
            using (var writer = new BinaryWriter(ws))
            {
                writer.Write(val.Count);
                foreach (var addr in val)
                {
                    writer.Write(addr);
                }
            }
        }

        public static void Read(this Stream rs, out List<int> val)
        {
            using (var reader = new BinaryReader(rs))
            {
                var c = reader.ReadInt32();
                val = new List<int>();
                for (var i = 0; i < c; i++)
                {
                    val.Add(reader.ReadInt32());
                }
            }
        }

        public static void Write(this Stream ws, List<int> val)
        {
            using (var writer = new BinaryWriter(ws))
            {
                writer.Write(val.Count);
                foreach (var v in val)
                {
                    writer.Write(v);
                }
            }
        }
    }

}
