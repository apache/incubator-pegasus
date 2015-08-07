using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.InteropServices;
using System.IO;

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

        public override bool IsInvalid
        {
            get { return handle == IntPtr.Zero; }
        }
    }

    public class Message : SafeHandleZeroIsInvalid
    {
        private bool _isOwner;
        public Message(IntPtr msg, bool owner)
            : base(IntPtr.Zero, owner)
        {
            SetHandle(msg);
            _isOwner = owner;
        }

        protected override bool ReleaseHandle()
        {
            if (!IsInvalid)
            {
                if (_isOwner)
                    Native.dsn_msg_release_ref(handle);

                return true;
            }
            else
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

        public override bool CanRead { get { return _isRead; } }

        public override bool CanSeek { get { return false; } }

        [ComVisible(false)]
        public override bool CanTimeout { get { return false; } }
        public override bool CanWrite { get { return !_isRead; } }
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
        public RpcWriteStream(TaskCode code, int timeoutMilliseconds, int hash)
            : base(Native.dsn_msg_create_request(code, timeoutMilliseconds, hash), false, false)
        {
            _currentWriteOffset = 0;
            _currentBufferLength = IntPtr.Zero;
            _length = 0;
        }

        public RpcWriteStream(IntPtr msg, bool owner, int minSize = 256)
            : base(msg, owner, false)
        {
            _currentWriteOffset = 0;
            _currentBufferLength = IntPtr.Zero;
            _length = 0;
        }

        public override long Length { get { return _length; } }
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

                int cp = count > ((int)_currentBufferLength - _currentWriteOffset) ?
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

        public override long Length { get { return (long)_length; } }
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

    public static partial class RpcStreamIoHelper
    {
        public static void Read(this RpcReadStream rs, out UInt64 val)
        {
            using (BinaryReader reader = new BinaryReader(rs))
            {
                val = reader.ReadUInt64();
            }
        }

        public static void Write(this RpcWriteStream ws, UInt64 val)
        {
            using (BinaryWriter writer = new BinaryWriter(ws))
            {
                writer.Write(val);
            }
        }

        public static void Read(this RpcReadStream rs, out UInt32 val)
        {
            using (BinaryReader reader = new BinaryReader(rs))
            {
                val = reader.ReadUInt32();
            }
        }

        public static void Write(this RpcWriteStream ws, UInt32 val)
        {
            using (BinaryWriter writer = new BinaryWriter(ws))
            {
                writer.Write(val);
            }
        }

        public static void Read(this RpcReadStream rs, out UInt16 val)
        {
            using (BinaryReader reader = new BinaryReader(rs))
            {
                val = reader.ReadUInt16();
            }
        }

        public static void Write(this RpcWriteStream ws, UInt16 val)
        {
            using (BinaryWriter writer = new BinaryWriter(ws))
            {
                writer.Write(val);
            }
        }

        public static void Read(this RpcReadStream rs, out byte val)
        {
            using (BinaryReader reader = new BinaryReader(rs))
            {
                val = reader.ReadByte();
            }
        }

        public static void Write(this RpcWriteStream ws, byte val)
        {
            using (BinaryWriter writer = new BinaryWriter(ws))
            {
                writer.Write(val);
            }
        }

        public static void Read(this RpcReadStream rs, out Int64 val)
        {
            using (BinaryReader reader = new BinaryReader(rs))
            {
                val = reader.ReadInt64();
            }
        }

        public static void Write(this RpcWriteStream ws, Int64 val)
        {
            using (BinaryWriter writer = new BinaryWriter(ws))
            {
                writer.Write(val);
            }
        }

        public static void Read(this RpcReadStream rs, out Int32 val)
        {
            using (BinaryReader reader = new BinaryReader(rs))
            {
                val = reader.ReadInt32();
            }
        }

        public static void Write(this RpcWriteStream ws, Int32 val)
        {
            using (BinaryWriter writer = new BinaryWriter(ws))
            {
                writer.Write(val);
            }
        }

        public static void Read(this RpcReadStream rs, out Int16 val)
        {
            using (BinaryReader reader = new BinaryReader(rs))
            {
                val = reader.ReadInt16();
            }
        }

        public static void Write(this RpcWriteStream ws, Int16 val)
        {
            using (BinaryWriter writer = new BinaryWriter(ws))
            {
                writer.Write(val);
            }
        }

        public static void Read(this RpcReadStream rs, out sbyte val)
        {
            using (BinaryReader reader = new BinaryReader(rs))
            {
                val = reader.ReadSByte();
            }
        }

        public static void Write(this RpcWriteStream ws, sbyte val)
        {
            using (BinaryWriter writer = new BinaryWriter(ws))
            {
                writer.Write(val);
            }
        }

        public static void Read(this RpcReadStream rs, out bool val)
        {
            using (BinaryReader reader = new BinaryReader(rs))
            {
                val = reader.ReadBoolean();
            }
        }

        public static void Write(this RpcWriteStream ws, bool val)
        {
            using (BinaryWriter writer = new BinaryWriter(ws))
            {
                writer.Write(val);
            }
        }

        public static void Read(this RpcReadStream rs, out double val)
        {
            using (BinaryReader reader = new BinaryReader(rs))
            {
                val = reader.ReadDouble();
            }
        }

        public static void Write(this RpcWriteStream ws, double val)
        {
            using (BinaryWriter writer = new BinaryWriter(ws))
            {
                writer.Write(val);
            }
        }

        public static void Read(this RpcReadStream rs, out float val)
        {
            using (BinaryReader reader = new BinaryReader(rs))
            {
                val = reader.ReadSingle();
            }
        }

        public static void Write(this RpcWriteStream ws, float val)
        {
            using (BinaryWriter writer = new BinaryWriter(ws))
            {
                writer.Write(val);
            }
        }

        public static void Read(this RpcReadStream rs, out string val)
        {
            using (BinaryReader reader = new BinaryReader(rs))
            {
                val = reader.ReadString();
            }
        }

        public static void Write(this RpcWriteStream ws, string val)
        {
            using (BinaryWriter writer = new BinaryWriter(ws))
            {
                writer.Write(val);
            }
        }
    }

}
