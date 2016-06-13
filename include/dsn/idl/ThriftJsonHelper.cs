using System.IO;

using Thrift.Protocol;
using Thrift.Transport;

namespace dsn.dev.csharp
{
    internal static partial class ThriftJsonHelper
    {
        public static void Read<T>(this Stream rs, out T val) where T : TBase, new()
        {
            TProtocol prt = new TJSONProtocol(new TStreamTransport(rs, null));
            val = new T();
            val.Read(prt);
        }

        public static void Write<T>(this Stream ws, T val) where T : TBase
        {
            TProtocol prt = new TJSONProtocol(new TStreamTransport(null, ws));
            val.Write(prt);
            ws.Flush();
        }
    }
}