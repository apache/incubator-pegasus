using System.IO;
using Google.Protobuf;

namespace dsn.dev.csharp
{
    public static partial class GProtoHelper
    {
        public static void Read<T>(this Stream rs, out T val) where T : IMessage, new()
        {
            CodedInputStream s = new CodedInputStream(rs);
            val = new T();
            val.MergeFrom(s);
        }

        public static void Write<T>(this Stream ws, T val) where T : IMessage
        {
            CodedOutputStream s = new CodedOutputStream(ws);
            val.WriteTo(s);
            s.Flush();
        }
    }
}