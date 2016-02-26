using System.IO;
using Google.Protobuf;

namespace dsn.dev.csharp
{
    public static partial class GProtoHelper
    {
        public static void Read<T>(this Stream rs, out T val) where T : IMessage, new()
        {
            byte[] bytes = new byte[rs.Length];
            rs.Read(bytes, 0, bytes.Length);
            char[] chars = new char[bytes.Length / sizeof(char)];
            System.Buffer.BlockCopy(bytes, 0, chars, 0, bytes.Length);
            string jstr = new string(chars);
            JsonParser jparser = JsonParser.Default;
            val = jparser.Parse<T>(jstr);
        }

        public static void Write<T>(this Stream ws, T val) where T : IMessage
        {
            JsonFormatter jformatter = JsonFormatter.Default;
            string jstr = jformatter.Format(val);
            byte[] bytes = new byte[jstr.Length * sizeof(char)];
            System.Buffer.BlockCopy(jstr.ToCharArray(), 0, bytes, 0, bytes.Length);
            ws.Write(bytes, 0, bytes.Length);
            ws.Flush();
        }
    }
}