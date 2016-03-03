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

            string jstr = System.Text.Encoding.ASCII.GetString(bytes);
            JsonParser jparser = JsonParser.Default;
            val = jparser.Parse<T>(jstr);
        }

        public static void Write<T>(this Stream ws, T val) where T : IMessage
        {
            JsonFormatter jformatter = JsonFormatter.Default;
            string jstr = jformatter.Format(val);
            byte[] bytes = System.Text.Encoding.ASCII.GetBytes(jstr);
                        
            ws.Write(bytes, 0, bytes.Length);
            ws.Flush();
        }
    }
}