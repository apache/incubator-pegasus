using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using System.IO;
using System.Threading;

namespace rDSN.Tron.Utility
{
    public class LogConsumer : TraceListener
    {
        private enum ConsumerType
        { 
            LocalFile,
            Network,
            Stdout
        }

        private ConsumerType _mode;
        private StreamWriter _localWriter = null;

        public LogConsumer(string name)
        {
            if (name == "stdout")
                _mode = ConsumerType.Stdout;
            else if (name == "net")
                _mode = ConsumerType.Network;
            else
            {
                _mode = ConsumerType.LocalFile;
                var fs = new FileStream(name + ".PID." + Process.GetCurrentProcess().Id + ".log", FileMode.Create);
                if (fs != null)
                {
                    _localWriter = new StreamWriter(fs);
                }
            }
        }

        public override void Write(string message)
        {
            string rMsg = DateTime.Now.ToString("HH:mm:ss.fff") + ",TID=" 
                + Thread.CurrentThread.ManagedThreadId
                + ": " + message;
            switch (_mode)
            { 
                case ConsumerType.LocalFile:
                    if (_localWriter != null)
                    {
                        _localWriter.Write(rMsg);
                        _localWriter.Flush();
                    }                    
                    break;

                case ConsumerType.Stdout:
                    Console.Write(rMsg);
                    break;

                case ConsumerType.Network:
                    ServiceNode.Instance().SendLog(message);
                    break;
            }
        }

        public override void WriteLine(string message)
        {
            Write(message + "\r\n");
        }
    }
}
