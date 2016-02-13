using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;

namespace rDSN.Tron.Utility
{
    public class CodeBuilder
    {
        private List<int> _indents;
        private List<string> _scripts;

        private int _indent;

        public int Indent { get { return _indent; } }

        public CodeBuilder()
        {
            _indents = new List<int>();
            _scripts = new List<string>();

            _indent = 0;
        }

        public CodeBuilder(int initIndent)
        {
            _indents = new List<int>();
            _scripts = new List<string>();

            _indent = initIndent;
            Trace.Assert(initIndent >= 0);
        }

        void increaseIndent() 
        {
            ++_indent;
        }

        void decreaseIndent()
        {
            --_indent;
            Trace.Assert(_indent >= 0);
        }

        public void Insert(int after, CodeBuilder cb)
        {
            // TODO
        }

        public void BeginBlock()
        {
            AppendLine("{");
            _indent++;
        }

        public void EndBlock()
        {
            _indent--;
            AppendLine("}");
        }

        public void AppendLine(string script)
        {
            _indents.Add(_indent);
            _scripts.Add(script);
        }

        public void AppendLine()
        {
            _indents.Add(_indent);
            _scripts.Add("");
        }
                
        public static CodeBuilder operator ++(CodeBuilder cb)
        {
            cb.increaseIndent();
            return cb;
        }

        public static CodeBuilder operator --(CodeBuilder cb)
        {
            cb.decreaseIndent();
            return cb;
        }

        public override string ToString()
        {
            string ps = "";

            for (int i = 0; i < _indents.Count; ++i)
            {
                for (int k = 0; k < _indents[i]; ++k)
                    ps += "\t";
                ps += _scripts[i];
                ps += "\r\n";
            }

            //Console.WriteLine(ps);

            return ps;
        }
    }
}
