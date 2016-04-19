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
 *     Feb., 2016, @imzhenyu (Zhenyu Guo), done in Tron project and copied here
 *     xxxx-xx-xx, author, fix bug about xxx
 */

using System.Collections.Generic;
using System.Diagnostics;

namespace rDSN.Tron.Utility
{
    public class CodeBuilder
    {
        private List<int> _indents;
        private List<string> _scripts;

        public int Indent { get; private set; }

        public CodeBuilder()
        {
            _indents = new List<int>();
            _scripts = new List<string>();

            Indent = 0;
        }

        public CodeBuilder(int initIndent)
        {
            _indents = new List<int>();
            _scripts = new List<string>();

            Indent = initIndent;
            Trace.Assert(initIndent >= 0);
        }

        void increaseIndent() 
        {
            ++Indent;
        }

        void decreaseIndent()
        {
            --Indent;
            Trace.Assert(Indent >= 0);
        }

        public void Insert(int after, CodeBuilder cb)
        {
            // TODO
        }

        public void BeginBlock()
        {
            AppendLine("{");
            Indent++;
        }

        public void EndBlock()
        {
            Indent--;
            AppendLine("}");
        }

        public void AppendLine(string script)
        {
            _indents.Add(Indent);
            _scripts.Add(script);
        }

        public void AppendLine()
        {
            _indents.Add(Indent);
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
            var ps = "";

            for (var i = 0; i < _indents.Count; ++i)
            {
                for (var k = 0; k < _indents[i]; ++k)
                    ps += "\t";
                ps += _scripts[i];
                ps += "\r\n";
            }

            //Console.WriteLine(ps);

            return ps;
        }
    }
}
