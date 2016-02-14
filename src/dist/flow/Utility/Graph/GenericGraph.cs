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
 
using System;
using System.Collections.Generic;
using System.Text;
using System.Diagnostics;
using System.IO;

namespace rDSN.Tron.Utility
{
    public class GenericGraph<VertexT, EdgeT, GraphT> : AbstractGraphElement<GraphT>
        where VertexT : GenericVertex<VertexT, EdgeT, GraphT>
        where EdgeT : GenericEdge<VertexT, EdgeT, GraphT>
        where GraphT : GenericGraph<VertexT, EdgeT, GraphT>
    {
        #region FIELDS
        //private HashSet<VertexT> _vertices = new HashSet<VertexT>();
        private Dictionary<UInt64, VertexT> _vertices = new Dictionary<UInt64, VertexT>();
        private HashSet<EdgeT> _edges = new HashSet<EdgeT>();
        private UInt64 _vertexId = 0;
        #endregion

        #region PROPERTIES
        //public HashSet<VertexT> Vertices { get { return _vertices; } }
        public Dictionary<UInt64, VertexT> Vertices { get { return _vertices; } }
        public HashSet<EdgeT> Edges { get { return _edges; } }
        #endregion

        public GenericGraph()
        {
        }

        public void Clear()
        {
            foreach (VertexT v in _vertices.Values)
            {
                RemoveVertex(v);
            }

            _vertices.Clear();
            _edges.Clear();
        }

        //public VertexT CreateVertex()
        //{
        //    return CreateVertex(UInt64.MaxValue);
        //}

        //public VertexT CreateVertex(UInt64 id)
        //{
        //    if (id == UInt64.MaxValue) // -1
        //    {
        //        id = (UInt64)(_vertices.Count) + 1;
        //    }

        //    Type[] ps = new Type[2] { typeof(GraphT), typeof(UInt64) };
        //    Object[] pss = new Object[2] { this, id };
        //    VertexT vertex = (VertexT)typeof(VertexT).GetConstructor(ps).Invoke(pss);

        //    _vertices.Add(id, vertex);

        //    return vertex;
        //}

        public VT CreateVertex<VT>() where VT : VertexT
        {
            return CreateVertex(typeof(VT), UInt64.MaxValue) as VT;
        }

        public VertexT CreateVertex(Type vt)
        {
            return CreateVertex(vt, UInt64.MaxValue);
        }

        public VertexT CreateVertex(Type vt, UInt64 id)
        {
            if (id == UInt64.MaxValue) // -1
            {
                id = ++_vertexId;
            }

            Type[] ps = new Type[2] { typeof(GraphT), typeof(UInt64) };
            Object[] pss = new Object[2] { this, id };
            VertexT vertex = (VertexT)vt.GetConstructor(ps).Invoke(pss);

            _vertices.Add(id, vertex);

            return vertex;
        }
        
        public void RemoveVertex(VertexT vertex)
        {
            HashSet<EdgeT> es = new HashSet<EdgeT>();
            es.UnionWith(vertex.InEdges);

            foreach (EdgeT edge in es)
            {
                RemoveEdge(edge);
            }

            es.Clear();
            es.UnionWith(vertex.OutEdges);

            foreach (EdgeT edge in es)
            {
                RemoveEdge(edge);
            }

            _vertices.Remove(vertex.Id);
        }


        public void RemoveEdge(EdgeT edge)
        {
            edge.StartVertex.OutEdges.Remove(edge);
            edge.EndVertex.InEdges.Remove(edge);
            _edges.Remove(edge);
            edge.Clear();
        }

        public List<VertexT> GetRootVertices(bool downStream)
        {
            List<VertexT> _rootVertices = new List<VertexT>();

            foreach (VertexT v in _vertices.Values)
            {
                if (downStream)
                {
                    if (v.InEdges.Count == 0)
                    {
                        _rootVertices.Add(v);
                    }
                }
                else
                {
                    if (v.OutEdges.Count == 0)
                    {
                        _rootVertices.Add(v);
                    }
                }
            }

            return _rootVertices;
        }

        public VertexT GetVertexById(UInt64 id)
        {
            VertexT v;

            _vertices.TryGetValue(id, out v);

            return v;
        }

        public string VisualizeGraph()
        {
            MemoryStream membuf = new MemoryStream(8192);
            TextWriter output = new StreamWriter(membuf);

            GenerateGraphHeader(output);
            foreach (VertexT v in Vertices.Values)
            {
                v.Visualize(output);
            }
            foreach (EdgeT e in Edges)
            {
                e.Visualize(output);
            }
            GenerateGraphFooter(output);

            output.Flush();
            membuf.Seek(0, SeekOrigin.Begin);
            TextReader input = new StreamReader(membuf);
            return input.ReadToEnd();
        }

        public virtual bool VisualizeGraph(string path, string file)
        {
            int rfs = (new Random()).Next();
            string s = VisualizeGraph();
            StreamWriter r = new StreamWriter(path + "\\" + rfs + ".dot");
            r.Write(s);
            r.Close();

            var proc = SystemHelper.StartProcess(
                "dot.exe", 
                " -Tjpg " + path + "\\" + rfs + ".dot" + " -o " + path + "\\" + file + ".jpg", 
                true,
                "");
            proc.WaitForExit();
            return proc.ExitCode == 0;
        }

        private void GenerateGraphHeader(TextWriter output)
        {
            output.WriteLine("digraph G");
            output.WriteLine("{");
            output.WriteLine("    node [fontname=\"Arial\", shape=box, style=filled]");
            output.WriteLine("    edge [fontname=\"Arial\", style=filled]");
            output.WriteLine("    fontname=\"Arial\"");
            output.WriteLine();
        }

        private void GenerateGraphFooter(TextWriter output)
        {
            output.WriteLine("}");
        }

        private void GenerateVertices(TextWriter output)
        {
            foreach (VertexT v in Vertices.Values)
            {
                String fillColor = "green";
                output.Write("    \"" + v.Id + "\"");
                output.Write(" [");
                if ((v.Name.Length != 0) || (v.Description.Length != 0))
                {
                    output.Write("label=\"");

                    if (v.Name.Length != 0)
                    {
                        output.Write(v.Name);
                        if (v.Description.Length != 0)
                        {
                            output.Write("\\r\n");
                        }
                    }

                    if (v.Description.Length != 0)
                    {
                        output.Write(v.Description);
                    }

                    output.Write("\", ");
                }

                output.WriteLine("fillcolor=\"" + fillColor + "\"]");
            }
        }

        private void GenerateEdges(TextWriter output)
        {
            foreach (EdgeT e in Edges)
            {
                output.Write("    \"" + e.StartVertex.Id + "\"");
                output.Write("->");
                output.Write("\"" + e.EndVertex.Id + "\"");
                output.Write(" ");
                output.Write("[");
                output.Write("style=bold");
                output.Write("]");
                output.WriteLine();
            }
        }
    }
}
