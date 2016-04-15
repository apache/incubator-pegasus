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

using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace rDSN.Tron.Utility
{
    public class GenericVertex<VertexT, EdgeT, GraphT> : AbstractGraphElement<VertexT>
        where VertexT : GenericVertex<VertexT, EdgeT, GraphT>
        where EdgeT : GenericEdge<VertexT, EdgeT, GraphT>
        where GraphT : GenericGraph<VertexT, EdgeT, GraphT>
    {
        public enum VertexFlags
        {
            VertexFlags_Visited = 0,
            VertexFlags_MAX = 1
        }

        #region FIELDS

        #endregion

        #region PROPERTIES
        public HashSet<EdgeT> InEdges { get; } = new HashSet<EdgeT>();
        public HashSet<EdgeT> OutEdges { get; } = new HashSet<EdgeT>();
        public GraphT Graph { get; }
        public BitArray Flags { get; } = new BitArray((int)VertexFlags.VertexFlags_MAX);
        public ulong Id { get; }

        public List<VertexT> InVertices
        {
            get
            {
                return InEdges.Select(e => e.StartVertex).ToList();
            }
        }

        public List<VertexT> OutVertices
        {
            get
            {
                return OutEdges.Select(e => e.EndVertex).ToList();
            }
        }
        #endregion

        public GenericVertex(GraphT graph, ulong id)
        {
            Graph = graph;
            Id = id;
        }

        public HashSet<VertexT> GetVerticesClosure(bool upStream)
        {
            var vs = new HashSet<VertexT> {(VertexT) this};

            var pendingVertices = new Queue<VertexT>();
            pendingVertices.Enqueue((VertexT)this);

            while (pendingVertices.Count > 0)
            {
                var v = pendingVertices.Dequeue();
                if (upStream)
                {
                    v.InVertices.Select(iv => { if (!vs.Contains(iv)) { vs.Add(iv); pendingVertices.Enqueue(iv); } return 0; }).Count();
                }
                else
                {
                    v.OutVertices.Select(iv => { if (!vs.Contains(iv)) { vs.Add(iv); pendingVertices.Enqueue(iv); } return 0; }).Count();
                }
            }
            return vs;
        }
        
        public ET ConnectTo<ET>(VertexT targetVertex) where ET : EdgeT
        {
            var ps = new[] { typeof(GraphT), typeof(VertexT), typeof(VertexT) };
            var pss = new object[] {Graph, this, targetVertex};
            var e = (ET)typeof(ET).GetConstructor(ps).Invoke(pss);
            Graph.Edges.Add(e);

            OutEdges.Add(e);
            targetVertex.InEdges.Add(e);
            return e;
        }

        protected virtual string GetVisualizationProperties()
        {
            var s = "";
            if (Name != null)
            {
                s += "label=\"" + Name + "\", ";
            }
            s += "fillcolor=\"" + "green" + "\"";
            return s;
        }

        public void Visualize(TextWriter output)
        {
            output.Write("    \"" + Id + "\"");
            output.Write(" [");
            output.Write(GetVisualizationProperties());
            output.WriteLine("]");
        }
    }
}

