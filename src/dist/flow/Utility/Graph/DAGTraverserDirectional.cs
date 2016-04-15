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
using System.Linq;

namespace rDSN.Tron.Utility
{
    public class DAGTraverserDirectional<VertexT, EdgeT, GraphT> : DAGTraverser<VertexT, EdgeT, GraphT>
        where VertexT : GenericVertex<VertexT, EdgeT, GraphT>
        where EdgeT : GenericEdge<VertexT, EdgeT, GraphT>
        where GraphT : GenericGraph<VertexT, EdgeT, GraphT>
    {
        private List<VertexT> _pendingVertices = new List<VertexT>();
        private HashSet<VertexT> _visitedVertices = new HashSet<VertexT>();

        private bool _isDownStream;
        
        public DAGTraverserDirectional(bool isDownStream)
        {
            _isDownStream = isDownStream;
        }

        public override bool Traverse(GraphT graph, VisitVertex visitVertex, bool breakIt, bool breakOnFalse)
        {
            graph.Vertices.Select(v => { v.Value.SetVisited(false); return 0; }).Count();
            _pendingVertices = graph.GetRootVertices(true);
            return TraverseWithPendingStartPoints(visitVertex, breakIt, breakOnFalse);
        }

        private bool TraverseWithPendingStartPoints(VisitVertex visitVertex, bool breakIt, bool breakOnFalse)
        {
            while (_pendingVertices.Count != 0)
            {
                var v = _pendingVertices[0];
                _pendingVertices.RemoveAt(0);
                var r = visitVertex(v);
                if (breakIt)
                {
                    if (r != breakOnFalse)
                    {
                        return r;
                    }
                }

                _visitedVertices.Add(v);

                if (!_isDownStream)
                {
                    foreach (var vv in v.InVertices)
                    {
                        if (!_visitedVertices.Contains(vv))
                        {
                            _pendingVertices.Add(vv);
                        }
                    }
                }
                else
                {
                    foreach (var vv in v.OutVertices)
                    {
                        if (!_visitedVertices.Contains(vv))
                        {
                            _pendingVertices.Add(vv);
                        }
                    }
                }
            }

            return true;
        }
    }
}
