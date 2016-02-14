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
using System.Linq;
using System.Text;
using rDSN.Tron.Utility;

namespace rDSN.Tron.Utility
{
    public class DAGTraverserSatisfied<VertexT, EdgeT, GraphT> : DAGTraverser<VertexT, EdgeT, GraphT>
        where VertexT : GenericVertex<VertexT, EdgeT, GraphT>
        where EdgeT : GenericEdge<VertexT, EdgeT, GraphT>
        where GraphT : GenericGraph<VertexT, EdgeT, GraphT>
    {
        private HashSet<VertexT> _pendingVertices = new HashSet<VertexT>();

        private bool _isDownStream;

        public DAGTraverserSatisfied(bool isDownStream)
        {
            _isDownStream = isDownStream;
        }

        public override bool Traverse(GraphT graph, VisitVertex visitVertex, bool breakIt, bool breakOnFalse)
        {
            graph.Vertices.Select(v => { v.Value.SetVisited(false); return 0; }).Count();

            _pendingVertices.Clear();
            _pendingVertices.UnionWith(graph.GetRootVertices(_isDownStream));

            while (_pendingVertices.Count != 0)
            {
                foreach (VertexT v in _pendingVertices)
                {
                    bool isAllDirectionalVerticesSatisfied = true;

                    if (_isDownStream)
                    {
                        foreach (VertexT inVertex in v.InVertices)
                        {
                            if (!inVertex.IsVisited())
                            {
                                isAllDirectionalVerticesSatisfied = false;
                                break;
                            }
                        }
                    }
                    else
                    {
                        foreach (VertexT outVertex in v.OutVertices)
                        {
                            if (!outVertex.IsVisited())
                            {
                                isAllDirectionalVerticesSatisfied = false;
                                break;
                            }
                        }
                    }

                    if (isAllDirectionalVerticesSatisfied)
                    {
                        bool r = visitVertex(v);
                        if (breakIt)
                        {
                            if (r != breakOnFalse)
                                return r;
                        }
                        v.SetVisited(true);
                        _pendingVertices.Remove(v);
                        _pendingVertices.UnionWith(_isDownStream ? v.OutVertices : v.InVertices);
                        break;
                    }
                }
            }
            return true;
        }
    }
}
