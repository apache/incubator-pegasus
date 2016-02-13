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
