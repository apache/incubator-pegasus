using System;
using System.Collections.Generic;
using System.Text;
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
                VertexT v = _pendingVertices[0];
                _pendingVertices.RemoveAt(0);
                bool r = visitVertex(v);
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
                    foreach (VertexT vv in v.InVertices)
                    {
                        if (!_visitedVertices.Contains(vv))
                        {
                            _pendingVertices.Add(vv);
                        }
                    }
                }
                else
                {
                    foreach (VertexT vv in v.OutVertices)
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
