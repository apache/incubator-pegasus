using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace rDSN.Tron.Utility
{
    public abstract class DAGTraverser<VertexT, EdgeT, GraphT>
        where VertexT : GenericVertex<VertexT, EdgeT, GraphT>
        where EdgeT : GenericEdge<VertexT, EdgeT, GraphT>
        where GraphT : GenericGraph<VertexT, EdgeT, GraphT>
    {
        public enum TraverseDirection
        {
            UpStream,
            DownStream
        }

        public delegate bool VisitVertex(VertexT vertex);

        public abstract bool Traverse(GraphT graph, VisitVertex visitor, bool breakIt, bool breakOnFalse);
    }
}
