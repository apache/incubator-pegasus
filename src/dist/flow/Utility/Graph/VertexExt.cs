using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using rDSN.Tron.Utility;

namespace rDSN.Tron.Utility
{
    public static class VertexExt
    {
        public static bool IsVisited<VertexT, EdgeT, GraphT>(this GenericVertex<VertexT, EdgeT, GraphT> v)
            where VertexT : GenericVertex<VertexT, EdgeT, GraphT>
            where EdgeT : GenericEdge<VertexT, EdgeT, GraphT>
            where GraphT : GenericGraph<VertexT, EdgeT, GraphT>
        {
            return v.Flags[(int)GenericVertex<VertexT, EdgeT, GraphT>.VertexFlags.VertexFlags_Visited];
        }

        public static void SetVisited<VertexT, EdgeT, GraphT>(this GenericVertex<VertexT, EdgeT, GraphT> v, bool visitedOrNot)
            where VertexT : GenericVertex<VertexT, EdgeT, GraphT>
            where EdgeT : GenericEdge<VertexT, EdgeT, GraphT>
            where GraphT : GenericGraph<VertexT, EdgeT, GraphT>
        {
            v.Flags[(int)GenericVertex<VertexT, EdgeT, GraphT>.VertexFlags.VertexFlags_Visited] = visitedOrNot;
        }
    }
}
