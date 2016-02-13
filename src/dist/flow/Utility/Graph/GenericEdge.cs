using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace rDSN.Tron.Utility
{
    public class GenericEdge<VertexT, EdgeT, GraphT> : AbstractGraphElement<EdgeT>
        where VertexT : GenericVertex<VertexT, EdgeT, GraphT>
        where EdgeT : GenericEdge<VertexT, EdgeT, GraphT>
        where GraphT : GenericGraph<VertexT, EdgeT, GraphT>
    {
        #region FIELDS
        private VertexT _startVertex;
        private VertexT _endVertex;
        private GraphT _graph;
        #endregion

        #region PROPERTIES
        public VertexT StartVertex { get { return _startVertex; } }
        public VertexT EndVertex { get { return _endVertex; } }
        public GraphT Graph { get { return _graph; } }
        #endregion

        public GenericEdge(GraphT graph, VertexT startVertex, VertexT endVertex)
        {
            _graph = graph;
            _startVertex = startVertex;
            _endVertex = endVertex;
        }

        protected virtual string GetVisualizationProperties()
        {
            return "style=\"bold\"";
        }

        public void Visualize(TextWriter output)
        {
            output.Write("    \"" + StartVertex.Id + "\"");
            output.Write("->");
            output.Write("\"" + EndVertex.Id + "\"");
            output.Write(" ");
            output.Write("[");
            output.Write(GetVisualizationProperties());
            output.Write("]");
            output.WriteLine();
        }

        public void Clear()
        {
            _startVertex = null;
            _endVertex = null;
            _graph = null;
        }
    }
}
