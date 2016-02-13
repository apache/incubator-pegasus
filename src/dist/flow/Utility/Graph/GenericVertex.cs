using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using System.Collections;
using System.IO;

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
            VertexFlags_MAX = 1,
        }

        #region FIELDS
        private HashSet<EdgeT> _in = new HashSet<EdgeT>();
        private HashSet<EdgeT> _out = new HashSet<EdgeT>();
        private GraphT _graph;
        private BitArray _flags = new BitArray((int)VertexFlags.VertexFlags_MAX);
        private UInt64 _id;
        #endregion

        #region PROPERTIES
        public HashSet<EdgeT> InEdges { get { return _in; } }
        public HashSet<EdgeT> OutEdges { get { return _out; } }
        public GraphT Graph { get { return _graph; } }
        public BitArray Flags { get { return _flags; } }
        public UInt64 Id { get { return _id; } }

        public List<VertexT> InVertices
        {
            get
            {
                List<VertexT> inVertices = new List<VertexT>();
                foreach (EdgeT e in InEdges)
                {
                    inVertices.Add(e.StartVertex);
                }
                return inVertices;
            }
        }

        public List<VertexT> OutVertices
        {
            get
            {
                List<VertexT> outVertices = new List<VertexT>();
                foreach (EdgeT e in OutEdges)
                {
                    outVertices.Add(e.EndVertex);
                }
                return outVertices;
            }
        }
        #endregion

        public GenericVertex(GraphT graph, UInt64 id)
        {
            _graph = graph;
            _id = id;
        }

        public HashSet<VertexT> GetVerticesClosure(bool upStream)
        {
            HashSet<VertexT> vs = new HashSet<VertexT>();
            vs.Add((VertexT)this);

            Queue<VertexT> pendingVertices = new Queue<VertexT>();
            pendingVertices.Enqueue((VertexT)this);

            while (pendingVertices.Count > 0)
            {
                VertexT v = pendingVertices.Dequeue();
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
            Type[] ps = new Type[3] { typeof(GraphT), typeof(VertexT), typeof(VertexT) };
            Object[] pss = new Object[3] {_graph, this, targetVertex};
            ET e = (ET)typeof(ET).GetConstructor(ps).Invoke(pss);
            _graph.Edges.Add(e);

            _out.Add(e);
            targetVertex._in.Add(e);
            return e;
        }

        protected virtual string GetVisualizationProperties()
        {
            string s = "";
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

