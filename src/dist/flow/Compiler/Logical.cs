using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Linq.Expressions;

using rDSN.Tron.Utility;

namespace rDSN.Tron.Compiler
{
    public class LVertex : GenericVertex<LVertex, LEdge, LGraph>
    {
        public LVertex(LGraph graph, ulong id)
            : base(graph, id)
        {
            Instructions = new Dictionary<LambdaExpression, Instruction[]>();
        }

        public MethodCallExpression Exp { get; set; }

        protected override string GetVisualizationProperties()
        {
            return "color=\"" + (Exp == null ? "gray" : "green") + "\";"
             + "label=\"" + Name + "\";"             
             + "shape=" + "circle" + ";"
             ;
        }

        // for each Non-ISymbol lambda, we generate the instruction set for it
        public Dictionary<LambdaExpression, Instruction[]> Instructions { get; set; }

        public void DumpInstructions()
        {
            Console.WriteLine(Name + " : ");

            foreach (var lb in Instructions)
            {
                Console.WriteLine("\t" + lb.Key.ToString());
                foreach (var inst in lb.Value)
                {
                    Console.WriteLine("\t\t" + inst.ToString());
                }
            }
        }
    }
            
    public class LEdge : GenericEdge<LVertex, LEdge, LGraph>
    {
        public LEdge(LGraph graph, LVertex startVertex, LVertex endVertex)
            : base(graph, startVertex, endVertex)
        {
            Type = FlowType.Unknown;
        }

        public enum FlowType
        { 
            Unknown,
            Data,
            Lambda,
            Call,
            Return
        }

        public FlowType Type {get; set; }

        protected override string GetVisualizationProperties()
        {
            return "color=\"" + (Type == FlowType.Data ? "black" : 
                (Type == FlowType.Call ? "red" : "purple"))  + "\"";
        }
    }

    public class LGraph : GenericGraph<LVertex, LEdge, LGraph>
    {
        public LGraph()
            : base()
        {        
        }
    }
}
