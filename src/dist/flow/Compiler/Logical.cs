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
                Console.WriteLine("\t" + lb.Key);
                foreach (var inst in lb.Value)
                {
                    Console.WriteLine("\t\t" + inst);
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
    }
}
