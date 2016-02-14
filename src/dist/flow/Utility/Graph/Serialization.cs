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
using System.IO;

namespace rDSN.Tron.Utility
{
    public static partial class Serializer
    {
        public partial class GraphSerializer
        {
            public static void Read<instantiatedClassT>(BinaryReader reader, AbstractGraphElement<instantiatedClassT> ae)
            {
                ae.Name = Encoding.ASCII.GetString(reader.ReadBytes(reader.ReadInt32()));

                ae.Description = Encoding.ASCII.GetString(reader.ReadBytes(reader.ReadInt32()));
            }

            public static void Write<instantiatedClassT>(BinaryWriter writer, AbstractGraphElement<instantiatedClassT> ae)
            {
                writer.Write(Encoding.ASCII.GetBytes(ae.Name));

                writer.Write(Encoding.ASCII.GetBytes(ae.Description));
            }

            public static void Read<VertexT, EdgeT, GraphT>(BinaryReader reader, GenericEdge<VertexT, EdgeT, GraphT> edge)
                where VertexT : GenericVertex<VertexT, EdgeT, GraphT>
                where EdgeT : GenericEdge<VertexT, EdgeT, GraphT>
                where GraphT : GenericGraph<VertexT, EdgeT, GraphT>
            {
                Read<EdgeT>(reader, (AbstractGraphElement<EdgeT>)edge);
            }

            public static void Write<VertexT, EdgeT, GraphT>(BinaryWriter writer, GenericEdge<VertexT, EdgeT, GraphT> edge)
                where VertexT : GenericVertex<VertexT, EdgeT, GraphT>
                where EdgeT : GenericEdge<VertexT, EdgeT, GraphT>
                where GraphT : GenericGraph<VertexT, EdgeT, GraphT>
            {
                Write<EdgeT>(writer, (AbstractGraphElement<EdgeT>)edge);
            }

            public static void Read<VertexT, EdgeT, GraphT>(BinaryReader reader, GenericVertex<VertexT, EdgeT, GraphT> vertex)
                where VertexT : GenericVertex<VertexT, EdgeT, GraphT>
                where EdgeT : GenericEdge<VertexT, EdgeT, GraphT>
                where GraphT : GenericGraph<VertexT, EdgeT, GraphT>
            {
                int i;
                UInt32 flag;
                bool b;

                Read<VertexT>(reader, (AbstractGraphElement<VertexT>)vertex);

                flag = reader.ReadUInt32();
                for (i = 0; i < (int)GenericVertex<VertexT, EdgeT, GraphT>.VertexFlags.VertexFlags_MAX; i++)
                {
                    b = (flag & (1U << i)) != 0;
                    vertex.Flags.Set(i, b);
                }
            }

            public static void Write<VertexT, EdgeT, GraphT>(BinaryWriter writer, GenericVertex<VertexT, EdgeT, GraphT> vertex)
                where VertexT : GenericVertex<VertexT, EdgeT, GraphT>
                where EdgeT : GenericEdge<VertexT, EdgeT, GraphT>
                where GraphT : GenericGraph<VertexT, EdgeT, GraphT>
            {
                int i;
                UInt32 flag = 0;

                Write<VertexT>(writer, (AbstractGraphElement<VertexT>)vertex);

                for (i = 0; i < (int)GenericVertex<VertexT, EdgeT, GraphT>.VertexFlags.VertexFlags_MAX; i++)
                {
                    if (vertex.Flags[i])
                    {
                        flag |= (1U << i);
                    }
                }
                writer.Write(flag);
            }

            public static void Read<VT, ET, VertexT, EdgeT, GraphT>(BinaryReader reader, GenericGraph<VertexT, EdgeT, GraphT> graph)
                where VT : VertexT
                where ET : EdgeT
                where VertexT : GenericVertex<VertexT, EdgeT, GraphT>
                where EdgeT : GenericEdge<VertexT, EdgeT, GraphT>
                where GraphT : GenericGraph<VertexT, EdgeT, GraphT>
            {
                  int i;
                int size;
                UInt64 id, id2;
                VertexT v, v2;
              
                graph.Clear();

                Read<GraphT>(reader, (AbstractGraphElement<GraphT>)graph);

                size = reader.ReadInt32();
                for (i = 0; i < size; i++)
                {
                    id = reader.ReadUInt64();
                    v = graph.CreateVertex(typeof(VT), id);
                    Read(reader, v);
                }

                size = reader.ReadInt32();
                for (i = 0; i < size; i++)
                {
                    id = reader.ReadUInt64();
                    v = graph.GetVertexById(id);

                    id2 = reader.ReadUInt64();
                    v2 = graph.GetVertexById(id2);

                    EdgeT e = v.ConnectTo<ET>(v2);

                    Read(reader, e);
                }
            }

            public static void Write<VertexT, EdgeT, GraphT>(BinaryWriter writer, GenericGraph<VertexT, EdgeT, GraphT> graph)
                where VertexT : GenericVertex<VertexT, EdgeT, GraphT>
                where EdgeT : GenericEdge<VertexT, EdgeT, GraphT>
                where GraphT : GenericGraph<VertexT, EdgeT, GraphT>
            {
                Write<GraphT>(writer, (AbstractGraphElement<GraphT>)graph);

                writer.Write(graph.Vertices.Count);
                foreach (KeyValuePair<UInt64, VertexT> pair in graph.Vertices)
                {
                    writer.Write(pair.Key);
                    Write(writer, pair.Value);
                }

                writer.Write(graph.Edges.Count);
                foreach (EdgeT e in graph.Edges)
                {
                    writer.Write(e.StartVertex.Id);
                    writer.Write(e.EndVertex.Id);
                    Write(writer, e);
                }
            }
        }
    }
}
