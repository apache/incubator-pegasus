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

using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace rDSN.Tron.Utility
{
    public interface StatisticNode : ISerializable
    {
        string Print();
    }

    public class StatisticTree<StatisticType> : ISerializable
        where StatisticType : StatisticNode, new()
    {
        public StatisticType Stat { get; set; }

        private List<StatisticTree<StatisticType>> Children = new List<StatisticTree<StatisticType>>();

        public StatisticTree(StatisticType statistic)
        {
            Stat = statistic;
        }

        public StatisticTree()
        {
            Stat = new StatisticType();
        }

        public void AddChild(StatisticTree<StatisticType> child)
        {
            Children.Add(child);
        }

        public void SetChild(List<StatisticTree<StatisticType>> children)
        {
            Children = children;
        }

        public List<StatisticTree<StatisticType>> GetChildrenList()
        {
            return Children;
        }

        public bool Write(BinaryWriter writer)
        {
            Stat.Write(writer);
            writer.Write(Children.Count);
            foreach (var child in Children)
                child.Write(writer);

            return true;
        }

        public bool Read(BinaryReader reader)
        {
            Stat.Read(reader);
            var count = reader.ReadInt32();
            for (var i = 0; i < count; i++)
            {
                var temp = new StatisticTree<StatisticType>();
                temp.Read(reader);
                AddChild(temp);
            }
            return true;
        }

        private string PrintTree(int layer)
        {
            var s = "";
            for (var i = 0; i < layer; i++)
                s += ("    ");
            if (Children.Count != 0)
                s += ("+");
            else
                s += (" ");
            s += Stat.Print() + "\r\n";
            return Children.Aggregate(s, (current, child) => current + child.PrintTree(layer + 1));
        }

        public string ToVerbose()
        {
            return PrintTree(0);
        }
    }
}
