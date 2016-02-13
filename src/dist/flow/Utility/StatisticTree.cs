using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Diagnostics;

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
            writer.Write(Children.Count());
            foreach (var child in Children)
                child.Write(writer);

            return true;
        }

        public bool Read(BinaryReader reader)
        {
            Stat.Read(reader);
            int count = reader.ReadInt32();
            for (int i = 0; i < count; i++)
            {
                StatisticTree<StatisticType> temp = new StatisticTree<StatisticType>();
                temp.Read(reader);
                AddChild(temp);
            }
            return true;
        }

        private string PrintTree(int layer)
        {
            string s = "";
            for (int i = 0; i < layer; i++)
                s += ("    ");
            if (Children.Count() != 0)
                s += ("+");
            else
                s += (" ");
            s += Stat.Print() + "\r\n";
            foreach (var child in Children)
                s += child.PrintTree(layer + 1);
            return s;
        }

        public string ToVerbose()
        {
            return this.PrintTree(0);
        }
    }
}
