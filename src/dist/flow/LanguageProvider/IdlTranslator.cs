using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.IO;
using System.Diagnostics;
using System.Runtime.InteropServices;




using rDSN.Tron.Utility;
using rDSN.Tron.Contract;

namespace rDSN.Tron.LanguageProvider
{
    public abstract class IdlTranslator
    {
        public IdlTranslator()
        {

        }
        public IdlTranslator(ServiceSpecType t)
        {
            specType = t;
        }

        public ServiceSpecType GetType()
        {
            return specType;
        }

        public static IdlTranslator GetInstance(ServiceSpecType t)
        {
            switch(t)
            {
                case ServiceSpecType.Bond_3_0:
                    return new BondTranslator();
                case ServiceSpecType.Thrift_0_9:
                    return new ThriftTranslator();
                case ServiceSpecType.Proto_Buffer_1_0:
                    throw new NotImplementedException();
                default:
                    return null;
            }
        }

        public virtual bool ToCommonInterface(string dir, string file)
        {
            return ToCommonInterface(dir, file, dir, null, false);
        }
        public virtual bool ToCommonInterface(string dir, string file, List<string> args)
        {
            return ToCommonInterface(dir, file, dir, args, false);
        }
        public virtual bool ToCommonInterface(string dir, string file, string outDir) 
        {
            return ToCommonInterface(dir, file, outDir, null, false);
        }

        /// <summary>
        /// translate the target IDL file to Common Interface
        /// </summary>
        /// <param name="dir"></param>
        /// <param name="file"></param>
        /// <param name="outDir"></param>
        /// /// <param name="args"></param>
        /// <returns></returns>
        public abstract bool ToCommonInterface(string dir, string file, string outDir, List<string> args, bool needCompiled = false);


        
        protected bool Compile(string input)
        {
            return Compile(input, Path.GetDirectoryName(input));
        }

        protected bool Compile(string input, string outDir)
        {
            var dir = Path.GetDirectoryName(input);
            var file = Path.GetFileName(input);

            var output = Path.Combine(outDir, Path.GetFileNameWithoutExtension(file) + ".dll");
            var cscPath = Path.Combine(RuntimeEnvironment.GetRuntimeDirectory(), "csc.exe");
            var libFiles = "rDSN.Tron.Contract.dll ";
            var arguments = "/target:library /out:" + output + " " + input + " /r:" + libFiles;

            // if input is a folder, the default action is to compile all *.cs files in the folder recursively
            if (Directory.Exists(input))
            {
                return false;
            }

            if (!Directory.Exists(dir))
            {
                Console.WriteLine(dir + "does not exist!");
                return false;
            }
            if (!File.Exists(input))
            {
                Console.WriteLine(file + " does not exist!");
                return false;
            }
            
            return SystemHelper.RunProcess(cscPath, arguments) == 0;

        }

        

        /*
        #region generate common interface from Bond file
        private static void generateEnums(ParseTree tree, CodeBuilder c)
        {
            var declarations = tree.Root.ChildNodes[0].ChildNodes;
            var enumDeclarations = declarations
                .Select(d => d.ChildNodes[0]).Where(d => d.Term.Name == "type-declaration")
                .Select(d => d.ChildNodes[0]).Where(d => d.Term.Name == "enum-declaration");

            foreach (var e in enumDeclarations)
            {
                var classOpt = e.ChildNodes.Find(n => n.Term.Name == "class-option");
                var classOptName = classOpt.ChildNodes.Count() == 0 ? "public " : classOpt.ChildNodes[0].Token.Text + " ";
                var name = e.ChildNodes.Find(n => n.Term.Name == "user-type").Token.Text;
                var fields = e.ChildNodes.Find(n => n.Term.Name == "enum-declarations").ChildNodes.Select(n => n.ChildNodes[0].Token.Text).ToList();
                c.AppendLine(classOptName + "enum " + name);
                c.BeginBlock();

                for (int i = 0; i < fields.Count() - 1; i++)
                {
                    c.AppendLine(fields[i] + ",");
                }
                c.AppendLine(fields.Last());

                c.EndBlock();
                c.AppendLine();
            }

        }


        private static void generateStructs(ParseTree tree, CodeBuilder c)
        {
            var declarations = tree.Root.ChildNodes[0].ChildNodes;
            var structDeclarations = declarations
                .Select(d => d.ChildNodes[0]).Where(d => d.Term.Name == "type-declaration")
                .Select(d => d.ChildNodes[0]).Where(d => d.Term.Name == "struct-declaration");

            foreach (var s in structDeclarations)
            {
                var classOpt = s.ChildNodes.Find(n => n.Term.Name == "class-option");

                var classOptName = classOpt.ChildNodes.Count() == 0 ? "public " : classOpt.ChildNodes[0].Token.Text + " ";
                var name = s.ChildNodes.Find(n => n.Term.Name == "user-type").Token.Text;
                var fields = getNodesByTermName(s.ChildNodes.Find(n => n.Term.Name == "block"), "field-declaration", false);
                c.AppendLine(classOptName + "class " + name);
                c.BeginBlock();

                foreach (var f in fields)
                {
                    var runType = f.ChildNodes.Find(n => n.Term.Name == "runtime-type");
                    var fieldTypeName = getTypeName(runType);

                    var fieldName = f.ChildNodes.Find(n => n.Term.Name == "identifier").Token.Text;
                    c.AppendLine("public " + fieldTypeName + " " + fieldName + ";");
                }

                c.EndBlock();
                c.AppendLine();
            }



        }
        private static void generateServices(ParseTree tree, CodeBuilder c)
        {

            var serviceDeclarations = getNodesByTermName(tree.Root, "service-declaration", false);

            foreach (var s in serviceDeclarations)
            {
                var classOpt = getNodeByTermName(s, "class-option");
                var classOptName = classOpt.ChildNodes.Count() == 0 ? "public " : classOpt.ChildNodes[0].Token.Text + " ";
                var serviceName = getNodeByTermName(s, "identifier").Token.Text;
                var methods = getNodesByTermName(getNodeByTermName(s, "block"), "method-declaration", false);
                c.AppendLine(classOptName + "interface " + serviceName);
                c.BeginBlock();
                foreach (var m in methods)
                {
                    var outputType = getTypeName(getNodeByTermName(m, "runtime-type"));
                    var methodName = getNodeByTermName(m, "method-name").Token.Text;
                    var parameters = getNodesByTermName(m, "parameters", true);
                    List<string> parameterList = new List<string>();
                    foreach (var p in parameters)
                    {
                        parameterList.Add(getTypeName(getNodeByTermName(p, "runtime-type")) + " " + getNodeByTermName(p, "identifier").Token.Text);
                    }
                    c.AppendLine(outputType + " " + methodName + "(" + string.Join(", ", parameterList) + ");");

                }
                c.EndBlock();
                c.AppendLine();








            }


        }

        private static ParseTreeNodeList getNodesByTermName(ParseTreeNode parent, string name, bool deepSearch)
        {
            ParseTreeNodeList res = new ParseTreeNodeList();
            if (parent.Term.Name == name)
            {
                res.Add(parent);
                if (!deepSearch)
                {
                    return res;
                }
            }

            foreach (var node in parent.ChildNodes)
            {
                var children = getNodesByTermName(node, name, deepSearch);
                if (children != null)
                {
                    res.AddRange(children);
                }
            }
            return res;
        }
        private static ParseTreeNode getNodeByTermName(ParseTreeNode parent, string name)
        {

            if (parent.Term.Name == name)
            {
                return parent;
            }

            foreach (var node in parent.ChildNodes)
            {
                var children = getNodeByTermName(node, name);
                if (children != null)
                {
                    return children;
                }
            }
            return null;

        }

        private static string getTypeName(ParseTreeNode parent)
        {
            string name = "";
            var nodeList = getNodesByTermName(parent, "generic-type", false);
            if (nodeList.Count() > 0)
            {
                List<string> genericTypes = new List<string>();
                List<string> nodeTokens = new List<string>();
                foreach (var node in nodeList[0].ChildNodes)
                {
                    if (node.Term.Name.EndsWith("type"))
                    {
                        genericTypes.Add(getTypeName(node));
                    }
                    else
                    {
                        nodeTokens.Add(node.Token.Text);
                    }
                }
                nodeTokens.Insert(nodeTokens.Count() - 1, string.Join(", ", genericTypes));
                name = string.Join("", nodeTokens);

            }
            else
            {
                nodeList = getNodesByTermName(parent, "user-type", false);
                if (nodeList.Count() > 0)
                {
                    name = nodeList[0].Token.Text;
                }
                else
                {
                    nodeList = getNodesByTermName(parent, "variable-type", false);
                    name = nodeList[0].ChildNodes[0].Token.Text;
                    // map from Bond type to C# type
                }
            }
            return name;
        }


        #endregion
        */


        

        protected ServiceSpecType specType;
        protected const string extension = ".cs";

    }
}
