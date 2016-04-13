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
using System.Diagnostics;
using System.Dynamic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using rDSN.Tron.Contract;
using rDSN.Tron.Utility;

namespace rDSN.Tron.Compiler
{
    //class Expression2Code : ExpressionVisitor<string, int>
    public class ExpressionToCode 
    {
        // Fields
        private Dictionary<object, int> _ids = new Dictionary<object,int>();
        private StringBuilder _out = new StringBuilder();
        private Expression _exp;
        private QueryContext _context;
        private Dictionary<Expression, object> _externalObjects = new Dictionary<Expression, object>();
        private HashSet<Expression> _tempVarsUndefined = new HashSet<Expression>();
        private int _indent;

        private void NewLine()
        {
            Out("\r\n");
            for (var i = 0; i < _indent; i++)
                Out("\t");
        }

        public ExpressionToCode(Expression exp, QueryContext context)
        {
            _exp = exp;
            _context = context;

            foreach (var x in _context.TempSymbols)
            {
                _tempVarsUndefined.Add(x.Key);
            }
        }

        public string GenCode(int indent = 0)
        {
            _indent = indent;

            if (_tempVarsUndefined.Count == 0)
                Out("return ");

            Visit(_exp);
            return ToString();
        }

        private bool GetValue(Expression exp, out object value)
        {
            value = null;

            try
            {
                var lambda = Expression.Lambda(exp);
                value = lambda.Compile().DynamicInvoke();
                return true;
            }
            catch (Exception)
            {
                //Console.WriteLine("Cannot get value of exp '" + exp.ToString() + "', with error '" + e.Message + "'");
                return false;
            }
        }

        private void AddLabel(LabelTarget label)
        {
            if (!_ids.ContainsKey(label))
            {
                _ids.Add(label, _ids.Count);
            }
        }

        private void AddParam(ParameterExpression p)
        {
            if (!_ids.ContainsKey(p))
            {
                _ids.Add(p, _ids.Count);
            }
        }
                
        private void DumpLabel(LabelTarget target)
        {
            if (!string.IsNullOrEmpty(target.Name))
            {
                Out(target.Name);
            }
            else
            {
                Out("UnamedLabel_" + GetLabelId(target));
            }
        }

        private static string FormatBinder(CallSiteBinder binder)
        {
            var binder2 = binder as ConvertBinder;
            if (binder2 != null)
            {
                return (" " + binder2.Type);
            }
            var binder3 = binder as GetMemberBinder;
            if (binder3 != null)
            {
                return ("GetMember " + binder3.Name);
            }
            var binder4 = binder as SetMemberBinder;
            if (binder4 != null)
            {
                return ("SetMember " + binder4.Name);
            }
            var binder5 = binder as DeleteMemberBinder;
            if (binder5 != null)
            {
                return ("DeleteMember " + binder5.Name);
            }
            if (binder is GetIndexBinder)
            {
                return "GetIndex";
            }
            if (binder is SetIndexBinder)
            {
                return "SetIndex";
            }
            if (binder is DeleteIndexBinder)
            {
                return "DeleteIndex";
            }
            var binder6 = binder as InvokeMemberBinder;
            if (binder6 != null)
            {
                return ("Call " + binder6.Name);
            }
            if (binder is InvokeBinder)
            {
                return "Invoke";
            }
            if (binder is CreateInstanceBinder)
            {
                return "Create";
            }
            var binder7 = binder as UnaryOperationBinder;
            if (binder7 != null)
            {
                return binder7.Operation.ToString();
            }
            var binder8 = binder as BinaryOperationBinder;
            return binder8?.Operation.ToString() ?? "CallSiteBinder";
        }

        private int GetLabelId(LabelTarget label)
        {
            int count;
            if (_ids == null)
            {
                _ids = new Dictionary<object, int>();
                AddLabel(label);
                return 0;
            }
            if (_ids.TryGetValue(label, out count)) return count;
            count = _ids.Count;
            AddLabel(label);
            return count;
        }

        private int GetParamId(ParameterExpression p)
        {
            int count;
            if (_ids == null)
            {
                _ids = new Dictionary<object, int>();
                AddParam(p);
                return 0;
            }
            if (_ids.TryGetValue(p, out count)) return count;
            count = _ids.Count;
            AddParam(p);
            return count;
        }

        private void Out(char c)
        {
            _out.Append(c);
        }

        private void Out(string s)
        {
            _out.Append(s);
        }

        public override string ToString()
        {
            return _out.ToString();
        }

        internal void VisitBinary(BinaryExpression node)
        {
            string str;
            switch (node.NodeType)
            {
                case ExpressionType.Add:
                    str = "+";
                    break;

                case ExpressionType.AddChecked:
                    str = "+";
                    break;

                case ExpressionType.And:
                    if (!(node.Type == typeof(bool)) && !(node.Type == typeof(bool?)))
                    {
                        str = "&";
                    }
                    else
                    {
                        str = "&"; // And
                    }
                    break;

                case ExpressionType.AndAlso:
                    str = "&&";
                    break;

                case ExpressionType.Coalesce:
                    str = "??";
                    break;

                case ExpressionType.Divide:
                    str = "/";
                    break;

                case ExpressionType.Equal:
                    str = "==";
                    break;

                case ExpressionType.ExclusiveOr:
                    str = "^";
                    break;

                case ExpressionType.GreaterThan:
                    str = ">";
                    break;

                case ExpressionType.GreaterThanOrEqual:
                    str = ">=";
                    break;

                case ExpressionType.LeftShift:
                    str = "<<";
                    break;

                case ExpressionType.LessThan:
                    str = "<";
                    break;

                case ExpressionType.LessThanOrEqual:
                    str = "<=";
                    break;

                case ExpressionType.Modulo:
                    str = "%";
                    break;

                case ExpressionType.Multiply:
                    str = "*";
                    break;

                case ExpressionType.MultiplyChecked:
                    str = "*";
                    break;

                case ExpressionType.NotEqual:
                    str = "!=";
                    break;

                case ExpressionType.Or:
                    if (!(node.Type == typeof(bool)) && !(node.Type == typeof(bool?)))
                    {
                        str = "|";
                    }
                    else
                    {
                        str = "|"; // or
                    }
                    break;

                case ExpressionType.OrElse:
                    str = "||"; // OrElse
                    break;

                case ExpressionType.Power:
                    str = "^";
                    break;

                case ExpressionType.RightShift:
                    str = ">>";
                    break;

                case ExpressionType.Subtract:
                    str = "-";
                    break;

                case ExpressionType.SubtractChecked:
                    str = "-";
                    break;

                case ExpressionType.Assign:
                    str = "=";
                    break;

                case ExpressionType.AddAssign:
                    str = "+=";
                    break;

                case ExpressionType.AndAssign:
                    if (!(node.Type == typeof(bool)) && !(node.Type == typeof(bool?)))
                    {
                        str = "&=";
                    }
                    else
                    {
                        str = "&&=";
                    }
                    break;

                case ExpressionType.DivideAssign:
                    str = "/=";
                    break;

                case ExpressionType.ExclusiveOrAssign:
                    str = "^=";
                    break;

                case ExpressionType.LeftShiftAssign:
                    str = "<<=";
                    break;

                case ExpressionType.ModuloAssign:
                    str = "%=";
                    break;

                case ExpressionType.MultiplyAssign:
                    str = "*=";
                    break;

                case ExpressionType.OrAssign:
                    if (!(node.Type == typeof(bool)) && !(node.Type == typeof(bool?)))
                    {
                        str = "|=";
                    }
                    else
                    {
                        str = "||=";
                    }
                    break;

                case ExpressionType.PowerAssign:
                    str = "**=";
                    break;

                case ExpressionType.RightShiftAssign:
                    str = ">>=";
                    break;

                case ExpressionType.SubtractAssign:
                    str = "-=";
                    break;

                case ExpressionType.AddAssignChecked:
                    str = "+=";
                    break;

                case ExpressionType.MultiplyAssignChecked:
                    str = "*=";
                    break;

                case ExpressionType.SubtractAssignChecked:
                    str = "-=";
                    break;

                case ExpressionType.ArrayIndex:
                    Visit(node.Left);
                    Out("[");
                    Visit(node.Right);
                    Out("]");
                    return;

                default:
                    throw new InvalidOperationException();
            }
            Out("(");
            Visit(node.Left);
            Out(' ');
            Out(str);
            Out(' ');
            Visit(node.Right);
            Out(")");
        }

        internal void VisitBlock(BlockExpression node)
        {
            Out("{");
            foreach (var expression in node.Variables)
            {
                Out("var ");
                Visit(expression);
                Out(";");
            }
            Out(" ... }");
        }

        internal void VisitCatchBlock(CatchBlock node)
        {
            Out("catch (" + node.Test.Name);
            if (node.Variable != null)
            {
                Out(node.Variable.Name);
            }
            Out(") { ... }");
        }

        internal void VisitConditional(ConditionalExpression node)
        {
            Out("(");
            Visit(node.Test);
            Out(") ? (");
            Visit(node.IfTrue);
            Out(") : (");
            Visit(node.IfFalse);
            Out(")");
        }

        internal void VisitConstant(ConstantExpression node)
        {
            string v;
            var r = LocalTypeHelper.ConstantValue2String(node.Value, out v);
            if (!r)
            {
                if (node.Type.IsGenericType && node.Type.BaseType == typeof(ISymbol))
                {
                    v = "request";
                }
            }
            Out(v);
        }

        internal void VisitDebugInfo(DebugInfoExpression node)
        {
            var s = string.Format(CultureInfo.CurrentCulture, "<DebugInfo({0}: {1}, {2}, {3}, {4})>", node.Document.FileName, node.StartLine, node.StartColumn, node.EndLine, node.EndColumn);
            Out(s);
        }

        internal void VisitDefault(DefaultExpression node)
        {
            Out("default(");
            Out(node.Type.FullName.Replace('+', '.'));
            Out(")");
        }

        internal void VisitDynamic(DynamicExpression node)
        {
            Out(FormatBinder(node.Binder));
            VisitExpressions('(', node.Arguments, ')');
        }

        ElementInit VisitElementInit(ElementInit initializer)
        {
            Out(initializer.AddMethod.ToString());
            VisitExpressions('(', initializer.Arguments, ')');
            return initializer;
        }

        private void VisitExpressions<T>(char open, IList<T> expressions, char close) where T : Expression
        {
            Out(open);
            if (expressions != null)
            {
                var flag = true;
                foreach (var local in expressions)
                {
                    if (flag)
                    {
                        flag = false;
                    }
                    else
                    {
                        Out(", ");
                    }
                    Visit(local);
                }
            }
            Out(close);
        }

        internal void VisitExtension(Expression node)
        {
            const BindingFlags bindingAttr = BindingFlags.ExactBinding | BindingFlags.Public | BindingFlags.Instance;
            if (node.GetType().GetMethod("ToString", bindingAttr, null, Type.EmptyTypes, null).DeclaringType != typeof(Expression))
            {
                Out(node.ToString());
                return;
            }
            Out("[");
            Out(node.NodeType == ExpressionType.Extension ? node.GetType().FullName : node.NodeType.ToString());
            Out("]");
        }

        internal void VisitGoto(GotoExpression node)
        {
            Out(node.Kind.ToString().ToLower(CultureInfo.CurrentCulture));
            DumpLabel(node.Target);
            if (node.Value == null) return;
            Out(" (");
            Visit(node.Value);
            Out(") ");
        }

        internal void VisitIndex(IndexExpression node)
        {
            if (node.Object != null)
            {
                Visit(node.Object);
            }
            else
            {
                Out(node.Indexer.DeclaringType.Name);
            }
            if (node.Indexer != null)
            {
                Out(".");
                Out(node.Indexer.Name);
            }
            VisitExpressions('[', node.Arguments, ']');
        }

        internal void VisitInvocation(InvocationExpression node)
        {
            Out("Invoke(");
            Visit(node.Expression);
            var num = 0;
            var count = node.Arguments.Count;
            while (num < count)
            {
                Out(", ");
                Visit(node.Arguments[num]);
                num++;
            }
            Out(")");
        }

        internal void VisitLabel(LabelExpression node)
        {
            Out("{ ... } ");
            DumpLabel(node.Target);
            Out(":");
        }

        internal void VisitLambda(LambdaExpression node)
        {
            if (node.Parameters.Count == 1)
            {
                Visit(node.Parameters[0]);
            }
            else
            {
                VisitExpressions('(', node.Parameters, ')');
            }
            Out(" => ");

            var needIndent = node.Parameters[0].Type.IsSymbol()
                || node.Parameters[0].Type.IsSymbols()
                || node.Parameters[0].Type.IsEnumerable();

            if (needIndent) ++_indent;

            Visit(node.Body);

            if (needIndent) --_indent;
        }

        internal void VisitListInit(ListInitExpression node)
        {
            Visit(node.NewExpression);
            Out(" {");
            var num = 0;
            var count = node.Initializers.Count;
            while (num < count)
            {
                if (num > 0)
                {
                    Out(", ");
                }
                Out(node.Initializers[num].ToString());
                num++;
            }
            Out("}");
        }

        internal void VisitLoop(LoopExpression node)
        {
            Out("loop { ... }");
        }

        internal void VisitMember(MemberExpression node)
        {
            string s;
            object value;
            if (_externalObjects.ContainsKey(node))
            {
                var r = LocalTypeHelper.ConstantValue2String(_externalObjects[node], out s);
                if (r)
                {
                    Out(s);
                    return;
                }
            }
            else if (GetValue(node, out value))
            {
                _externalObjects[node] = value;
                var r = LocalTypeHelper.ConstantValue2String(_externalObjects[node], out s);
                if (r)
                {
                    Out(s);
                    return;
                }
                if (node.Type.IsSymbol() || node.Type.IsSymbols())
                {
                    if (value != null)
                    {
                        Visit((value as ISymbol).Expression);
                    }
                    else
                    {
                        Out(node.Member.Name);
                    }
                    return;
                }
            }

            if (node.Expression != null)
            {
                Visit(node.Expression);
                Out(".");
            }
            Out(node.Member.Name);
        }

        internal void VisitMemberAssignment(MemberAssignment assignment)
        {
            Out(assignment.Member.Name);
            Out(" = ");
            Visit(assignment.Expression);
        }

        internal void VisitMemberInit(MemberInitExpression node)
        {
            if ((node.NewExpression.Arguments.Count == 0) && node.NewExpression.Type.Name.Contains("<"))
            {
                Out("new");
            }
            else
            {
                Visit(node.NewExpression);
            }
            Out(" {");
            var num = 0;
            var count = node.Bindings.Count;
            while (num < count)
            {
                var binding = node.Bindings[num];
                if (num > 0)
                {
                    Out(", ");
                }
                VisitMemberBinding(binding);
                num++;
            }
            Out("}");
        }

        internal void VisitMemberListBinding(MemberListBinding binding)
        {
            Out(binding.Member.Name);
            Out(" = {");
            var num = 0;
            var count = binding.Initializers.Count;
            while (num < count)
            {
                if (num > 0)
                {
                    Out(", ");
                }
                VisitElementInit(binding.Initializers[num]);
                num++;
            }
            Out("}");
        }

        internal void VisitMemberMemberBinding(MemberMemberBinding binding)
        {
            Out(binding.Member.Name);
            Out(" = {");
            var num = 0;
            var count = binding.Bindings.Count;
            while (num < count)
            {
                if (num > 0)
                {
                    Out(", ");
                }
                VisitMemberBinding(binding.Bindings[num]);
                num++;
            }
            Out("}");
        }

        internal void VisitMethodCall(MethodCallExpression node)
        {
            var tempVar = string.Empty;
            if ((node.Type.IsSymbol() || node.Type.IsSymbols()) && _context.TempSymbols.ContainsKey(node))
            {
                tempVar = _context.TempSymbols[node];
                _tempVarsUndefined.Remove(node);

                NewLine();
                Out("var " + tempVar + " = ");
            }

            var num = 0;
            var expression = node.Object;
            if (Attribute.GetCustomAttribute(node.Method, typeof(ExtensionAttribute)) != null)
            {
                num = 1;
                expression = node.Arguments[0];
            }
            
            // primitive service call hack
            if (node.Object != null && node.Object.Type.IsInheritedTypeOf(typeof(Service)))
            {
                object svc;
                var r = GetValue(node.Object, out svc);
                Trace.Assert(r);

                Out(" Call_" + (svc as Service).PlainTypeName() + "_" + node.Method.Name);
            }

            // composed serivce call hack
            else if ((node.Type.IsSymbol() || node.Type.IsSymbols()) && Attribute.GetCustomAttribute(node.Method, typeof(Primitive)) == null)
            {
                num = 0;
                Out(" " + node.Method.Name);
            }

            // else
            else
            {
                if (expression != null)
                {
                    Visit(expression);
                    if (expression.Type.IsSymbol() || expression.Type.IsSymbols() || expression.Type.IsEnumerable())
                    {
                        NewLine();
                    }

                    Out(".");
                }

                //if (!node.Method.IsPublic)
                //{
                //    throw new Exception("External object reference is not allowed, in '" + node.ToString() + "'!");
                //}

                if (expression == null)
                {
                    Out(node.Method.DeclaringType.FullName.Replace('+', '.') + "." + node.Method.Name);

                    if (node.Method.IsGenericMethod)
                    {
                        var paramList = node.Method.GetGenericArguments().Aggregate("", (current, p) => current + (p.GetCompilableTypeName(_context.RewrittenTypes) + ", "));
                        paramList = paramList.Substring(0, paramList.Length - ", ".Length);
                        Out("<" + paramList + ">");
                    }
                }
                else
                {
                    Out(node.Method.Name);
                }
            }

            Out("(");

            var isNewLine = node.Arguments.Any(a => a.NodeType == ExpressionType.Quote);

            if (isNewLine)
            {
                _indent++;
                NewLine();
            }

            var num2 = num;
            var count = node.Arguments.Count;

            Trace.Assert(node.Method.GetParameters().Length == node.Arguments.Count);
            while (num2 < count)
            {
                if (num2 > num)
                {
                    Out(", ");
                    if (isNewLine)
                        NewLine();
                }

                var paramInfo = node.Method.GetParameters()[num2];
                if (paramInfo.IsIn && paramInfo.IsOut)
                    Out("ref ");
                else if (paramInfo.IsOut)
                    Out("out ");

                Visit(node.Arguments[num2]);                

                num2++;
            }

            if (isNewLine)
            {
                _indent--;
                NewLine();
            }

            Out(")");

            if (tempVar != string.Empty)
            {
                Out(";");
                NewLine();
                NewLine();
                if (_tempVarsUndefined.Count == 0)
                    Out("return " + tempVar);
            }
        }

        internal void VisitNew(NewExpression node)
        {
            var typeName = node.Type.GetCompilableTypeName(_context.RewrittenTypes);
            
            Out("new " + typeName);
            Out(node.Members != null ? "() {" : "(");

            for (var i = 0; i < node.Arguments.Count; i++)
            {
                if (i > 0)
                {
                    Out(", ");
                }

                if (node.Members != null)
                {
                    Out(node.Members[i].Name);
                    Out(" = ");
                }

                Visit(node.Arguments[i]);
            }

            Out(node.Members != null ? "}" : ")");
        }

        internal void VisitNewArray(NewArrayExpression node)
        {
            switch (node.NodeType)
            {
                case ExpressionType.NewArrayInit:
                    Out("new [] ");
                    VisitExpressions('{', node.Expressions, '}');
                    break;

                case ExpressionType.NewArrayBounds:
                    Out("new " + node.Type);
                    VisitExpressions('(', node.Expressions, ')');
                    break;
            }
        }

        internal void VisitParameter(ParameterExpression node)
        {
            if (node.IsByRef)
            {
                Out("ref ");
            }
            if (string.IsNullOrEmpty(node.Name))
            {
                Out("param_" + GetParamId(node));
            }
            else
            {
                Out(node.Name + "_" + GetParamId(node));
            }
        }

        internal void VisitRuntimeVariables(RuntimeVariablesExpression node)
        {
            VisitExpressions('(', node.Variables, ')');
        }

        internal void VisitSwitch(SwitchExpression node)
        {
            Out("switch ");
            Out("(");
            Visit(node.SwitchValue);
            Out(") { ... }");
        }

        internal void VisitSwitchCase(SwitchCase node)
        {
            Out("case ");
            VisitExpressions('(', node.TestValues, ')');
            Out(": ...");
        }

        internal void VisitTry(TryExpression node)
        {
            Out("try { ... }");
        }

        internal void VisitTypeBinary(TypeBinaryExpression node)
        {
            Out("(");
            Visit(node.Expression);
            switch (node.NodeType)
            {
                case ExpressionType.TypeIs:
                    Out(" Is ");
                    break;

                case ExpressionType.TypeEqual:
                    Out(" TypeEqual ");
                    goto Label_0043;
            }
        Label_0043:
            Out(node.TypeOperand.Name);
            Out(")");
        }

        internal void VisitUnary(UnaryExpression node)
        {
            switch (node.NodeType)
            {
                case ExpressionType.TypeAs:
                    Out("(");
                    break;

                case ExpressionType.Decrement:
                    Out("-(");
                    break;

                case ExpressionType.Negate:
                case ExpressionType.NegateChecked:
                    Out("-");
                    break;

                case ExpressionType.UnaryPlus:
                    Out("+");
                    break;

                case ExpressionType.Not:
                    Out("!(");
                    break;

                case ExpressionType.Quote:
                    break;

                case ExpressionType.Increment:
                    Out("+(");
                    break;

                case ExpressionType.Throw:
                    Out("throw (");
                    break;

                case ExpressionType.PreIncrementAssign:
                    Out("++");
                    break;

                case ExpressionType.PreDecrementAssign:
                    Out("--");
                    break;

                case ExpressionType.OnesComplement:
                    Out("~(");
                    break;

                default:
                    //this.Out(node.NodeType.ToString());
                    Out("(");
                    break;
            }
            Visit(node.Operand);
            switch (node.NodeType)
            {
                case ExpressionType.PreIncrementAssign:
                case ExpressionType.PreDecrementAssign:
                    return;

                case ExpressionType.PostIncrementAssign:
                    Out("++");
                    return;

                case ExpressionType.PostDecrementAssign:
                    Out("--");
                    return;

                case ExpressionType.TypeAs:
                    Out(" As ");
                    Out(node.Type.Name);
                    Out(")");
                    return;

                case ExpressionType.Negate:
                case ExpressionType.UnaryPlus:
                case ExpressionType.NegateChecked:
                    return;

                case ExpressionType.Quote:
                    return;
            }
            Out(")");
        }

        internal void VisitMemberBinding(MemberBinding node)
        {
            switch (node.BindingType)
            {
                case MemberBindingType.Assignment:
                    VisitMemberAssignment((MemberAssignment)node);
                    return;

                case MemberBindingType.MemberBinding:
                    VisitMemberMemberBinding((MemberMemberBinding)node);
                    return;

                case MemberBindingType.ListBinding:
                    VisitMemberListBinding((MemberListBinding)node);
                    return;
            }
            //throw Error.UnhandledBindingType(node.BindingType);
            //throw new System.ArgumentException("Error.UnhandledBindingType(node.BindingType);");
        }

        public void VisitTypeIs(TypeBinaryExpression b)
        {
            Visit(b.Expression);
        }

        public void VisitMemberAccess(MemberExpression m)
        {
            //throw new Exception("Overloaded visitors should implement this method");
            Visit(m.Expression);
        }

        public void Visit(Expression exp)
        {
            if (_context.TempSymbols.ContainsKey(exp) && !_tempVarsUndefined.Contains(exp))
            {
                Out(_context.TempSymbols[exp]);
                return;
            }

            switch (exp.NodeType)
            {
                case ExpressionType.Negate:
                case ExpressionType.NegateChecked:
                case ExpressionType.Not:
                case ExpressionType.Convert:
                case ExpressionType.ConvertChecked:
                case ExpressionType.ArrayLength:
                case ExpressionType.Quote:
                case ExpressionType.TypeAs:
                    {
                        VisitUnary((UnaryExpression)exp);
                        break;
                    }
                case ExpressionType.Add:
                case ExpressionType.AddChecked:
                case ExpressionType.Subtract:
                case ExpressionType.SubtractChecked:
                case ExpressionType.Multiply:
                case ExpressionType.MultiplyChecked:
                case ExpressionType.Divide:
                case ExpressionType.Modulo:
                case ExpressionType.And:
                case ExpressionType.AndAlso:
                case ExpressionType.Or:
                case ExpressionType.OrElse:
                case ExpressionType.LessThan:
                case ExpressionType.LessThanOrEqual:
                case ExpressionType.GreaterThan:
                case ExpressionType.GreaterThanOrEqual:
                case ExpressionType.Equal:
                case ExpressionType.NotEqual:
                case ExpressionType.Coalesce:
                case ExpressionType.ArrayIndex:
                case ExpressionType.RightShift:
                case ExpressionType.LeftShift:
                case ExpressionType.ExclusiveOr:
                    {
                        VisitBinary((BinaryExpression)exp);
                        break;
                    }
                case ExpressionType.TypeIs:
                    {
                        VisitTypeIs((TypeBinaryExpression)exp);
                        break;
                    }
                case ExpressionType.Conditional:
                    {
                        VisitConditional((ConditionalExpression)exp);
                        break;
                    }
                case ExpressionType.Constant:
                    {
                        VisitConstant((ConstantExpression)exp);
                        break;
                    }
                case ExpressionType.Parameter:
                    {
                        VisitParameter((ParameterExpression)exp);
                        break;
                    }
                case ExpressionType.MemberAccess:
                    {
                        VisitMember((MemberExpression)exp);
                        break;
                    }
                case ExpressionType.Call:
                    {
                        VisitMethodCall((MethodCallExpression)exp);
                        break;
                    }
                case ExpressionType.Lambda:
                    {
                        VisitLambda((LambdaExpression)exp);
                        break;
                    }
                case ExpressionType.New:
                    {
                        VisitNew((NewExpression)exp);
                        break;
                    }
                case ExpressionType.NewArrayInit:
                case ExpressionType.NewArrayBounds:
                    {
                        VisitNewArray((NewArrayExpression)exp);
                        break;
                    }
                case ExpressionType.Invoke:
                    {
                        VisitInvocation((InvocationExpression)exp);
                        break;
                    }
                case ExpressionType.MemberInit:
                    {
                        VisitMemberInit((MemberInitExpression)exp);
                        break;
                    }
                case ExpressionType.ListInit:
                    {
                        VisitListInit((ListInitExpression)exp);
                        break;
                    }
                default:
                    {
                        throw new Exception("Visitor: Expression of type " + exp.NodeType + " is not handled");
                    }
            }
        }
    }
}
