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
using System.Linq.Expressions;
using System.Reflection;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Dynamic;
using System.Globalization;

using rDSN.Tron.Utility;
using rDSN.Tron.Contract;

namespace rDSN.Tron.Compiler
{
    //class Expression2Code : ExpressionVisitor<string, int>
    public class ExpressionToCode 
    {
        // Fields
        private Dictionary<object, int> _ids = new Dictionary<object,int>();
        private StringBuilder _out = new StringBuilder();
        private Expression _exp = null;
        private QueryContext _context = null;
        private Dictionary<Expression, object> _externalObjects = new Dictionary<Expression, object>();
        private HashSet<Expression> _tempVarsUndefined = new HashSet<Expression>();
        private int _indent = 0;

        private void NewLine()
        {
            this.Out("\r\n");
            for (int i = 0; i < _indent; i++)
                this.Out("\t");
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
                this.Out("return ");

            Visit(_exp);
            return this.ToString();
        }

        private bool GetValue(Expression exp, out object value)
        {
            value = null;

            try
            {
                var lambda = Expression.Lambda(exp, new ParameterExpression[] { });
                value = lambda.Compile().DynamicInvoke(new object[] { });
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
            if (!this._ids.ContainsKey(label))
            {
                this._ids.Add(label, this._ids.Count);
            }
        }

        private void AddParam(ParameterExpression p)
        {
            if (!this._ids.ContainsKey(p))
            {
                this._ids.Add(p, this._ids.Count);
            }
        }
                
        private void DumpLabel(LabelTarget target)
        {
            if (!string.IsNullOrEmpty(target.Name))
            {
                this.Out(target.Name);
            }
            else
            {
                this.Out("UnamedLabel_" + this.GetLabelId(target));
            }
        }

        private static string FormatBinder(CallSiteBinder binder)
        {
            ConvertBinder binder2 = binder as ConvertBinder;
            if (binder2 != null)
            {
                return (" " + binder2.Type);
            }
            GetMemberBinder binder3 = binder as GetMemberBinder;
            if (binder3 != null)
            {
                return ("GetMember " + binder3.Name);
            }
            SetMemberBinder binder4 = binder as SetMemberBinder;
            if (binder4 != null)
            {
                return ("SetMember " + binder4.Name);
            }
            DeleteMemberBinder binder5 = binder as DeleteMemberBinder;
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
            InvokeMemberBinder binder6 = binder as InvokeMemberBinder;
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
            UnaryOperationBinder binder7 = binder as UnaryOperationBinder;
            if (binder7 != null)
            {
                return binder7.Operation.ToString();
            }
            BinaryOperationBinder binder8 = binder as BinaryOperationBinder;
            if (binder8 != null)
            {
                return binder8.Operation.ToString();
            }
            return "CallSiteBinder";
        }

        private int GetLabelId(LabelTarget label)
        {
            int count;
            if (this._ids == null)
            {
                this._ids = new Dictionary<object, int>();
                this.AddLabel(label);
                return 0;
            }
            if (!this._ids.TryGetValue(label, out count))
            {
                count = this._ids.Count;
                this.AddLabel(label);
            }
            return count;
        }

        private int GetParamId(ParameterExpression p)
        {
            int count;
            if (this._ids == null)
            {
                this._ids = new Dictionary<object, int>();
                this.AddParam(p);
                return 0;
            }
            if (!this._ids.TryGetValue(p, out count))
            {
                count = this._ids.Count;
                this.AddParam(p);
            }
            return count;
        }

        private void Out(char c)
        {
            this._out.Append(c);
        }

        private void Out(string s)
        {
            this._out.Append(s);
        }

        public override string ToString()
        {
            return this._out.ToString();
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
                    this.Visit(node.Left);
                    this.Out("[");
                    this.Visit(node.Right);
                    this.Out("]");
                    return;

                default:
                    throw new InvalidOperationException();
            }
            this.Out("(");
            this.Visit(node.Left);
            this.Out(' ');
            this.Out(str);
            this.Out(' ');
            this.Visit(node.Right);
            this.Out(")");
        }

        internal void VisitBlock(BlockExpression node)
        {
            this.Out("{");
            foreach (ParameterExpression expression in node.Variables)
            {
                this.Out("var ");
                this.Visit(expression);
                this.Out(";");
            }
            this.Out(" ... }");
        }

        internal void VisitCatchBlock(CatchBlock node)
        {
            this.Out("catch (" + node.Test.Name);
            if (node.Variable != null)
            {
                this.Out(node.Variable.Name ?? "");
            }
            this.Out(") { ... }");
        }

        internal void VisitConditional(ConditionalExpression node)
        {
            this.Out("(");
            this.Visit(node.Test);
            this.Out(") ? (");
            this.Visit(node.IfTrue);
            this.Out(") : (");
            this.Visit(node.IfFalse);
            this.Out(")");
        }

        internal void VisitConstant(ConstantExpression node)
        {
            string v;
            bool r = LocalTypeHelper.ConstantValue2String(node.Value, out v);
            if (!r)
            {
                if (node.Type.IsGenericType && node.Type.BaseType == typeof(ISymbol))
                {
                    v = "request";
                }
            }
            this.Out(v);
        }

        internal void VisitDebugInfo(DebugInfoExpression node)
        {
            string s = string.Format(CultureInfo.CurrentCulture, "<DebugInfo({0}: {1}, {2}, {3}, {4})>", new object[] { node.Document.FileName, node.StartLine, node.StartColumn, node.EndLine, node.EndColumn });
            this.Out(s);
        }

        internal void VisitDefault(DefaultExpression node)
        {
            this.Out("default(");
            this.Out(node.Type.FullName.Replace('+', '.'));
            this.Out(")");
        }

        internal void VisitDynamic(DynamicExpression node)
        {
            this.Out(FormatBinder(node.Binder));
            this.VisitExpressions<Expression>('(', node.Arguments, ')');
        }

        ElementInit VisitElementInit(ElementInit initializer)
        {
            this.Out(initializer.AddMethod.ToString());
            this.VisitExpressions<Expression>('(', initializer.Arguments, ')');
            return initializer;
        }

        private void VisitExpressions<T>(char open, IList<T> expressions, char close) where T : Expression
        {
            this.Out(open);
            if (expressions != null)
            {
                bool flag = true;
                foreach (T local in expressions)
                {
                    if (flag)
                    {
                        flag = false;
                    }
                    else
                    {
                        this.Out(", ");
                    }
                    this.Visit(local);
                }
            }
            this.Out(close);
        }

        internal void VisitExtension(Expression node)
        {
            BindingFlags bindingAttr = BindingFlags.ExactBinding | BindingFlags.Public | BindingFlags.Instance;
            if (node.GetType().GetMethod("ToString", bindingAttr, null, Type.EmptyTypes, null).DeclaringType != typeof(Expression))
            {
                this.Out(node.ToString());
                return;
            }
            this.Out("[");
            if (node.NodeType == ExpressionType.Extension)
            {
                this.Out(node.GetType().FullName);
            }
            else
            {
                this.Out(node.NodeType.ToString());
            }
            this.Out("]");
        }

        internal void VisitGoto(GotoExpression node)
        {
            this.Out(node.Kind.ToString().ToLower(CultureInfo.CurrentCulture));
            this.DumpLabel(node.Target);
            if (node.Value != null)
            {
                this.Out(" (");
                this.Visit(node.Value);
                this.Out(") ");
            }
        }

        internal void VisitIndex(IndexExpression node)
        {
            if (node.Object != null)
            {
                this.Visit(node.Object);
            }
            else
            {
                this.Out(node.Indexer.DeclaringType.Name);
            }
            if (node.Indexer != null)
            {
                this.Out(".");
                this.Out(node.Indexer.Name);
            }
            this.VisitExpressions<Expression>('[', node.Arguments, ']');
        }

        internal void VisitInvocation(InvocationExpression node)
        {
            this.Out("Invoke(");
            this.Visit(node.Expression);
            int num = 0;
            int count = node.Arguments.Count;
            while (num < count)
            {
                this.Out(", ");
                this.Visit(node.Arguments[num]);
                num++;
            }
            this.Out(")");
        }

        internal void VisitLabel(LabelExpression node)
        {
            this.Out("{ ... } ");
            this.DumpLabel(node.Target);
            this.Out(":");
        }

        internal void VisitLambda(LambdaExpression node)
        {
            if (node.Parameters.Count == 1)
            {
                this.Visit(node.Parameters[0]);
            }
            else
            {
                this.VisitExpressions<ParameterExpression>('(', node.Parameters, ')');
            }
            this.Out(" => ");

            bool needIndent = node.Parameters[0].Type.IsSymbol()
                || node.Parameters[0].Type.IsSymbols()
                || node.Parameters[0].Type.IsEnumerable();

            if (needIndent) ++_indent;

            this.Visit(node.Body);

            if (needIndent) --_indent;
        }

        internal void VisitListInit(ListInitExpression node)
        {
            this.Visit(node.NewExpression);
            this.Out(" {");
            int num = 0;
            int count = node.Initializers.Count;
            while (num < count)
            {
                if (num > 0)
                {
                    this.Out(", ");
                }
                this.Out(node.Initializers[num].ToString());
                num++;
            }
            this.Out("}");
        }

        internal void VisitLoop(LoopExpression node)
        {
            this.Out("loop { ... }");
        }

        internal void VisitMember(MemberExpression node)
        {
            string s;
            Object value;
            if (_externalObjects.ContainsKey(node))
            {
                bool r = LocalTypeHelper.ConstantValue2String(_externalObjects[node], out s);
                if (r)
                {
                    this.Out(s);
                    return;
                }
            }
            else if (GetValue(node, out value))
            {
                _externalObjects[node] = value;
                bool r = LocalTypeHelper.ConstantValue2String(_externalObjects[node], out s);
                if (r)
                {
                    this.Out(s);
                    return;
                }
                else if (node.Type.IsSymbol() || node.Type.IsSymbols())
                {
                    if (value != null)
                    {
                        Visit((value as ISymbol).Expression);
                    }
                    else
                    {
                        this.Out(node.Member.Name);
                    }
                    return;
                }
            }

            if (node.Expression != null)
            {
                this.Visit(node.Expression);
                this.Out(".");
            }
            this.Out(node.Member.Name);
        }

        internal void VisitMemberAssignment(MemberAssignment assignment)
        {
            this.Out(assignment.Member.Name);
            this.Out(" = ");
            this.Visit(assignment.Expression);
        }

        internal void VisitMemberInit(MemberInitExpression node)
        {
            if ((node.NewExpression.Arguments.Count == 0) && node.NewExpression.Type.Name.Contains("<"))
            {
                this.Out("new");
            }
            else
            {
                this.Visit(node.NewExpression);
            }
            this.Out(" {");
            int num = 0;
            int count = node.Bindings.Count;
            while (num < count)
            {
                MemberBinding binding = node.Bindings[num];
                if (num > 0)
                {
                    this.Out(", ");
                }
                this.VisitMemberBinding(binding);
                num++;
            }
            this.Out("}");
        }

        internal void VisitMemberListBinding(MemberListBinding binding)
        {
            this.Out(binding.Member.Name);
            this.Out(" = {");
            int num = 0;
            int count = binding.Initializers.Count;
            while (num < count)
            {
                if (num > 0)
                {
                    this.Out(", ");
                }
                this.VisitElementInit(binding.Initializers[num]);
                num++;
            }
            this.Out("}");
        }

        internal void VisitMemberMemberBinding(MemberMemberBinding binding)
        {
            this.Out(binding.Member.Name);
            this.Out(" = {");
            int num = 0;
            int count = binding.Bindings.Count;
            while (num < count)
            {
                if (num > 0)
                {
                    this.Out(", ");
                }
                this.VisitMemberBinding(binding.Bindings[num]);
                num++;
            }
            this.Out("}");
        }

        internal void VisitMethodCall(MethodCallExpression node)
        {
            string tempVar = string.Empty;
            if ((node.Type.IsSymbol() || node.Type.IsSymbols()) && _context.TempSymbols.ContainsKey(node))
            {
                tempVar = _context.TempSymbols[node];
                _tempVarsUndefined.Remove(node);

                NewLine();
                this.Out("var " + tempVar + " = ");
            }

            int num = 0;
            Expression expression = node.Object;
            if (Attribute.GetCustomAttribute(node.Method, typeof(ExtensionAttribute)) != null)
            {
                num = 1;
                expression = node.Arguments[0];
            }
            
            // primitive service call hack
            if (node.Object != null && node.Object.Type.IsInheritedTypeOf(typeof(Service)))
            {
                object svc;
                bool r = GetValue(node.Object, out svc);
                Trace.Assert(r);

                this.Out(" Call_" + (svc as Service).PlainTypeName() + "_" + node.Method.Name);
            }

            // composed serivce call hack
            else if ((node.Type.IsSymbol() || node.Type.IsSymbols()) && Attribute.GetCustomAttribute(node.Method, typeof(Primitive)) == null)
            {
                num = 0;
                expression = null;
                this.Out(" " + node.Method.Name);
            }

            // else
            else
            {
                if (expression != null)
                {
                    this.Visit(expression);
                    if (expression.Type.IsSymbol() || expression.Type.IsSymbols() || expression.Type.IsEnumerable())
                    {
                        NewLine();
                    }

                    this.Out(".");
                }

                //if (!node.Method.IsPublic)
                //{
                //    throw new Exception("External object reference is not allowed, in '" + node.ToString() + "'!");
                //}

                if (expression == null)
                {
                    this.Out(node.Method.DeclaringType.FullName.Replace('+', '.') + "." + node.Method.Name);

                    if (node.Method.IsGenericMethod)
                    {
                        string paramList = "";
                        foreach (var p in node.Method.GetGenericArguments())
                        {
                            paramList += p.GetCompilableTypeName(_context.RewrittenTypes) + ", ";
                        }
                        paramList = paramList.Substring(0, paramList.Length - ", ".Length);
                        this.Out("<" + paramList + ">");
                    }
                }
                else
                {
                    this.Out(node.Method.Name);
                }
            }

            this.Out("(");

            bool isNewLine = node.Arguments.Where(a => a.NodeType == ExpressionType.Quote).Count() > 0;

            if (isNewLine)
            {
                _indent++;
                NewLine();
            }

            int num2 = num;
            int count = node.Arguments.Count;

            Trace.Assert(node.Method.GetParameters().Length == node.Arguments.Count);
            while (num2 < count)
            {
                if (num2 > num)
                {
                    this.Out(", ");
                    if (isNewLine)
                        NewLine();
                }

                var paramInfo = node.Method.GetParameters()[num2];
                if (paramInfo.IsIn && paramInfo.IsOut)
                    this.Out("ref ");
                else if (paramInfo.IsOut)
                    this.Out("out ");

                this.Visit(node.Arguments[num2]);                

                num2++;
            }

            if (isNewLine)
            {
                _indent--;
                NewLine();
            }

            this.Out(")");

            if (tempVar != string.Empty)
            {
                this.Out(";");
                NewLine();
                NewLine();
                if (_tempVarsUndefined.Count == 0)
                    this.Out("return " + tempVar);
            }
        }

        internal void VisitNew(NewExpression node)
        {
            string typeName = node.Type.GetCompilableTypeName(_context.RewrittenTypes);
            
            this.Out("new " + typeName);
            if (node.Members != null)
                this.Out("() {");
            else
                this.Out("(");

            for (int i = 0; i < node.Arguments.Count; i++)
            {
                if (i > 0)
                {
                    this.Out(", ");
                }

                if (node.Members != null)
                {
                    this.Out(node.Members[i].Name);
                    this.Out(" = ");
                }

                this.Visit(node.Arguments[i]);
            }

            if (node.Members != null)
                this.Out("}");
            else
                this.Out(")");
        }

        internal void VisitNewArray(NewArrayExpression node)
        {
            switch (node.NodeType)
            {
                case ExpressionType.NewArrayInit:
                    this.Out("new [] ");
                    this.VisitExpressions<Expression>('{', node.Expressions, '}');
                    break;

                case ExpressionType.NewArrayBounds:
                    this.Out("new " + node.Type.ToString());
                    this.VisitExpressions<Expression>('(', node.Expressions, ')');
                    break;
            }
        }

        internal void VisitParameter(ParameterExpression node)
        {
            if (node.IsByRef)
            {
                this.Out("ref ");
            }
            if (string.IsNullOrEmpty(node.Name))
            {
                this.Out("param_" + this.GetParamId(node));
            }
            else
            {
                this.Out(node.Name + "_" + this.GetParamId(node));
            }
        }

        internal void VisitRuntimeVariables(RuntimeVariablesExpression node)
        {
            this.VisitExpressions<ParameterExpression>('(', node.Variables, ')');
        }

        internal void VisitSwitch(SwitchExpression node)
        {
            this.Out("switch ");
            this.Out("(");
            this.Visit(node.SwitchValue);
            this.Out(") { ... }");
        }

        internal void VisitSwitchCase(SwitchCase node)
        {
            this.Out("case ");
            this.VisitExpressions<Expression>('(', node.TestValues, ')');
            this.Out(": ...");
        }

        internal void VisitTry(TryExpression node)
        {
            this.Out("try { ... }");
        }

        internal void VisitTypeBinary(TypeBinaryExpression node)
        {
            this.Out("(");
            this.Visit(node.Expression);
            switch (node.NodeType)
            {
                case ExpressionType.TypeIs:
                    this.Out(" Is ");
                    break;

                case ExpressionType.TypeEqual:
                    this.Out(" TypeEqual ");
                    goto Label_0043;
            }
        Label_0043:
            this.Out(node.TypeOperand.Name);
            this.Out(")");
        }

        internal void VisitUnary(UnaryExpression node)
        {
            switch (node.NodeType)
            {
                case ExpressionType.TypeAs:
                    this.Out("(");
                    break;

                case ExpressionType.Decrement:
                    this.Out("-(");
                    break;

                case ExpressionType.Negate:
                case ExpressionType.NegateChecked:
                    this.Out("-");
                    break;

                case ExpressionType.UnaryPlus:
                    this.Out("+");
                    break;

                case ExpressionType.Not:
                    this.Out("!(");
                    break;

                case ExpressionType.Quote:
                    break;

                case ExpressionType.Increment:
                    this.Out("+(");
                    break;

                case ExpressionType.Throw:
                    this.Out("throw (");
                    break;

                case ExpressionType.PreIncrementAssign:
                    this.Out("++");
                    break;

                case ExpressionType.PreDecrementAssign:
                    this.Out("--");
                    break;

                case ExpressionType.OnesComplement:
                    this.Out("~(");
                    break;

                default:
                    //this.Out(node.NodeType.ToString());
                    this.Out("(");
                    break;
            }
            this.Visit(node.Operand);
            switch (node.NodeType)
            {
                case ExpressionType.PreIncrementAssign:
                case ExpressionType.PreDecrementAssign:
                    return;

                case ExpressionType.PostIncrementAssign:
                    this.Out("++");
                    return;

                case ExpressionType.PostDecrementAssign:
                    this.Out("--");
                    return;

                case ExpressionType.TypeAs:
                    this.Out(" As ");
                    this.Out(node.Type.Name);
                    this.Out(")");
                    return;

                case ExpressionType.Negate:
                case ExpressionType.UnaryPlus:
                case ExpressionType.NegateChecked:
                    return;

                case ExpressionType.Quote:
                    return;
            }
            this.Out(")");
            return;
        }

        internal void VisitMemberBinding(MemberBinding node)
        {
            switch (node.BindingType)
            {
                case MemberBindingType.Assignment:
                    this.VisitMemberAssignment((MemberAssignment)node);
                    return;

                case MemberBindingType.MemberBinding:
                    this.VisitMemberMemberBinding((MemberMemberBinding)node);
                    return;

                case MemberBindingType.ListBinding:
                    this.VisitMemberListBinding((MemberListBinding)node);
                    return;
            }
            //throw Error.UnhandledBindingType(node.BindingType);
            //throw new System.ArgumentException("Error.UnhandledBindingType(node.BindingType);");
        }

        public void VisitTypeIs(TypeBinaryExpression b)
        {
            this.Visit(b.Expression);
        }

        public void VisitMemberAccess(MemberExpression m)
        {
            //throw new Exception("Overloaded visitors should implement this method");
            this.Visit(m.Expression);
        }

        public void Visit(Expression exp)
        {
            if (_context.TempSymbols.ContainsKey(exp) && !_tempVarsUndefined.Contains(exp))
            {
                this.Out(_context.TempSymbols[exp]);
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
                        this.VisitUnary((UnaryExpression)exp);
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
                        this.VisitBinary((BinaryExpression)exp);
                        break;
                    }
                case ExpressionType.TypeIs:
                    {
                        this.VisitTypeIs((TypeBinaryExpression)exp);
                        break;
                    }
                case ExpressionType.Conditional:
                    {
                        this.VisitConditional((ConditionalExpression)exp);
                        break;
                    }
                case ExpressionType.Constant:
                    {
                        this.VisitConstant((ConstantExpression)exp);
                        break;
                    }
                case ExpressionType.Parameter:
                    {
                        this.VisitParameter((ParameterExpression)exp);
                        break;
                    }
                case ExpressionType.MemberAccess:
                    {
                        this.VisitMember((MemberExpression)exp);
                        break;
                    }
                case ExpressionType.Call:
                    {
                        this.VisitMethodCall((MethodCallExpression)exp);
                        break;
                    }
                case ExpressionType.Lambda:
                    {
                        this.VisitLambda((LambdaExpression)exp);
                        break;
                    }
                case ExpressionType.New:
                    {
                        this.VisitNew((NewExpression)exp);
                        break;
                    }
                case ExpressionType.NewArrayInit:
                case ExpressionType.NewArrayBounds:
                    {
                        this.VisitNewArray((NewArrayExpression)exp);
                        break;
                    }
                case ExpressionType.Invoke:
                    {
                        this.VisitInvocation((InvocationExpression)exp);
                        break;
                    }
                case ExpressionType.MemberInit:
                    {
                        this.VisitMemberInit((MemberInitExpression)exp);
                        break;
                    }
                case ExpressionType.ListInit:
                    {
                        this.VisitListInit((ListInitExpression)exp);
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
