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
    internal class BasicBlockBuilder
    {
        private LGraph _graph;
        private Dictionary<Expression, Instruction> _indexedInsts = new Dictionary<Expression, Instruction>();
        private List<Instruction> _insts = new List<Instruction>();
        private Dictionary<Expression, Variable> _constants = new Dictionary<Expression, Variable>();
        private Dictionary<Expression, Variable> _parameters = new Dictionary<Expression, Variable>();

        private Instruction GetInstruction(Expression exp, OpCode code)
        {
            Instruction inst = null;

            if (_indexedInsts.TryGetValue(exp, out inst))
                return inst;
            else
            {
                inst = new Instruction(code);
                _indexedInsts.Add(exp, inst);
                _insts.Add(inst);
                return inst;
            }
        }

        private Instruction CreateNonIndexedInstruction(OpCode code)
        {
            var inst = new Instruction(code);
            _insts.Add(inst);
            return inst;
        }
             
        
        private Variable ComputeTempVar(Expression exp, OpCode code, Variable[] sources)
        {
            Instruction inst;
            if (_indexedInsts.TryGetValue(exp, out inst))
                return inst.Destinations[0];
            else
            {
                inst = new Instruction(code);
                _indexedInsts.Add(exp, inst);
                _insts.Add(inst);

                foreach (var s in sources)
                    inst.Sources.Add(s);

                Variable d = Variable.CreateTemp(exp.Type, inst);
                inst.Destinations.Add(d);
                return d;
            }
        }

        internal BasicBlockBuilder(LGraph graph)
        {
            _graph = graph;
        }

        internal void Build()
        {
            foreach (var v in _graph.Vertices)
            {
                if (v.Value.Exp != null)
                {
                    Build(v.Value);
                }
            }
        }

        private void Build(LVertex v)
        {
            foreach (var arg in v.Exp.Arguments)
            {
                if (arg.NodeType != ExpressionType.Quote)
                    continue;

                UnaryExpression uexp = arg as UnaryExpression;
                if (uexp.Operand.NodeType != ExpressionType.Lambda)
                    continue;

                LambdaExpression lexp = uexp.Operand as LambdaExpression;
                if (lexp.Parameters.Count > 0 && lexp.Parameters[0].Type.IsSymbol() && lexp.Body.NodeType == ExpressionType.Call)
                {
                    // nothing to do, other LVertex will handle it
                }
                else
                {
                    Visit(lexp);

                    v.Instructions.Add(lexp, _insts.ToArray());

                    _indexedInsts.Clear();
                    _insts.Clear();
                    _parameters.Clear();
                }
            }
        }

        private Variable VisitBinary(BinaryExpression node)
        {
            OpCode code;
            switch (node.NodeType)
            {
                case ExpressionType.Add:
                case ExpressionType.AddChecked:
                    code = OpCode.Add;
                    break;

                case ExpressionType.And:
                    code = OpCode.And;
                    break;

                case ExpressionType.AndAlso:
                    code = OpCode.AndAlso;
                    break;

                case ExpressionType.Divide:
                    code = OpCode.Divide;
                    break;

                case ExpressionType.Equal:
                    code = OpCode.Equal;
                    break;

                case ExpressionType.ExclusiveOr:
                    code = OpCode.ExclusiveOr;
                    break;

                case ExpressionType.GreaterThan:
                    code = OpCode.GreaterThan;
                    break;

                case ExpressionType.GreaterThanOrEqual:
                    code = OpCode.GreaterThanOrEqual;
                    break;

                case ExpressionType.LeftShift:
                    code = OpCode.LeftShift;
                    break;

                case ExpressionType.LessThan:
                    code = OpCode.LessThan;
                    break;

                case ExpressionType.LessThanOrEqual:
                    code = OpCode.LessThanOrEqual;
                    break;

                case ExpressionType.Modulo:
                    code = OpCode.Modulo;
                    break;

                case ExpressionType.Multiply:
                case ExpressionType.MultiplyChecked:
                    code = OpCode.Multiply;
                    break;

                case ExpressionType.NotEqual:
                    code = OpCode.NotEqual;
                    break;

                case ExpressionType.Or:
                    code = OpCode.Or;
                    break;

                case ExpressionType.OrElse:
                    code = OpCode.OrElse;
                    break;

                case ExpressionType.Power:
                    code = OpCode.Power;
                    break;

                case ExpressionType.RightShift:
                    code = OpCode.RightShift;
                    break;

                case ExpressionType.Subtract:
                case ExpressionType.SubtractChecked:
                    code = OpCode.Subtract;
                    break;

                case ExpressionType.Assign:
                    code = OpCode.Assign;
                    break;
                    
                case ExpressionType.AddAssign:
                case ExpressionType.AddAssignChecked:
                    code = OpCode.AddAssign;
                    break;

                case ExpressionType.AndAssign:
                    code = OpCode.AndAssign;
                    break;

                case ExpressionType.DivideAssign:
                    code = OpCode.DivideAssign;
                    break;

                case ExpressionType.ExclusiveOrAssign:
                    code = OpCode.ExclusiveOrAssign;
                    break;

                case ExpressionType.LeftShiftAssign:
                    code = OpCode.LeftShiftAssign;
                    break;

                case ExpressionType.ModuloAssign:
                    code = OpCode.ModuloAssign;
                    break;

                case ExpressionType.MultiplyAssign:
                case ExpressionType.MultiplyAssignChecked:
                    code = OpCode.MultiplyAssign;
                    break;

                case ExpressionType.OrAssign:
                    code = OpCode.OrAssign;
                    break;

                case ExpressionType.PowerAssign:
                    code = OpCode.PowerAssign;
                    break;

                case ExpressionType.RightShiftAssign:
                    code = OpCode.RightShiftAssign;
                    break;

                case ExpressionType.SubtractAssign:
                case ExpressionType.SubtractAssignChecked:
                    code = OpCode.SubtractAssign;
                    break;
                    
                case ExpressionType.ArrayIndex:
                    code = OpCode.ArrayIndex;
                    break;

                default:
                    throw new NotSupportedException("Expression of type " + node.NodeType + " is not supported");
            }

            var l = Visit(node.Left);
            var r = Visit(node.Right);
            return ComputeTempVar(node, code, new Variable[]{l, r});
        }

        private Variable VisitBlock(BlockExpression node)
        {
            throw new NotSupportedException("Expression of type " + node.NodeType + " is not supported");

            //this.Out("{");
            //foreach (ParameterExpression expression in node.Variables)
            //{
            //    this.Out("var ");
            //    this.Visit(expression);
            //    this.Out(";");
            //}
            //this.Out(" ... }");
        }

        private Variable VisitCatchBlock(CatchBlock node)
        {
            throw new NotSupportedException();
            //this.Out("catch (" + node.Test.Name);
            //if (node.Variable != null)
            //{
            //    this.Out(node.Variable.Name ?? "");
            //}
            //this.Out(") { ... }");
        }

        private Variable VisitConditional(ConditionalExpression node)
        {
            // (a ? b : c)
            var a = Visit(node.Test);
            var b = Visit(node.IfTrue);
            var c = Visit(node.IfFalse);

            return ComputeTempVar(node, OpCode.Conditional, new Variable[]{a, b, c});
        }

        private Variable VisitConstant(ConstantExpression node)
        {
            //string v;
            //bool r = LocalTypeHelper.ConstantValue2String(node.Value, out v);
            //if (!r)
            //{
            //    if (node.Type.IsGenericType && node.Type.BaseType == typeof(ISymbol))
            //    {
            //        v = "request";
            //    }
            //}

            Variable v;
            if (_constants.TryGetValue(node, out v))
                return v;
            else
            {
                v = Variable.CreateConstant(node.Type, node.Value);
                _constants[node] = v;
                return v;
            }
        }

        private Variable VisitDebugInfo(DebugInfoExpression node)
        {
            // nothing to do
            throw new NotSupportedException("Expression of type " + node.NodeType + " is not supported");
        }

        private Variable VisitDefault(DefaultExpression node)
        {
            throw new NotSupportedException("Expression of type " + node.NodeType + " is not supported");
        }

        private Variable VisitDynamic(DynamicExpression node)
        {
            //this.Out(FormatBinder(node.Binder));
            //this.VisitExpressions<Expression>('(', node.Arguments, ')');
            throw new NotSupportedException("Expression of type " + node.NodeType + " is not supported");
        }

        ElementInit VisitElementInit(ElementInit initializer)
        {
            throw new NotSupportedException();

            //this.Out(initializer.AddMethod.ToString());
            //this.VisitExpressions<Expression>('(', initializer.Arguments, ')');
            //return initializer;
        }

        private Variable[] VisitExpressions(Expression[] expressions)
        {
            List<Variable> vars = new List<Variable>();
            foreach (var exp in expressions)
            {
                vars.Add(Visit(exp));
            }
            return vars.ToArray();

            //this.Out(open);
            //if (expressions != null)
            //{
            //    bool flag = true;
            //    foreach (T local in expressions)
            //    {
            //        if (flag)
            //        {
            //            flag = false;
            //        }
            //        else
            //        {
            //            this.Out(", ");
            //        }
            //        this.Visit(local);
            //    }
            //}
            //this.Out(close);
        }

        private Variable VisitExtension(Expression node)
        {
            throw new NotSupportedException("Expression of type " + node.NodeType + " is not supported");

            //BindingFlags bindingAttr = BindingFlags.ExactBinding | BindingFlags.Public | BindingFlags.Instance;
            //if (node.GetType().GetMethod("ToString", bindingAttr, null, Type.EmptyTypes, null).DeclaringType != typeof(Expression))
            //{
            //    this.Out(node.ToString());
            //    return;
            //}
            //this.Out("[");
            //if (node.NodeType == ExpressionType.Extension)
            //{
            //    this.Out(node.GetType().FullName);
            //}
            //else
            //{
            //    this.Out(node.NodeType.ToString());
            //}
            //this.Out("]");
        }

        private Variable VisitGoto(GotoExpression node)
        {
            throw new NotSupportedException("Expression of type " + node.NodeType + " is not supported");
            //this.Out(node.Kind.ToString().ToLower(CultureInfo.CurrentCulture));
            //this.DumpLabel(node.Target);
            //if (node.Value != null)
            //{
            //    this.Out(" (");
            //    this.Visit(node.Value);
            //    this.Out(") ");
            //}
        }

        private Variable VisitIndex(IndexExpression node)
        {
            List<Variable> args = new List<Variable>();

            if (node.Object != null)
            {
                args.Add(this.Visit(node.Object));
            }
            else
            {
                args.Add(Variable.CreateConstant(typeof(object), null));
            }

            if (node.Indexer != null)
            {
                args.Add(Variable.CreateConstant(typeof(string), node.Indexer.Name));
            }
            else
            {
                args.Add(Variable.CreateConstant(typeof(string), ""));
            }

            foreach (var p in this.VisitExpressions(node.Arguments.ToArray()))
            {
                args.Add(p);
            }

            return ComputeTempVar(node, OpCode.Index, args.ToArray());

            //if (node.Object != null)
            //{
            //    this.Visit(node.Object);
            //}
            //else
            //{
            //    this.Out(node.Indexer.DeclaringType.Name);
            //}
            //if (node.Indexer != null)
            //{
            //    this.Out(".");
            //    this.Out(node.Indexer.Name);
            //}
            //this.VisitExpressions<Expression>('[', node.Arguments, ']');
        }

        private Variable VisitInvocation(InvocationExpression node)
        {
            throw new NotSupportedException("Expression of type " + node.NodeType + " is not supported");

            //this.Out("Invoke(");
            //this.Visit(node.Expression);
            //int num = 0;
            //int count = node.Arguments.Count;
            //while (num < count)
            //{
            //    this.Out(", ");
            //    this.Visit(node.Arguments[num]);
            //    num++;
            //}
            //this.Out(")");
        }

        private Variable VisitLabel(LabelExpression node)
        {
            throw new NotSupportedException("Expression of type " + node.NodeType + " is not supported");
            //this.Out("{ ... } ");
            //this.DumpLabel(node.Target);
            //this.Out(":");
        }

        private Variable VisitLambda(LambdaExpression node)
        {
            this.VisitExpressions(node.Parameters.ToArray());
            return this.Visit(node.Body);
        }

        private Variable VisitListInit(ListInitExpression node)
        {
            var r = this.Visit(node.NewExpression);

            foreach (var init in node.Initializers)
            {
                throw new NotSupportedException();
            }

            return r;
        }

        private Variable VisitLoop(LoopExpression node)
        {
            throw new NotSupportedException("Expression of type " + node.NodeType + " is not supported");
            //this.Out("loop { ... }");
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

        private Variable VisitMember(MemberExpression node)
        {
            if (node.Expression != null)
            {
                var host = Visit(node.Expression);
                var member = ComputeTempVar(node, OpCode.MemberRead, new Variable[] { 
                    host,
                    Variable.CreateConstant(typeof(string), node.Member.Name)
                });
                return member;
            }
            else
            {
                Variable v;
                if (_constants.TryGetValue(node, out v))
                    return v;
                else
                { 
                    object value;
                    if (GetValue(node, out value))
                    {
                        v = Variable.CreateConstant(node.Type, value);
                        _constants[node] = v;
                        return v;
                    }
                    else
                    {
                        throw new NotSupportedException();
                    }
                }
            }
        }

        private Variable VisitMemberAssignment(Variable host, MemberAssignment assignment)
        {
            var inst = CreateNonIndexedInstruction(OpCode.MemberWrite);
            var val = Visit(assignment.Expression);
            inst.Destinations.Add(host);
            inst.Destinations.Add(Variable.CreateConstant(typeof(string), assignment.Member.Name));
            inst.Sources.Add(val);

            return val;
        }

        private Variable VisitMemberInit(MemberInitExpression node)
        {
            var obj = Visit(node.NewExpression);

            foreach (var b in node.Bindings)
            { 
                VisitMemberBinding(obj, b);
            }

            return obj;
        }

        private Variable VisitMemberListBinding(Variable host, MemberListBinding binding)
        {
            throw new NotSupportedException();
            //this.Out(binding.Member.Name);
            //this.Out(" = {");
            //int num = 0;
            //int count = binding.Initializers.Count;
            //while (num < count)
            //{
            //    if (num > 0)
            //    {
            //        this.Out(", ");
            //    }
            //    this.VisitElementInit(binding.Initializers[num]);
            //    num++;
            //}
            //this.Out("}");
        }

        private Variable VisitMemberMemberBinding(Variable host, MemberMemberBinding binding)
        {
            throw new NotSupportedException();

            //this.Out(binding.Member.Name);
            //this.Out(" = {");
            //int num = 0;
            //int count = binding.Bindings.Count;
            //while (num < count)
            //{
            //    if (num > 0)
            //    {
            //        this.Out(", ");
            //    }
            //    this.VisitMemberBinding(binding.Bindings[num]);
            //    num++;
            //}
            //this.Out("}");
        }

        private Variable VisitMethodCall(MethodCallExpression node)
        {
            List<Variable> allArgs = new List<Variable>();
            if (node.Object != null)
            {
                allArgs.Add(Visit(node.Object));
            }

            foreach (var arg in node.Arguments)
            {
                allArgs.Add(Visit(arg));
            }

            var r = ComputeTempVar(node, OpCode.Call, allArgs.ToArray());
            r.DefineInstruction.MethodCall = node.Method;

            return r;
        }

        private Variable VisitNew(NewExpression node)
        {
            if (node.Members == null)
            {
                var args = VisitExpressions(node.Arguments.ToArray());
                return ComputeTempVar(node, OpCode.New, args);
            }
            else
            {
                var obj = ComputeTempVar(node, OpCode.New, new Variable[]{});

                for (int i = 0; i < node.Members.Count; i++)
                {
                    var inst = CreateNonIndexedInstruction(OpCode.MemberWrite);
                    inst.Destinations.Add(obj);
                    inst.Destinations.Add(Variable.CreateConstant(typeof(string), node.Members[i].Name));
                    inst.Sources.Add(Visit(node.Arguments[i]));
                }

                return obj;
            }
        }

        private Variable VisitNewArray(NewArrayExpression node)
        {
            var args = this.VisitExpressions(node.Expressions.ToArray());
            switch (node.NodeType)
            {
                case ExpressionType.NewArrayInit:
                    return ComputeTempVar(node, OpCode.NewArrayInit, args);

                case ExpressionType.NewArrayBounds:
                    return ComputeTempVar(node, OpCode.NewArrayBounds, args);

                default:
                    throw new NotSupportedException();
            }
        }

        private Variable VisitParameter(ParameterExpression node)
        {
            Variable v;
            if (_parameters.TryGetValue(node, out v))
                return v;
            else
            {
                v = Variable.CreateParam(node.Name, node.Type);
                _parameters[node] = v;
                return v;
            }
        }

        private Variable VisitRuntimeVariables(RuntimeVariablesExpression node)
        {
            throw new NotSupportedException("Expression of type " + node.NodeType + " is not supported");
            //this.VisitExpressions<ParameterExpression>('(', node.Variables, ')');
        }

        private Variable VisitSwitch(SwitchExpression node)
        {
            throw new NotSupportedException("Expression of type " + node.NodeType + " is not supported");

            //this.Out("switch ");
            //this.Out("(");
            //this.Visit(node.SwitchValue);
            //this.Out(") { ... }");
        }

        private Variable VisitSwitchCase(SwitchCase node)
        {
            throw new NotSupportedException();

            //this.Out("case ");
            //this.VisitExpressions<Expression>('(', node.TestValues, ')');
            //this.Out(": ...");
        }

        private Variable VisitTry(TryExpression node)
        {
            throw new NotSupportedException("Expression of type " + node.NodeType + " is not supported");

            //this.Out("try { ... }");
        }

        private Variable VisitTypeBinary(TypeBinaryExpression node)
        {
            throw new NotSupportedException("Expression of type " + node.NodeType + " is not supported");

        //    this.Out("(");
        //    this.Visit(node.Expression);
        //    switch (node.NodeType)
        //    {
        //        case ExpressionType.TypeIs:
        //            this.Out(" Is ");
        //            break;

        //        case ExpressionType.TypeEqual:
        //            this.Out(" TypeEqual ");
        //            goto Label_0043;
        //    }
        //Label_0043:
        //    this.Out(node.TypeOperand.Name);
        //    this.Out(")");
        }

        private Variable VisitUnary(UnaryExpression node)
        {
            Variable s = Visit(node.Operand);

            OpCode code;
            switch (node.NodeType)
            {
                case ExpressionType.TypeAs:
                    code = OpCode.Convert;
                    break;

                case ExpressionType.Decrement:
                    code = OpCode.Decrement;
                    break;

                case ExpressionType.Negate:
                case ExpressionType.NegateChecked:
                    code = OpCode.Negate;
                    break;

                case ExpressionType.UnaryPlus:
                    code = OpCode.Add;
                    break;

                case ExpressionType.Not:
                    code = OpCode.Not;
                    break;

                case ExpressionType.Quote:
                    return s;

                case ExpressionType.Increment:
                    code = OpCode.Increment;
                    break;

                case ExpressionType.PreIncrementAssign:
                    code = OpCode.PreIncrementAssign;
                    break;

                case ExpressionType.PreDecrementAssign:
                    code = OpCode.PreDecrementAssign;
                    break;


                case ExpressionType.PostIncrementAssign:
                    code = OpCode.PostIncrementAssign;
                    break;

                case ExpressionType.PostDecrementAssign:
                    code = OpCode.PostDecrementAssign;
                    break;

                case ExpressionType.OnesComplement:
                    code = OpCode.OnesComplement;
                    break;

                default:
                    throw new NotSupportedException("Expression of type " + node.NodeType + " is not supported");
            }

            return ComputeTempVar(node, code, new Variable[] { s });
        }

        private Variable VisitMemberBinding(Variable host, MemberBinding node)
        {
            switch (node.BindingType)
            {
                case MemberBindingType.Assignment:
                    return this.VisitMemberAssignment(host, (MemberAssignment)node);

                case MemberBindingType.MemberBinding:
                    return this.VisitMemberMemberBinding(host, (MemberMemberBinding)node);

                case MemberBindingType.ListBinding:
                    return this.VisitMemberListBinding(host, (MemberListBinding)node);
                default:
                    throw new System.ArgumentException("Error.UnhandledBindingType(node.BindingType);");
            }
            //throw Error.UnhandledBindingType(node.BindingType);
            //throw new System.ArgumentException("Error.UnhandledBindingType(node.BindingType);");
        }

        public Variable VisitTypeIs(TypeBinaryExpression b)
        {
            throw new NotSupportedException("Expression of type " + b.NodeType + " is not supported");
            //return this.Visit(b.Expression);
        }

        public Variable VisitMemberAccess(MemberExpression m)
        {
            throw new NotSupportedException("Expression of type " + m.NodeType + " is not supported");
            //throw new Exception("Overloaded visitors should implement this method");
            //            return this.Visit(m.Expression);
        }

        public Variable Visit(Expression exp)
        {
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
                    return this.VisitUnary((UnaryExpression)exp);
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
                        return this.VisitBinary((BinaryExpression)exp);
                    }
                case ExpressionType.TypeIs:
                    {
                        return this.VisitTypeIs((TypeBinaryExpression)exp);
                    }
                case ExpressionType.Conditional:
                    {
                        return this.VisitConditional((ConditionalExpression)exp);
                    }
                case ExpressionType.Constant:
                    {
                        return this.VisitConstant((ConstantExpression)exp);
                    }
                case ExpressionType.Parameter:
                    {
                        return this.VisitParameter((ParameterExpression)exp);
                    }
                case ExpressionType.MemberAccess:
                    {
                        return this.VisitMember((MemberExpression)exp);
                    }
                case ExpressionType.Call:
                    {
                        return this.VisitMethodCall((MethodCallExpression)exp);
                    }
                case ExpressionType.Lambda:
                    {
                        return this.VisitLambda((LambdaExpression)exp);
                    }
                case ExpressionType.New:
                    {
                        return this.VisitNew((NewExpression)exp);
                    }
                case ExpressionType.NewArrayInit:
                case ExpressionType.NewArrayBounds:
                    {
                        return this.VisitNewArray((NewArrayExpression)exp);
                    }
                case ExpressionType.Invoke:
                    {
                        return this.VisitInvocation((InvocationExpression)exp);
                    }
                case ExpressionType.MemberInit:
                    {
                        return this.VisitMemberInit((MemberInitExpression)exp);
                    }
                case ExpressionType.ListInit:
                    {
                        return this.VisitListInit((ListInitExpression)exp);
                    }
                default:
                    {
                        throw new NotSupportedException("Expression of type " + exp.NodeType + " is not supported");
                    }
            }
        }
    }
}
