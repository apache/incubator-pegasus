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
using System.Collections.ObjectModel;
using System.Linq.Expressions;

namespace rDSN.Tron.Utility
{
    // This class implements a generic expression visitor. The code was stolen
    // from LINQ source code.
    public abstract class ExpressionVisitor<ReturnT, ContextT>
    {
        protected object GetValue(Expression exp)
        {
            var lambda = Expression.Lambda(exp);
            return lambda.Compile().DynamicInvoke();
        }

        protected bool GetValue(Expression exp, out object value)
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

        public virtual ReturnT Visit(Expression exp, ContextT ctx)
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
                    {
                        return VisitUnary((UnaryExpression)exp, ctx);
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
                        return VisitBinary((BinaryExpression)exp, ctx);
                    }
                case ExpressionType.TypeIs:
                    {
                        return VisitTypeIs((TypeBinaryExpression)exp, ctx);
                    }
                case ExpressionType.Conditional:
                    {
                        return VisitConditional((ConditionalExpression)exp, ctx);
                    }
                case ExpressionType.Constant:
                    {
                        return VisitConstant((ConstantExpression)exp, ctx);
                    }
                case ExpressionType.Parameter:
                    {
                        return VisitParameter((ParameterExpression)exp, ctx);
                    }
                case ExpressionType.MemberAccess:
                    {
                        return VisitMemberAccess((MemberExpression)exp, ctx);
                    }
                case ExpressionType.Call:
                    {
                        return VisitMethodCall((MethodCallExpression)exp, ctx);
                    }
                case ExpressionType.Lambda:
                    {
                        return VisitLambda((LambdaExpression)exp, ctx);
                    }
                case ExpressionType.New:
                    {
                        return VisitNew((NewExpression)exp, ctx);
                    }
                case ExpressionType.NewArrayInit:
                case ExpressionType.NewArrayBounds:
                    {
                        return VisitNewArray((NewArrayExpression)exp, ctx);
                    }
                case ExpressionType.Invoke:
                    {
                        return VisitInvocation((InvocationExpression)exp, ctx);
                    }
                case ExpressionType.MemberInit:
                    {
                        return VisitMemberInit((MemberInitExpression)exp, ctx);
                    }
                case ExpressionType.ListInit:
                    {
                        return VisitListInit((ListInitExpression)exp, ctx);
                    }
                default:
                    {
                        throw new Exception("Visitor: Expression of type " + exp.NodeType + " is not handled");
                    }
            }
        }

        public virtual ReturnT VisitBinding(MemberBinding binding, ContextT ctx)
        {
            switch (binding.BindingType)
            {
                case MemberBindingType.Assignment:
                    {
                        return VisitMemberAssignment((MemberAssignment)binding, ctx);
                    }
                case MemberBindingType.MemberBinding:
                    {
                        return VisitMemberMemberBinding((MemberMemberBinding)binding, ctx);
                    }
                case MemberBindingType.ListBinding:
                    {
                        return VisitMemberListBinding((MemberListBinding)binding, ctx);
                    }
                default:
                    {
                        throw new Exception("Visitor: Expression of type " + binding.BindingType + " is not handled");
                    }
            }
        }

        public virtual ReturnT VisitElementInitializer(ElementInit initializer, ContextT ctx)
        {
            //throw new Exception("Overloaded visitors should implement this method");
            VisitExpressionList(initializer.Arguments, ctx);
            return default(ReturnT);
        }

        public virtual ReturnT VisitUnary(UnaryExpression u, ContextT ctx)
        {
            return Visit(u.Operand, ctx);
        }

        public virtual ReturnT VisitBinary(BinaryExpression b, ContextT ctx)
        {
            //throw new Exception("Overloaded visitors should implement this method");
            Visit(b.Left, ctx);
            var t2 = Visit(b.Right, ctx);            
            return t2;
        }

        public virtual ReturnT VisitTypeIs(TypeBinaryExpression b, ContextT ctx)
        {
            return Visit(b.Expression, ctx);
        }

        public virtual ReturnT VisitConstant(ConstantExpression c, ContextT ctx)
        {
            //throw new Exception("Overloaded visitors should implement this method");
            return default(ReturnT);
        }

        public virtual ReturnT VisitConditional(ConditionalExpression c, ContextT ctx)
        {
            //throw new Exception("Overloaded visitors should implement this method");
            var t1 = Visit(c.Test, ctx);
            var t2 = Visit(c.IfTrue, ctx);
            var t3 = Visit(c.IfFalse, ctx);
            return t1;
        }

        public virtual ReturnT VisitParameter(ParameterExpression p, ContextT ctx)
        {
            //throw new Exception("Overloaded visitors should implement this method");
            return default(ReturnT);
        }

        public virtual ReturnT VisitMemberAccess(MemberExpression m, ContextT ctx)
        {
            //throw new Exception("Overloaded visitors should implement this method");
            if (m.Expression != null)
                return Visit(m.Expression, ctx);
            return default(ReturnT);
        }

        public virtual ReturnT VisitMethodCall(MethodCallExpression m, ContextT ctx)
        {
            //throw new Exception("Overloaded visitors should implement this method");
            var t1 = default(ReturnT);
            if (m.Object != null)
            {
                t1 = Visit(m.Object, ctx);
            }

            var t2 = VisitExpressionList(m.Arguments, ctx);
            return t1;
        }

        public virtual ReturnT[] VisitExpressionList(ReadOnlyCollection<Expression> original, ContextT ctx)
        {
            var list = new List<ReturnT>();
            for (int i = 0, n = original.Count; i < n; i++)
            {
                list.Add(Visit(original[i], ctx));
            }
            return list.ToArray();
        }

        public virtual ReturnT VisitMemberAssignment(MemberAssignment assignment, ContextT ctx)
        {
            return Visit(assignment.Expression, ctx);
        }

        public virtual ReturnT VisitMemberMemberBinding(MemberMemberBinding binding, ContextT ctx)
        {
            throw new Exception("Overloaded visitors should implement this method");
            //return this.VisitBindingList(binding.Bindings, ctx);
        }

        public virtual ReturnT VisitMemberListBinding(MemberListBinding binding, ContextT ctx)
        {
            throw new Exception("Overloaded visitors should implement this method");
            //return this.VisitElementInitializerList(binding.Initializers, ctx);
        }

        public virtual ReturnT[] VisitBindingList(ReadOnlyCollection<MemberBinding> original, ContextT ctx)
        {
            var t1 = new List<ReturnT>();
            for (int i = 0, n = original.Count; i < n; i++)
            {
                t1.Add(VisitBinding(original[i], ctx));
            }
            return t1.ToArray();
        }

        public virtual ReturnT[] VisitElementInitializerList(ReadOnlyCollection<ElementInit> original, ContextT ctx)
        {
            var t1 = new List<ReturnT>();
            for (int i = 0, n = original.Count; i < n; i++)
            {
                t1.Add(VisitElementInitializer(original[i], ctx));
            }
            return t1.ToArray();
        }

        public virtual ReturnT VisitLambda(LambdaExpression lambda, ContextT ctx)
        {
            return Visit(lambda.Body, ctx);
        }

        public virtual ReturnT VisitNew(NewExpression nex, ContextT ctx)
        {
            var t = VisitExpressionList(nex.Arguments, ctx);
            return default(ReturnT);
        }

        public virtual ReturnT VisitMemberInit(MemberInitExpression init, ContextT ctx)
        {
            var t1 = VisitNew(init.NewExpression, ctx);
            var t2 = VisitBindingList(init.Bindings, ctx);
            return t1;
        }

        public virtual ReturnT VisitListInit(ListInitExpression init, ContextT ctx)
        {
            var t1 = VisitNew(init.NewExpression, ctx);
            var t2 = VisitElementInitializerList(init.Initializers, ctx);
            return t1;
        }

        public virtual ReturnT VisitNewArray(NewArrayExpression na, ContextT ctx)
        {
            var t = VisitExpressionList(na.Expressions, ctx);
            return default(ReturnT);
        }

        public virtual ReturnT VisitInvocation(InvocationExpression iv, ContextT ctx)
        {
            var t1 = VisitExpressionList(iv.Arguments, ctx);
            var t2 = Visit(iv.Expression, ctx);
            return t2;
        }
    }

}
