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
using System.Collections.ObjectModel;
using System.Linq.Expressions;

namespace rDSN.Tron.Utility
{
    // This class implements a generic expression visitor. The code was stolen
    // from LINQ source code.
    public abstract class ExpressionVisitor2
    {
        public virtual void Visit(Expression exp)
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
                        VisitMemberAccess((MemberExpression)exp);
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

        public virtual void VisitBinding(MemberBinding binding)
        {
            switch (binding.BindingType)
            {
                case MemberBindingType.Assignment:
                    {
                        VisitMemberAssignment((MemberAssignment)binding);
                        break;
                    }
                case MemberBindingType.MemberBinding:
                    {
                        VisitMemberMemberBinding((MemberMemberBinding)binding);
                        break;
                    }
                case MemberBindingType.ListBinding:
                    {
                        VisitMemberListBinding((MemberListBinding)binding);
                        break;
                    }
                default:
                    {
                        throw new Exception("Visitor: Expression of type " + binding.BindingType + " is not handled");
                    }
            }
        }

        public virtual void VisitElementInitializer(ElementInit initializer)
        {
            //throw new Exception("Overloaded visitors should implement this method");
            VisitExpressionList(initializer.Arguments);
        }

        public virtual void VisitUnary(UnaryExpression u)
        {
            Visit(u.Operand);
        }

        public virtual void VisitBinary(BinaryExpression b)
        {
            //throw new Exception("Overloaded visitors should implement this method");
            Visit(b.Left);
            Visit(b.Right);
        }

        public virtual void VisitTypeIs(TypeBinaryExpression b)
        {
            Visit(b.Expression);
        }

        public virtual void VisitConstant(ConstantExpression c)
        {
            
        }

        public virtual void VisitConditional(ConditionalExpression c)
        {
            //throw new Exception("Overloaded visitors should implement this method");
            Visit(c.Test);
            Visit(c.IfTrue);
            Visit(c.IfFalse);
        }

        public virtual void VisitParameter(ParameterExpression p)
        {
            //throw new Exception("Overloaded visitors should implement this method");
        }

        public virtual void VisitMemberAccess(MemberExpression m)
        {
            //throw new Exception("Overloaded visitors should implement this method");
            if (m.Expression != null)
                Visit(m.Expression);
        }

        public virtual void VisitMethodCall(MethodCallExpression m)
        {
            //throw new Exception("Overloaded visitors should implement this method");
            Visit(m.Object);
            VisitExpressionList(m.Arguments);
        }

        public virtual void VisitExpressionList(ReadOnlyCollection<Expression> original)
        {
            for (int i = 0, n = original.Count; i < n; i++)
            {
                Visit(original[i]);
            }
        }

        public virtual void VisitMemberAssignment(MemberAssignment assignment)
        {
            Visit(assignment.Expression);
        }

        public virtual void VisitMemberMemberBinding(MemberMemberBinding binding)
        {
            VisitBindingList(binding.Bindings);
        }

        public virtual void VisitMemberListBinding(MemberListBinding binding)
        {
            VisitElementInitializerList(binding.Initializers);
        }

        public virtual void VisitBindingList(ReadOnlyCollection<MemberBinding> original)
        {
            for (int i = 0, n = original.Count; i < n; i++)
            {
                VisitBinding(original[i]);
            }
        }

        public virtual void VisitElementInitializerList(ReadOnlyCollection<ElementInit> original)
        {
            for (int i = 0, n = original.Count; i < n; i++)
            {
                VisitElementInitializer(original[i]);
            }
        }

        public virtual void VisitLambda(LambdaExpression lambda)
        {
            Visit(lambda.Body);
        }

        public virtual void VisitNew(NewExpression nex)
        {
            VisitExpressionList(nex.Arguments);
        }

        public virtual void VisitMemberInit(MemberInitExpression init)
        {
            VisitNew(init.NewExpression);
            VisitBindingList(init.Bindings);
        }

        public virtual void VisitListInit(ListInitExpression init)
        {
            VisitNew(init.NewExpression);
            VisitElementInitializerList(init.Initializers);
        }

        public virtual void VisitNewArray(NewArrayExpression na)
        {
            VisitExpressionList(na.Expressions);
        }

        public virtual void VisitInvocation(InvocationExpression iv)
        {
            VisitExpressionList(iv.Arguments);
            Visit(iv.Expression);
        }
    }

}
