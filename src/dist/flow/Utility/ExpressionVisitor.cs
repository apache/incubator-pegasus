using System;
using System.Collections.Generic;
using System.Text;
using System.Collections.ObjectModel;
using System.Reflection;
using System.Linq.Expressions;

namespace rDSN.Tron.Utility
{
    // This class implements a generic expression visitor. The code was stolen
    // from LINQ source code.
    public abstract class ExpressionVisitor<ReturnT, ContextT>
    {
        public ExpressionVisitor()
        {
        }

        protected object GetValue(Expression exp)
        {
            var lambda = Expression.Lambda(exp, new ParameterExpression[] { });
            return lambda.Compile().DynamicInvoke(new object[] { });
        }

        protected bool GetValue(Expression exp, out object value)
        {
            value = null;

            try
            {
                var lambda = Expression.Lambda(exp, new ParameterExpression[] { });
                value = lambda.Compile().DynamicInvoke(new object[] { });
                return true;
            }
            catch (Exception e)
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
                        return this.VisitUnary((UnaryExpression)exp, ctx);
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
                        return this.VisitBinary((BinaryExpression)exp, ctx);
                    }
                case ExpressionType.TypeIs:
                    {
                        return this.VisitTypeIs((TypeBinaryExpression)exp, ctx);
                    }
                case ExpressionType.Conditional:
                    {
                        return this.VisitConditional((ConditionalExpression)exp, ctx);
                    }
                case ExpressionType.Constant:
                    {
                        return this.VisitConstant((ConstantExpression)exp, ctx);
                    }
                case ExpressionType.Parameter:
                    {
                        return this.VisitParameter((ParameterExpression)exp, ctx);
                    }
                case ExpressionType.MemberAccess:
                    {
                        return this.VisitMemberAccess((MemberExpression)exp, ctx);
                    }
                case ExpressionType.Call:
                    {
                        return this.VisitMethodCall((MethodCallExpression)exp, ctx);
                    }
                case ExpressionType.Lambda:
                    {
                        return this.VisitLambda((LambdaExpression)exp, ctx);
                    }
                case ExpressionType.New:
                    {
                        return this.VisitNew((NewExpression)exp, ctx);
                    }
                case ExpressionType.NewArrayInit:
                case ExpressionType.NewArrayBounds:
                    {
                        return this.VisitNewArray((NewArrayExpression)exp, ctx);
                    }
                case ExpressionType.Invoke:
                    {
                        return this.VisitInvocation((InvocationExpression)exp, ctx);
                    }
                case ExpressionType.MemberInit:
                    {
                        return this.VisitMemberInit((MemberInitExpression)exp, ctx);
                    }
                case ExpressionType.ListInit:
                    {
                        return this.VisitListInit((ListInitExpression)exp, ctx);
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
                        return this.VisitMemberAssignment((MemberAssignment)binding, ctx);
                    }
                case MemberBindingType.MemberBinding:
                    {
                        return this.VisitMemberMemberBinding((MemberMemberBinding)binding, ctx);
                    }
                case MemberBindingType.ListBinding:
                    {
                        return this.VisitMemberListBinding((MemberListBinding)binding, ctx);
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
            ReturnT[] t = this.VisitExpressionList(initializer.Arguments, ctx);
            return default(ReturnT);
        }

        public virtual ReturnT VisitUnary(UnaryExpression u, ContextT ctx)
        {
            return this.Visit(u.Operand, ctx);
        }

        public virtual ReturnT VisitBinary(BinaryExpression b, ContextT ctx)
        {
            //throw new Exception("Overloaded visitors should implement this method");
            ReturnT t1 = this.Visit(b.Left, ctx);
            ReturnT t2 = this.Visit(b.Right, ctx);            
            return t2;
        }

        public virtual ReturnT VisitTypeIs(TypeBinaryExpression b, ContextT ctx)
        {
            return this.Visit(b.Expression, ctx);
        }

        public virtual ReturnT VisitConstant(ConstantExpression c, ContextT ctx)
        {
            //throw new Exception("Overloaded visitors should implement this method");
            return default(ReturnT);
        }

        public virtual ReturnT VisitConditional(ConditionalExpression c, ContextT ctx)
        {
            //throw new Exception("Overloaded visitors should implement this method");
            ReturnT t1 = this.Visit(c.Test, ctx);
            ReturnT t2 = this.Visit(c.IfTrue, ctx);
            ReturnT t3 = this.Visit(c.IfFalse, ctx);
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
                return this.Visit(m.Expression, ctx);
            else
                return default(ReturnT);
        }

        public virtual ReturnT VisitMethodCall(MethodCallExpression m, ContextT ctx)
        {
            //throw new Exception("Overloaded visitors should implement this method");
            ReturnT t1 = default(ReturnT);
            if (m.Object != null)
            {
                t1 = this.Visit(m.Object, ctx);
            }

            ReturnT[] t2 = this.VisitExpressionList(m.Arguments, ctx);
            return t1;
        }

        public virtual ReturnT[] VisitExpressionList(ReadOnlyCollection<Expression> original, ContextT ctx)
        {
            List<ReturnT> list = new List<ReturnT>();
            for (int i = 0, n = original.Count; i < n; i++)
            {
                list.Add(this.Visit(original[i], ctx));
            }
            return list.ToArray();
        }

        public virtual ReturnT VisitMemberAssignment(MemberAssignment assignment, ContextT ctx)
        {
            return this.Visit(assignment.Expression, ctx);
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
            List<ReturnT> t1 = new List<ReturnT>();
            for (int i = 0, n = original.Count; i < n; i++)
            {
                t1.Add(this.VisitBinding(original[i], ctx));
            }
            return t1.ToArray();
        }

        public virtual ReturnT[] VisitElementInitializerList(ReadOnlyCollection<ElementInit> original, ContextT ctx)
        {
            List<ReturnT> t1 = new List<ReturnT>();
            for (int i = 0, n = original.Count; i < n; i++)
            {
                t1.Add(this.VisitElementInitializer(original[i], ctx));
            }
            return t1.ToArray();
        }

        public virtual ReturnT VisitLambda(LambdaExpression lambda, ContextT ctx)
        {
            return this.Visit(lambda.Body, ctx);
        }

        public virtual ReturnT VisitNew(NewExpression nex, ContextT ctx)
        {
            ReturnT[] t = this.VisitExpressionList(nex.Arguments, ctx);
            return default(ReturnT);
        }

        public virtual ReturnT VisitMemberInit(MemberInitExpression init, ContextT ctx)
        {
            ReturnT t1 = this.VisitNew(init.NewExpression, ctx);
            ReturnT[] t2 = this.VisitBindingList(init.Bindings, ctx);
            return t1;
        }

        public virtual ReturnT VisitListInit(ListInitExpression init, ContextT ctx)
        {
            ReturnT t1 = this.VisitNew(init.NewExpression, ctx);
            ReturnT[] t2 = this.VisitElementInitializerList(init.Initializers, ctx);
            return t1;
        }

        public virtual ReturnT VisitNewArray(NewArrayExpression na, ContextT ctx)
        {
            ReturnT[] t = this.VisitExpressionList(na.Expressions, ctx);
            return default(ReturnT);
        }

        public virtual ReturnT VisitInvocation(InvocationExpression iv, ContextT ctx)
        {
            ReturnT[] t1 = this.VisitExpressionList(iv.Arguments, ctx);
            ReturnT t2 = this.Visit(iv.Expression, ctx);
            return t2;
        }
    }

}
