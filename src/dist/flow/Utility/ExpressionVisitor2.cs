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
    public abstract class ExpressionVisitor2
    {
        public ExpressionVisitor2()
        {
        }

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
                        this.VisitMemberAccess((MemberExpression)exp);
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

        public virtual void VisitBinding(MemberBinding binding)
        {
            switch (binding.BindingType)
            {
                case MemberBindingType.Assignment:
                    {
                        this.VisitMemberAssignment((MemberAssignment)binding);
                        break;
                    }
                case MemberBindingType.MemberBinding:
                    {
                        this.VisitMemberMemberBinding((MemberMemberBinding)binding);
                        break;
                    }
                case MemberBindingType.ListBinding:
                    {
                        this.VisitMemberListBinding((MemberListBinding)binding);
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
            this.VisitExpressionList(initializer.Arguments);
        }

        public virtual void VisitUnary(UnaryExpression u)
        {
            this.Visit(u.Operand);
        }

        public virtual void VisitBinary(BinaryExpression b)
        {
            //throw new Exception("Overloaded visitors should implement this method");
            this.Visit(b.Left);
            this.Visit(b.Right);
        }

        public virtual void VisitTypeIs(TypeBinaryExpression b)
        {
            this.Visit(b.Expression);
        }

        public virtual void VisitConstant(ConstantExpression c)
        {
            
        }

        public virtual void VisitConditional(ConditionalExpression c)
        {
            //throw new Exception("Overloaded visitors should implement this method");
            this.Visit(c.Test);
            this.Visit(c.IfTrue);
            this.Visit(c.IfFalse);
        }

        public virtual void VisitParameter(ParameterExpression p)
        {
            //throw new Exception("Overloaded visitors should implement this method");
        }

        public virtual void VisitMemberAccess(MemberExpression m)
        {
            //throw new Exception("Overloaded visitors should implement this method");
            if (m.Expression != null)
                this.Visit(m.Expression);
            else
            {}
        }

        public virtual void VisitMethodCall(MethodCallExpression m)
        {
            //throw new Exception("Overloaded visitors should implement this method");
            this.Visit(m.Object);
            this.VisitExpressionList(m.Arguments);
        }

        public virtual void VisitExpressionList(ReadOnlyCollection<Expression> original)
        {
            for (int i = 0, n = original.Count; i < n; i++)
            {
                this.Visit(original[i]);
            }
        }

        public virtual void VisitMemberAssignment(MemberAssignment assignment)
        {
            this.Visit(assignment.Expression);
        }

        public virtual void VisitMemberMemberBinding(MemberMemberBinding binding)
        {
            this.VisitBindingList(binding.Bindings);
        }

        public virtual void VisitMemberListBinding(MemberListBinding binding)
        {
            this.VisitElementInitializerList(binding.Initializers);
        }

        public virtual void VisitBindingList(ReadOnlyCollection<MemberBinding> original)
        {
            for (int i = 0, n = original.Count; i < n; i++)
            {
                this.VisitBinding(original[i]);
            }
        }

        public virtual void VisitElementInitializerList(ReadOnlyCollection<ElementInit> original)
        {
            for (int i = 0, n = original.Count; i < n; i++)
            {
                this.VisitElementInitializer(original[i]);
            }
        }

        public virtual void VisitLambda(LambdaExpression lambda)
        {
            this.Visit(lambda.Body);
        }

        public virtual void VisitNew(NewExpression nex)
        {
            this.VisitExpressionList(nex.Arguments);
        }

        public virtual void VisitMemberInit(MemberInitExpression init)
        {
            this.VisitNew(init.NewExpression);
            this.VisitBindingList(init.Bindings);
        }

        public virtual void VisitListInit(ListInitExpression init)
        {
            this.VisitNew(init.NewExpression);
            this.VisitElementInitializerList(init.Initializers);
        }

        public virtual void VisitNewArray(NewArrayExpression na)
        {
            this.VisitExpressionList(na.Expressions);
        }

        public virtual void VisitInvocation(InvocationExpression iv)
        {
            this.VisitExpressionList(iv.Arguments);
            this.Visit(iv.Expression);
        }
    }

}
