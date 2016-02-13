using System;
using System.Collections.Generic;
using System.Reflection;

using rDSN.Tron.Utility;
using rDSN.Tron.Contract;

namespace rDSN.Tron.Compiler
{
    public class Variable
    {
        public enum SymbolType
        { 
            ST_TEMP,
            ST_LOCAL,
            ST_PARAMETER,
            ST_CONSTANT
        }

        public string Name { get; private set; }
        public SymbolType SymType { get; private set; }
        public Type ValType { get; private set; }

        // for temp and local
        public Instruction DefineInstruction { get; private set; }

        // for constant
        public object ConstantValue { get; private set; }
        
        public override string ToString()
        {
            switch (SymType)
            { 
                case SymbolType.ST_PARAMETER:
                case SymbolType.ST_LOCAL:
                case SymbolType.ST_TEMP:
                    return Name + "(" + SymType.ToString() + ", " + ValType.Name + ")";
                case SymbolType.ST_CONSTANT:
                    return ConstantValue.ToString() + "(" + SymType.ToString() + ", " + ValType.Name + ")";
                default:
                    throw new Exception();
            }
        }

        public static Variable CreateTemp(Type type, Instruction defineInstr)
        {
            Variable var = new Variable();
            var.Name = "t" + (_tempVarId++);
            var.SymType = SymbolType.ST_TEMP;
            var.ValType = type;

            var.DefineInstruction = defineInstr;
            return var;
        }

        public static Variable CreateLocal(string name, Type type, Instruction defineInstr)
        {
            Variable var = new Variable();
            var.Name = name;
            var.SymType = SymbolType.ST_LOCAL;
            var.ValType = type;

            var.DefineInstruction = defineInstr;
            return var;
        }

        public static Variable CreateParam(string name, Type type)
        {
            Variable var = new Variable();
            var.Name = name;
            var.SymType = SymbolType.ST_PARAMETER;
            var.ValType = type;

            return var;
        }

        public static Variable CreateConstant(Type type, object value)
        {
            Variable var = new Variable();
            var.Name = "c" + (_constVarId++);
            var.SymType = SymbolType.ST_CONSTANT;
            var.ValType = type;

            var.ConstantValue = value;
            return var;
        }

        private static int _tempVarId = 1;
        private static int _constVarId = 1;
    }

    public class Instruction
    {
        public List<Variable> Destinations { get; set; }
        public List<Variable> Sources { get; set; }
        public OpCode Code { get; private set; }
        public MethodInfo MethodCall { get; set; }

        public Instruction(OpCode code)
        {
            Code = code;
            Destinations = new List<Variable>();
            Sources = new List<Variable>();
            MethodCall = null;
        }

        public override string ToString()
        {
            if (Code == OpCode.Call)
            {
                return Destinations.VerboseCombine(" ", d => d.ToString()) + " = " + Code.ToString() + " " + MethodCall.Name + " " + Sources.VerboseCombine(" ", s => s.ToString());
            }
            else
            {
                return Destinations.VerboseCombine(" ", d => d.ToString()) + " = " + Code.ToString() + " " + Sources.VerboseCombine(" ", s => s.ToString());
            }
        }
    }
    
    public enum OpCode
    {
        // Summary:
        //     An addition operation, such as a + b, without overflow checking, for numeric
        //     operands.
        Add = 0,
        //
        // Summary:
        //     A bitwise or logical AND operation, such as (a & b) in C# and (a And b) in
        //     Visual Basic.
        And = 2,
        //
        // Summary:
        //     A conditional AND operation that evaluates the second operand only if the
        //     first operand evaluates to true. It corresponds to (a && b) in C# and (a
        //     AndAlso b) in Visual Basic.
        AndAlso = 3,
        //
        // Summary:
        //     An operation that obtains the length of a one-dimensional array, such as
        //     array.Length.
        ArrayLength = 4,
        //
        // Summary:
        //     An indexing operation in a one-dimensional array, such as array[index] in
        //     C# or array(index) in Visual Basic.
        ArrayIndex = 5,
        //
        // Summary:
        //     A method call, such as in the obj.sampleMethod() expression.
        Call = 6,
        //
        // Summary:
        //     A conditional operation, such as a > b ? a : b in C# or If(a > b, a, b) in
        //     Visual Basic.
        Conditional = 8,
        //
        // Summary:
        //     A constant value.
        Constant = 9,
        //
        // Summary:
        //     A cast or conversion operation, such as (SampleType)obj in C#or CType(obj,
        //     SampleType) in Visual Basic. For a numeric conversion, if the converted value
        //     is too large for the destination type, no exception is thrown.
        Convert = 10,
        //
        // Summary:
        //     A division operation, such as (a / b), for numeric operands.
        Divide = 12,
        //
        // Summary:
        //     A node that represents an equality comparison, such as (a == b) in C# or
        //     (a = b) in Visual Basic.
        Equal = 13,
        //
        // Summary:
        //     A bitwise or logical XOR operation, such as (a ^ b) in C# or (a Xor b) in
        //     Visual Basic.
        ExclusiveOr = 14,
        //
        // Summary:
        //     A "greater than" comparison, such as (a > b).
        GreaterThan = 15,
        //
        // Summary:
        //     A "greater than or equal to" comparison, such as (a >= b).
        GreaterThanOrEqual = 16,
        //
        // Summary:
        //     A bitwise left-shift operation, such as (a << b).
        LeftShift = 19,
        //
        // Summary:
        //     A "less than" comparison, such as (a < b).
        LessThan = 20,
        //
        // Summary:
        //     A "less than or equal to" comparison, such as (a <= b).
        LessThanOrEqual = 21,
        //
        // Summary:
        //     An operation that creates a new System.Collections.IEnumerable object and
        //     initializes it from a list of elements, such as new List<SampleType>(){ a,
        //     b, c } in C# or Dim sampleList = { a, b, c } in Visual Basic.
        ListInit = 22,
        //
        // Summary:
        //     An operation that reads from a field or property, such as obj.SampleProperty.
        MemberRead = 23,
        //
        // Summary:
        //     An operation that creates a new object and initializes one or more of its
        //     members, such as new Point { X = 1, Y = 2 } in C# or New Point With {.X =
        //     1, .Y = 2} in Visual Basic.
        MemberWrite = 24,
        //
        // Summary:
        //     An arithmetic remainder operation, such as (a % b) in C# or (a Mod b) in
        //     Visual Basic.
        Modulo = 25,
        //
        // Summary:
        //     A multiplication operation, such as (a * b), without overflow checking, for
        //     numeric operands.
        Multiply = 26,
        //
        // Summary:
        //     An arithmetic negation operation, such as (-a). The object a should not be
        //     modified in place.
        Negate = 28,
        //
        // Summary:
        //     An operation that calls a constructor to create a new object, such as new
        //     SampleType().
        New = 31,
        //
        // Summary:
        //     An operation that creates a new one-dimensional array and initializes it
        //     from a list of elements, such as new SampleType[]{a, b, c} in C# or New SampleType(){a,
        //     b, c} in Visual Basic.
        NewArrayInit = 32,
        //
        // Summary:
        //     An operation that creates a new array, in which the bounds for each dimension
        //     are specified, such as new SampleType[dim1, dim2] in C# or New SampleType(dim1,
        //     dim2) in Visual Basic.
        NewArrayBounds = 33,
        //
        // Summary:
        //     A bitwise complement or logical negation operation. In C#, it is equivalent
        //     to (~a) for integral types and to (!a) for Boolean values. In Visual Basic,
        //     it is equivalent to (Not a). The object a should not be modified in place.
        Not = 34,
        //
        // Summary:
        //     An inequality comparison, such as (a != b) in C# or (a <> b) in Visual Basic.
        NotEqual = 35,
        //
        // Summary:
        //     A bitwise or logical OR operation, such as (a | b) in C# or (a Or b) in Visual
        //     Basic.
        Or = 36,
        //
        // Summary:
        //     A short-circuiting conditional OR operation, such as (a || b) in C# or (a
        //     OrElse b) in Visual Basic.
        OrElse = 37,
        //
        // Summary:
        //     A mathematical operation that raises a number to a power, such as (a ^ b)
        //     in Visual Basic.
        Power = 39,
        //
        // Summary:
        //     A bitwise right-shift operation, such as (a >> b).
        RightShift = 41,
        //
        // Summary:
        //     A subtraction operation, such as (a - b), without overflow checking, for
        //     numeric operands.
        Subtract = 42,
        //
        // Summary:
        //     A type test, such as obj is SampleType in C# or TypeOf obj is SampleType
        //     in Visual Basic.
        TypeIs = 45,
        //
        // Summary:
        //     An assignment operation, such as (a = b).
        Assign = 46,
        //
        // Summary:
        //     A unary decrement operation, such as (a - 1) in C# and Visual Basic. The
        //     object a should not be modified in place.
        Decrement = 49,
        //
        // Summary:
        //     A unary increment operation, such as (a + 1) in C# and Visual Basic. The
        //     object a should not be modified in place.
        Increment = 54,
        //
        // Summary:
        //     An index operation or an operation that accesses a property that takes arguments.
        Index = 55,
        //
        // Summary:
        //     An addition compound assignment operation, such as (a += b), without overflow
        //     checking, for numeric operands.
        AddAssign = 63,
        //
        // Summary:
        //     A bitwise or logical AND compound assignment operation, such as (a &= b)
        //     in C#.
        AndAssign = 64,
        //
        // Summary:
        //     An division compound assignment operation, such as (a /= b), for numeric
        //     operands.
        DivideAssign = 65,
        //
        // Summary:
        //     A bitwise or logical XOR compound assignment operation, such as (a ^= b)
        //     in C#.
        ExclusiveOrAssign = 66,
        //
        // Summary:
        //     A bitwise left-shift compound assignment, such as (a <<= b).
        LeftShiftAssign = 67,
        //
        // Summary:
        //     An arithmetic remainder compound assignment operation, such as (a %= b) in
        //     C#.
        ModuloAssign = 68,
        //
        // Summary:
        //     A multiplication compound assignment operation, such as (a *= b), without
        //     overflow checking, for numeric operands.
        MultiplyAssign = 69,
        //
        // Summary:
        //     A bitwise or logical OR compound assignment, such as (a |= b) in C#.
        OrAssign = 70,
        //
        // Summary:
        //     A compound assignment operation that raises a number to a power, such as
        //     (a ^= b) in Visual Basic.
        PowerAssign = 71,
        //
        // Summary:
        //     A bitwise right-shift compound assignment operation, such as (a >>= b).
        RightShiftAssign = 72,
        //
        // Summary:
        //     A subtraction compound assignment operation, such as (a -= b), without overflow
        //     checking, for numeric operands.
        SubtractAssign = 73,
        //
        // Summary:
        //     A unary prefix increment, such as (++a). The object a should be modified
        //     in place.
        PreIncrementAssign = 77,
        //
        // Summary:
        //     A unary prefix decrement, such as (--a). The object a should be modified
        //     in place.
        PreDecrementAssign = 78,
        //
        // Summary:
        //     A unary postfix increment, such as (a++). The object a should be modified
        //     in place.
        PostIncrementAssign = 79,
        //
        // Summary:
        //     A unary postfix decrement, such as (a--). The object a should be modified
        //     in place.
        PostDecrementAssign = 80,
        //
        // Summary:
        //     A ones complement operation, such as (~a) in C#.
        OnesComplement = 82,
        //
        // Summary:
        //     A true condition value.
        IsTrue = 83,
        //
        // Summary:
        //     A false condition value.
        IsFalse = 84,
    }
}
