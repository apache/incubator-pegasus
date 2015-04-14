namespace cpp dsn.example

struct count_op
{
   1: string name;
   2: i32    operand;
}

enum op_type
{
	OT_ADD,
	OT_SUB,
	OT_MUL,
	OT_DIV
}

struct count_bop
{
	1:op_type ot;
	2:double op1;
	3:double op2;
}

struct count_mop
{
	1: list<i32> ops;
}

service counter
{
    i32 add(1:count_op op);
    i32 read(1:string name);
	double any(1:count_bop op);
	void save(1:count_mop op);
}