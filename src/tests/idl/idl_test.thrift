namespace cpp dsn.idl.test

struct test_thrift_item
{
   1: bool bool_item;
   2: byte byte_item;
   3: i16  i16_item;
   4: i32  i32_item;
   5: i64  i64_item;
   6: double double_item;
   7: string string_item;
   8: list<i32> list_i32_item;
   9: set<i32> set_i32_item;
   10: map<i32, i32> map_i32_item;
}