# pragma once
# include <dsn/internal/serialization.h>

namespace dsn { namespace fd { 
	// ---------- beacon_msg -------------
	struct beacon_msg
	{
		int64_t time;
		::dsn::end_point from;
		::dsn::end_point to;
	};

	inline void marshall(::dsn::binary_writer& writer, const beacon_msg& val)
	{
		marshall(writer, val.time);
		marshall(writer, val.from);
		marshall(writer, val.to);
	};

	inline void unmarshall(::dsn::binary_reader& reader, __out_param beacon_msg& val)
	{
		unmarshall(reader, val.time);
		unmarshall(reader, val.from);
		unmarshall(reader, val.to);
	};

	// ---------- beacon_ack -------------
	struct beacon_ack
	{
		int64_t time;
		::dsn::end_point this_node;
		::dsn::end_point primary_node;
		bool is_master;
		bool allowed;
	};

	inline void marshall(::dsn::binary_writer& writer, const beacon_ack& val)
	{
		marshall(writer, val.time);
		marshall(writer, val.this_node);
		marshall(writer, val.primary_node);
		marshall(writer, val.is_master);
		marshall(writer, val.allowed);
	};

	inline void unmarshall(::dsn::binary_reader& reader, __out_param beacon_ack& val)
	{
		unmarshall(reader, val.time);
		unmarshall(reader, val.this_node);
		unmarshall(reader, val.primary_node);
		unmarshall(reader, val.is_master);
		unmarshall(reader, val.allowed);
	};

} } 
