# pragma once
# include <dsn/internal/serialization.h>

namespace dsn { namespace replication { namespace application { 
	// ---------- kv_pair -------------
	struct kv_pair
	{
		std::string key;
		std::string value;
	};

	inline void marshall(::dsn::binary_writer& writer, const kv_pair& val)
	{
		marshall(writer, val.key);
		marshall(writer, val.value);
	};

	inline void unmarshall(::dsn::binary_reader& reader, __out_param kv_pair& val)
	{
		unmarshall(reader, val.key);
		unmarshall(reader, val.value);
	};

} } } 
