# pragma once

//
// uncomment the following line if you want to use 
// data encoding/decoding from the original tool instead of rDSN
// in this case, you need to use these tools to generate
// type files with --gen=cpp etc. options
//
// !!! WARNING: not feasible for replicated service yet!!! 
//
# define DSN_NOT_USE_DEFAULT_SERIALIZATION

# include <dsn/internal/serialization.h>

# ifdef DSN_NOT_USE_DEFAULT_SERIALIZATION

# include <dsn/thrift_helper.h>
# include "simple_kv_types.h" 

namespace dsn { namespace replication { namespace application { 
	// ---------- kv_pair -------------
	inline void marshall(::dsn::binary_writer& writer, const kv_pair& val)
	{
		boost::shared_ptr<::dsn::binary_writer_transport> transport(new ::dsn::binary_writer_transport(writer));
		::apache::thrift::protocol::TBinaryProtocol proto(transport);
		::dsn::marshall_rpc_args<kv_pair>(&proto, val, &kv_pair::write);
	};

	inline void unmarshall(::dsn::binary_reader& reader, __out_param kv_pair& val)
	{
		boost::shared_ptr<::dsn::binary_reader_transport> transport(new ::dsn::binary_reader_transport(reader));
		::apache::thrift::protocol::TBinaryProtocol proto(transport);
		::dsn::unmarshall_rpc_args<kv_pair>(&proto, val, &kv_pair::read);
	};

} } } 


# else // use rDSN's data encoding/decoding

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

#endif 
