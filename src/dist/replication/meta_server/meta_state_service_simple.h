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
 *     a simple version of meta state service for development
 *
 * Revision history:
 *     2015-11-03, @imzhenyu (Zhenyu.Guo@microsoft.com), setup the sketch
 *     2015-11-11, Tianyi WANG, first version done
 */

# include <dsn/dist/meta_state_service.h>
# include "replication_common.h"

#include <queue>

using namespace dsn::service;

namespace dsn
{
    namespace dist
    {
        DEFINE_TASK_CODE_AIO(LPC_META_STATE_SERVICE_SIMPLE_INTERNAL, TASK_PRIORITY_HIGH, THREAD_POOL_DEFAULT);

        class meta_state_service_simple
            : public meta_state_service, public clientlet
        {
        public:
            explicit meta_state_service_simple() : _root("/", nullptr), _quick_map({std::make_pair("/", &_root)}), _log_lock(true), _log(nullptr), _offset(0){}

            // work_path = (argc > 0 ? argv[0] : current_app_data_dir)
            virtual error_code initialize(const std::vector<std::string>& args) override;
            virtual error_code finalize() override { return ERR_OK; }

            virtual std::shared_ptr<meta_state_service::transaction_entries> new_transaction_entries(unsigned int capacity) override;

            virtual task_ptr submit_transaction(const std::shared_ptr<meta_state_service::transaction_entries>& t_entries,
                task_code cb_code,
                const err_callback& cb_create_tree,
                clientlet* tracker = nullptr
                ) override;

            virtual task_ptr create_node(
                const std::string& node,
                task_code cb_code,
                const err_callback& cb_create,
                const blob& value = blob(),
                clientlet* tracker = nullptr
                ) override;

            virtual task_ptr delete_node(
                const std::string& node,
                bool recursively_delete,
                task_code cb_code,
                const err_callback& cb_delete,
                clientlet* tracker = nullptr) override;

            virtual task_ptr node_exist(
                const std::string& node,
                task_code cb_code,
                const err_callback& cb_exist,
                clientlet* tracker = nullptr) override;

            virtual task_ptr get_data(
                const std::string& node,
                task_code cb_code,
                const err_value_callback& cb_get_data,
                clientlet* tracker = nullptr) override;

            virtual task_ptr set_data(
                const std::string& node,
                const blob& value,
                task_code cb_code,
                const err_callback& cb_set_data,
                clientlet* tracker = nullptr) override;

            virtual task_ptr get_children(
                const std::string& node,
                task_code cb_code,
                const err_stringv_callback& cb_get_children,
                clientlet* tracker = nullptr) override;
            virtual ~meta_state_service_simple() override;

        private:
            struct operation
            {
                bool done;
                std::function<void(bool)> cb;
                operation(bool done, std::function<void(bool)>&&cb) : done(done), cb(move(cb)) {}
            };

            struct log_header
            {
                int magic;
                size_t size;
                static const int default_magic = 0xdeadbeef;
                log_header() : magic(default_magic), size(0)
                {}
            };

            struct state_node
            {
                std::string name;
                blob   data;

                state_node* parent;
                std::unordered_map<std::string, state_node*> children;

                state_node(const std::string& nm, state_node* pt, const blob& dt = {})
                    : name(nm), data(dt), parent(pt)
                {}
            };

            enum class operation_type
            {
                create_node,
                delete_node,
                set_data,
            };
            
            struct operation_entry {
                operation_type _type;
                std::string _node;
                blob _value; // for only creaet/set
                error_code _result;
            };

            struct simple_transaction_entries: public meta_state_service::transaction_entries
            {
                std::vector<operation_entry> _ops;
                unsigned int _offset;

                simple_transaction_entries(unsigned int capacity): _ops(capacity), _offset(0){}
                virtual ~simple_transaction_entries() {}

                virtual error_code create_node(const std::string &node, const blob &value) override
                {
                    return append(operation_type::create_node, node, value);
                }

                virtual error_code delete_node(const std::string &node) override
                {
                    return append(operation_type::delete_node, node, blob());
                }

                virtual error_code set_data(const std::string& node, const blob& value) override
                {
                    return append(operation_type::set_data, node, value);
                }

                error_code append(operation_type type, const std::string &node, const blob &value)
                {
                    if (_offset >= _ops.size())
                        return ERR_ARRAY_INDEX_OUT_OF_RANGE;
                    _ops[_offset]._type = type;
                    _ops[_offset]._node = node;
                    _ops[_offset]._value = value;
                    ++_offset;

                    return ERR_OK;
                }

                virtual error_code get_result(unsigned int entry_index) override
                {
                    if (entry_index >= _offset)
                        return ERR_ARRAY_INDEX_OUT_OF_RANGE;
                    return _ops[entry_index]._result;
                }
            };

            template<operation_type op, typename ... Args> struct log_struct;
            template<operation_type op, typename Head, typename ...Tail>
            struct log_struct<op, Head, Tail...> {
                static blob get_log(const Head& head, Tail... tail)
                {
                    binary_writer writer;
                    writer.write_pod(log_header());
                    writer.write(static_cast<int>(op));
                    write(writer, head, tail...);
                    auto shared_blob = writer.get_buffer();
                    reinterpret_cast<log_header*>((char*)shared_blob.data())->size = shared_blob.length() - sizeof(log_header);
                    return shared_blob;
                }
                static void write(binary_writer& writer, const Head& head, const Tail&... tail)
                {
                    marshall(writer, head, DSF_THRIFT_BINARY);
                    log_struct<op, Tail...>::write(writer, tail...);
                }
                static void parse(binary_reader& reader, Head&head, Tail&... tail)
                {
                    unmarshall(reader, head, DSF_THRIFT_BINARY);
                    log_struct<op, Tail...>::parse(reader, tail...);
                }
            };
            template<operation_type op, typename Head>
            struct log_struct<op, Head> {
                static void write(binary_writer& writer, const Head& head)
                {
                    marshall(writer, head, DSF_THRIFT_BINARY);
                }
                static void parse(binary_reader& reader, Head&head)
                {
                    unmarshall(reader, head, DSF_THRIFT_BINARY);
                }
            };

            using create_node_log = log_struct<operation_type::create_node, std::string, blob>;
            using delete_node_log = log_struct<operation_type::delete_node, std::string, bool>;
            using set_data_log = log_struct<operation_type::set_data, std::string, blob>;

            static std::string normalize_path(const std::string& s);
            static error_code extract_name_parent_from_path(
                const std::string& s,
                /*out*/ std::string& name,
                /*out*/ std::string& parent
                );

            void write_log(blob&& log_blob, std::function<error_code(void)> internal_operation, task_ptr task);

            error_code create_node_internal(const std::string &node, const blob& blob);
            error_code delete_node_internal(const std::string &node, bool recursive);
            error_code set_data_internal(const std::string &node, const blob& blob);
            error_code apply_transaction(const std::shared_ptr<meta_state_service::transaction_entries>& t_entries);
            
            typedef std::unordered_map<std::string, state_node*> quick_map;

            zlock _queue_lock;
            std::queue<std::unique_ptr<operation>> _task_queue;

            zlock          _state_lock;
            state_node     _root;  // tree          
            quick_map      _quick_map; // <path, node*>

            zlock          _log_lock;
            dsn_handle_t   _log;
            uint64_t       _offset;
        };
    }
}

