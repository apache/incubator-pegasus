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

#include "log_file.h"

#include <fcntl.h>

#include "utils/filesystem.h"
#include "utils/crc.h"
#include "utils/fmt_logging.h"

#include "log_file_stream.h"

namespace dsn {
namespace replication {

log_file::~log_file() { close(); }
/*static */ log_file_ptr log_file::open_read(const char *path, /*out*/ error_code &err)
{
    char splitters[] = {'\\', '/', 0};
    std::string name = utils::get_last_component(std::string(path), splitters);

    // log.index.start_offset
    if (name.length() < strlen("log.") || name.substr(0, strlen("log.")) != std::string("log.")) {
        err = ERR_INVALID_PARAMETERS;
        LOG_WARNING("invalid log path %s", path);
        return nullptr;
    }

    auto pos = name.find_first_of('.');
    CHECK(pos != std::string::npos, "invalid log_file, name = {}", name);
    auto pos2 = name.find_first_of('.', pos + 1);
    if (pos2 == std::string::npos) {
        err = ERR_INVALID_PARAMETERS;
        LOG_WARNING("invalid log path %s", path);
        return nullptr;
    }

    /* so the log file format is log.index_str.start_offset_str */
    std::string index_str = name.substr(pos + 1, pos2 - pos - 1);
    std::string start_offset_str = name.substr(pos2 + 1);
    if (index_str.empty() || start_offset_str.empty()) {
        err = ERR_INVALID_PARAMETERS;
        LOG_WARNING("invalid log path %s", path);
        return nullptr;
    }

    char *p = nullptr;
    int index = static_cast<int>(strtol(index_str.c_str(), &p, 10));
    if (*p != 0) {
        err = ERR_INVALID_PARAMETERS;
        LOG_WARNING("invalid log path %s", path);
        return nullptr;
    }
    int64_t start_offset = static_cast<int64_t>(strtoll(start_offset_str.c_str(), &p, 10));
    if (*p != 0) {
        err = ERR_INVALID_PARAMETERS;
        LOG_WARNING("invalid log path %s", path);
        return nullptr;
    }

    disk_file *hfile = file::open(path, O_RDONLY | O_BINARY, 0);
    if (!hfile) {
        err = ERR_FILE_OPERATION_FAILED;
        LOG_WARNING("open log file %s failed", path);
        return nullptr;
    }

    auto lf = new log_file(path, hfile, index, start_offset, true);
    lf->reset_stream();
    blob hdr_blob;
    err = lf->read_next_log_block(hdr_blob);
    if (err == ERR_INVALID_DATA || err == ERR_INCOMPLETE_DATA || err == ERR_HANDLE_EOF ||
        err == ERR_FILE_OPERATION_FAILED) {
        std::string removed = std::string(path) + ".removed";
        LOG_ERROR("read first log entry of file %s failed, err = %s. Rename the file to %s",
                  path,
                  err.to_string(),
                  removed.c_str());
        delete lf;
        lf = nullptr;

        // rename file on failure
        dsn::utils::filesystem::rename_path(path, removed);

        return nullptr;
    }

    binary_reader reader(std::move(hdr_blob));
    lf->read_file_header(reader);
    if (!lf->is_right_header()) {
        std::string removed = std::string(path) + ".removed";
        LOG_ERROR(
            "invalid log file header of file %s. Rename the file to %s", path, removed.c_str());
        delete lf;
        lf = nullptr;

        // rename file on failure
        dsn::utils::filesystem::rename_path(path, removed);

        err = ERR_INVALID_DATA;
        return nullptr;
    }

    err = ERR_OK;
    return lf;
}

/*static*/ log_file_ptr log_file::create_write(const char *dir, int index, int64_t start_offset)
{
    char path[512];
    sprintf(path, "%s/log.%d.%" PRId64, dir, index, start_offset);

    if (dsn::utils::filesystem::path_exists(std::string(path))) {
        LOG_WARNING("log file %s already exist", path);
        return nullptr;
    }

    disk_file *hfile = file::open(path, O_RDWR | O_CREAT | O_BINARY, 0666);
    if (!hfile) {
        LOG_WARNING("create log %s failed", path);
        return nullptr;
    }

    return new log_file(path, hfile, index, start_offset, false);
}

log_file::log_file(
    const char *path, disk_file *handle, int index, int64_t start_offset, bool is_read)
    : _is_read(is_read)
{
    _start_offset = start_offset;
    _end_offset = start_offset;
    _handle = handle;
    _path = path;
    _index = index;
    _crc32 = 0;
    _last_write_time = 0;
    memset(&_header, 0, sizeof(_header));

    if (is_read) {
        int64_t sz;
        CHECK(dsn::utils::filesystem::file_size(_path, sz), "fail to get file size of {}.", _path);
        _end_offset += sz;
    }
}

void log_file::close()
{
    zauto_lock lock(_write_lock);

    //_stream implicitly refer to _handle so it needs to be cleaned up first.
    // TODO: We need better abstraction to avoid those manual stuffs..
    _stream.reset(nullptr);
    if (_handle) {
        error_code err = file::close(_handle);
        CHECK_EQ_MSG(err, ERR_OK, "file::close failed");

        _handle = nullptr;
    }
}

void log_file::flush() const
{
    CHECK(!_is_read, "log file must be of write mode");
    zauto_lock lock(_write_lock);

    if (_handle) {
        error_code err = file::flush(_handle);
        CHECK_EQ_MSG(err, ERR_OK, "file::flush failed");
    }
}

error_code log_file::read_next_log_block(/*out*/ ::dsn::blob &bb)
{
    CHECK(_is_read, "log file must be of read mode");
    auto err = _stream->read_next(sizeof(log_block_header), bb);
    if (err != ERR_OK || bb.length() != sizeof(log_block_header)) {
        if (err == ERR_OK || err == ERR_HANDLE_EOF) {
            // if read_count is 0, then we meet the end of file
            err = (bb.length() == 0 ? ERR_HANDLE_EOF : ERR_INCOMPLETE_DATA);
        } else {
            LOG_ERROR("read data block header failed, size = %d vs %d, err = %s",
                      bb.length(),
                      (int)sizeof(log_block_header),
                      err.to_string());
        }

        return err;
    }
    log_block_header hdr = *reinterpret_cast<const log_block_header *>(bb.data());

    if (hdr.magic != 0xdeadbeef) {
        LOG_ERROR("invalid data header magic: 0x%x", hdr.magic);
        return ERR_INVALID_DATA;
    }

    err = _stream->read_next(hdr.length, bb);
    if (err != ERR_OK || hdr.length != bb.length()) {
        LOG_ERROR("read data block body failed, size = %d vs %d, err = %s",
                  bb.length(),
                  (int)hdr.length,
                  err.to_string());

        if (err == ERR_OK || err == ERR_HANDLE_EOF) {
            // because already read log_block_header above, so here must be imcomplete data
            err = ERR_INCOMPLETE_DATA;
        }

        return err;
    }

    auto crc = dsn::utils::crc32_calc(
        static_cast<const void *>(bb.data()), static_cast<size_t>(hdr.length), _crc32);
    if (crc != hdr.body_crc) {
        LOG_ERROR("crc checking failed");
        return ERR_INVALID_DATA;
    }
    _crc32 = crc;

    return ERR_OK;
}

aio_task_ptr log_file::commit_log_block(log_block &block,
                                        int64_t offset,
                                        dsn::task_code evt,
                                        dsn::task_tracker *tracker,
                                        aio_handler &&callback,
                                        int hash)
{
    log_appender pending(offset, block);
    return commit_log_blocks(pending, evt, tracker, std::move(callback), hash);
}
aio_task_ptr log_file::commit_log_blocks(log_appender &pending,
                                         dsn::task_code evt,
                                         dsn::task_tracker *tracker,
                                         aio_handler &&callback,
                                         int hash)
{
    CHECK(!_is_read, "log file must be of write mode");
    CHECK_GT(pending.size(), 0);

    zauto_lock lock(_write_lock);
    if (!_handle) {
        return nullptr;
    }

    auto size = (long long)pending.size();
    size_t vec_size = pending.blob_count();
    std::vector<dsn_file_buffer_t> buffer_vector(vec_size);
    int buffer_idx = 0;
    for (log_block &block : pending.all_blocks()) {
        int64_t local_offset = block.start_offset() - start_offset();
        auto hdr = reinterpret_cast<log_block_header *>(const_cast<char *>(block.front().data()));

        CHECK_EQ(hdr->magic, 0xdeadbeef);
        hdr->local_offset = local_offset;
        hdr->length = static_cast<int32_t>(block.size() - sizeof(log_block_header));
        hdr->body_crc = _crc32;

        for (int i = 0; i < block.data().size(); i++) {
            auto &blk = block.data()[i];
            buffer_vector[buffer_idx].buffer = static_cast<void *>(const_cast<char *>(blk.data()));
            buffer_vector[buffer_idx].size = blk.length();

            // skip block header
            if (i > 0) {
                hdr->body_crc = dsn::utils::crc32_calc(static_cast<const void *>(blk.data()),
                                                       static_cast<size_t>(blk.length()),
                                                       hdr->body_crc);
            }
            buffer_idx++;
        }
        _crc32 = hdr->body_crc;
    }

    aio_task_ptr tsk;
    int64_t local_offset = pending.start_offset() - start_offset();
    if (callback) {
        tsk = file::write_vector(_handle,
                                 buffer_vector.data(),
                                 vec_size,
                                 static_cast<uint64_t>(local_offset),
                                 evt,
                                 tracker,
                                 std::forward<aio_handler>(callback),
                                 hash);
    } else {
        tsk = file::write_vector(_handle,
                                 buffer_vector.data(),
                                 vec_size,
                                 static_cast<uint64_t>(local_offset),
                                 evt,
                                 tracker,
                                 nullptr,
                                 hash);
    }

    if (utils::FLAGS_enable_latency_tracer) {
        tsk->_tracer->set_parent_point_name("commit_pending_mutations");
        tsk->_tracer->set_description("log");
        for (const auto &mutation : pending.mutations()) {
            mutation->_tracer->add_sub_tracer(tsk->_tracer);
        }
    }

    _end_offset.fetch_add(size);
    return tsk;
}

void log_file::reset_stream(size_t offset /*default = 0*/)
{
    if (_stream == nullptr) {
        _stream.reset(new file_streamer(_handle, offset));
    } else {
        _stream->reset(offset);
    }
    if (offset == 0) {
        _crc32 = 0;
    }
}

decree log_file::previous_log_max_decree(const dsn::gpid &pid)
{
    auto it = _previous_log_max_decrees.find(pid);
    return it == _previous_log_max_decrees.end() ? 0 : it->second.max_decree;
}

int log_file::read_file_header(binary_reader &reader)
{
    /*
     * the log file header structure:
     *   log_file_header +
     *   count + count * (gpid + replica_log_info)
     */
    reader.read_pod(_header);

    int count = 0;
    reader.read(count);
    for (int i = 0; i < count; i++) {
        gpid gpid;
        replica_log_info info;

        reader.read_pod(gpid);
        reader.read_pod(info);

        _previous_log_max_decrees[gpid] = info;
    }

    return get_file_header_size();
}

int log_file::get_file_header_size() const
{
    int count = static_cast<int>(_previous_log_max_decrees.size());
    return static_cast<int>(sizeof(log_file_header) + sizeof(count) +
                            (sizeof(gpid) + sizeof(replica_log_info)) * count);
}

bool log_file::is_right_header() const
{
    return _header.magic == 0xdeadbeef && _header.start_global_offset == _start_offset;
}

int log_file::write_file_header(binary_writer &writer, const replica_log_info_map &init_max_decrees)
{
    /*
     * the log file header structure:
     *   log_file_header +
     *   count + count * (gpid + replica_log_info)
     */
    _previous_log_max_decrees = init_max_decrees;

    _header.magic = 0xdeadbeef;
    _header.version = 0x1;
    _header.start_global_offset = start_offset();

    writer.write_pod(_header);

    int count = static_cast<int>(_previous_log_max_decrees.size());
    writer.write(count);
    for (auto &kv : _previous_log_max_decrees) {
        writer.write_pod(kv.first);
        writer.write_pod(kv.second);
    }

    return get_file_header_size();
}

} // namespace replication
} // namespace dsn
