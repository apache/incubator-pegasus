#ifndef FDS_SERVICE_H
#define FDS_SERVICE_H

#include <dsn/dist/block_service.h>

namespace galaxy {
namespace fds {
class GalaxyFDSClient;
}
}

namespace dsn {
namespace dist {
namespace block_service {

class fds_service : public block_filesystem
{
public:
    static const std::string FILE_MD5_KEY;
    static const std::string FILE_LENGTH_KEY;
    static const std::string FILE_LENGTH_CUSTOM_KEY;

public:
    fds_service();
    galaxy::fds::GalaxyFDSClient *get_client() { return _client.get(); }
    const std::string &get_bucket_name() { return _bucket_name; }

    virtual ~fds_service() override;
    virtual error_code initialize(const std::vector<std::string> &args) override;
    virtual dsn::task_ptr list_dir(const ls_request &req,
                                   dsn::task_code code,
                                   const ls_callback &callback,
                                   dsn::task_tracker *tracker) override;

    virtual dsn::task_ptr create_file(const create_file_request &req,
                                      dsn::task_code code,
                                      const create_file_callback &cb,
                                      dsn::task_tracker *tracker) override;
    //
    // Attention:
    //  delete file directly on fds, will not enter trash
    //
    virtual dsn::task_ptr delete_file(const delete_file_request &req,
                                      dsn::task_code code,
                                      const delete_file_callback &cb,
                                      dsn::task_tracker *tracker) override;

    virtual dsn::task_ptr exist(const exist_request &req,
                                dsn::task_code code,
                                const exist_callback &cb,
                                dsn::task_tracker *tracker) override;

    //
    // Attention：
    //   -- remove the path directly on fds, will not enter trash
    //   -- when req.path is a directory, this operation may consume much time if there are many
    //      files under this directory
    //
    virtual dsn::task_ptr remove_path(const remove_path_request &req,
                                      dsn::task_code code,
                                      const remove_path_callback &cb,
                                      dsn::task_tracker *tracker) override;

private:
    std::shared_ptr<galaxy::fds::GalaxyFDSClient> _client;
    std::string _bucket_name;
};

class fds_file_object : public block_file
{
public:
    fds_file_object(fds_service *s, const std::string &name, const std::string &fds_path);
    fds_file_object(fds_service *s,
                    const std::string &name,
                    const std::string &fds_path,
                    const std::string &md5,
                    uint64_t size);

    virtual ~fds_file_object();
    virtual uint64_t get_size() override { return _size; }
    virtual const std::string &get_md5sum() override { return _md5sum; }

    virtual dsn::task_ptr write(const write_request &req,
                                dsn::task_code code,
                                const write_callback &cb,
                                dsn::task_tracker *tracker) override;

    virtual dsn::task_ptr read(const read_request &req,
                               dsn::task_code code,
                               const read_callback &cb,
                               dsn::task_tracker *tracker) override;

    virtual dsn::task_ptr upload(const upload_request &req,
                                 dsn::task_code code,
                                 const upload_callback &cb,
                                 dsn::task_tracker *tracker) override;

    virtual dsn::task_ptr download(const download_request &req,
                                   dsn::task_code code,
                                   const download_callback &cb,
                                   dsn::task_tracker *tracker) override;

private:
    dsn::error_code get_content(uint64_t pos,
                                int64_t length,
                                /*out*/ std::ostream &os,
                                /*out*/ uint64_t &transfered_bytes);
    dsn::error_code put_content(/*in-out*/ std::istream &is, /*out*/ uint64_t &transfered_bytes);
    fds_service *_service;
    std::string _fds_path;
    std::string _md5sum;
    uint64_t _size;
    bool _has_meta_synced;

    static const size_t PIECE_SIZE = 16384; // 16k
};
}
}
}
#endif // FDS_SERVICE_H
