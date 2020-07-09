#pragma once

#include <fstream>

#include <dsn/dist/block_service.h>

namespace dsn {
namespace dist {
namespace block_service {

class local_service : public block_filesystem
{
public:
    local_service();
    local_service(const std::string &root);
    virtual error_code initialize(const std::vector<std::string> &args) override;
    virtual dsn::task_ptr list_dir(const ls_request &req,
                                   dsn::task_code code,
                                   const ls_callback &callback,
                                   dsn::task_tracker *tracker = nullptr) override;

    virtual dsn::task_ptr create_file(const create_file_request &req,
                                      dsn::task_code code,
                                      const create_file_callback &cb,
                                      dsn::task_tracker *tracker = nullptr) override;

    virtual dsn::task_ptr delete_file(const delete_file_request &req,
                                      dsn::task_code code,
                                      const delete_file_callback &cb,
                                      dsn::task_tracker *tracker = nullptr) override;

    virtual dsn::task_ptr exist(const exist_request &req,
                                dsn::task_code code,
                                const exist_callback &cb,
                                dsn::task_tracker *tracker = nullptr) override;

    virtual dsn::task_ptr remove_path(const remove_path_request &req,
                                      dsn::task_code code,
                                      const remove_path_callback &cb,
                                      dsn::task_tracker *tracker = nullptr) override;

    virtual ~local_service();

    static std::string get_metafile(const std::string &filepath);

private:
    std::string _root;
};

class local_file_object : public block_file
{
public:
    local_file_object(const std::string &name);

    virtual ~local_file_object();

    virtual uint64_t get_size() override;
    virtual const std::string &get_md5sum() override;

    virtual dsn::task_ptr write(const write_request &req,
                                dsn::task_code code,
                                const write_callback &cb,
                                dsn::task_tracker *tracker = nullptr) override;

    virtual dsn::task_ptr read(const read_request &req,
                               dsn::task_code code,
                               const read_callback &cb,
                               dsn::task_tracker *tracker = nullptr) override;

    virtual dsn::task_ptr upload(const upload_request &req,
                                 dsn::task_code code,
                                 const upload_callback &cb,
                                 dsn::task_tracker *tracker = nullptr) override;

    virtual dsn::task_ptr download(const download_request &req,
                                   dsn::task_code code,
                                   const download_callback &cb,
                                   dsn::task_tracker *tracker = nullptr) override;

    error_code load_metadata();
    error_code store_metadata();

private:
    std::string compute_md5();

private:
    uint64_t _size;
    std::string _md5_value;
    bool _has_meta_synced;
};
}
}
}
