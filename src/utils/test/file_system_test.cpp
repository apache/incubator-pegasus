// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <dsn/utility/filesystem.h>
#include <gtest/gtest.h>

namespace dsn {
namespace utils {
namespace filesystem {

TEST(verify_file, verify_file_test)
{
    const std::string &fname = "test_file";
    std::string expected_md5;
    int64_t expected_fsize;
    create_file(fname);
    md5sum(fname, expected_md5);
    file_size(fname, expected_fsize);

    ASSERT_TRUE(verify_file(fname, expected_md5, expected_fsize));
    ASSERT_FALSE(verify_file(fname, "wrong_md5", 10086));
    ASSERT_FALSE(verify_file("file_not_exists", "wrong_md5", 10086));

    remove_path(fname);
}

} // namespace filesystem
} // namespace utils
} // namespace dsn
