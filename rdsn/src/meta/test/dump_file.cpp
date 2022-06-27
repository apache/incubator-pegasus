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

#include <gtest/gtest.h>
#include "meta/dump_file.h"

TEST(dump_file, read_write)
{
    unsigned int total_length = 4096;
    std::shared_ptr<char> buffer(dsn::utils::make_shared_array<char>(total_length));
    char *ptr = buffer.get();
    for (int i = 0; i != total_length; ++i)
        ptr[i] = i % 256;

    std::vector<unsigned int> length_blocks;
    {
        std::shared_ptr<dump_file> f = dump_file::open_file("test_file", true);
        ASSERT_TRUE(f != nullptr);

        unsigned int current_length = 10;
        unsigned int step = 10;
        unsigned int copyed = 0;

        while (copyed < total_length) {
            if (copyed + current_length > total_length)
                current_length = total_length - copyed;

            int ans = f->append_buffer(ptr + copyed, current_length);
            ASSERT_TRUE(ans == 0);

            copyed += current_length;
            length_blocks.push_back(current_length);
            current_length += step;
        }
    }

    {
        std::shared_ptr<dump_file> f = dump_file::open_file("test_file", false);
        ASSERT_TRUE(f != nullptr);

        std::shared_ptr<char> out_buffer(dsn::utils::make_shared_array<char>(total_length));
        ptr = out_buffer.get();
        dsn::blob bb;
        int block_offset = 0;
        while (true) {
            int ans = f->read_next_buffer(bb);
            ASSERT_TRUE(ans != -1);
            if (ans == 0)
                break;

            ASSERT_TRUE(bb.length() == length_blocks[block_offset]);
            memcpy(ptr, bb.data(), bb.length());
            block_offset++;
            ptr += bb.length();
        }

        ASSERT_EQ(block_offset, length_blocks.size());
        ASSERT_EQ(memcmp(out_buffer.get(), buffer.get(), total_length), 0);
    }

    // corrupted end
    {
        FILE *fp = fopen("test_file", "rb+");
        fseek(fp, -4, SEEK_END);
        uint32_t num = 0;
        fwrite(&num, sizeof(num), 1, fp);
        fclose(fp);

        std::shared_ptr<dump_file> f = dump_file::open_file("test_file", false);
        dsn::blob bb;
        int block_offset = 0;
        while (true) {
            int ans = f->read_next_buffer(bb);
            if (ans == 0)
                break;

            if (block_offset < length_blocks.size() - 1)
                ASSERT_EQ(ans, 1);
            else
                ASSERT_EQ(ans, -1);
            block_offset++;
        }
    }

    // data loss in the end
    {
        FILE *fp = fopen("test_file", "rb");
        FILE *fp2 = fopen("test_file2", "wb");

        fseek(fp, 0, SEEK_END);
        auto size = ftell(fp);
        fseek(fp, 0, SEEK_SET);
        std::unique_ptr<char[]> buf(new char[size - 4]);
        size_t cnt = fread(buf.get(), 1, size - 4, fp);
        ASSERT_EQ(cnt, size - 4);
        cnt = fwrite(buf.get(), 1, cnt, fp2);
        ASSERT_EQ(cnt, size - 4);

        fclose(fp);
        fclose(fp2);

        std::shared_ptr<dump_file> f = dump_file::open_file("test_file2", false);
        dsn::blob bb;
        int block_offset = 0;
        while (true) {
            int ans = f->read_next_buffer(bb);
            if (ans == 0)
                break;
            if (block_offset < length_blocks.size() - 1)
                ASSERT_EQ(ans, 1);
            else
                ASSERT_EQ(ans, -1);
            block_offset++;
        }
    }
}
