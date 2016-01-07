#include <gtest/gtest.h>
#include <dump_file.h>

TEST(dump_file, read_write)
{
    unsigned int total_length = 4096;
    std::shared_ptr<char> buffer(new char[total_length], [](char* output){ delete []output; });
    char* ptr = buffer.get();
    for (int i=0; i!=total_length; ++i)
        ptr[i] = i%256;

    std::vector<unsigned int> length_blocks;
    {
        std::shared_ptr<dump_file> f = dump_file::open_file("test_file", true);
        ASSERT_TRUE(f != nullptr);

        unsigned int current_length = 10;
        unsigned int step = 10;
        unsigned int copyed = 0;

        while (copyed < total_length)
        {
            if (copyed+current_length > total_length)
                current_length = total_length-copyed;

            int ans = f->append_buffer(ptr+copyed, current_length);
            ASSERT_TRUE(ans == 0);

            copyed += current_length;
            length_blocks.push_back(current_length);
            current_length += step;
        }
    }

    {
        std::shared_ptr<dump_file> f = dump_file::open_file("test_file", false);
        ASSERT_TRUE(f != nullptr);

        std::shared_ptr<char> out_buffer(new char[total_length], [](char* output){ delete []output; });
        ptr = out_buffer.get();
        dsn::blob bb;
        int block_offset = 0;
        while ( true )
        {
            dinfo("block offset: %d", block_offset);
            int ans = f->read_next_buffer(bb);
            ASSERT_TRUE(ans != -1);
            if ( ans == 0)
                break;

            ASSERT_TRUE(bb.length() == length_blocks[block_offset]);
            memcpy(ptr, bb.data(), bb.length());
            block_offset++;
            ptr += bb.length();
        }

        ASSERT_EQ(block_offset, length_blocks.size());
        ASSERT_EQ(memcmp(out_buffer.get(), buffer.get(), total_length), 0);
    }
}
