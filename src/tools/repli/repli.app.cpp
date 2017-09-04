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
 *     Repli util app.
 *
 * Revision history:
 *     Nov., 2015, @qinzuoyan (Zuoyan Qin), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include <dsn/utility/utils.h>
#include <dsn/utility/configuration.h>

#include "dist/replication/lib/mutation_log.h"

#include "repli.app.h"

#include <iostream>

namespace dsn {
namespace service {

using namespace ::dsn::replication;

repli_app::repli_app(dsn_gpid gpid) : service_app(gpid) {}

void repli_app::usage()
{
    std::cout << "------------ commands -----------" << std::endl;
    std::cout << "help" << std::endl;
    std::cout << "log_file <file_name>" << std::endl;
    std::cout << "config_get <config_file> <section> <key>" << std::endl;
    std::cout << "---------------------------------" << std::endl;
}

error_code repli_app::start(int argc, char **argv)
{
    if (s_args.size() == 0) {
        g_done = true;
        return ERR_OK;
    }

    std::string cmd = s_args[0];

    if (cmd == "help") {
        usage();
    } else if (cmd == "log_file") {
        if (s_args.size() < 2) {
            std::cerr << "ERROR: lack of param <file_name>" << std::endl;
            usage();
            g_done = true;
            return ERR_OK;
        }
        std::string file_name = s_args[1];

        dsn::error_code err;
        log_file_ptr lf = log_file::open_read(file_name.c_str(), err);
        if (lf == nullptr) {
            std::cerr << "ERROR: open file '" << file_name << "' failed: " << err.to_string()
                      << std::endl;
            g_done = true;
            return ERR_OK;
        }

        int64_t sz;
        dsn::utils::filesystem::file_size(file_name, sz);
        std::cout << "file_size=" << sz << std::endl;
        std::cout << "file_index=" << lf->index() << std::endl;
        std::cout << "start_offset=" << lf->start_offset() << std::endl;
        std::cout << "end_offset=" << lf->end_offset() << std::endl;
        std::cout << "previous_log_max_decrees={" << std::endl;
        std::cout << "  /* app_id.partition_index : previous_max_decree : valid_start_offset */"
                  << std::endl;
        const replica_log_info_map &previous = lf->previous_log_max_decrees();
        for (auto &kv : previous) {
            std::cout << "  " << kv.first.get_app_id() << "." << kv.first.get_partition_index()
                      << " : " << kv.second.max_decree << " : " << kv.second.valid_start_offset
                      << std::endl;
        }
        std::cout << "}" << std::endl;
        std::cout << "mutations={" << std::endl;
        std::cout
            << "  /* app_id.partition_index.ballot.decree : last_committed_decree : log_offset */"
            << std::endl;
        lf->close();
        lf = nullptr;
        int64_t offset = 0;
        std::vector<std::string> files;
        files.push_back(file_name);
        err = mutation_log::replay(files,
                                   [](int log_length, mutation_ptr mu) -> bool {
                                       std::cout << "  " << mu->name() << " : "
                                                 << mu->data.header.last_committed_decree << " : "
                                                 << mu->data.header.log_offset << std::endl;
                                       return true;
                                   },
                                   offset);
        std::cout << "}" << std::endl;
        std::cout << "read_return_err=" << dsn_error_to_string(err) << std::endl;
        std::cout << "read_end_offset=" << offset << std::endl;
    } else if (cmd == "config_get") {
        if (s_args.size() < 4) {
            std::cerr << "ERROR: lack of param" << std::endl;
            usage();
            g_done = true;
            return ERR_OK;
        }

        ::dsn::configuration_ptr config(new ::dsn::configuration());
        if (!config->load(s_args[1].c_str(), nullptr)) {
            std::cerr << "ERROR: failed to load config file \"" << s_args[1] << "\"" << std::endl;
            g_done = true;
            return ERR_OK;
        }
        config->set_warning(true);
        const char *result = config->get_string_value(s_args[2].c_str(), s_args[3].c_str(), "", "");
        std::cout << result << std::endl;
    } else {
        std::cerr << "ERROR: invalid command: " << cmd << std::endl;
        usage();
    }

    g_done = true;
    return ERR_OK;
}

error_code repli_app::stop(bool cleanup) { return ::dsn::ERR_OK; }

std::vector<std::string> repli_app::s_args;

void repli_app::set_args(int argc, char **argv)
{
    for (int i = 0; i < argc; i++) {
        s_args.push_back(argv[i]);
    }
}
}
}
