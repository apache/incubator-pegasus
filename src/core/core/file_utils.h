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
#pragma once

# include <string>
# include <vector>

namespace dsn {
	namespace utils {
		
		typedef int (*sftw_fn_t)(const char* fpath, void* ctx, int typeflag);
		
		bool sftw(
			const char* dirpath,
			void* ctx,
			sftw_fn_t fn,
			bool recursive = true
			);

		bool exists(const std::string& path);

		bool directory_exists(const std::string& path);

		bool file_exists(const std::string& path);

		bool get_files(const std::string& path, std::vector<std::string>& file_list, bool recursive);

		bool remove(const std::string& path);

		bool create_directory(const std::string& path);

		bool create_file(const std::string& path);
	}
}
