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

# include "file_utils.h"
# include <dsn/ports.h>

#ifdef _WIN32

# include <windows.h>
# include <deque>

enum
{
	FTW_F,        /* Regular file.  */
#define FTW_F    FTW_F
	FTW_D,        /* Directory.  */
#define FTW_D    FTW_D
	FTW_DNR,      /* Unreadable directory.  */
#define FTW_DNR  FTW_DNR
	FTW_NS,       /* Unstatable file.  */
#define FTW_NS   FTW_NS

	FTW_SL,       /* Symbolic link.  */
# define FTW_SL  FTW_SL
				  /* These flags are only passed from the `nftw' function.  */
	FTW_DP,       /* Directory, all subdirs have been visited. */
# define FTW_DP  FTW_DP
	FTW_SLN       /* Symbolic link naming non-existing file.  */
# define FTW_SLN FTW_SLN
};

enum
{
	FTW_CONTINUE = 0, /* Continue with next sibling or for FTW_D with the
					  first child.  */
# define FTW_CONTINUE   FTW_CONTINUE
	FTW_STOP = 1,     /* Return from `ftw' or `nftw' with FTW_STOP as return
					  value.  */
# define FTW_STOP   FTW_STOP
	FTW_SKIP_SUBTREE = 2, /* Only meaningful for FTW_D: Don't walk through the
						  subtree, instead just continue with its next
						  sibling. */
# define FTW_SKIP_SUBTREE FTW_SKIP_SUBTREE
	FTW_SKIP_SIBLINGS = 3,/* Continue with FTW_DP callback for current directory
						  (if FTW_DEPTH) and then its siblings.  */
# define FTW_SKIP_SIBLINGS FTW_SKIP_SIBLINGS
};

#else

# include <sys/stat.h>

#ifndef _XOPEN_SOURCE
# define _XOPEN_SOURCE 500
#endif
# include <ftw.h>

#endif


namespace dsn {
	namespace utils {

#ifndef _WIN32
		__thread struct
		{
			void*	ctx;
			sftw_fn	fn;
			bool	recursive;
		} sftw_ctx;

		static int ftw_wrapper(const char* fpath, const struct stat* sb, int typeflag, struct FTW* ftwbuf)
		{
			if (!sftw_ctx.recursive && (ftwbuf->level > 1))
			{
				return FTW_STOP;
			}

			return sftw_ctx.fn(fpath, sftw_ctx.ctx, typeflag);
		}
#endif
		bool sftw(
			const char* dirpath,
			void* ctx,
			sftw_fn fn,
			bool recursive
			)
		{
		#ifdef _WIN32
			WIN32_FIND_DATAA ffd;
			HANDLE hFind;
			DWORD dwError = ERROR_SUCCESS;
			std::deque<std::string> queue;
			std::deque<std::string> queue2;
			std::string name;
			std::string subpath;
			int fn_ret;

			queue.push_back(dirpath);
			while (!queue.empty())
			{
				auto path = queue.front();
				queue.pop_front();

				hFind = FindFirstFileA((path + "\\*").c_str(), &ffd);
				if (INVALID_HANDLE_VALUE == hFind)
				{
					return false;
				}

				do
				{
					name = ffd.cFileName;
					if ((name == ".") || (name == ".."))
					{
						continue;
					}

					subpath = path + "\\" + name;

					if (ffd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)
					{
						if (recursive)
						{
							queue.push_front(subpath);
						}
					}
					else
					{
						fn_ret = fn(subpath.c_str(), ctx, FTW_F);
						if (fn_ret == FTW_STOP)
						{
							FindClose(hFind);
							return false;
						}
					}
				} while (FindNextFileA(hFind, &ffd) != 0);

				dwError = GetLastError();
				FindClose(hFind);
				if (dwError != ERROR_NO_MORE_FILES)
				{
					return false;
				}

				queue2.push_front(path);
			}

			while (!queue2.empty())
			{
				auto path = queue2.front();
				queue2.pop_front();
				fn_ret = fn(path.c_str(), ctx, FTW_DP);
				if (fn_ret == FTW_STOP)
				{
					return false;
				}
			}

			return true;
		#else
			sftw_ctx.ctx = ctx;
			sftw_ctx.fn = fn;
			sftw_ctx.recursive = recursive;
			int flags = recursive ? FTW_DEPTH : 0;

			return (nftw(dirpath, ftw_wrapper, 1, flags) == 0);
		#endif
		}

		bool directory_exists(std::string& dir)
		{
#ifdef _WIN32
			DWORD attr = ::GetFileAttributesA(dir.c_str());

			return ((attr != INVALID_FILE_ATTRIBUTES) && ((attr & FILE_ATTRIBUTE_DIRECTORY) != 0));
#else
			struct stat sb;

			return ((stat(dir.c_str(), &sb) == 0) && S_ISDIR(sb.st_mode));
#endif
		}
		static int get_file_names_fn(const char* fpath, void* ctx, int typeflag)
		{
			std::vector<std::string>& file_list = *((std::vector<std::string>*)ctx);
			if (typeflag == FTW_F)
			{
				file_list.push_back(fpath);
			}

			return FTW_CONTINUE;
		}

		bool get_file_names(std::string& dir, std::vector<std::string>& file_list, bool recursive)
		{
			if (!directory_exists(dir))
			{
				return false;
			}

			return sftw(dir.c_str(), &file_list, get_file_names_fn, recursive);
		}

		static int delete_directory_fn(const char* fpath, void* ctx, int typeflag)
		{
			bool succ;

#ifdef _WIN32
			if (typeflag == FTW_F)
			{
				succ = (DeleteFileA(fpath) == TRUE);
			}
			else if (typeflag = FTW_DP)
			{
				succ = (RemoveDirectoryA(fpath) == TRUE);
			}

#else
			succ = (remove(fpath) == 0);
#endif
			
			return (succ ? FTW_CONTINUE : FTW_STOP);
		}

		bool delete_directory(std::string& dir)
		{
			if (!directory_exists(dir))
			{
				return false;
			}

			return sftw(dir.c_str(), NULL, delete_directory_fn, true);
		}
	}
}
