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
# include <dsn/service_api_c.h>

#ifdef _WIN32

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

#ifndef _XOPEN_SOURCE
# define _XOPEN_SOURCE 500
#endif
# include <ftw.h>

#endif


namespace dsn {
	namespace utils {

		static char UnixPathSeparator = '/';
		static char WinPathSeparator = '\\';

#ifndef _WIN32
		static __thread struct
		{
			void*		ctx;
			sftw_fn_t	fn;
			bool		recursive;
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
			sftw_fn_t fn,
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

		static bool exists_internal(const std::string& path, bool isfile)
		{
#ifdef _WIN32
			DWORD attr = ::GetFileAttributesA(path.c_str());
			DWORD test;

			if (attr == INVALID_FILE_ATTRIBUTES)
			{
				return false;
			}

			test = (attr & FILE_ATTRIBUTE_DIRECTORY);

			return (isfile ? (test == 0) : (test != 0));
#else
			struct stat sb;

			if (stat(path.c_str(), &sb) != 0)
			{
				return false;
			}

			return (isfile ? S_ISREG(sb.st_mode) : S_ISDIR(sb.st_mode));
#endif
		}

		bool exists(const std::string& path)
		{
			return (directory_exists(path) || file_exists(path));
		}

		bool directory_exists(const std::string& path)
		{
			return exists_internal(path, false);
		}
		
		bool file_exists(const std::string& path)
		{
			return exists_internal(path, true);
		}

		static int get_files_fn(const char* fpath, void* ctx, int typeflag)
		{
			std::vector<std::string>& file_list = *((std::vector<std::string>*)ctx);
			if (typeflag == FTW_F)
			{
				file_list.push_back(fpath);
			}

			return FTW_CONTINUE;
		}

		bool get_files(const std::string& path, std::vector<std::string>& file_list, bool recursive)
		{
			if (!directory_exists(path))
			{
				return false;
			}

			return sftw(path.c_str(), &file_list, get_files_fn, recursive);
		}

		static int remove_directory_fn(const char* fpath, void* ctx, int typeflag)
		{
			bool succ;

			dassert((typeflag == FTW_F) || (typeflag == FTW_DP), "Invalid typeflag = %d.", typeflag);
#ifdef _WIN32
			if (typeflag == FTW_F)
			{
				succ = (DeleteFileA(fpath) == TRUE);
			}
			else 
			{
				succ = (RemoveDirectoryA(fpath) == TRUE);
			}

#else
			succ = (std::remove(fpath) == 0);
#endif
			
			return (succ ? FTW_CONTINUE : FTW_STOP);
		}

		static bool remove_directory(const std::string& path)
		{
			if (!directory_exists(path))
			{
				return false;
			}

			return sftw(path.c_str(), NULL, remove_directory_fn, true);
		}

		bool remove(const std::string& path)
		{
			if (directory_exists(path))
			{
				return remove_directory(path);
			}
			else
			{
				return (std::remove(path.c_str()) == 0);
			}
		}

		static std::string get_normalized_path(const std::string& path)
		{
			char sep;
			size_t start;
			size_t end;
			std::string ret;

			start = 0;
			end = path.length() - 1;
#ifdef _WIN32
			sep = WinPathSeparator;
#else
			sep = UnixPathSeparator;
			while ((path[start] == sep) && (start < end))
			{
				start++;				
			}
			if (start > 0)
			{
				start--;
			}
#endif

			while (
#ifdef _WIN32
				(
#endif
				(path[end] == sep)
#ifdef _WIN32
					|| (path[end] == UnixPathSeparator))
#endif
					&& (start < end))
			{
				end--;
			}
#ifdef _WIN32
			if (path[end] == ':')
			{
				end++;
			}
#endif

			ret = path.substr(start, (end - start + 1));
#ifdef _WIN32
			for (char& c : ret)
			{
				if (c == UnixPathSeparator)
				{
					c = WinPathSeparator;
				}
			}
#endif

			return ret;
		}

		bool create_directory(const std::string& path)
		{
			size_t prev = 0;
			size_t pos;
			char sep;
			std::string npath;
			size_t len;

			if (path.empty())
			{
				return false;
			}
			
			if (directory_exists(path))
			{
				return !file_exists(path);
			}

			npath = get_normalized_path(path);
			len = npath.length();
#ifdef _WIN32
			sep = WinPathSeparator;
			if (npath.compare(0, 4, "\\\\?\\") == 0)
			{
				prev = 4;
			}
			else if (npath.compare(0, 2 "\\\\") == 0)
			{
				prev = 2
			}
#else
			sep = UnixPathSeparator;
#endif

			while ((pos = npath.find_first_of(sep, prev)) != std::string::npos)
			{
				auto ppath = npath.substr(0,pos++);
				prev = pos;
				if (!directory_exists(ppath))
				{
					if (mkdir_(ppath.c_str()) != 0)
					{
						return false;
					}
				}
			}
			
			if (prev < len)
			{
				if (mkdir_(npath.c_str()) != 0)
				{
					return false;
				}
			}

			return true;
		}


		bool create_file(const std::string& path)
		{
			size_t len;
			size_t pos;
			char endc;
			std::string dir;

			if (path.empty())
			{
				return false;
			}

			len = path.length();
			endc = path[len - 1];
			if ((endc == UnixPathSeparator) || (endc == WinPathSeparator))
			{
				return false;
			}

			if (dsn::utils::directory_exists(path))
			{
				return false;
			}

			pos = path.find_last_of("/\\");
			if (pos == std::string::npos)
			{
				return false;
			}
			dir = path.substr(0, pos);

			if (!create_directory(dir))
			{
				return false;
			}

			int fd;
			int mode;
#ifdef _WIN32
			mode = _S_IREAD | _S_IWRITE;
			if (_sopen_s(&fd,
					path.c_str(),
					_O_WRONLY | _O_CREAT | _O_TRUNC,
					_SH_DENYRW,
					mode) != 0)
			{
				return false;
			}
#else
			mode = 0775;
			fd = creat(path.c_str(), mode);
			if (fd == -1)
			{
				return false;
			}
#endif

			return (close_(fd) == 0);
		}
	}
}
