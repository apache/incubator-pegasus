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

#ifndef FTW_CONTINUE
# define FTW_CONTINUE 0
#endif

#ifndef FTW_STOP
# define FTW_STOP 1
#endif

#endif


namespace dsn {
	namespace utils {

# define _FS_COLON		':'
# define _FS_PERIOD		'.'
# define _FS_SLASH		'/'
# define _FS_BSLASH		'\\'
# define _FS_STAR		'*'
# define _FS_QUESTION	'?'
# define _FS_NULL		'\0'
# define _FS_ISSEP(x)	((x) == _FS_SLASH || (x) == _FS_BSLASH)

		bool get_normalized_path(const std::string& path, std::string& npath)
		{
			char sep;
			size_t i;
			size_t pos;
			size_t len;
			char* buf;
			char c;

			if (path.empty())
			{
				npath = "";
				return true;
			}

			len = path.length();
			buf = new char[len + 1];
			if (buf == nullptr)
			{
				return false;
			}

#ifdef _WIN32
			sep = _FS_BSLASH;
#else
			sep = _FS_SLASH;
#endif
			i = 0;
			pos = 0;
			while (i < len)
			{
				c = path[i++];
				if (
#ifdef _WIN32
					_FS_ISSEP(c)
#else
					c == _FS_SLASH
#endif
					)
				{
#ifdef _WIN32
					c = sep;
					if (i > 1)
#endif
						while ((i < len) && _FS_ISSEP(path[i]))
						{
							i++;
						}
				}

				buf[pos++] = c;
			}

			buf[pos] = _FS_NULL;
			if ((c == sep) && (pos > 1))
			{
#ifdef _WIN32
				c = buf[pos - 2];
				if ((c != _FS_COLON) && (c != _FS_QUESTION) && (c != _FS_BSLASH))
#endif
					buf[pos - 1] = _FS_NULL;
			}

			npath = buf;
			delete[] buf;

			return true;
		}

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
#ifdef __linux__
				if ((typeflag == FTW_D) || (typeflag == FTW_DP))
				{
					return FTW_SKIP_SUBTREE;
				}
				else
				{
					return FTW_SKIP_SIBLINGS;
				}
#else
				return 0;
#endif
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
			char c;
			std::string dir;
			std::string path;
			int fn_ret;

			path.reserve(MAX_PATH);
			if (!dsn::utils::get_normalized_path(dirpath, path) || path.empty())
			{
				return false;
			}

			dir.reserve(MAX_PATH);
			queue.push_back(path);
			while (!queue.empty())
			{
				dir = queue.front();
				queue.pop_front();

				c = dir[dir.length() - 1];
				path = dir;
				if ((c != _FS_BSLASH) && (c != _FS_COLON))
				{
					path.append(1, _FS_BSLASH);
				}
				path.append(1, _FS_STAR);

				hFind = ::FindFirstFileA(path.c_str(), &ffd);
				if (INVALID_HANDLE_VALUE == hFind)
				{
					return false;
				}

				do
				{
					if ((ffd.cFileName[0] == _FS_NULL)
						|| (strcmp(ffd.cFileName, ".") == 0)
						|| (strcmp(ffd.cFileName, "..") == 0)
						)
					{
						continue;
					}

					path = dir;
					if ((c != _FS_BSLASH) && (c != _FS_COLON))
					{
						path.append(1, _FS_BSLASH);
					}
					path.append(ffd.cFileName);

					if (ffd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)
					{
						if (recursive)
						{
							queue.push_front(path);
						}
					}
					else
					{
						fn_ret = fn(path.c_str(), ctx, FTW_F);
						if (fn_ret != FTW_CONTINUE)
						{
							::FindClose(hFind);
							return false;
						}
					}
				} while (::FindNextFileA(hFind, &ffd) != 0);

				dwError = ::GetLastError();
				::FindClose(hFind);
				if (dwError != ERROR_NO_MORE_FILES)
				{
					return false;
				}

				queue2.push_front(dir);
			}

			for (auto& dir2 : queue2)
			{
				fn_ret = fn(dir2.c_str(), ctx, FTW_DP);
				if (fn_ret != FTW_CONTINUE)
				{
					return false;
				}
			}

			return true;
		#else
			sftw_ctx.ctx = ctx;
			sftw_ctx.fn = fn;
			sftw_ctx.recursive = recursive;
			int flags = 
#ifdef __linux__
				FTW_ACTIONRETVAL
#else
				0
#endif
				;
			if (recursive)
			{
				flags |= FTW_DEPTH;
			}
			int ret = ::nftw(dirpath, ftw_wrapper, 1, flags);

			return (ret == 0);
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

			if (::stat(path.c_str(), &sb) != 0)
			{
				return false;
			}

			return (isfile ? S_ISREG(sb.st_mode) : S_ISDIR(sb.st_mode));
#endif
		}

		bool exists(const std::string& path)
		{
			return (dsn::utils::directory_exists(path) || dsn::utils::file_exists(path));
		}

		bool directory_exists(const std::string& path)
		{
			return dsn::utils::exists_internal(path, false);
		}
		
		bool file_exists(const std::string& path)
		{
			return dsn::utils::exists_internal(path, true);
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
			if (!dsn::utils::directory_exists(path))
			{
				return false;
			}

			return dsn::utils::sftw(path.c_str(), &file_list, dsn::utils::get_files_fn, recursive);
		}

		static int remove_directory_fn(const char* fpath, void* ctx, int typeflag)
		{
			bool succ;

			dassert((typeflag == FTW_F) || (typeflag == FTW_DP), "Invalid typeflag = %d.", typeflag);

#ifdef _WIN32
			if (typeflag != FTW_F)
			{
				succ = (::RemoveDirectoryA(fpath) == TRUE);
			}
			else
			{
#endif
				succ = (std::remove(fpath) == 0);
#ifdef _WIN32
			}
#endif

			return (succ ? FTW_CONTINUE : FTW_STOP);
		}

		static bool remove_directory(const std::string& path)
		{
			if (!dsn::utils::directory_exists(path))
			{
				return false;
			}

			return dsn::utils::sftw(path.c_str(), NULL, dsn::utils::remove_directory_fn, true);
		}

		bool remove(const std::string& path)
		{
			if (!dsn::utils::exists(path))
			{
				return true;
			}

			if (dsn::utils::directory_exists(path))
			{
				return dsn::utils::remove_directory(path);
			}
			else
			{
				return (std::remove(path.c_str()) == 0);
			}
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
			
			if (dsn::utils::directory_exists(path))
			{
				return !dsn::utils::file_exists(path);
			}

			if (!dsn::utils::get_normalized_path(path, npath) || npath.empty())
			{
				return false;
			}
			
			len = npath.length();
#ifdef _WIN32
			sep = _FS_BSLASH;
			if (npath.compare(0, 4, "\\\\?\\") == 0)
			{
				prev = 4;
			}
			else if (npath.compare(0, 2, "\\\\") == 0)
			{
				prev = 2;
			}
			else if (npath.compare(1, 2, ":\\") == 0)
			{
				prev = 3;
			}
#else
			sep = _FS_SLASH;
			if (npath[0] == sep)
			{
				prev = 1;
			}
#endif

			while ((pos = npath.find_first_of(sep, prev)) != std::string::npos)
			{
				auto ppath = npath.substr(0, pos++);
				prev = pos;
				if (!dsn::utils::directory_exists(ppath))
				{
					if (::mkdir_(ppath.c_str()) != 0)
					{
						return false;
					}
				}
			}
			
			if (prev < len)
			{
				if (::mkdir_(npath.c_str()) != 0)
				{
					return false;
				}
			}

			return true;
		}


		bool create_file(const std::string& path)
		{
			size_t pos;
			char c;
			std::string npath;
			int fd;
			int mode;

			if (!dsn::utils::get_normalized_path(path, npath) || npath.empty())
			{
				return false;
			}

			c = npath[npath.length() - 1];
			if (_FS_ISSEP(c))
			{
				return false;
			}

			if (dsn::utils::directory_exists(npath))
			{
				return false;
			}

			pos = npath.find_last_of("/\\");
			if ((pos != std::string::npos) && (pos > 0))
			{
				auto dir = npath.substr(0, pos);
				if (!dsn::utils::create_directory(dir))
				{
					return false;
				}
			}

#ifdef _WIN32
			mode = _S_IREAD | _S_IWRITE;
			if (_sopen_s(&fd,
					npath.c_str(),
					_O_WRONLY | _O_CREAT | _O_TRUNC,
					_SH_DENYRW,
					mode) != 0)
#else
			mode = 0775;
			fd = creat(npath.c_str(), mode);
			if (fd == -1)
#endif
			{
				return false;
			}

			return (close_(fd) == 0);
		}
	}
}
