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

# include <dsn/cpp/utils.h>

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

#ifndef __S_ISTYPE
# define __S_ISTYPE(mode, mask)  (((mode) & S_IFMT) == (mask))
#endif

#ifndef S_ISREG
# define S_ISREG(mode)    __S_ISTYPE((mode), S_IFREG)
#endif

#ifndef S_ISDIR
# define S_ISDIR(mode)    __S_ISTYPE((mode), S_IFDIR)
#endif

#ifndef PATH_MAX
# define PATH_MAX MAX_PATH
#endif

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
		namespace filesystem {

# define _FS_COLON		':'
# define _FS_PERIOD		'.'
# define _FS_SLASH		'/'
# define _FS_BSLASH		'\\'
# define _FS_STAR		'*'
# define _FS_QUESTION	'?'
# define _FS_NULL		'\0'
# define _FS_ISSEP(x)	((x) == _FS_SLASH || (x) == _FS_BSLASH)

			static __thread char path_buffer[PATH_MAX];

			bool get_normalized_path(const std::string& path, std::string& npath)
			{
				char sep;
				size_t i;
				size_t pos;
				size_t len;
				char c;

				if (path.empty())
				{
					npath = "";
					return true;
				}

				len = path.length();

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

					path_buffer[pos++] = c;
				}

				path_buffer[pos] = _FS_NULL;
				if ((c == sep) && (pos > 1))
				{
#ifdef _WIN32
					c = path_buffer[pos - 2];
					if ((c != _FS_COLON) && (c != _FS_QUESTION) && (c != _FS_BSLASH))
#endif
						path_buffer[pos - 1] = _FS_NULL;
				}

				npath = path_buffer;

				return true;
			}

#ifndef _WIN32
			static __thread struct
			{
				ftw_handler*	handler;
				bool			recursive;
			} ftw_ctx;

			static int ftw_wrapper(const char* fpath, const struct stat* sb, int typeflag, struct FTW* ftwbuf)
			{
				if (!ftw_ctx.recursive && (ftwbuf->level > 1))
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

				return (*ftw_ctx.handler)(fpath, typeflag);
			}
#endif
			bool file_tree_walk(
				const char* dirpath,
				ftw_handler handler,
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
				int ret;

				path.reserve(MAX_PATH);
				if (!dsn::utils::filesystem::get_normalized_path(dirpath, path) || path.empty())
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
							ret = handler(path.c_str(), FTW_F);
							if (ret != FTW_CONTINUE)
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
					ret = handler(dir2.c_str(), FTW_DP);
					if (ret != FTW_CONTINUE)
					{
						return false;
					}
				}

				return true;
#else
				ftw_ctx.handler = &handler;
				ftw_ctx.recursive = recursive;
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

			static bool exists_internal(const std::string& path, int type)
			{
				bool ret;
#ifdef _WIN32
				struct _stat64 st;
#else
				struct stat64 st;
#endif

				if (stat64_(path.c_str(), &st) != 0)
				{
					return false;
				}

				switch (type)
				{
				case 0:
					ret = S_ISREG(st.st_mode);
					break;
				case 1:
					ret = S_ISDIR(st.st_mode);
					break;
				case 2:
					ret = S_ISREG(st.st_mode) || S_ISDIR(st.st_mode);
					break;
				default:
					ret = false;
					break;
				}

				return ret;
			}

			bool exists(const std::string& path)
			{
				return dsn::utils::filesystem::exists_internal(path, 2);
			}

			bool directory_exists(const std::string& path)
			{
				return dsn::utils::filesystem::exists_internal(path, 1);
			}

			bool file_exists(const std::string& path)
			{
				return dsn::utils::filesystem::exists_internal(path, 0);
			}

			bool get_files(const std::string& path, std::vector<std::string>& file_list, bool recursive)
			{
				if (!dsn::utils::filesystem::directory_exists(path))
				{
					return false;
				}

				return dsn::utils::filesystem::file_tree_walk(path.c_str(),
					[&file_list](const char* fpath, int typeflag)
				{
					if (typeflag == FTW_F)
					{
						file_list.push_back(fpath);
					}

					return FTW_CONTINUE;
				},
					recursive
					);
			}

			static bool remove_directory(const std::string& path)
			{
				if (!dsn::utils::filesystem::directory_exists(path))
				{
					return false;
				}

				return dsn::utils::filesystem::file_tree_walk(path.c_str(),
					[](const char* fpath, int typeflag)
				{
					bool succ;

					dassert((typeflag == FTW_F) || (typeflag == FTW_DP),
						"Invalid typeflag = %d.", typeflag);
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
				},
					true
					);
			}

			bool remove(const std::string& path)
			{
				if (dsn::utils::filesystem::file_exists(path))
				{
					return (std::remove(path.c_str()) == 0);
				}
				else if(dsn::utils::filesystem::directory_exists(path))
				{
					return dsn::utils::filesystem::remove_directory(path);
				}
				else
				{
					return true;
				}
			}

			bool rename(const std::string& path1, const std::string& path2)
			{
				return (::rename(path1.c_str(), path2.c_str()) == 0);
			}

			bool file_size(const std::string& path, int64_t& sz)
			{
				struct stat64_ st;

				if (::stat64_(path.c_str(), &st) != 0)
				{
					return false;
				}

				if (!S_ISREG(st.st_mode))
				{
					return false;
				}

				sz = st.st_size;

				return true;
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

				if (dsn::utils::filesystem::directory_exists(path))
				{
					return true;
				}

				if (dsn::utils::filesystem::file_exists(path))
				{
					return false;
				}

				if (!dsn::utils::filesystem::get_normalized_path(path, npath) || npath.empty())
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
					if (!dsn::utils::filesystem::directory_exists(ppath))
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

				if (!dsn::utils::filesystem::get_normalized_path(path, npath) || npath.empty())
				{
					return false;
				}

				c = npath[npath.length() - 1];
				if (_FS_ISSEP(c))
				{
					return false;
				}

				if (dsn::utils::filesystem::directory_exists(npath))
				{
					return false;
				}

				pos = npath.find_last_of("/\\");
				if ((pos != std::string::npos) && (pos > 0))
				{
					auto dir = npath.substr(0, pos);
					if (!dsn::utils::filesystem::create_directory(dir))
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

			bool get_absolute_path(const std::string& path1, std::string& path2)
			{
				bool succ;
# if defined(_WIN32)
				char* component;
				succ = (0 != ::GetFullPathNameA(path1.c_str(), PATH_MAX, path_buffer, &component));
# else
				succ = (realpath(path1.c_str(), path_buffer) != nullptr);
# endif
				if (succ)
				{
					path2 = path_buffer;
				}
				
				return succ;
			}

			std::string remove_file_name(const std::string& path)
			{
				return path.substr(0, path.find_last_of("\\/"));
			}

		}
	}
}
