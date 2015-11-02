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

# include <direct.h>
# include <io.h>
# include <sys/types.h>
# include <sys/stat.h>
# include <deque>

# define getcwd_ _getcwd
# define rmdir_ _rmdir
# define mkdir_ _mkdir
# define close_ _close
# define stat_ _stat64

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

# include <sys/stat.h>

# define getcwd_ getcwd
# define rmdir_ rmdir
# define mkdir_(path) mkdir(path, 0775)
# define close_ close
# define stat_ stat

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

			static __thread char tls_path_buffer[PATH_MAX];

			// npath need to be a normalized path
			static inline bool get_stat_internal(const std::string& npath, struct stat_& st)
			{
				return (::stat_(npath.c_str(), &st) == 0);
			}

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

					tls_path_buffer[pos++] = c;
				}

				tls_path_buffer[pos] = _FS_NULL;
				if ((c == sep) && (pos > 1))
				{
#ifdef _WIN32
					c = tls_path_buffer[pos - 2];
					if ((c != _FS_COLON) && (c != _FS_QUESTION) && (c != _FS_BSLASH))
#endif
						tls_path_buffer[pos - 1] = _FS_NULL;
				}

				dassert(tls_path_buffer[0] != _FS_NULL, "Normalized path cannot be empty!");
				npath = tls_path_buffer;

				return true;
			}

#ifndef _WIN32
			static __thread struct
			{
				ftw_handler*	handler;
				bool			recursive;
			} tls_ftw_ctx;

			static int ftw_wrapper(const char* fpath, const struct stat* sb, int typeflag, struct FTW* ftwbuf)
			{
				if (!tls_ftw_ctx.recursive && (ftwbuf->level > 1))
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

				return (*tls_ftw_ctx.handler)(fpath, typeflag, ftwbuf);
			}
#endif

#ifdef _WIN32
			struct win_ftw_info
			{
				struct FTW ftw;
				std::string path;
			};
#endif

			bool file_tree_walk(
				const std::string& dirpath,
				ftw_handler handler,
				bool recursive
				)
			{
#ifdef _WIN32
				WIN32_FIND_DATAA ffd;
				HANDLE hFind;
				DWORD dwError = ERROR_SUCCESS;
				std::deque<win_ftw_info> queue;
				std::deque<win_ftw_info> queue2;
				char c;
				size_t pos;
				win_ftw_info info;
				win_ftw_info info2;
				int ret;

				info.path.reserve(PATH_MAX);
				if (!dsn::utils::filesystem::get_normalized_path(dirpath, info.path))
				{
					return false;
				}
				info2.path.reserve(PATH_MAX);
				pos = info.path.find_last_of("\\");
				info.ftw.base = (info.ftw.base == std::string::npos) ? 0 : (int)(pos + 1);
				info.ftw.level = 0;
				queue.push_back(info);

				while (!queue.empty())
				{
					info = queue.front();
					queue.pop_front();

					c = info.path[info.path.length() - 1];
					info2.path = info.path;
					pos = info2.path.length() + 1;
					info2.ftw.base = (int)pos;
					info2.ftw.level = info.ftw.level + 1;
					if ((c != _FS_BSLASH) && (c != _FS_COLON))
					{
						info2.path.append(1, _FS_BSLASH);
					}
					info2.path.append(1, _FS_STAR);

					hFind = ::FindFirstFileA(info2.path.c_str(), &ffd);
					if (INVALID_HANDLE_VALUE == hFind)
					{
						return false;
					}

					do
					{
						if ((ffd.cFileName[0] == _FS_NULL)
							|| (::strcmp(ffd.cFileName, ".") == 0)
							|| (::strcmp(ffd.cFileName, "..") == 0)
							)
						{
							continue;
						}

						info2.path.replace(pos, std::string::npos, ffd.cFileName);

						if (ffd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)
						{
							if (recursive)
							{
								queue.push_front(info2);
							}
                            else
                            {
                                queue2.push_front(info2);
                            }
						}
						else
						{
							ret = handler(info2.path.c_str(), FTW_F, &info2.ftw);
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

					queue2.push_front(info);
				}

				for (auto& info3 : queue2)
				{
					ret = handler(info3.path.c_str(), FTW_DP, &info3.ftw);
					if (ret != FTW_CONTINUE)
					{
						return false;
					}
				}

				return true;
#else
				tls_ftw_ctx.handler = &handler;
				tls_ftw_ctx.recursive = recursive;
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
				int ret = ::nftw(dirpath.c_str(), ftw_wrapper, 1, flags);

				return (ret == 0);
#endif
			}

			// npath need to be a normalized path
			static bool path_exists_internal(const std::string& npath, int type)
			{
				bool ret;
				struct stat_ st;

				if (!dsn::utils::filesystem::get_stat_internal(npath, st))
				{
					return false;
				}

				switch (type)
				{
				case FTW_F:
					ret = S_ISREG(st.st_mode);
					break;
				case FTW_D:
					ret = S_ISDIR(st.st_mode);
					break;
				case FTW_NS:
					ret = S_ISREG(st.st_mode) || S_ISDIR(st.st_mode);
					break;
				default:
					ret = false;
					break;
				}

				return ret;
			}

			bool path_exists(const std::string& path)
			{
				std::string npath;

				if (path.empty())
				{
					return false;
				}

				if (!get_normalized_path(path, npath))
				{
					return false;
				}

				return dsn::utils::filesystem::path_exists_internal(npath, FTW_NS);
			}

			bool directory_exists(const std::string& path)
			{
				std::string npath;

				if (path.empty())
				{
					return false;
				}

				if (!get_normalized_path(path, npath))
				{
					return false;
				}

				return dsn::utils::filesystem::path_exists_internal(npath, FTW_D);
			}

			bool file_exists(const std::string& path)
			{
				std::string npath;

				if (path.empty())
				{
					return false;
				}

				if (!get_normalized_path(path, npath))
				{
					return false;
				}

				return dsn::utils::filesystem::path_exists_internal(npath, FTW_F);
			}

			static bool get_subpaths(const std::string& path, std::vector<std::string>& sub_list, bool recursive, int typeflags)
			{
				std::string npath;
				bool ret;

				if (path.empty())
				{
					return false;
				}

				if (!get_normalized_path(path, npath))
				{
					return false;
				}

				if (!dsn::utils::filesystem::path_exists_internal(npath, FTW_D))
				{
					return false;
				}

				switch (typeflags)
				{
				case FTW_F:
					ret = dsn::utils::filesystem::file_tree_walk(
						npath, [&sub_list](const char* fpath, int typeflag, struct FTW* ftwbuf)
					{
						if (typeflag == FTW_F)
						{
							sub_list.push_back(fpath);
						}

						return FTW_CONTINUE;
					},
						recursive
						);
					break;

				case FTW_D:
					ret = dsn::utils::filesystem::file_tree_walk(
						npath, [&sub_list](const char* fpath, int typeflag, struct FTW* ftwbuf)
					{
						if (((typeflag == FTW_D) || (typeflag == FTW_DP)) && (ftwbuf->level > 0))
						{
							sub_list.push_back(fpath);
						}

						return FTW_CONTINUE;
					},
						recursive
						);
					break;

				case FTW_NS:
					ret = dsn::utils::filesystem::file_tree_walk(
						npath, [&sub_list](const char* fpath, int typeflag, struct FTW* ftwbuf)
					{
						if (ftwbuf->level > 0)
						{
							sub_list.push_back(fpath);
						}

						return FTW_CONTINUE;
					},
						recursive
						);
					break;

				default:
					ret = false;
					break;
				}

				return ret;
			}

			bool get_subfiles(const std::string& path, std::vector<std::string>& sub_list, bool recursive)
			{
				return dsn::utils::filesystem::get_subpaths(path, sub_list, recursive, FTW_F);
			}

			bool get_subdirectories(const std::string& path, std::vector<std::string>& sub_list, bool recursive)
			{
				return dsn::utils::filesystem::get_subpaths(path, sub_list, recursive, FTW_D);
			}

			bool get_subpaths(const std::string& path, std::vector<std::string>& sub_list, bool recursive)
			{
				return dsn::utils::filesystem::get_subpaths(path, sub_list, recursive, FTW_NS);
			}

			static bool remove_directory(const std::string& npath)
			{
				return dsn::utils::filesystem::file_tree_walk(
					npath, [](const char* fpath, int typeflag, struct FTW* ftwbuf)
				{
					bool succ;

					dassert((typeflag == FTW_F) || (typeflag == FTW_DP),
						"Invalid typeflag = %d.", typeflag);
#ifdef _WIN32
					if (typeflag != FTW_F)
					{
						succ = (::RemoveDirectoryA(fpath) == TRUE);
                        if (!succ)
                        {
                            derror("remove directory %s failed, err = %d", fpath, ::GetLastError());
                        }
					}
					else
					{
#endif
						succ = (std::remove(fpath) == 0);
                        if (!succ)
                        {
                            derror("remove file %s failed, err = %s", fpath, strerror(errno));
                        }
#ifdef _WIN32
					}
#endif

					return (succ ? FTW_CONTINUE : FTW_STOP);
				},
					true
					);
			}

			bool remove_path(const std::string& path)
			{
				std::string npath;

				if (path.empty())
				{
					return false;
				}

				if (!get_normalized_path(path, npath))
				{
					return false;
				}

				if (dsn::utils::filesystem::path_exists_internal(npath, FTW_F))
				{
					bool ret = (std::remove(npath.c_str()) == 0);
                    if (!ret)
                    {
                        derror("remove file %s failed, err = %s", path.c_str(), strerror(errno));
                    }
                    return ret;
				}
				else if (dsn::utils::filesystem::path_exists_internal(npath, FTW_D))
				{
					return dsn::utils::filesystem::remove_directory(npath);
				}
				else
				{
					return true;
				}
			}

			bool rename_path(const std::string& path1, const std::string& path2, bool overwrite)
			{
                bool ret;
                
                // We don't check this existence of path2 when overwrite is false
                // since ::rename() will do this.
                if (overwrite && dsn::utils::filesystem::path_exists(path2))
                {
                    ret = dsn::utils::filesystem::remove_path(path2);
                    if (!ret)
                    {
                        dwarn("rename from '%s' to '%s' failed to remove the existed destinate path, err = %s",
                        path1.c_str(),
                        path2.c_str(),
                        strerror(errno));

                        return ret;
                    }
                }

                ret = (::rename(path1.c_str(), path2.c_str()) == 0);
                if (!ret)
                {
                    dwarn("rename from '%s' to '%s' failed, err = %s",
                        path1.c_str(),
                        path2.c_str(),
                        strerror(errno));
                }

                return ret;
			}

			bool file_size(const std::string& path, int64_t& sz)
			{
				struct stat_ st;
				std::string npath;

				if (path.empty())
				{
					return false;
				}

				if (!get_normalized_path(path, npath))
				{
					return false;
				}

				if (!dsn::utils::filesystem::get_stat_internal(npath, st))
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

            static int create_directory_component(const std::string& npath)
            {
                int err;

                if (::mkdir_(npath.c_str()) == 0)
                {
                    return 0;
                }

                err = errno;
                if (err != EEXIST)
                {
                    return err;
                }

                return (dsn::utils::filesystem::path_exists_internal(npath, FTW_F) ? EEXIST : 0);
            }

			bool create_directory(const std::string& path)
			{
				size_t prev = 0;
				size_t pos;
				char sep;
				std::string npath;
                std::string cpath;
				size_t len;
                int err;

				if (path.empty())
				{
					return false;
				}

				if (!get_normalized_path(path, npath))
				{
					return false;
				}

                err = dsn::utils::filesystem::create_directory_component(npath);
                if (err == 0)
                {
                    return true;
                }
                else if (err != ENOENT)
                {
                    cpath = path;
                    goto out_error;
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
					cpath = npath.substr(0, pos++);
					prev = pos;

                    err = dsn::utils::filesystem::create_directory_component(cpath);
                    if (err != 0)
                    {
                        goto out_error;
                    }
				}

				if (prev < len)
				{
                    err = dsn::utils::filesystem::create_directory_component(npath);
                    if (err != 0)
                    {
                        cpath = npath;
                        goto out_error;
                    }
				}

				return true;

out_error:
                dwarn("create_directory %s failed due to cannot create the component: %s, err = %d, err string = %s",
                    path.c_str(),
                    cpath.c_str(),
                    err,
                    strerror(err)
                    );
                return false;
			}


			bool create_file(const std::string& path)
			{
				size_t pos;
				std::string npath;
				int fd;
				int mode;

				if (path.empty())
				{
					return false;
				}

				if (_FS_ISSEP(path.back()))
				{
					return false;
				}

				if (!get_normalized_path(path, npath))
				{
					return false;
				}

				if (dsn::utils::filesystem::path_exists_internal(npath, FTW_F))
				{
					return true;
				}

				if (dsn::utils::filesystem::path_exists_internal(npath, FTW_D))
				{
					return false;
				}

				pos = npath.find_last_of("\\/");
				if ((pos != std::string::npos) && (pos > 0))
				{
					auto ppath = npath.substr(0, pos);
					if (!dsn::utils::filesystem::create_directory(ppath))
					{
						return false;
					}
				}

#ifdef _WIN32
				mode = _S_IREAD | _S_IWRITE;
				if (::_sopen_s(&fd,
					npath.c_str(),
					_O_WRONLY | _O_CREAT | _O_TRUNC,
					_SH_DENYRW,
					mode) != 0)
#else
				mode = 0775;
				fd = ::creat(npath.c_str(), mode);
				if (fd == -1)
#endif
				{
					return false;
				}

				return (::close_(fd) == 0);
			}

			bool get_absolute_path(const std::string& path1, std::string& path2)
			{
				bool succ;
# if defined(_WIN32)
				char* component;
				succ = (0 != ::GetFullPathNameA(path1.c_str(), PATH_MAX, tls_path_buffer, &component));
# else
				succ = (::realpath(path1.c_str(), tls_path_buffer) != nullptr);
# endif
				if (succ)
				{
					path2 = tls_path_buffer;
				}
				
				return succ;
			}

			std::string remove_file_name(const std::string& path)
			{
				size_t len;
				size_t pos;

				len = path.length();
				if (len == 0)
				{
					return "";
				}

				pos = path.find_last_of("\\/");
				if (pos == std::string::npos)
				{
					return "";
				}

				if (pos == len)
				{
					return path;
				}
				
				return path.substr(0, pos);
			}

			std::string get_file_name(const std::string& path)
			{
				size_t len;
                size_t last;
				size_t pos;

				len = path.length();
				if (len == 0)
				{
					return "";
				}

                last = len - 1;

				pos = path.find_last_of("\\/");

				if (pos == last)
				{
                    return "";
                }

				if (pos == std::string::npos)
				{
#ifdef _WIN32
                    pos = path.find_last_of(_FS_COLON);
                    if (pos == last)
                    {
                        return "";
                    }
#else
                    return path;
#endif                        
				}

				return path.substr((pos + 1), (len - pos));
			}

			std::string path_combine(const std::string& path1, const std::string& path2)
			{
                bool ret;
                char c;
                std::string path3;
                std::string npath;

                if (path1.empty())
                {
                    ret = dsn::utils::filesystem::get_normalized_path(path2, npath);
                }
                else if (path2.empty())
                {
                    ret = dsn::utils::filesystem::get_normalized_path(path1, npath);
                }
                else
                {
                    path3 = path1;
                    c = path1[path1.length() - 1];
#ifdef _WIN32
                    if (c != _FS_COLON)
                    {
#endif
                        path3.append(1,
#ifdef _WIN32
                            _FS_BSLASH
#else
                            _FS_SLASH
#endif
                            );
#ifdef _WIN32
                    }
#endif
                    path3.append(path2);

                    ret = dsn::utils::filesystem::get_normalized_path(path3, npath);
                }

                return ret ? npath : "";
			}

			bool get_current_directory(std::string& path)
			{
				bool succ;

				succ = (::getcwd_(tls_path_buffer, PATH_MAX) != nullptr);
				if (succ)
				{
					path = tls_path_buffer;
				}

				return succ;
			}

			bool last_write_time(std::string& path, time_t& tm)
			{
				struct stat_ st;
				std::string npath;

				if (path.empty())
				{
					return false;
				}

				if (!get_normalized_path(path, npath))
				{
					return false;
				}

				if (!dsn::utils::filesystem::get_stat_internal(npath, st))
				{
					return false;
				}

				tm = st.st_mtime;

				return true;
			}
		}
	}
}
