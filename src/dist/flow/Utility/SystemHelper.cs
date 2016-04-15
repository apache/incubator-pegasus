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
 *     What is this file about?
 *
 * Revision history:
 *     Feb., 2016, @imzhenyu (Zhenyu Guo), done in Tron project and copied here
 *     xxxx-xx-xx, author, fix bug about xxx
 */

using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Management;
using System.Reflection;
using System.Runtime.Serialization;
using System.Text;
using System.Threading;
using ThreadState = System.Threading.ThreadState;

// msn.common
namespace rDSN.Tron.Utility
{
    /// <summary>
    /// A class used to pass arguments/data to the threaded robocopy commands.
    /// </summary>
    
    public class RobocopyData
    {
        public string srcDir;
        public string destDir;
        public ICollection<string> files = new[] { "*.*" };
        public string logFile;
        public bool recurseSubdirs;
        public bool excludePdbs;
        public ManualResetEvent signal;
        public Exception exception;
    }

    public static class RandomGenerator
    {
        public static ulong Random64()
        {
            return (ulong)((new Random()).Next());
        }
    }

    public static class PathUtil
    {
        public static string PathClean(this string path)
        {
            return path.Replace("\\\\", "\\");
        }
    }

    /// <summary>
    /// Helper functions to do various things
    /// </summary>
    public class SystemHelper
    {
        public static void SafeCopy(string from, string to, bool force)
        {
            try
            {
                File.Copy(from, to, force);
            }
            catch (Exception)
            {
                // ignored
            }
        }
            

        public static string[] LeveledDirs(string path)
        {
            var dirs = new List<string>();

            var spliters = new[] { '\\', '/' };
            int idx;
            var lastIndex = 0;

            path = path.PathClean();
            while (-1 != (idx = path.IndexOfAny(spliters, lastIndex + 1)))
            {
                lastIndex = idx;
                if (lastIndex > 2)
                {
                    dirs.Add(path.Substring(0, lastIndex));
                }
            }

            if (lastIndex < path.Length - 1)
            {
                dirs.Add(path);
            }

            return dirs.ToArray();
        }

        public static byte[] FileToByteArray(string fileName)
        {
            var fs = new FileStream(fileName,
                                           FileMode.Open,
                                           FileAccess.Read);
            var br = new BinaryReader(fs);
            var numBytes = new FileInfo(fileName).Length;
            var buff = br.ReadBytes((int)numBytes);
            br.Close();
            fs.Close();
            return buff;
        }

        public static void ByteArrayToFile(byte[] content, int offset, int count, string path)
        {
            if (File.Exists(path))
                File.Delete(path);

            var f = new FileStream(path, FileMode.Create);
            f.Write(content, offset, count);
            f.Close();
        }
        
        public static string FileToString(string fileName)
        {
            TextReader br = new StreamReader(fileName);
            var s = br.ReadToEnd();
            br.Close();
            return s;
        }

        public static void StringToFile(string code, string fileName)
        {
            var writer = new StreamWriter(fileName);
            writer.Write(code);
            writer.Close();
        }

        public static HashSet<string> KnownSystemAssemblies = new HashSet<string>
        {
            "System.dll",
            "mscorlib.dll"
        };

        public static string[] GetDependentNonSystemAssemblies(Assembly asm)
        {
            var asmnames = new HashSet<string> {asm.Location};

            if (asm.FullName.Contains("MySQL"))        // hack for referencing libmysqld
            {
                asmnames.Add(Path.GetDirectoryName(asm.Location) + "\\libmysqld.dll");
            }

            var asms = new Queue<Assembly>();
            asms.Enqueue(asm);

            while (asms.Count > 0)
            {
                asm = asms.Dequeue();

                foreach (var s in asm.GetReferencedAssemblies())
                {
                    if (s.FullName.StartsWith("System."))
                        continue;

                    asm = Assembly.Load(s);
                    if (!asmnames.Contains(asm.Location))
                    {
                        if (KnownSystemAssemblies.Contains(Path.GetFileName(asm.Location)))
                            continue;

                        asmnames.Add(asm.Location);
                        asms.Enqueue(asm);
                    }
                }
            }

            return asmnames.ToArray();
        }

        public static long GetCurrentMillieseconds()
        {
            return DateTime.Now.Ticks / 10000;
        }

        /// <summary>
        /// Find the latest updated file information under one directory
        /// </summary>
        /// <param name="directoryPath">directory path</param>
        /// <param name="filePattern">file pattern to search for</param>
        /// <returns></returns>
        public static FileInfo GetLatestModifiedFileInfo(string directoryPath, string filePattern)
        {
            var files = new DirectoryInfo(directoryPath).GetFiles(filePattern);
            if (files.Length == 0)
            {
                return null;
            }

            var last = 0;
            for (var current = 1; current < files.Length; current++)
            {
                if (files[current].LastWriteTime > files[last].LastWriteTime)
                {
                    last = current;
                }
            }

            return files[last];
        }

        /// <summary>
        /// Search the PATH for the given exe (uses the "where" command)
        /// </summary>
        /// <param name="exe">Exe name (e.g. "blah.exe")</param>
        /// <returns>The path to the command</returns>
        public static string EnsureExeInPath(string exe)
        {
            try
            {
                // TODO: if there are multiple copies in the path this will return a string with multiple
                // paths, one per line.  That's not what the caller expects.
                return RunCommand("where " + exe);
            }
            catch (ApplicationException e)
            {
                throw new ApplicationException("Could not find exe " + exe + " in path", e);
            }
        }

        /// <summary>
        /// Run robocopy, ignoring exit codes that indicate that it is OK
        /// </summary>
        /// <param name="command">Full command (e.g. robocopy /mir a b)</param>
        /// <param name="logfile">Optional logfile to pipe robocopy output to</param>
        /// <exception cref="CommandFailureException">If the command fails</exception>
        private static void RunRobocopyCommand(string command, string logfile)
        {
            try
            {
                // Run robocopy /mir
                RunLoggedCommand(command, logfile, true);
            }
            catch (CommandFailureException e)
            {
                // Ignore exit codes below 8 (they are OK)
                if (e.ExitCode < 0 || e.ExitCode >= 8)
                {
                    throw;
                }
            }
        }

        /// <summary>
        /// Copy one directory into another, recursively
        /// </summary>
        /// <param name="srcdir">Source directory</param>
        /// <param name="destdir">Destination directory</param>
        /// <param name="logfile">Where to log information about the copy</param>
        /// <exception cref="CommandFailureException">If the command fails</exception>
        public static void CopyDir(string srcdir, string destdir, string logfile)
        {
            RunRobocopyCommand($"robocopy /s \"{srcdir.TrimEnd('\\')}\" \"{destdir.TrimEnd('\\')}\"", logfile);
        }

        /// <summary>
        /// Copy one directory into another, recursively
        /// </summary>
        /// <param name="srcdir">Source directory</param>
        /// <param name="destdir">Destination directory</param>
        /// <param name="wildcard">Wildcard specifying what to copy</param>
        /// <param name="logfile">Where to log information about the copy</param>
        /// <exception cref="CommandFailureException">If the command fails</exception>
        public static void CopyDir(string srcdir, string destdir, string wildcard, string logfile)
        {
            RunRobocopyCommand($"robocopy /s \"{srcdir.TrimEnd('\\')}\" \"{destdir.TrimEnd('\\')}\" {wildcard}", logfile);
        }

        /// <summary>
        /// Copies single file through intermediate .tmp file.
        /// This helps to avoid locking/reading incomplete file issues.
        /// </summary>
        /// <param name="sourceFileName">Source filename</param>
        /// <param name="destFileName">Destination filename</param>
        public static void CopyFileThroughTmp(string sourceFileName, string destFileName)
        {
            File.Copy(sourceFileName, destFileName + ".tmp", true);
            File.Move(destFileName + ".tmp", destFileName);
        }

        /// <summary>
        /// Delete file with multiple try and delay before throwing IOException
        /// </summary>
        /// <param name="filePath">absolute path of the file to be deleted</param>
        /// <param name="numberOfTry">number of retry</param>
        /// <param name="delayInSeconds">delay in seconds between retry</param>
        public static void RemoveFileWithRetry(string filePath, int numberOfTry, int delayInSeconds)
        {
            if (string.IsNullOrEmpty(filePath) || !File.Exists(filePath))
            {
                return;
            }

            numberOfTry = Math.Max(1, numberOfTry);
            delayInSeconds = Math.Max(0, delayInSeconds);

            // Try multiple times to delete the file
            for (var i = 0; i < numberOfTry; i++)
            {
                try
                {
                    File.Delete(filePath);

                    // Break from the loop when successfully delete the file
                    break;
                }
                catch (IOException)
                {
                    if (i == numberOfTry - 1)
                    {
                        // Failed the last time, re-throw the exception
                        throw;
                    }
                    // Sleep for a while before next try
                    Thread.Sleep(delayInSeconds * 1000);
                }
            }
        }

        /// <summary>
        /// Remove all files within a given directory except the specified files
        /// </summary>
        /// <param name="dir">The directory from which to remove files</param>
        /// <param name="excludedFiles">The list of files to exclude</param>
        public static void RemoveAllFilesExcept(string dir, string[] excludedFiles)
        {
            RemoveAllFilesExcept(dir, "*", excludedFiles);
        }

        /// <summary>
        /// Remove all files with a specific search pattern within a give directory except the specified files
        /// </summary>
        /// <param name="dir">The directory from which to remove files</param>
        /// <param name="searchPattern">The pattern to search for files</param>
        /// <param name="excludedFiles">The list of files to exclude</param>
        public static void RemoveAllFilesExcept(string dir, string searchPattern, string[] excludedFiles)
        {
            var dirInfo = new DirectoryInfo(dir);
            foreach (var fi in dirInfo.GetFiles(searchPattern))
            {
                var isExcludedFile = false;
                if (excludedFiles != null)
                {
                    if (excludedFiles.Any(file => fi.Name.ToLower().Equals(file.ToLower())))
                    {
                        isExcludedFile = true;
                    }
                }
                if (!isExcludedFile)
                    File.Delete(fi.FullName);
            }
        }

        /// <summary>
        /// Remove all subdirectories within a given directory except the specified directories
        /// </summary>
        /// <param name="">The directory from which to remove directories</param>
        /// <param name="excludedSubdirs">The list of directories to exclude</param>
        /// <param name="areExcludedDirsLowered">A bool indicating if the list of excludedDirs are all lower case.
        /// If false, each entry in excludedSubdirs will be lowered to facilitate string matching.
        /// If true, this process will be skipped.</param>
        public static void RemoveAllDirsExcept(string dir, string[] excludedDirs, bool areExcludedDirsLowered = false)
        {
            // lower the root dir to facilitate comparison
            //
            dir = dir.ToLower();

            // create a lowered version of excluded dirs to keep from repeating the process
            //
            string[] loweredExcludedDirs = null;

            // if they are already lowered there is nothing to do
            //
            if (areExcludedDirsLowered)
            {
                loweredExcludedDirs = excludedDirs;
            }
            // if they aren't lowered already and excluded dirs isn't null, do the dirty work
            //
            else if (excludedDirs != null)
            {
                // excluded dirs exists, so create its lowered analog
                //
                loweredExcludedDirs = new string[excludedDirs.Length];

                // iterate through the array and lower each entry
                //
                for (var i = 0; i < excludedDirs.Length; i++)
                {
                    loweredExcludedDirs[i] = excludedDirs[i].ToLower();
                }
            }

            var dirInfo = new DirectoryInfo(dir);
            foreach (var subdirInfo in dirInfo.GetDirectories())
            {
                var isExcludedDir = false;
                if (loweredExcludedDirs != null)
                {
                    foreach (var subdir in loweredExcludedDirs)
                    {
                        // create a full path to this subdir
                        //
                        var fullSubDir = new DirectoryInfo(Path.Combine(dir, subdir));

                        // if this exact directory is excluded no need to process any further
                        //
                        if (fullSubDir.FullName == subdirInfo.FullName.ToLower())
                        {
                            isExcludedDir = true;
                            break;
                        }
                        // if a subdir of this directory is excluded, need to exclude this dir
                        // and perform a recursive call to nuke subdirs
                        //
                        if (fullSubDir.FullName.StartsWith(subdirInfo.FullName.ToLower()))
                        {
                            // filter the excluded dirs to only those that are 
                            // contained under this subdir
                            //

                            // make a recursive call that won't re-lower the filtered excluded dirs
                            //
                            RemoveAllDirsExcept(subdirInfo.FullName, (from filterCandidate in loweredExcludedDirs where filterCandidate.StartsWith(subdirInfo.Name.ToLower() + Path.DirectorySeparatorChar) select filterCandidate.Remove(0, subdirInfo.Name.Length + 1)).ToArray(), true);

                            // mark this dir as excluded as we handled it in the recursive call
                            //
                            isExcludedDir = true;
                            break;
                        }
                    }
                }

                if (!isExcludedDir)
                    NukeDirectory(subdirInfo.FullName);
            }
        }

        /// <summary>
        /// Remove all files under a directory except the specified list of files and subdirectories
        /// </summary>
        /// <param name="dir"></param>
        /// <param name="subdirs"></param>
        public static void RemoveAllExcept(string dir, string[] excludedItems)
        {
            RemoveAllFilesExcept(dir, excludedItems);
            RemoveAllDirsExcept(dir, excludedItems);
        }

        /// <summary>
        /// Delete a directory and its subdirectories, including read-only files
        /// </summary>
        /// <param name="dir">The directory to nuke</param>
        public static void NukeDirectory(string dir)
        {
            NukeDirectory(dir, true);
        }

        /// <summary>
        /// Delete a directory and its subdirectories, including read-only files
        /// </summary>
        /// <param name="dir">The directory to nuke</param>
        /// <param name="deleteItself">Whether to delete the top level directory</param>
        public static void NukeDirectory(string dir, bool deleteItself)
        {
            if (Directory.Exists(dir))
            {
                // Delete any subdirectories
                var rootDirInfo = new DirectoryInfo(dir);
                foreach (var dirInfo in rootDirInfo.GetDirectories())
                {
                    NukeDirectory(dirInfo.FullName);
                }

                // Delete any files, even read-only ones
                foreach (var fileInfo in rootDirInfo.GetFiles())
                {
                    if ((fileInfo.Attributes & FileAttributes.ReadOnly) != 0)
                        fileInfo.Attributes -= FileAttributes.ReadOnly;
                    File.Delete(fileInfo.FullName);
                }

                // Finally delete the directory itself
                if (deleteItself)
                {
                    Directory.Delete(dir, true);
                }
            }
        }

        /// <summary>
        /// Delete a directory and its subdirectories with number of retry and delay seconds
        /// </summary>
        /// <param name="dir"></param>
        /// <param name="numberOfTry"></param>
        /// <param name="delayInSeconds"></param>
        public static void NukeDirectoryWithRetry(string dir, int numberOfTry, int delayInSeconds)
        {
            NukeDirectoryWithRetry(dir, numberOfTry, delayInSeconds, true);
        }

        /// <summary>
        /// Delete a directory and its subdirectories with number of retry and delay seconds
        /// </summary>
        /// <param name="dir"></param>
        /// <param name="numberOfTry"></param>
        /// <param name="delayInSeconds"></param>
        /// <param name="deleteItself"></param>
        public static void NukeDirectoryWithRetry(string dir, int numberOfTry, int delayInSeconds, bool deleteItself)
        {
            if (Directory.Exists(dir))
            {
                // Delete any subdirectories
                var rootDirInfo = new DirectoryInfo(dir);
                foreach (var dirInfo in rootDirInfo.GetDirectories())
                {
                    NukeDirectoryWithRetry(dirInfo.FullName, numberOfTry, delayInSeconds, deleteItself);
                }

                // Delete any files, even read-only ones
                foreach (var fileInfo in rootDirInfo.GetFiles())
                {
                    if ((fileInfo.Attributes & FileAttributes.ReadOnly) != 0)
                    {
                        fileInfo.Attributes -= FileAttributes.ReadOnly;
                    }

                    RemoveFileWithRetry(fileInfo.FullName, numberOfTry, delayInSeconds);
                }

                // Finally delete the directory itself
                if (deleteItself)
                {
                    Directory.Delete(dir, true);
                }
            }
        }

        /// <summary>
        /// An overload for MirrorDir that specifies both the excludePdbs and useMultipleRobocopyInstances options.
        /// </summary>
        /// <param name="srcdir">The source to be mirrored.</param>
        /// <param name="destdir">The destination to be mirrored.</param>
        /// <param name="logfile">The logfile to which to be written.</param>
        /// <param name="recurseSubdirs">A bool indicating if subdirectories should be copied or not.</param>
        /// <param name="excludePdbs">A bool indicating if pdb files should be excluded.</param>
        /// <param name="useMultipleRobocopyInstances">A bool indicating if multiple instances of robocopy should be used.
        /// If true, a maximum of ThreadPool.GetMaxThreads() robocopy instances will be spawned.</param>
        /// <returns>If useMultipleRobocopyInstances is true, a RobocopyData object is returned, and its signal field will be signaled
        /// when the robocopy completes.  Otherwise, null is returned.</returns>
        public static RobocopyData MirrorDir(string srcdir, string destdir, string logfile, bool recurseSubdirs,
            bool excludePdbs, bool useMultipleRobocopyInstances)
        {
            return MirrorDir(srcdir, destdir, null, logfile, recurseSubdirs, excludePdbs, useMultipleRobocopyInstances);
        }

        /// <summary>
        /// An overload for MirrorDir that specifies both the excludePdbs and useMultipleRobocopyInstances options.
        /// </summary>
        /// <param name="srcdir">The source to be mirrored.</param>
        /// <param name="destdir">The destination to be mirrored.</param>
        /// <param name="files">The specific files or search patterns to be mirrored.  If null, *.* is used.</param>
        /// <param name="logfile">The logfile to which to be written.</param>
        /// <param name="recurseSubdirs">A bool indicating if subdirectories should be copied or not.</param>
        /// <param name="excludePdbs">A bool indicating if pdb files should be excluded.</param>
        /// <param name="useMultipleRobocopyInstances">A bool indicating if multiple instances of robocopy should be used.
        /// If true, a maximum of ThreadPool.GetMaxThreads() robocopy instances will be spawned.</param>
        /// <returns>If useMultipleRobocopyInstances is true, a RobocopyData object is returned, and its signal field will be signaled
        /// when the robocopy completes.  Otherwise, null is returned.</returns>
        public static RobocopyData MirrorDir(string srcdir, string destdir, ICollection<string> files, string logfile, bool recurseSubdirs,
            bool excludePdbs, bool useMultipleRobocopyInstances)
        {
            if (useMultipleRobocopyInstances)
            {
                // multiple robocopy instances were requested, so queue up some background threads
                // that will each spawn a robocopy process and signal a wait handle when done
                // return the wait handle so that the caller can monitor the robocopy progress
                //
                var data = new RobocopyData
                {
                    srcDir = srcdir,
                    destDir = destdir
                };
                if (files != null)
                {
                    data.files = files;
                }
                data.logFile = logfile;
                data.recurseSubdirs = recurseSubdirs;
                data.excludePdbs = excludePdbs;
                data.signal = new ManualResetEvent(false);

                ThreadPool.QueueUserWorkItem(ThreadedMirrorDir, data);
                return data;
            }
            // make sure we didn't get any null arguments
            // 
            if (srcdir == null)
            {
                srcdir = string.Empty;
            }
            if (destdir == null)
            {
                destdir = string.Empty;
            }

            // call robocopy directly and allow it to block the current thread
            // first build up the robocopy command to be run
            //

            var robocopyCmd = string.Format(recurseSubdirs ? "robocopy /mir /np /nfl /r:5 /w:5 \"{0}\" \"{1}\"" : "robocopy /copy:dat /purge /np /r:5 /w:5 \"{0}\" \"{1}\"", srcdir.TrimEnd('\\'), destdir.TrimEnd('\\'));

            // iterate through the files to be copied and add each to the command line
            //
            if (files != null)
            {
                robocopyCmd = files.Aggregate(robocopyCmd, (current, f) => current + $" \"{f}\" ");
            }

            // if excludepdbs is true then add this as a command line param to robocopy
            //
            if (excludePdbs)
            {
                robocopyCmd += " /XF *.pdb";
            }

            // run robocopy
            //
            RunRobocopyCommand(robocopyCmd, logfile);
            return null;
        }

        /// <summary>
        /// Mirror one directory onto another.  useMultipleRobocopyInstances defaults to false.
        /// </summary>
        /// <param name="srcdir">Source directory</param>
        /// <param name="destdir">Destination directory</param>
        /// <param name="logfile">Where to log information about the copy</param>
        /// <param name="recurseSubdirs">A bool indicating if subdirectories should be copied or not.</param>
        /// <param name="excludePdbs">A bool indicating if pdb files should be excluded.</param>
        /// <exception cref="CommandFailureException">If the command fails</exception>
        public static void MirrorDir(string srcdir, string destdir, string logfile, bool recurseSubdirs, bool excludePdbs)
        {
            MirrorDir(srcdir, destdir, null, logfile, recurseSubdirs, excludePdbs, false);
        }

        /// <summary>
        /// Mirror one directory onto another.  
        ///     recurseSubdirs defaults to true
        ///     excludePdbs defaults to false 
        ///     useMultipleRobocopyInstances defaults to false
        /// </summary>
        /// <param name="srcdir">Source directory</param>
        /// <param name="destdir">Destination directory</param>
        /// <param name="logfile">Where to log information about the copy</param>
        /// <exception cref="CommandFailureException">If the command fails</exception>
        public static void MirrorDir(string srcdir, string destdir, string logfile)
        {
            MirrorDir(srcdir, destdir, logfile, true, false);
        }

        /// <summary>
        /// An "overload" of MirrorDir to be called by background threads.
        /// </summary>
        /// <param name="callInfo">A RobocopyData object containing the params to MirrorDir.</param>
        protected static void ThreadedMirrorDir(object callInfo)
        {
            // unpackage the robocopy data object
            // 
            var data = callInfo as RobocopyData;

            // make sure valid data was passed in
            // 
            if (callInfo != null)
            {
                try
                {
                    // call MirrorDir with useMultipleRobocopyInstances = false
                    // this ensures that the current thread will be blocked until robocopy completes
                    // 
                    //Log.Info("Calling MirrorDir({0}, {1}, {2}, {3}, {4}, {5}, {6})",
                    //    data.srcDir, data.destDir, data.files, data.logFile, data.recurseSubdirs, data.excludePdbs, false);
                    MirrorDir(data.srcDir, data.destDir, data.files, data.logFile, data.recurseSubdirs, data.excludePdbs, false);
                    //Log.Info("End MirrorDir({0}, {1}, {2}, {3}, {4}, {5}, {6})",
                    //    data.srcDir, data.destDir, data.files, data.logFile, data.recurseSubdirs, data.excludePdbs, false);
                }
                catch (Exception e)
                {
                    // save the exception, so we can bubble it up to the controlling thread
                    //
                    data.exception = e;

                    // log the error, as the main thread will only rethrow one exception
                    //
                    //Log.Error("A call to MirrorDir({0}, {1}, {2}, {3}, {4}, {5}) threw a {6}.",
                    //    data.srcDir, data.destDir, data.logFile, data.recurseSubdirs, data.excludePdbs, false, e.GetType().ToString());
                    //Log.Error(e.Message);
                    //Log.Error(e.StackTrace);
                }
                finally
                {
                    // signal the main thread that this robocopy has completed
                    //
                    data.signal.Set();
                }
            }
        }

        /// <summary>
        /// Copy specified subdirectories and delete anything in the destination that isn't one of the subdirectories
        /// </summary>
        /// <param name="srcrootdir">The source's root directory.</param>
        /// <param name="subdirs">An array of subdirs of the source to be mirrored.</param>
        /// <param name="destdir">The destination directory.</param>
        /// <param name="logfile">The file to which to log.</param>
        /// <param name="recurseSubdirs">A bool indicating if subdirectories should be copied or not.</param>
        /// <param name="excludePdbs">A bool indicating if pdb files should be excluded.</param>
        /// <param name="useMultipleRobocopyInstances">A bool indicating if multiple instances of robocopy should be used.
        /// If true, a maximum of ThreadPool.GetMaxThreads() robocopy instances will be spawned.</param>
        /// <returns>If useMultipleRobocopyInstances is true, a List of RobocopyData objects is returned.  Each RobocopyData
        /// represents a robocopy instance and it's signal field will be signaled when that robocopy completes.  
        /// Otherwise, null is returned.</returns>
        public static List<RobocopyData> MirrorSubdirs(string srcrootdir, string[] subdirs, string destdir,
            string logfile, bool recurseSubdirs, bool excludePdbs, bool useMultipleRobocopyInstances)
        {
            // create a list of robocopy instances on which to wait
            // 
            var robocopyInstances = subdirs.Select(subdir => MirrorDir(Path.Combine(srcrootdir, subdir), Path.Combine(destdir, subdir), null, null, recurseSubdirs, excludePdbs, useMultipleRobocopyInstances)).Where(curInstance => useMultipleRobocopyInstances && (curInstance != null)).ToList();

            // iterate through each subdir 
            //

            // if multiple instances of robocopy are being created, return the list of wait handles
            //
            if (useMultipleRobocopyInstances && (robocopyInstances.Count > 0))
            {
                return robocopyInstances;
            }

            // this thread was blocked during the robocopy so there is no WaitHandle to return
            //
            return null;
        }

        /// <summary>
        /// Copy specified subdirectories and delete anything in the destination that isn't one of the subdirectories.
        /// useMultipleRobocopyInstances defaults to false.
        /// </summary>
        /// <param name="srcrootdir">Source directory containing the subdirectories</param>
        /// <param name="subdirs">List of subdirectories</param>
        /// <param name="destdir">Destination directory</param>
        /// <param name="recurseSubdirs">A bool indicating if subdirectories should be copied or not.</param>
        /// <param name="logfile">Where to log information about the copy</param>
        /// <param name="excludePdbs">A bool indicating if pdb files should be excluded.</param>
        public static void MirrorSubdirs(string srcrootdir, string[] subdirs, string destdir, string logfile,
            bool recurseSubdirs, bool excludePdbs)
        {
            MirrorSubdirs(srcrootdir, subdirs, destdir, logfile, recurseSubdirs, excludePdbs, false);
        }

        /// <summary>
        /// Copy specified subdirectories and delete anything in the destination that isn't one of the subdirectories.
        ///     recurseSubdirs defaults to true
        ///     excludePdbs defaults to false 
        ///     useMultipleRobocopyInstances defaults to false
        /// </summary>
        /// <param name="srcrootdir">Source directory containing the subdirectories</param>
        /// <param name="subdirs">List of subdirectories</param>
        /// <param name="destdir">Destination directory</param>
        /// <param name="logfile">Where to log information about the copy</param>
        public static void MirrorSubdirs(string srcrootdir, string[] subdirs, string destdir, string logfile)
        {
            MirrorSubdirs(srcrootdir, subdirs, destdir, logfile, true, false);
        }

        /// <summary>
        /// Run a command, ignoring any exit codes that are OK
        /// </summary>
        /// <param name="command">Full command (e.g. echo true)</param>
        /// <exception cref="CommandFailureException">If the command fails</exception>
        public static string RunCommand(string command)
        {
            try
            {
                var result = RunCommand(command, null, false);
                return result;
            }
            catch (Exception)
            {
                // ignored
            }
            return "";
        }

        /// <summary>
        /// this is a private container class used to perform thread-safe, deadlock free
        /// reading of a redirected output stream from a process spawned by RunCommand()
        /// </summary>
        /// <remarks>
        /// RunCommand() initializes this container by instantiating it with the 
        /// redirected output stream of a new process, then spawns a new thread
        /// using the ReadProcessOutputStream member.  RunCommand() then waits
        /// for the spawned thread to finish and retrieves output string by
        /// invoking the GetOutputString member.
        /// </remarks>
        ///
        private class CommandOutputStreamHolder
        {
            /// <summary>
            /// constructor, takes in a reference to the redirected output stream
            /// to consume
            /// </summary>
            /// <param name="commandOutputStream"></param>
            /// 
            public CommandOutputStreamHolder(StreamReader commandOutputStream)
            {
                outputStream = commandOutputStream;
                outputString = string.Empty;
            }

            /// <summary>
            /// Used to spawn a separate thread process to consume the redirected standard output
            /// from a process spawned by RunCommand()
            /// </summary>
            ///
            public void ReadProcessOutputStream()
            {
                if (outputStream != null)
                {
                    outputString = outputStream.ReadToEnd();
                }
            }

            /// <summary>
            /// Used by RunCommand() to retrieve the process output string once
            /// the spawned thread has completed
            /// </summary>
            /// <returns>the consumed output of the redirected output stream</returns>
            public string GetOutputString()
            {
                return outputString;
            }

            // private member to hold the redirected output stream to consume
            //
            private StreamReader outputStream;

            // private member to hold the consumed output string for retrieval
            private string outputString;
        }


        /// <summary>
        /// Run a command, ignoring any exit codes that are OK
        /// </summary>
        /// <param name="command">Full command (e.g. echo true)</param>
        /// <param name="standardInput">stdin to pass to the command</param>
        /// <exception cref="CommandFailureException">If the command fails</exception>
        public static string RunCommand(string command, string standardInput, bool needRoot = false)
        {
            // Start the process
            var process = StartProcess("cmd.exe", "/c " + command, true, standardInput);

            // trying to read both redirected standard output and standard error
            // synchronously can result in deadlock as the parent process can't
            // finish reading until the child process exits, but the child process
            // can't exit until the parent process finishes reading.  The work around
            // for this is .NET 1.0 is to create a separate thread to read one of the
            // streams.  
            // TODO: when all test / test framework code has moved to .NET 2.0, we 
            //       should re-write this using the new async BeginRead() API.

            //string stdout = process.StandardOutput.ReadToEnd();

            var streamHolder = new CommandOutputStreamHolder(process.StandardOutput);
            var stdoutReaderThread = new Thread(streamHolder.ReadProcessOutputStream);
            stdoutReaderThread.Start();

            // read redirected stderr synchronously
            var stderr = process.StandardError.ReadToEnd();

            // Wait for process to finish
            while (!process.HasExited)
            {
                Thread.Sleep(1000);
            }

            while (stdoutReaderThread.ThreadState != ThreadState.Stopped &&
                   stdoutReaderThread.ThreadState != ThreadState.Aborted)
            {
                Thread.Sleep(1000);
            }
            var stdout = streamHolder.GetOutputString();

            // Check for error
            var exitCode = process.ExitCode;
            if (exitCode != 0)
            {
                //throw new CommandFailureException(command, process.ExitCode, stdout + stderr);
                //RunCommand(command, standardInput, true);
            }
            return stdout + stderr;
        }

        /// <summary>
        /// Run a command, ignoring any exit codes that are OK and logging
        /// output to a file. Defaults WindowStyle to not run hidden
        /// </summary>
        /// <param name="command">Full command (e.g. echo true)</param>
        /// <param name="logfile">File path into which output will be piped</param>
        /// <exception cref="CommandFailureException">If the command fails</exception>
        public static void RunLoggedCommand(string command, string logfile)
        {
            RunLoggedCommand(command, logfile, false);
        }

        /// <summary>
        /// Run a command, ignoring any exit codes that are OK and logging
        /// output to logfile. 
        /// </summary>
        /// <param name="command">Full command (e.g. echo true)</param>
        /// <param name="logfile">File path into which output will be piped</param>
        /// <param name="runHidden">If true, run with ProcessWindowStyle.Hidden, else uses default ProcessWindowStyle</param>
        /// <exception cref="CommandFailureException">If the command fails</exception>
        public static void RunLoggedCommand(string command, string logfile, bool runHidden)
        {
            // Get portion of string that pipes this to a file
            var logpipe = "";
            if (logfile != null)
            {
                logpipe = " >> " + logfile + " 2>&1";
            }

            // Create the ProcessStartInfo object
            var cmd = "cmd.exe";
            var cmdArguments = "/c " + command + logpipe;
            var startInfo = new ProcessStartInfo(cmd, cmdArguments);
            if (runHidden)
            {
                startInfo.WindowStyle = ProcessWindowStyle.Hidden;
            }

            // Run the command
            var process = Process.Start(startInfo);
            process.WaitForExit();

            // Check for error
            var exitCode = process.ExitCode;
            if (exitCode != 0)
            {
                throw new CommandFailureException(command, process.ExitCode);
            }
        }

        /// <summary>
        /// Runs a set of commands in the same shell (for env vars and chdirs and such).
        /// If a command fails, no more commands will run and the exit code will be command's #
        /// (1, 2, 3, 4) which failed, or -1 if a setup command failed.  (TODO: find a way to
        /// preserve the original error code as well as to differentiate which command failed.)
        /// The shell will start out in the corext root dir.
        /// </summary>
        /// <param name="logfile">The log file to pipe to (null for stdout)</param>
        /// <param name="corextdir">The corext directory (i.e. c:\src\main)</param>
        /// <param name="commands">The commands to run (will be run in sequence)</param>
        /// <exception cref="CommandFailureException">If the command fails</exception>
        public static void RunCommandsInCorextLogged(string logfile, string corextdir, params string[] commands)
        {
            try
            {
                var newCommands = new string[3 + commands.Length];
                newCommands[0] = "set inetroot=" + corextdir;
                newCommands[1] = "call " + corextdir + @"\tools\path1st\myenv.cmd";
                newCommands[2] = "cd /d " + corextdir;
                Array.Copy(commands, 0, newCommands, 3, commands.Length);
                RunCommandsInSameShellLogged(
                    logfile,
                    newCommands
                );
            }
            catch (CommandFailureException e)
            {
                // Adjust for the extra commands we put in there
                if (e.ExitCode <= 3)
                {
                    e.ExitCode = -1;
                }
                else
                {
                    e.ExitCode -= 3;
                }

                // TODO make sure this really throws the right number
                throw;
            }
        }

        /// <summary>
        /// Runs a set of commands in the same shell (for env vars and chdirs and such).
        /// If a command fails, no more commands will run and the exit code will be command's #
        /// (1, 2, 3, 4) which failed.  (TODO: find a way to preserve the original
        /// error code as well as to differentiate which command failed.)
        /// </summary>
        /// <param name="logfile">The log file to pipe to (null for stdout)</param>
        /// <param name="commands">The commands to run (will be run in sequence)</param>
        /// <exception cref="CommandFailureException">If the command fails</exception>
        public static void RunCommandsInSameShellLogged(string logfile, params string[] commands)
        {
            // Write out the build batch file we will run
            var batchFile = Path.GetTempFileName() + ".cmd";
            var writer = new StreamWriter(batchFile);
            for (var i = 0; i < commands.Length; i++)
            {
                // Write the command itself
                var command = commands[i];
                writer.WriteLine(command);
                // When a command fails, use the command # as the error so we know which failed
                writer.WriteLine("if errorlevel 1 exit /b " + (i + 1));
            }
            writer.Close();

            // Run the batch file
            RunLoggedCommand(batchFile, logfile);
        }

        /// <summary>
        /// Starts the process associated with the given file
        /// under the working directory of the file and returns
        /// a process object related to the process
        /// </summary>
        /// <param name="fileName">The name of the executable file
        /// to start</param>
        /// <param name="arguments">Command line arguments that are 
        /// passed to the process</param>
        /// <param name="standardInput">Standard input to the process</param>
        /// <returns>A process object associated with the process just 
        /// started</returns>
        public static Process StartProcess(string fileName, string arguments,
            bool redirectOutput, string standardInput)
        {
            return StartProcess(fileName, arguments, redirectOutput, standardInput, new StringDictionary());
        }

        /// <summary>
        /// Starts the process associated with the given file
        /// under the working directory of the file and returns
        /// a process object related to the process
        /// </summary>
        /// <param name="fileName">The name of the executable file
        /// to start</param>
        /// <param name="arguments">Command line arguments that are 
        /// passed to the process</param>
        /// <param name="standardInput">Standard input to the process</param>
        /// <param name="environmentVariables">A dictionary of environment variables to be set on the process object.</param>
        /// <returns>A process object associated with the process just 
        /// started</returns>
        public static Process StartProcess(string fileName, string arguments,
                bool redirectOutput, string standardInput, StringDictionary environmentVariables)
        {
            //Create a start info for the process based on filename
            //and arguments
            var startInfo =
                new ProcessStartInfo(fileName, arguments);

            // add the requested environment variables
            //
            foreach (string environmentVariableName in environmentVariables.Keys)
            {
                startInfo.EnvironmentVariables.Add(environmentVariableName, environmentVariables[environmentVariableName]);
            }

            //Set the working directory for the process
            var file = new FileInfo(fileName);
            startInfo.WorkingDirectory = file.DirectoryName;
            startInfo.UseShellExecute = false;
            if (redirectOutput)
            {
                startInfo.RedirectStandardOutput = true;
                startInfo.RedirectStandardError = true;
            }
            if (standardInput != null)
            {
                startInfo.RedirectStandardInput = true;
            }

            //Create a new process based on the startinfo
            var process = new Process {StartInfo = startInfo};

            //Start the process and return the process object
            process.Start();
            if (standardInput != null)
            {
                process.StandardInput.Write(standardInput);
            }

            return process;
        }

        /// <summary>
        /// Starts the process associated with the given file
        /// under the working directory of the file and returns
        /// a process object related to the process
        /// </summary>
        /// <param name="fileName">The name of the executable file
        /// to start</param>
        /// <param name="arguments">Command line arguments that are 
        /// passed to the process</param>        
        /// <returns>A process object associated with the process just 
        /// started</returns>
        public static Process StartProcess(string fileName, string arguments)
        {
            return StartProcess(fileName, arguments, false, null);
        }

        /// <summary>
        /// Starts the process associated with the given file
        /// under the working directory of the file and returns
        /// a process object related to the process
        /// </summary>
        /// <param name="fileName">The name of the executable file
        /// to start</param>
        /// <returns> process object associated with the process just 
        /// started</returns>
        public static Process StartProcess(string fileName)
        {
            return StartProcess(fileName, null);
        }


        /// <summary>
        /// Gets all processes whose filename equals one of a
        /// list of values (case-insensitive).
        /// </summary>
        /// <param name="processList">The module filename portions to
        /// look for</param>
        /// <returns>Array of Process objects matching processList</returns>
        public static Process[] FindAllProcessesByNames(params string[] processList)
        {
            var processes = Process.GetProcesses();
            var found = new ArrayList();
            foreach (var process in processes)
            {
                string name;

                try
                {
                    var file = new FileInfo(process.MainModule.FileName);
                    name = file.Name;
                }
                catch
                {
                    // null == skip parsing (for System and Idle process, MainModule
                    // will throw an exception)
                    name = null;
                }

                if (name == null) continue;
                foreach (var match in processList.Where(match => name.ToLower().Equals(match.ToLower())))
                {
                    found.Add(process);
                }
            }
            return (Process[])found.ToArray(typeof(Process));
        }

        /// <summary>
        /// Kills all processes whose filename equals one of a
        /// list of values (case-insensitive).
        /// </summary>
        /// <param name="processList">The module filename portions to
        /// kill</param>
        public static void KillAllProcessesByNames(params string[] processList)
        {
            var processes = FindAllProcessesByNames(processList);
            if ((processes == null) || (processes.Length == 0)) return;

            foreach (var process in processes)
            {
                KillProcessTree(process.Id);
            }
        }


        /// <summary>
        /// Kills a process tree. Kills the parent process and all
        /// the child processes spawned by the parent        
        /// </summary>
        /// <param name="parentProcessID">Process ID of the parent process</param>
        public static void KillProcessTree(int parentProcessID)
        {
            KillProcessTree(parentProcessID, 0);
        }

        /// <summary>
        /// Kills a process tree. Kills the parent process and all
        /// the child processes spawned by the parent        
        /// </summary>
        /// <param name="parentProcessID">Process ID of the parent process</param>
        /// <param name="timeoutMS">Maximum time (in milliseconds) to wait for
        /// each process to die.  Use 0 to skip wait.</param>
        public static void KillProcessTree(int parentProcessID, int timeoutMS)
        {
            //Query WMI class Win32_Process to get all processes with 
            //the specified parent processID
            var query =
                "select * from Win32_Process where ParentProcessID = " +
                parentProcessID;
            var objectQuery = new ObjectQuery(query);

            //Send the query and gets all the child processes
            var searcher =
                new ManagementObjectSearcher(objectQuery);
            ICollection childProcesses = searcher.Get();

            //Loop through every child process and kill everything
            //spawned by the process
            foreach (var childProcessId in from ManagementObject childProcess in childProcesses select Convert.ToInt32(childProcess["ProcessID"]))
            {
                //Kill the processes recursively
                KillProcessTree(childProcessId, timeoutMS);

                //Now kill the child process
                KillProcessWithTimeout(childProcessId, timeoutMS);
            }

            // Now, kill the parent process...
            KillProcessWithTimeout(parentProcessID, timeoutMS);
        }


        /// <summary>
        /// Kills a given process (by ID).  Waits up to timeoutMS for the process
        /// to exit.
        /// </summary>
        /// <param name="timeoutMS">Maximum time (in milliseconds) to wait for
        /// each process to die.  Use 0 to indicate no wait.</param>
        public static void KillProcessWithTimeout(int processID, int timeoutMS)
        {
            Process process = null;

            try
            {
                process = Process.GetProcessById(processID);
            }
            catch (ArgumentException arge)
            {
                Trace.WriteLine(arge.Message);
            }

            if (null == process)
            {
                return;
            }

            process.Kill();
            if ((process.HasExited) || (timeoutMS == 0)) return;
            if (!process.WaitForExit(timeoutMS))
            {
                throw new ApplicationException(
                    $"Couldn't terminate process ID {processID} within specified timeout ({timeoutMS} ms)"
                    );
            }
        }

        public static void CreateOrCleanDirectory(string dir)
        {
            if (Directory.Exists(dir))
            {
                RemoveAllDirsExcept(dir, new string[] { });
                RemoveAllExcept(dir, new string[] { });
            }
            else
            {
                Directory.CreateDirectory(dir);
            }
        }

        public static bool CheckAndCreateDirectory(ref string workDir)
        {
            if (!Directory.Exists(workDir))
            {
                try
                {
                    var dirInfo = Directory.CreateDirectory(workDir);
                    workDir = dirInfo.FullName;
                    return true;
                }
                catch (Exception e)
                {
                    Trace.WriteLine("Create work dir " + workDir + " failed, exception = " + e.Message);
                    return false;
                }
            }
            return true;
        }

        public static StreamReader OpenFileTimeout(string file, TimeSpan timeout)
        {
            var start = DateTime.Now;
            // We loop until we pass the timeout
            while (true)
            {
                try
                {
                    return File.OpenText(file);
                }
                // The point of open file is to ignore intermittent failures or locking issues;
                // file not found is a permanent failure.
                catch (FileNotFoundException)
                {
                    throw;
                }
                catch (DirectoryNotFoundException)
                {
                    throw;
                }
                catch (Exception)
                {
                    if (DateTime.Now - start > timeout)
                    {
                        throw;
                    }

                    Trace.WriteLine("Waiting on lock for file " + file + "...");
                    Thread.Sleep(TimeSpan.FromMilliseconds(100));
                }
            }
        }

        public static FileStream OpenFileTimeout(string file, FileMode mode, FileAccess access, FileShare share, TimeSpan timeout)
        {
            var start = DateTime.Now;
            // We loop until we pass the timeout
            while (true)
            {
                try
                {
                    return File.Open(file, mode, access, share);
                }
                // The point of open file is to ignore intermittent failures or locking issues;
                // file not found is a permanent failure.
                catch (FileNotFoundException)
                {
                    throw;
                }
                catch (DirectoryNotFoundException)
                {
                    throw;
                }
                catch (Exception)
                {
                    if (DateTime.Now - start > timeout)
                    {
                        throw;
                    }

                    Trace.WriteLine("Waiting on lock for file " + file + "...");
                    Thread.Sleep(TimeSpan.FromMilliseconds(100));
                }
            }
        }

        public static StreamWriter CreateFileTimeout(string file, TimeSpan timeout)
        {
            var start = DateTime.Now;
            // We loop until we pass the timeout
            while (true)
            {
                try
                {
                    var writer = File.CreateText(file);
                    return writer;
                }
                catch (Exception)
                {
                    if (DateTime.Now - start > timeout)
                    {
                        throw;
                    }

                    Trace.WriteLine("Waiting on lock for file " + file + "...");
                    Thread.Sleep(TimeSpan.FromMilliseconds(100));
                }
            }
        }

        public static void WriteLogLine(string file, string message, TimeSpan timeout)
        {
            var start = DateTime.Now;
            // We loop until we pass the timeout
            while (true)
            {
                try
                {
                    var writer = File.AppendText(file);
                    writer.WriteLine(message);
                    writer.Flush();
                    writer.Close();
                    return;
                }
                catch (Exception)
                {
                    if (DateTime.Now - start > timeout)
                    {
                        throw;
                    }

                    Trace.WriteLine("Waiting on lock for file " + file + "...");
                    Thread.Sleep(TimeSpan.FromMilliseconds(100));
                }
            }
        }

        public static bool ArraysEqual(object[] array1, object[] array2, bool careAboutOrder)
        {
            // If the lengths aren't equal, don't bother.
            if (array1.Length != array2.Length)
            {
                return false;
            }

            // If we don't care about the order, we sort them so that they will compare in the right order
            if (!careAboutOrder)
            {
                array1 = (object[])array1.Clone();
                Array.Sort(array1);
                array2 = (object[])array2.Clone();
                Array.Sort(array2);
            }

            // If any are not equal, we return false.  Otherwise it is true.
            return !array1.Where((t, i) => !t.Equals(array2[i])).Any();
        }

        public static NameValueCollection GetOptions(params string[] args)
        {
            var retval = new NameValueCollection();
            foreach (var arg in args)
            {
                if (arg[0] == '/')
                {
                    // Read enlistment info
                    var nameValuePair = arg.Substring(1).Split(new[] { ':' }, 2);
                    retval.Add(nameValuePair[0], nameValuePair.Length == 2 ? nameValuePair[1] : "");
                }
                else
                {
                    retval.Add("remainingargs", arg);
                }
            }
            return retval;
        }

        public static FileSystemWatcher InitWatcher(string dir, string filter, FileSystemEventHandler handler)
        {
            // Strip off any trailing \ so that we will be able to display things appropriately when we call the initial handler() stuff
            while (dir.EndsWith("\\"))
            {
                dir = dir.Substring(0, dir.Length - 1);
            }

            // Create a new FileSystemWatcher and set its properties.
            var watcher = new FileSystemWatcher(dir, filter);

            // Add event handlers.
            watcher.Changed += handler;
            watcher.Created += handler;
            watcher.Deleted += handler;

            // Read the initial files and send Created notifications
            foreach (var file in Directory.GetFiles(dir, filter))
            {
                handler(null, new FileSystemEventArgs(WatcherChangeTypes.Created, dir, file.Substring(dir.Length + 1)));
            }

            // Begin watching.
            watcher.EnableRaisingEvents = true;
            return watcher;
        }

        public static string Dump(object o)
        {
            var builder = new StringBuilder();
            Dump(builder, o, new ArrayList(), 0, false);
            return builder.ToString();
        }

        private static void Indent(StringBuilder builder, int indent)
        {
            for (var i = 0; i < indent; i++)
            {
                builder.Append("  ");
            }
        }

        private static void Dump(StringBuilder builder, object o, ArrayList foundSoFar, int indent, bool indentNow)
        {
            if (indentNow)
            {
                Indent(builder, indent);
            }

            // Check if we have already printed this exact object
            foreach (var alreadyFound in foundSoFar.Cast<object>().Where(alreadyFound => o == alreadyFound))
            {
                builder.Append("<already printed this object>");
            }
            foundSoFar.Add(o);

            if (o is IDictionary)
            {
                builder.Append(o);
                builder.Append(":\n");
                foreach (DictionaryEntry entry in (IDictionary)o)
                {
                    Indent(builder, indent);
                    builder.Append("Key: ");
                    Dump(builder, entry.Key, foundSoFar, indent + 1, false);

                    Indent(builder, indent);
                    builder.Append("Value: ");
                    Dump(builder, entry.Value, foundSoFar, indent + 1, false);
                }
            }
            else if (o is ICollection)
            {
                builder.Append(o);
                builder.Append(":\n");
                foreach (var obj in (ICollection)o)
                {
                    Dump(builder, obj, foundSoFar, indent + 1, true);
                }
            }
            else
            {
                builder.Append(o);
                builder.Append("\n");
            }
        }

        public static void SendEmail(string from, string[] to, string[] cc, string subject, string body)
        {
            var args = "";
            args += " from:" + from;
            if (to != null)
            {
                args += " to:" + string.Join(";", to);
            }
            if (cc != null)
            {
                args += " cc:" + string.Join(";", cc);
            }
            args += " \"subject:" + subject + "\"";
            args += " server:smtphost.redmond.corp.microsoft.com";

            // Write out the body of the mail to a file
            var bodyFile = Path.GetTempFileName();
            var writer = new StreamWriter(bodyFile);
            try
            {
                try
                {
                    writer.Write(body);
                }
                finally
                {
                    writer.Close();
                }
                args += " \"body:" + bodyFile + "\"";

                RunCommand("smartmail smtp" + args);
            }
            finally
            {
                File.Delete(bodyFile);
            }
        }

        /// <summary>
        /// run process until process exits
        /// </summary>
        /// <param name="fileName">assembly path</param>
        /// <param name="arguments">arguments passed to process</param>
        /// <returns>process exitcode</returns>
        public static int RunProcess(string fileName, string arguments)
        {
            var process = new Process();
            try
            {
                var startInfo = new ProcessStartInfo(fileName, arguments) {UseShellExecute = false};
                process.StartInfo = startInfo;
                process.Start();
                process.WaitForExit();
                
            }
            catch (Exception ex)
            {

                Console.WriteLine(ex.Message);
                return process.ExitCode;
            }
        
            
            return process.ExitCode;
        }

        /// <summary>
        /// get the last written files founded by the searchPatten
        /// </summary>
        /// <param name="path">search root path</param>
        /// <param name="searchPattern">search patten</param>
        /// <param name="searchOption"></param>
        /// <param name="timespan">max seconds threshhold</param>
        /// <returns>file name lists</returns>
        public static List<string> GetFilesByLastWrite(string path, string searchPattern, SearchOption searchOption = 0, int timespan = 30)
        {
            var now = DateTime.Now;
            return Directory.GetFiles(path, searchPattern, searchOption)
                            .Where(f => (now - File.GetLastWriteTime(f)).TotalSeconds < timespan)
                            .ToList();
        }
    }

    [Serializable]
    public class CommandFailureException : Exception
    {
        public CommandFailureException(string command, int exitCode)
            : base("Command '" + command + "' failed with exit code " + exitCode)
        {
            Command = command;
            ExitCode = exitCode;
        }
        public CommandFailureException(string command, int exitCode, string output)
            : this(command, exitCode)
        {
            Output = output;
        }

        public string Output;
        public string Command;
        public int ExitCode;

        protected CommandFailureException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            Command = info.GetString("Command");
            ExitCode = info.GetInt32("ExitCode");
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue("Command", Command);
            info.AddValue("ExitCode", ExitCode);
        }

    }
}
