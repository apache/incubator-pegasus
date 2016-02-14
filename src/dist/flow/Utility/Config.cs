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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Diagnostics;

namespace rDSN.Tron.Utility
{
    public class Configuration : Singleton<Configuration>
    {
        private Dictionary<string, Dictionary<string, string>> _sectionParams = new Dictionary<string, Dictionary<string, string>>();
        private const string _defaultConfigFile = "config.ini";

        public Configuration()
        {
            Set("TempDir", ".\\");
            Set("Verbose", "true");
            Set("PacketBatchSize", "64"); // KB
            Set("PacketBatchInterval", "5"); // ms
            Set("AppDomainEnabled", "false");
            Set("LogFile", "stdout");
            Set("VerboseRpc", "false");
            Set("VerboseUDP", "false");
            Set("LogPort", "localhost:50002");
            Set("RpcThreadQueueThrottlingLength", "10000");
            Set("MaxSendQueueBufferPerSocketInMB", "32");
            Set("MaxLocalWorkingSetPerJobInMB", "512");
            Set("TcpBufferSizeInMbPerSocket", "32");
            Set("NodeStatReportInterval", "1000"); // ms
            Set("DefaultStageExecutionTimeout", "5000"); // ms

            //Set("Client", "XceedLicenseKey", "UNKNOWN");
            Set("Client", "XceedLicenseKey", "CHT43-G1ENG-TRMGE-3YNA");

            Set("Agent", "Port", "53001");
            Set("Agent", "GracePeriod", "15");
            Set("Agent", "LeasePeriod", "10");
            Set("Agent", "MaxWorkingSetMB", "200");

            LoadConfig();
            SetupLogFile();
            SetupMiniDump();

            if (Get<bool>("DebugBreak", false))
            {
                Console.WriteLine("Press any key to continue ...");
                Console.ReadKey();
            }
        }

        public Configuration(string configFile)
        {
            LoadConfig(configFile);
        }

        private void SetupLogFile()
        {
            string logFile = Get<string>("LogFile");
            LogConsumer consumer = new LogConsumer(logFile);
            Trace.Listeners.Add(consumer);
            //Debug.Listeners.Add(consumer);
        }

        private void SetupMiniDump()
        {
            AppDomain.CurrentDomain.UnhandledException += this.MyUnhandledExceptionEventHandler;
        }

        private void MyUnhandledExceptionEventHandler(object sender, UnhandledExceptionEventArgs e)
        {
            MiniDumper.Write((".\\Minidump." + DateTime.Now.ToLongTimeString()).Replace(':', '-') + ".mdmp");
        }

        /// <summary>
        /// Load config from the default config file.
        /// </summary>
        private void LoadConfig()
        {
            LoadConfig(_defaultConfigFile);
        }

        /// <summary>
        /// Load config from the specified config file.
        /// </summary>
        /// <param name="configFile">The path of the config file.</param>
        public void LoadConfig(string configFile)
        {
            string currSec = "";
            string newSec = "";
            string line;            
            StreamReader sr = null;
            Dictionary<string, string> currentDict = null;

            try
            {
                sr = new StreamReader(configFile);
                while (true)
                {
                    if (sr.EndOfStream)
                    {
                        break;
                    }
                    line = sr.ReadLine();

                    if (line.IndexOf('#') != -1)
                    {
                        line = line.Substring(0, line.IndexOf('#'));
                    }

                    line = line.Trim(new char[] {' ', '\t', '\r', '\n'});

                    if ((line == "") || (line[0] == '#')) continue;
                    if (line[0] == '[')
                    {
                        newSec = line.Substring(1, line.Length - 2).Trim();
                        if (_sectionParams.TryGetValue(newSec, out currentDict) == false)
                        {
                            currentDict = new Dictionary<string, string>();
                            _sectionParams.Add(newSec, currentDict);
                        }
                        currSec = newSec;
                    }
                    else
                    {
                        if (currentDict == null)
                        {
                            Console.WriteLine("No section defined for current config line: " + line);
                            continue;
                        }

                        // New value pair
                        string name, value = "";
                        int ep = line.IndexOf('=');
                        if (ep == -1)
                        {
                            name = line.Trim();
                        }
                        else
                        {
                            name = line.Substring(0, ep).Trim();
                            value = line.Substring(ep + 1).Trim();
                        }

                        if (currentDict.ContainsKey(name))
                        {
                            Console.WriteLine("[" + currSec + "]" + name + " is redefined: "
                                + currentDict[name] + " -> " + value);
                            currentDict[name] = value;
                        }
                        else
                        {
                            currentDict.Add(name, value);
                        }
                    }
                }
                sr.Close();
            }
            catch (Exception e)
            {
                if (e.GetType() == typeof(FileNotFoundException))
                {
                    Console.WriteLine("Config file not found. Using default.");
                }
                else
                {
                    Console.WriteLine("Error loading config. " + e.Message);
                }

                if (sr != null)
                    sr.Close();
            }
        }

        /// <summary>
        /// Set an option in a section to some value.
        /// </summary>
        /// <param name="section">The section of the option.</param>
        /// <param name="name">The name of the option.</param>
        /// <param name="value">The value of the option.</param>
        public void Set(string section, string name, string value)
        {
            if (!_sectionParams.ContainsKey(section))
                _sectionParams[section] = new Dictionary<string, string>();
            _sectionParams[section][name] = value;
        }

        public void Set(string name, string value)
        {
            Set("general", name, value);
        }
        
        public Dictionary<string, string> GetSection(string sectionName)
        {
            Dictionary<string, string> dict = null;
            _sectionParams.TryGetValue(sectionName, out dict);
            return dict;
        }

        /// <summary>
        /// Get the value of an option in one section of type T.
        /// </summary>
        /// <typeparam name="T">The type of the option.</typeparam>
        /// <param name="section">The section of the option.</param>
        /// <param name="name">The name of the option.</param>
        /// <returns>The value of the option.</returns>
        /// 
        private T GetValue<T>(string value, string name)
        {
            if (typeof(T) == typeof(string))
                return (T)(object)value;

            System.Reflection.MethodInfo method = null;
            try
            {
                method = typeof(T).GetMethod("Parse", new Type[] { typeof(string) });
            }
            catch (Exception) { }

            if (method != null)
            {
                try
                {
                    return (T)(method.Invoke(null, new object[] { value }));
                }
                catch (Exception e)
                {
                    if (e.InnerException.GetType() == typeof(FormatException))
                    {
                        Console.WriteLine("format wrong for item " + name);
                    }
                    return default(T);
                }
            }
            else if (typeof(T).IsEnum)
            {
                return (T)Enum.Parse(typeof(T), value);
            }
            else
            {
                Console.WriteLine("config item type wrong: " + name);
                return default(T);
            }
        }

        public bool TryGet<T>(string section, string name, out T val)
        {
            val = default(T);


            Dictionary<string, string> s;
            if (!_sectionParams.TryGetValue(section, out s))
                return false;

            string ret;
            if (!s.TryGetValue(name, out ret) || ret == null)
                return false;

            val = GetValue<T>(ret, name);
            return true;
        }

        public T Get<T>(string section, string name, T defaultValue = default(T))
        {
            T v;
            if (TryGet<T>(section, name, out v))
                return v;
            else
                return defaultValue;
        }

        public bool TryGet<T>(string name, out T val)
        {
            return TryGet<T>("general", name, out val);
        }

        public T Get<T>(string name, T defaultValue = default(T))
        {
            return Get<T>("general", name, defaultValue);
        }

        public List<string> GetAllSectionNames()
        {
            List<string> names = new List<string>();
            foreach (var kv in _sectionParams)
            {
                names.Add(kv.Key);
            }
            return names;
        }

        /// <summary>
        /// Save config to the default config file.
        /// </summary>
        public void SaveConfig()
        {
            SaveConfig(_defaultConfigFile);
        }

        /// <summary>
        /// Save config the the specified config file.
        /// </summary>
        /// <param name="configFile">The path of the config file.</param>
        public void SaveConfig(string configFile)
        {
            StreamWriter sw = new StreamWriter(configFile);
            if (sw == null)
            {
                Console.WriteLine("Failed to write config to " + configFile);
                return;
            }

            foreach (KeyValuePair<string, Dictionary<string, string>> sections in _sectionParams)
            {
                sw.WriteLine("[" + sections.Key + "]");
                foreach (KeyValuePair<string, string> pair in sections.Value)
                {
                    sw.WriteLine(pair.Key + " = " + pair.Value.ToString());
                }
                sw.WriteLine();
            }

            sw.Close();
        }
    }
}
