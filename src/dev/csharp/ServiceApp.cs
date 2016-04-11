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
 *     Feb., 2016, @imzhenyu (Zhenyu Guo), done in rDSN.CSharp project and copied here
 *     xxxx-xx-xx, author, fix bug about xxx
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.InteropServices;

namespace dsn.dev.csharp
{
    public abstract class ServiceApp
    {
        public ServiceApp()
        {
            _started = false;
            _address = new RpcAddress();
            _gch = GCHandle.Alloc(this);
        }

        private void ReleaseUnmanagedResources()
        {
            _gch.Free();
        }

        public abstract ErrorCode Start(string[] args);

        public abstract void Stop(bool cleanup = false);

        public bool IsStarted() { return _started; }

        public RpcAddress PrimaryAddress() { return _address; }

        public string Name() { return _name; }

        public UInt64 Gpid() { return _gpid; }

        private bool          _started;
        private RpcAddress    _address;
        private string        _name;
        private GCHandle      _gch;
        private UInt64        _gpid;

        private static IntPtr AppCreate<T>(string tname, UInt64 gpid)
            where T : ServiceApp, new()
        {
            ServiceApp app = new T();
            app._gpid = gpid;
            return (IntPtr)(app._gch);
        }

        private static int AppStart(IntPtr app_handle, string[] argv)
        {
            GCHandle h = (GCHandle)app_handle;
            ServiceApp sapp = h.Target as ServiceApp;
            var r = sapp.Start(argv);
            if (r == 0)
            {
                sapp._started = true;
                sapp._address = new RpcAddress(Native.dsn_primary_address());
                sapp._name = argv[0];
            }
            return r;
        }

        private static void AppDestroy(IntPtr app_handle, bool cleanup)
        {
            GCHandle h = (GCHandle)app_handle;
            ServiceApp sapp = h.Target as ServiceApp;
            sapp.Stop(cleanup);
            sapp._started = false;
            sapp.ReleaseUnmanagedResources();
        }

        public static void RegisterApp<T>(string type_name)
            where T : ServiceApp, new()
        {
            Native.dsn_register_app_managed(type_name, AppCreate<T>, AppStart, AppDestroy);
        }
    };

    
}
