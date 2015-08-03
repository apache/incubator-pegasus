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
            _address = dsn_address_t.New();
            Native.dsn_address_get_invalid(ref _address);
            _gch = GCHandle.Alloc(this);
        }

        private void ReleaseUnmanagedResources()
        {
            _gch.Free();
        }

        public abstract ErrorCode Start(string[] args);

        public abstract void Stop(bool cleanup = false);

        public bool IsStarted() { return _started; }

        public dsn_address_t PrimaryAddress() { return _address; }

        public string Name() { return _name; }

        private bool          _started;
        private dsn_address_t _address;
        private string        _name;
        private GCHandle      _gch;

        private static IntPtr AppCreate<T>()
            where T : ServiceApp, new()
        {
            ServiceApp app = new T();
            return (IntPtr)(app._gch);
        }

        private static int AppStart(IntPtr app_handle, int argc, string[] argv)
        {
            GCHandle h = (GCHandle)app_handle;
            ServiceApp sapp = h.Target as ServiceApp;
            var r = sapp.Start(argv);
            if (r == 0)
            {
                sapp._started = true;
                Native.dsn_primary_address2(ref sapp._address);
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
            Native.dsn_register_app_role(type_name, AppCreate<T>, AppStart, AppDestroy);
        }
    };

    
}
