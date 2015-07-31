using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.InteropServices;

namespace dsn.dev.csharp
{
    public abstract class service_app : ResourceHolder
    {
        public service_app()
        {
            _started = false;
            core.dsn_address_get_invalid(out _address);
            _gch = GCHandle.Alloc(this);
        }

        protected override void ReleaseUnmanagedResources()
        {
            _gch.Free();
        }

        public abstract error_code start(string[] args);

        public abstract void stop(bool cleanup = false);

        public bool is_started() { return _started; }

        public dsn_address_t primary_address() { return _address; }

        public string name() { return _name; }

        private bool          _started;
        private dsn_address_t _address;
        private string        _name;
        private GCHandle      _gch;

        private static IntPtr app_create<T>()
            where T : service_app, new()
        {
            service_app app = new T();
            return (IntPtr)(app._gch);
        }

        private static int app_start(IntPtr app_handle, int argc, string[] argv)
        {
            GCHandle h = (GCHandle)app_handle;
            service_app sapp = h.Target as service_app;
            var r = sapp.start(argv);
            if (r == 0)
            {
                sapp._started = true;
                core.dsn_primary_address2(out sapp._address);
                sapp._name = argv[0];
            }
            return r;
        }

        private static void app_destroy(IntPtr app_handle, bool cleanup)
        {
            GCHandle h = (GCHandle)app_handle;
            service_app sapp = h.Target as service_app;
            sapp.stop(cleanup);
            sapp._started = false;
        }

        public static void register_app<T>(string type_name)
            where T : service_app, new()
        {
            core.dsn_register_app_role(type_name, app_create<T>, app_start, app_destroy);
        }
    };

    
}
