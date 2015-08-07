
# include <dsn/service_api_c.h>
# include <dsn/internal/dsn_types.h>

# include <dsn/tool/simulator.h>
# include <dsn/tool/nativerun.h>
# include <dsn/toollet/tracer.h>
# include <dsn/toollet/profiler.h>
# include <dsn/toollet/fault_injector.h>

# include <dsn/tool/providers.common.h>
# include <dsn/tool/nfs_node_simple.h>

//# include <dsn/thrift_helper.h>

# if defined(__GNUC__) || defined(_WIN32)
# else
# error "dsn init on shared lib loading is not supported on this platform yet"
# endif

# if defined(__GNUC__)
__attribute__((constructor))
# endif
static void dsn_init_on_load()
{
    // register all providers
    dsn::tools::register_common_providers();
    dsn::tools::register_component_provider<::dsn::service::nfs_node_simple>("dsn::service::nfs_node_simple");

    //dsn::tools::register_component_provider<dsn::thrift_binary_message_parser>("thrift");

    // register all possible tools and toollets
    dsn::tools::register_tool<dsn::tools::nativerun>("nativerun");
    dsn::tools::register_tool<dsn::tools::simulator>("simulator");
    dsn::tools::register_toollet<dsn::tools::tracer>("tracer");
    dsn::tools::register_toollet<dsn::tools::profiler>("profiler");
    dsn::tools::register_toollet<dsn::tools::fault_injector>("fault_injector");
}

# ifdef _WIN32

#ifdef _MANAGED
#pragma managed(push, off)
#endif

bool APIENTRY DllMain(HMODULE hModule,
    DWORD  ul_reason_for_call,
    void* lpReserved
    )
{
    switch (ul_reason_for_call)
    {
    case DLL_PROCESS_ATTACH:
        dsn_init_on_load();
        break;
    case DLL_THREAD_ATTACH:
    case DLL_THREAD_DETACH:
        break;
    case DLL_PROCESS_DETACH:
        break;
    }
    return TRUE;
}

#ifdef _MANAGED
#pragma managed(pop)
#endif

# endif