# pragma once

# include <rdsn/internal/singleton_store.h>
# include <rdsn/internal/logging.h>

namespace rdsn { namespace utils {

template<typename TResult>
class factory_store
{
public:
    template<typename TFactory>
    static bool register_factory(const char* name, TFactory factory, int type)
    {
        factory_entry entry;
        entry.dummy = nullptr;
        entry.factory = (void*)factory;
        entry.type = type;
        return singleton_store<std::string, factory_entry>::instance().put(std::string(name), entry);
    }

    template<typename TFactory>
    static TFactory get_factory(const char* name, int type)
    {
        factory_entry entry;
        if (singleton_store<std::string, factory_entry>::instance().get(std::string(name), entry))
        {
            if (entry.type != type)
            {
                report_error(name, type);
                return nullptr;
            }
            else
            {
                TFactory f;
                f = *(TFactory*)&entry.factory;
                return f;
            }
        }
        else
        {
            report_error(name, type);
            return nullptr;
        }
    }

    template<typename T1, typename T2, typename T3, typename T4, typename T5>
    static TResult* create(const char* name, int type, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5)
    {
        typedef TResult* (*TFactory)(T1,T2,T3,T4,T5);
        TFactory f = get_factory<TFactory>(name, type);
        return f ? f(t1, t2, t3, t4, t5) : nullptr;
    }

    template<typename T1, typename T2, typename T3, typename T4>
    static TResult* create(const char* name, int type, T1 t1, T2 t2, T3 t3, T4 t4)
    {
        typedef TResult* (*TFactory)(T1,T2,T3,T4);
        TFactory f = get_factory<TFactory>(name, type);
        return f ? f(t1, t2, t3, t4) : nullptr;
    }

    template<typename T1, typename T2, typename T3>
    static TResult* create(const char* name, int type, T1 t1, T2 t2, T3 t3)
    {
        typedef TResult* (*TFactory)(T1,T2,T3);
        TFactory f = get_factory<TFactory>(name, type);
        return f ? f(t1, t2, t3) : nullptr;
    }

    template<typename T1, typename T2>
    static TResult* create(const char* name, int type, T1 t1, T2 t2)
    {
        typedef TResult* (*TFactory)(T1,T2);
        TFactory f = get_factory<TFactory>(name, type);
        return f ? f(t1, t2) : nullptr;
    }

    template<typename T1>
    static TResult* create(const char* name, int type, T1 t1)
    {
        typedef TResult* (*TFactory)(T1);
        TFactory f = get_factory<TFactory>(name, type);
        return f ? f(t1) : nullptr;
    }

    static TResult* create(const char* name, int type)
    {
        typedef TResult* (*TFactory)();
        TFactory f = get_factory<TFactory>(name, type);
        return f ? f() : nullptr;
    }

private:
    static void report_error(const char* name, int type)
    {
        rlog(log_level_FATAL, "Cannot find factory '%s' with factory type %s", name, type == PROVIDER_TYPE_MAIN ? "provider" : "aspect");

        printf("Cannot find factory '%s' with factory type %s\n", name, type == PROVIDER_TYPE_MAIN ? "provider" : "aspect");

        std::vector<std::string> keys;
        singleton_store<std::string, factory_entry>::instance().get_all_keys(keys);
        printf ("\tThe following %u factories are registered:\n", (int)keys.size());
        for (auto it = keys.begin(); it != keys.end(); it++)
        {
            factory_entry entry;
            singleton_store<std::string, factory_entry>::instance().get(*it, entry);        
            printf("\t\t%s (type: %s)\n", it->c_str(), entry.type == PROVIDER_TYPE_MAIN ? "provider" : "aspect");
        }
        printf ("\tPlease specify the correct factory name in your tool_app or in configuration file\n");
    }

private:
    struct factory_entry
    {
        TResult* dummy;
        void*    factory;
        int      type;
    };
};

}} // end namespace rdsn::utils
