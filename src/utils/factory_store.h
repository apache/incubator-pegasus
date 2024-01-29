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

#pragma once

#include "utils/singleton_store.h"

namespace dsn {

enum provider_type
{
    PROVIDER_TYPE_MAIN = 0,
    PROVIDER_TYPE_ASPECT = 1
};

namespace utils {

// Factory registry for object creation.
template <typename TResult>
class factory_store
{
public:
    template <typename TFactory>
    static bool register_factory(const char *name, TFactory factory, ::dsn::provider_type type)
    {
        factory_entry entry;
        entry.dummy = nullptr;
        entry.factory = (void *)factory;
        entry.type = type;
        return singleton_store<std::string, factory_entry>::instance().put(std::string(name),
                                                                           entry);
    }

    template <typename TFactory>
    static TFactory get_factory(const char *name, ::dsn::provider_type type)
    {
        factory_entry entry;
        if (singleton_store<std::string, factory_entry>::instance().get(std::string(name), entry)) {
            if (entry.type != type) {
                report_error(name, type);
                return nullptr;
            } else {
                TFactory f;
                f = *(TFactory *)&entry.factory;
                return f;
            }
        } else {
            report_error(name, type);
            return nullptr;
        }
    }

    template <typename T1, typename T2, typename T3, typename T4, typename T5>
    static TResult *
    create(const char *name, ::dsn::provider_type type, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5)
    {
        typedef TResult *(*TFactory)(T1, T2, T3, T4, T5);
        TFactory f = get_factory<TFactory>(name, type);
        return f ? f(t1, t2, t3, t4, t5) : nullptr;
    }

    template <typename T1, typename T2, typename T3, typename T4>
    static TResult *create(const char *name, ::dsn::provider_type type, T1 t1, T2 t2, T3 t3, T4 t4)
    {
        typedef TResult *(*TFactory)(T1, T2, T3, T4);
        TFactory f = get_factory<TFactory>(name, type);
        return f ? f(t1, t2, t3, t4) : nullptr;
    }

    template <typename T1, typename T2, typename T3>
    static TResult *create(const char *name, ::dsn::provider_type type, T1 t1, T2 t2, T3 t3)
    {
        typedef TResult *(*TFactory)(T1, T2, T3);
        TFactory f = get_factory<TFactory>(name, type);
        return f ? f(t1, t2, t3) : nullptr;
    }

    template <typename T1, typename T2>
    static TResult *create(const char *name, ::dsn::provider_type type, T1 t1, T2 t2)
    {
        typedef TResult *(*TFactory)(T1, T2);
        TFactory f = get_factory<TFactory>(name, type);
        return f ? f(t1, t2) : nullptr;
    }

    template <typename T1>
    static TResult *create(const char *name, ::dsn::provider_type type, T1 t1)
    {
        typedef TResult *(*TFactory)(T1);
        TFactory f = get_factory<TFactory>(name, type);
        return f ? f(t1) : nullptr;
    }

    static TResult *create(const char *name, ::dsn::provider_type type)
    {
        typedef TResult *(*TFactory)();
        TFactory f = get_factory<TFactory>(name, type);
        return f ? f() : nullptr;
    }

private:
    static void report_error(const char *name, ::dsn::provider_type type)
    {
        printf("cannot find factory '%s' with factory type %s\n",
               name,
               type == PROVIDER_TYPE_MAIN ? "provider" : "aspect");

        std::vector<std::string> keys;
        singleton_store<std::string, factory_entry>::instance().get_all_keys(keys);
        printf("\tthe following %u factories are registered:\n", static_cast<int>(keys.size()));
        for (auto it = keys.begin(); it != keys.end(); ++it) {
            factory_entry entry;
            singleton_store<std::string, factory_entry>::instance().get(*it, entry);
            printf("\t\t%s (type: %s)\n",
                   it->c_str(),
                   entry.type == PROVIDER_TYPE_MAIN ? "provider" : "aspect");
        }
        printf("\tPlease specify the correct factory name in your tool_app or in configuration "
               "file\n");
    }

private:
    struct factory_entry
    {
        TResult *dummy;
        void *factory;
        ::dsn::provider_type type;

        factory_entry()
        {
            dummy = nullptr;
            factory = nullptr;
            type = PROVIDER_TYPE_MAIN;
        }
    };
};
}
} // end namespace dsn::utils
