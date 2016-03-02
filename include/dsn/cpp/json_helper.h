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
 *     helper for json serialization
 *
 * Revision history:
 *     Dec., 2015, @Tianyi Wang, first version
 */

#pragma once

#include <vector>
#include <unordered_map>
#include <set>
#include <sstream>
#include <string>
#include <type_traits>
#include <dsn/cpp/autoref_ptr.h>

#define JSON_DICT_ENTRY(out, prefix, T) out << "\""#T"\":"; ::std::json_forwarder<std::decay<decltype((prefix).T)>::type>::call(out, (prefix).T)
#define JSON_DICT_ENTRIES2(out, prefix, T1, T2) JSON_DICT_ENTRY(out, prefix, T1); out << ","; JSON_DICT_ENTRY(out, prefix, T2)
#define JSON_DICT_ENTRIES3(out, prefix, T1, T2, T3) JSON_DICT_ENTRIES2(out, prefix, T1, T2); out << ","; JSON_DICT_ENTRY(out, prefix, T3)
#define JSON_DICT_ENTRIES4(out, prefix, T1, T2, T3, T4) JSON_DICT_ENTRIES3(out, prefix, T1, T2, T3); out << ","; JSON_DICT_ENTRY(out, prefix, T4)
#define JSON_DICT_ENTRIES5(out, prefix, T1, T2, T3, T4, T5) JSON_DICT_ENTRIES4(out, prefix, T1, T2, T3, T4); out << ","; JSON_DICT_ENTRY(out, prefix, T5)
#define JSON_DICT_ENTRIES6(out, prefix, T1, T2, T3, T4, T5, T6) JSON_DICT_ENTRIES5(out, prefix, T1, T2, T3, T4, T5); out << ","; JSON_DICT_ENTRY(out, prefix, T6)
#define JSON_DICT_ENTRIES7(out, prefix, T1, T2, T3, T4, T5, T6, T7) JSON_DICT_ENTRIES6(out, prefix, T1, T2, T3, T4, T5, T6); out << ","; JSON_DICT_ENTRY(out, prefix, T7)
#define JSON_DICT_ENTRIES8(out, prefix, T1, T2, T3, T4, T5, T6, T7, T8) JSON_DICT_ENTRIES7(out, prefix, T1, T2, T3, T4, T5, T6, T7); out << ","; JSON_DICT_ENTRY(out, prefix, T8)
#define JSON_DICT_ENTRIES9(out, prefix, T1, T2, T3, T4, T5, T6, T7, T8, T9) JSON_DICT_ENTRIES8(out, prefix, T1, T2, T3, T4, T5, T6, T7, T8); out << ","; JSON_DICT_ENTRY(out, prefix, T9)

#define JSON_ENTRIES_GET_MACRO(ph1,ph2,ph3,ph4,ph5,ph6,ph7,ph8,ph9, NAME, ...) NAME
//workaround due to the way VC handles "..."
#define JSON_ENTRIES_GET_MACRO_(tuple) JSON_ENTRIES_GET_MACRO tuple
#define JSON_DICT_ENTRIES(out, prefix, ...) out<<"{";JSON_ENTRIES_GET_MACRO_((__VA_ARGS__, JSON_DICT_ENTRIES9, JSON_DICT_ENTRIES8, JSON_DICT_ENTRIES7, JSON_DICT_ENTRIES6, JSON_DICT_ENTRIES5, JSON_DICT_ENTRIES4, JSON_DICT_ENTRIES3, JSON_DICT_ENTRIES2)) (out, prefix, __VA_ARGS__); out<<"}"

//parameters: fields to be serialized
#define DEFINE_JSON_SERIALIZATION(...) void json_state(std::stringstream& out) const {JSON_DICT_ENTRIES(out, *this, __VA_ARGS__);}
    
namespace std {

template<typename> class json_forwarder;


template<typename T> inline void json_encode(std::stringstream& out, const T& t)
{
    out << t;
}

template<typename T> inline void json_encode_iterable(std::stringstream& out, const T& t)
{
    out << "[";
    for (auto it = t.begin(); it != t.end(); ++it)
    {
        json_forwarder<typename std::decay < decltype(*it) > ::type>::call(out, *it);
        if (std::next(it) != t.end())
        {
            out << ",";
        }
    }
    out << "]";
}

template<typename T> inline void json_encode_map(std::stringstream& out, const T& t)
{
    out << "{";
    for (auto it = t.begin(); it != t.end(); ++it)
    {
        json_forwarder<typename std::decay < decltype(it->first) > :: type>::call(out, it->first);
        out << ":";
        json_forwarder<typename std::decay < decltype(it->second) > ::type>::call(out, it->second);
        if (std::next(it) != t.end())
        {
            out << ",";
        }
    }
    out << "}";
}

template<typename T> inline void json_encode(std::stringstream& out, const std::vector<T>& t)
{
    json_encode_iterable(out, t);
}

template<typename T> inline void json_encode(std::stringstream& out, const std::set<T>& t)
{
    json_encode_iterable(out, t);
}

template<typename T1, typename T2> inline void json_encode(std::stringstream& out, const std::unordered_map<T1, T2>& t)
{
    json_encode_map(out, t);
}

template<typename T1, typename T2> inline void json_encode(std::stringstream& out, const std::map<T1, T2>& t)
{
    json_encode_map(out, t);
}

template<typename T> inline void json_encode(std::stringstream& out, const dsn::ref_ptr<T>& t)
{
    json_encode(out, *t);
}

inline void json_encode(std::stringstream& out, const std::string& t)
{
    out << "\"" << t << "\"";
}
inline void json_encode(std::stringstream& out, const char* t)
{
    out << "\"" << t << "\"";
}

template<typename T>
class json_forwarder {
private:
    //check if C has C.json_state(sstream&) function
    template<typename C>
    static auto check_json_state(C*)
        -> typename std::is_same<decltype(std::declval<C>().json_state(std::declval<std::stringstream&>())), void>::type; 

    template<typename>
    static std::false_type check_json_state(...);

    //check if C has C->json_state(sstream&) function
    template<typename C>
    static auto p_check_json_state(C*)
        -> typename std::is_same<decltype(std::declval<C>()->json_state(std::declval<std::stringstream&>())),void>::type;

    template<typename>
    static std::false_type p_check_json_state(...);

    typedef decltype(check_json_state<T>(0)) has_json_state;
    typedef decltype(p_check_json_state<T>(0)) p_has_json_state;

    //internal serialization
    static void call_inner(std::stringstream&out, const T& t, std::true_type, std::false_type)
    {
        t.json_state(out);
    }
    //internal serialization
    static void call_inner(std::stringstream&out, const T& t, std::false_type, std::true_type)
    {
        t->json_state(out);
    }
    //internal serialization
    static void call_inner(std::stringstream&out, const T& t, std::true_type, std::true_type)
    {
        t->json_state(out);
    }
    //external serialization
    static void call_inner(std::stringstream&out, const T& t, std::false_type, std::false_type)
    {
        json_encode(out, t);
    }
public:
    static void call(std::stringstream&out, const T& t)
    {
        call_inner(out, t, has_json_state{}, p_has_json_state{});
    }
};

}
