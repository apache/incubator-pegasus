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
 *     state extension for cpp objects
 *
 * Revision history:
 *     Mar., 2015, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#pragma once

#include "utils/utils.h"
#include <vector>
#include <atomic>
#include <cstring>
#include <cassert>

namespace dsn {
/*!
@addtogroup tool-api-providers
@{
*/
typedef void (*extension_deletor)(void *);
typedef void *(*extension_creator)(void *);

class extensible
{
public:
    extensible(uint64_t *ptr, uint32_t count)
    {
        _ptr = ptr;
        _count = count;
    }

    void set_extension(uint32_t id, uint64_t data)
    {
        assert(id < _count);
        _ptr[id] = data;
    }

    uint64_t &get_extension(uint32_t id)
    {
        assert(id < _count);
        return _ptr[id];
    }

private:
    uint64_t *_ptr;
    uint32_t _count;
};

template <typename T, const int MAX_EXTENSION_COUNT>
class extensible_object : public extensible
{
public:
    static const uint32_t INVALID_SLOT = 0xffffffff;
    static const uint64_t INVALID_VALUE = 0x0ULL;

public:
    extensible_object() : extensible(_extensions, MAX_EXTENSION_COUNT)
    {
        memset((void *)_extensions, 0, sizeof(_extensions));
    }

    ~extensible_object()
    {
        int maxId = static_cast<int>(get_extension_count());

        for (int i = 0; i < maxId; i++) {
            if (_extensions[i] != extensible_object::INVALID_VALUE &&
                s_extensionDeletors[i] != nullptr) {
                s_extensionDeletors[i]((void *)_extensions[i]);
            }
        }
    }

    void copy_to(extensible_object<T, MAX_EXTENSION_COUNT> &r)
    {
        int maxId = static_cast<int>(get_extension_count());

        for (int i = 0; i < maxId; i++) {
            if (s_extensionDeletors[i] == nullptr) {
                r._extensions[i] = _extensions[i];
            }
        }
    }

    static uint32_t register_extension(extension_deletor deletor = nullptr)
    {
        int idx = s_nextExtensionIndex++;
        if (idx < MAX_EXTENSION_COUNT) {
            s_extensionDeletors[idx] = deletor;
        } else {
            idx = INVALID_SLOT;
            assert(!"allocate extension failed, not enough slots available");
        }
        return idx;
    }

    static uint32_t get_extension_count() { return s_nextExtensionIndex.load(); }

private:
    uint64_t _extensions[MAX_EXTENSION_COUNT];
    static extension_deletor s_extensionDeletors[MAX_EXTENSION_COUNT];
    static std::atomic<uint32_t> s_nextExtensionIndex;
};

/*!
ExtensionHelper

steps to use an ExtensionHelper
- implement an ExtensionHelper class, e.g.
    class F : public ExtensionHelper<F, T>, make sure T is an extension_object.
- add extra information as member fields of class F.
- invoke F::register() at system initialization
- use F::get(host_object) to retrive F object where host_object is of T type.
- once F object is here, you can access your extra information freely.
 */

template <typename TPlaceholder, typename TExtensibleObject>
class uint64_extension_helper
{
public:
    static uint32_t register_ext()
    {
        s_slotIdx = TExtensibleObject::register_extension();
        return s_slotIdx;
    }

    static uint64_t &get(TExtensibleObject *ctx) { return ctx->get_extension(s_slotIdx); }

    static void set(TExtensibleObject *ctx, uint64_t ext) { ctx->set_extension(s_slotIdx, ext); }

private:
    static uint32_t s_slotIdx;
};

template <typename TExtension, typename TExtensibleObject>
class object_extension_helper
{
public:
    static uint32_t register_ext(extension_deletor deletor = nullptr)
    {
        s_slotIdx = TExtensibleObject::register_extension(deletor);
        s_deletor = deletor;
        return s_slotIdx;
    }

    static uint32_t register_ext(extension_creator creator, extension_deletor deletor)
    {
        s_slotIdx = TExtensibleObject::register_extension(deletor);
        s_creator = creator;
        s_deletor = deletor;
        return s_slotIdx;
    }

    static TExtension *get(TExtensibleObject *ctx)
    {
        uint64_t &val = ctx->get_extension(s_slotIdx);
        return (TExtension *)val;
    }

    static void set(TExtensibleObject *ctx, TExtension *ext)
    {
        ctx->set_extension(s_slotIdx, (uint64_t)ext);
    }

    static TExtension *get_inited(TExtensibleObject *ctx)
    {
        uint64_t &val = ctx->get_extension(s_slotIdx);
        if (val != TExtensibleObject::INVALID_VALUE)
            return (TExtension *)val;

        if (s_creator == nullptr) {
            TExtension *obj = new TExtension();
            val = (uint64_t)obj;
        } else {
            val = (uint64_t)s_creator(ctx);
        }

        return (TExtension *)val;
    }

    static void clear(TExtensibleObject *ctx)
    {
        uint64_t &val = ctx->get_extension(s_slotIdx);
        if (val != TExtensibleObject::INVALID_VALUE) {
            s_deletor((TExtension *)val);
            val = TExtensibleObject::INVALID_VALUE;
        }
    }

private:
    static uint32_t s_slotIdx;
    static extension_deletor s_deletor;
    static extension_creator s_creator;
};

//--- inline implementation -----------
template <typename T, const int MAX_EXTENSION_COUNT>
extension_deletor
    extensible_object<T, MAX_EXTENSION_COUNT>::s_extensionDeletors[MAX_EXTENSION_COUNT];
template <typename T, const int MAX_EXTENSION_COUNT>
std::atomic<uint32_t> extensible_object<T, MAX_EXTENSION_COUNT>::s_nextExtensionIndex(0);

template <typename TPlaceholder, typename TExtensibleObject>
uint32_t uint64_extension_helper<TPlaceholder, TExtensibleObject>::s_slotIdx = 0;

template <typename TExtension, typename TExtensibleObject>
uint32_t object_extension_helper<TExtension, TExtensibleObject>::s_slotIdx = 0;
template <typename TExtension, typename TExtensibleObject>
extension_deletor object_extension_helper<TExtension, TExtensibleObject>::s_deletor = nullptr;
template <typename TExtension, typename TExtensibleObject>
extension_creator object_extension_helper<TExtension, TExtensibleObject>::s_creator = nullptr;
/*@}*/
} // end namespace dsn
