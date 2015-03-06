# include <rdsn/internal/join_point.h>
# include <rdsn/internal/logging.h>

namespace rdsn
{

join_point_base::join_point_base(const char* name)
{
    _name = std::string(name);
    _hdr.next = _hdr.prev = &_hdr;
    _hdr.name = "";
}

bool join_point_base::put_front(void* fn, const char* name, bool is_native)
{
    auto e = new_entry(fn, name, is_native);
    auto e1 = _hdr.prev;

    e1->next = e;
    e->next = &_hdr;
    _hdr.prev = e;
    e->prev = e1;

    return true;
}

bool join_point_base::put_back(void* fn, const char* name, bool is_native)
{
    auto e = new_entry(fn, name, is_native);
    auto e1 = _hdr.prev;

    e1->next = e;
    e->next = &_hdr;
    _hdr.prev = e;
    e->prev = e1;

    return true;
}

bool join_point_base::put_before(const char* base, void* fn, const char* name, bool is_native)
{
    auto e0 = get_by_name(name);
    if (e0 == nullptr)
    {
        rdsn_assert(false, "cannot find advice with name '%s' in '%s'", name, _name.c_str());
        return false;
    }

    auto e = new_entry(fn, name, is_native);

    auto e1 = e0->prev;
    e1->next = e;
    e->next = e0;
    e0->prev = e;
    e->prev = e1;
    
    return true;
}

bool join_point_base::put_after(const char* base, void* fn, const char* name, bool is_native)
{
    auto e0 = get_by_name(name);
    if (e0 == nullptr)
    {
        rdsn_assert(false, "cannot find advice with name '%s' in '%s'", name, _name.c_str());
        return false;
    }

    auto e = new_entry(fn, name, is_native);

    auto e1 = e0->next;
    e1->prev = e;
    e->prev = e0;
    e0->next = e;
    e->next = e1;
    
    return true;
}

bool join_point_base::put_replace(const char* base, void* fn, const char* name)
{
    auto e0 = get_by_name(name);
    if (e0 == nullptr)
    {
        rdsn_assert(false, "cannot find advice with name '%s' in '%s'", name, _name.c_str());
        return false;
    }
    else
    {
        e0->func = fn;
        e0->name = name;
        return true;
    }
}

bool join_point_base::remove(const char* name)
{
    auto e0 = get_by_name(name);
    if (e0 == nullptr)
    {
        rdsn_assert(false, "cannot find advice with name '%s' in '%s'", name, _name.c_str());
        return false;
    }

    e0->next->prev = e0->prev;
    e0->prev->next = e0->next;

    return true;
}

join_point_base::advice_entry* join_point_base::new_entry(void* fn, const char* name, bool is_native)
{
    auto e = new advice_entry();
    e->name = std::string(name);
    e->func = fn;
    e->is_native = is_native;
    e->next = e->prev = e;
    return e;
}

join_point_base::advice_entry* join_point_base::get_by_name(const char* name)
{
    auto p = _hdr.next;
    while (p != &_hdr)
    {
        if (strcmp(name, p->name.c_str()) == 0)
            return p;

        p = p->next;
    }

    return nullptr;
}

} // end namespace rdsn
