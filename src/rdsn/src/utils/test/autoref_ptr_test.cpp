// Copyright (c) 2012 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// this test is copy from
// https://github.com/chromium/chromium/blob/07eea964c3f60f501782d8eb51f62ca75ddf3908/base/memory/ref_counted_unittest.cc

#include <type_traits>
#include <utility>

#include <gtest/gtest.h>
#include "utils/autoref_ptr.h"

namespace {

class SelfAssign : public dsn::ref_counter
{
protected:
    virtual ~SelfAssign() = default;

private:
    friend class dsn::ref_counter;
};

class Derived : public SelfAssign
{
protected:
    ~Derived() override = default;

private:
    friend class dsn::ref_counter;
};

class ScopedRefPtrToSelf : public dsn::ref_counter
{
public:
    ScopedRefPtrToSelf() : self_ptr_(this) {}

    static bool was_destroyed() { return was_destroyed_; }

    static void reset_was_destroyed() { was_destroyed_ = false; }

    dsn::ref_ptr<ScopedRefPtrToSelf> self_ptr_;

private:
    friend class dsn::ref_counter;
    ~ScopedRefPtrToSelf() { was_destroyed_ = true; }

    static bool was_destroyed_;
};

bool ScopedRefPtrToSelf::was_destroyed_ = false;

class ScopedRefPtrCountBase : public dsn::ref_counter
{
public:
    ScopedRefPtrCountBase() { ++constructor_count_; }

    static int constructor_count() { return constructor_count_; }

    static int destructor_count() { return destructor_count_; }

    static void reset_count()
    {
        constructor_count_ = 0;
        destructor_count_ = 0;
    }

protected:
    virtual ~ScopedRefPtrCountBase() { ++destructor_count_; }

private:
    friend class dsn::ref_counter;

    static int constructor_count_;
    static int destructor_count_;
};

int ScopedRefPtrCountBase::constructor_count_ = 0;
int ScopedRefPtrCountBase::destructor_count_ = 0;

class ScopedRefPtrCountDerived : public ScopedRefPtrCountBase
{
public:
    ScopedRefPtrCountDerived() { ++constructor_count_; }

    static int constructor_count() { return constructor_count_; }

    static int destructor_count() { return destructor_count_; }

    static void reset_count()
    {
        constructor_count_ = 0;
        destructor_count_ = 0;
    }

protected:
    ~ScopedRefPtrCountDerived() override { ++destructor_count_; }

private:
    friend class dsn::ref_counter;

    static int constructor_count_;
    static int destructor_count_;
};

int ScopedRefPtrCountDerived::constructor_count_ = 0;
int ScopedRefPtrCountDerived::destructor_count_ = 0;

class Other : public dsn::ref_counter
{
private:
    friend class dsn::ref_counter;

    ~Other() = default;
};

dsn::ref_ptr<Other> Overloaded(dsn::ref_ptr<Other> other) { return other; }

dsn::ref_ptr<SelfAssign> Overloaded(dsn::ref_ptr<SelfAssign> self_assign) { return self_assign; }

class InitialRefCountIsOne : public dsn::ref_counter
{
public:
    InitialRefCountIsOne() = default;

private:
    friend class dsn::ref_counter;
    ~InitialRefCountIsOne() = default;
};

} // end namespace

TEST(RefCountedUnitTest, TestSelfAssignment)
{
    SelfAssign *p = new SelfAssign;
    dsn::ref_ptr<SelfAssign> var(p);
    // var = var;
    EXPECT_EQ(var.get(), p);
    // comment the following two lines because clang compiler would complain with "-Wself-move"
    // var = std::move(var);
    // EXPECT_EQ(var.get(), p);

    // please uncomment these lines when swap are supported in ref_ptr
    // var.swap(var);
    // EXPECT_EQ(var.get(), p);
    // swap(var, var);
    // EXPECT_EQ(var.get(), p);
}

TEST(RefCountedUnitTest, ScopedRefPtrToSelfPointerAssignment)
{
    ScopedRefPtrToSelf::reset_was_destroyed();

    ScopedRefPtrToSelf *check = new ScopedRefPtrToSelf();
    EXPECT_FALSE(ScopedRefPtrToSelf::was_destroyed());
    check->self_ptr_ = nullptr;
    EXPECT_TRUE(ScopedRefPtrToSelf::was_destroyed());
}

TEST(RefCountedUnitTest, ScopedRefPtrToSelfMoveAssignment)
{
    ScopedRefPtrToSelf::reset_was_destroyed();

    ScopedRefPtrToSelf *check = new ScopedRefPtrToSelf();
    EXPECT_FALSE(ScopedRefPtrToSelf::was_destroyed());
    // Releasing |check->self_ptr_| will delete |check|.
    // The move assignment operator must assign |check->self_ptr_| first then
    // release |check->self_ptr_|.
    check->self_ptr_ = dsn::ref_ptr<ScopedRefPtrToSelf>();
    EXPECT_TRUE(ScopedRefPtrToSelf::was_destroyed());
}

TEST(RefCountedUnitTest, BooleanTesting)
{
    dsn::ref_ptr<SelfAssign> ptr_to_an_instance = new SelfAssign;
    EXPECT_TRUE(ptr_to_an_instance);
    EXPECT_FALSE(!ptr_to_an_instance);

    if (ptr_to_an_instance) {
    } else {
        ADD_FAILURE() << "Pointer to an instance should result in true.";
    }

    if (!ptr_to_an_instance) { // check for operator!().
        ADD_FAILURE() << "Pointer to an instance should result in !x being false.";
    }

    dsn::ref_ptr<SelfAssign> null_ptr;
    EXPECT_FALSE(null_ptr);
    EXPECT_TRUE(!null_ptr);

    if (null_ptr) {
        ADD_FAILURE() << "Null pointer should result in false.";
    }

    if (!null_ptr) { // check for operator!().
    } else {
        ADD_FAILURE() << "Null pointer should result in !x being true.";
    }
}

TEST(RefCountedUnitTest, Equality)
{
    dsn::ref_ptr<SelfAssign> p1(new SelfAssign);
    dsn::ref_ptr<SelfAssign> p2(new SelfAssign);

    EXPECT_EQ(p1, p1);
    EXPECT_EQ(p2, p2);

    EXPECT_NE(p1, p2);
    EXPECT_NE(p2, p1);
}

TEST(RefCountedUnitTest, NullptrEquality)
{
    dsn::ref_ptr<SelfAssign> ptr_to_an_instance(new SelfAssign);
    dsn::ref_ptr<SelfAssign> ptr_to_nullptr;

    EXPECT_NE(nullptr, ptr_to_an_instance);
    EXPECT_NE(ptr_to_an_instance, nullptr);
    EXPECT_EQ(nullptr, ptr_to_nullptr);
    EXPECT_EQ(ptr_to_nullptr, nullptr);
}

TEST(RefCountedUnitTest, ConvertibleEquality)
{
    dsn::ref_ptr<Derived> p1(new Derived);
    dsn::ref_ptr<SelfAssign> p2;

    EXPECT_NE(p1, p2);
    EXPECT_NE(p2, p1);

    p2 = p1;

    EXPECT_EQ(p1, p2);
    EXPECT_EQ(p2, p1);
}

TEST(RefCountedUnitTest, MoveAssignment1)
{
    ScopedRefPtrCountBase::reset_count();

    {
        ScopedRefPtrCountBase *raw = new ScopedRefPtrCountBase();
        dsn::ref_ptr<ScopedRefPtrCountBase> p1(raw);
        EXPECT_EQ(1, ScopedRefPtrCountBase::constructor_count());
        EXPECT_EQ(0, ScopedRefPtrCountBase::destructor_count());

        {
            dsn::ref_ptr<ScopedRefPtrCountBase> p2;

            p2 = std::move(p1);
            EXPECT_EQ(1, ScopedRefPtrCountBase::constructor_count());
            EXPECT_EQ(0, ScopedRefPtrCountBase::destructor_count());
            EXPECT_EQ(nullptr, p1.get());
            EXPECT_EQ(raw, p2.get());

            // p2 goes out of scope.
        }
        EXPECT_EQ(1, ScopedRefPtrCountBase::constructor_count());
        EXPECT_EQ(1, ScopedRefPtrCountBase::destructor_count());

        // p1 goes out of scope.
    }
    EXPECT_EQ(1, ScopedRefPtrCountBase::constructor_count());
    EXPECT_EQ(1, ScopedRefPtrCountBase::destructor_count());
}

TEST(RefCountedUnitTest, MoveAssignment2)
{
    ScopedRefPtrCountBase::reset_count();

    {
        ScopedRefPtrCountBase *raw = new ScopedRefPtrCountBase();
        dsn::ref_ptr<ScopedRefPtrCountBase> p1;
        EXPECT_EQ(1, ScopedRefPtrCountBase::constructor_count());
        EXPECT_EQ(0, ScopedRefPtrCountBase::destructor_count());

        {
            dsn::ref_ptr<ScopedRefPtrCountBase> p2(raw);
            EXPECT_EQ(1, ScopedRefPtrCountBase::constructor_count());
            EXPECT_EQ(0, ScopedRefPtrCountBase::destructor_count());

            p1 = std::move(p2);
            EXPECT_EQ(1, ScopedRefPtrCountBase::constructor_count());
            EXPECT_EQ(0, ScopedRefPtrCountBase::destructor_count());
            EXPECT_EQ(raw, p1.get());
            EXPECT_EQ(nullptr, p2.get());

            // p2 goes out of scope.
        }
        EXPECT_EQ(1, ScopedRefPtrCountBase::constructor_count());
        EXPECT_EQ(0, ScopedRefPtrCountBase::destructor_count());

        // p1 goes out of scope.
    }
    EXPECT_EQ(1, ScopedRefPtrCountBase::constructor_count());
    EXPECT_EQ(1, ScopedRefPtrCountBase::destructor_count());
}

TEST(RefCountedUnitTest, MoveAssignmentSameInstance1)
{
    ScopedRefPtrCountBase::reset_count();

    {
        ScopedRefPtrCountBase *raw = new ScopedRefPtrCountBase();
        dsn::ref_ptr<ScopedRefPtrCountBase> p1(raw);
        EXPECT_EQ(1, ScopedRefPtrCountBase::constructor_count());
        EXPECT_EQ(0, ScopedRefPtrCountBase::destructor_count());

        {
            dsn::ref_ptr<ScopedRefPtrCountBase> p2(p1);
            EXPECT_EQ(1, ScopedRefPtrCountBase::constructor_count());
            EXPECT_EQ(0, ScopedRefPtrCountBase::destructor_count());

            p1 = std::move(p2);
            EXPECT_EQ(1, ScopedRefPtrCountBase::constructor_count());
            EXPECT_EQ(0, ScopedRefPtrCountBase::destructor_count());
            EXPECT_EQ(raw, p1.get());
            EXPECT_EQ(nullptr, p2.get());

            // p2 goes out of scope.
        }
        EXPECT_EQ(1, ScopedRefPtrCountBase::constructor_count());
        EXPECT_EQ(0, ScopedRefPtrCountBase::destructor_count());

        // p1 goes out of scope.
    }
    EXPECT_EQ(1, ScopedRefPtrCountBase::constructor_count());
    EXPECT_EQ(1, ScopedRefPtrCountBase::destructor_count());
}

TEST(RefCountedUnitTest, MoveAssignmentSameInstance2)
{
    ScopedRefPtrCountBase::reset_count();

    {
        ScopedRefPtrCountBase *raw = new ScopedRefPtrCountBase();
        dsn::ref_ptr<ScopedRefPtrCountBase> p1(raw);
        EXPECT_EQ(1, ScopedRefPtrCountBase::constructor_count());
        EXPECT_EQ(0, ScopedRefPtrCountBase::destructor_count());

        {
            dsn::ref_ptr<ScopedRefPtrCountBase> p2(p1);
            EXPECT_EQ(1, ScopedRefPtrCountBase::constructor_count());
            EXPECT_EQ(0, ScopedRefPtrCountBase::destructor_count());

            p2 = std::move(p1);
            EXPECT_EQ(1, ScopedRefPtrCountBase::constructor_count());
            EXPECT_EQ(0, ScopedRefPtrCountBase::destructor_count());
            EXPECT_EQ(nullptr, p1.get());
            EXPECT_EQ(raw, p2.get());

            // p2 goes out of scope.
        }
        EXPECT_EQ(1, ScopedRefPtrCountBase::constructor_count());
        EXPECT_EQ(1, ScopedRefPtrCountBase::destructor_count());

        // p1 goes out of scope.
    }
    EXPECT_EQ(1, ScopedRefPtrCountBase::constructor_count());
    EXPECT_EQ(1, ScopedRefPtrCountBase::destructor_count());
}

TEST(RefCountedUnitTest, MoveAssignmentDifferentInstances)
{
    ScopedRefPtrCountBase::reset_count();

    {
        ScopedRefPtrCountBase *raw1 = new ScopedRefPtrCountBase();
        dsn::ref_ptr<ScopedRefPtrCountBase> p1(raw1);
        EXPECT_EQ(1, ScopedRefPtrCountBase::constructor_count());
        EXPECT_EQ(0, ScopedRefPtrCountBase::destructor_count());

        {
            ScopedRefPtrCountBase *raw2 = new ScopedRefPtrCountBase();
            dsn::ref_ptr<ScopedRefPtrCountBase> p2(raw2);
            EXPECT_EQ(2, ScopedRefPtrCountBase::constructor_count());
            EXPECT_EQ(0, ScopedRefPtrCountBase::destructor_count());

            p1 = std::move(p2);
            EXPECT_EQ(2, ScopedRefPtrCountBase::constructor_count());
            EXPECT_EQ(1, ScopedRefPtrCountBase::destructor_count());
            EXPECT_EQ(raw2, p1.get());
            EXPECT_EQ(nullptr, p2.get());

            // p2 goes out of scope.
        }
        EXPECT_EQ(2, ScopedRefPtrCountBase::constructor_count());
        EXPECT_EQ(1, ScopedRefPtrCountBase::destructor_count());

        // p1 goes out of scope.
    }
    EXPECT_EQ(2, ScopedRefPtrCountBase::constructor_count());
    EXPECT_EQ(2, ScopedRefPtrCountBase::destructor_count());
}

TEST(RefCountedUnitTest, MoveAssignmentSelfMove)
{
    ScopedRefPtrCountBase::reset_count();

    {
        ScopedRefPtrCountBase *raw = new ScopedRefPtrCountBase;
        dsn::ref_ptr<ScopedRefPtrCountBase> p1(raw);
        dsn::ref_ptr<ScopedRefPtrCountBase> &p1_ref = p1;

        EXPECT_EQ(1, ScopedRefPtrCountBase::constructor_count());
        EXPECT_EQ(0, ScopedRefPtrCountBase::destructor_count());

        p1 = std::move(p1_ref);

        // |p1| is "valid but unspecified", so don't bother inspecting its
        // contents, just ensure that we don't crash.
    }

    EXPECT_EQ(1, ScopedRefPtrCountBase::constructor_count());
    EXPECT_EQ(1, ScopedRefPtrCountBase::destructor_count());
}

TEST(RefCountedUnitTest, MoveAssignmentDerived)
{
    ScopedRefPtrCountBase::reset_count();
    ScopedRefPtrCountDerived::reset_count();

    {
        ScopedRefPtrCountBase *raw1 = new ScopedRefPtrCountBase();
        dsn::ref_ptr<ScopedRefPtrCountBase> p1(raw1);
        EXPECT_EQ(1, ScopedRefPtrCountBase::constructor_count());
        EXPECT_EQ(0, ScopedRefPtrCountBase::destructor_count());
        EXPECT_EQ(0, ScopedRefPtrCountDerived::constructor_count());
        EXPECT_EQ(0, ScopedRefPtrCountDerived::destructor_count());

        {
            ScopedRefPtrCountDerived *raw2 = new ScopedRefPtrCountDerived();
            dsn::ref_ptr<ScopedRefPtrCountDerived> p2(raw2);
            EXPECT_EQ(2, ScopedRefPtrCountBase::constructor_count());
            EXPECT_EQ(0, ScopedRefPtrCountBase::destructor_count());
            EXPECT_EQ(1, ScopedRefPtrCountDerived::constructor_count());
            EXPECT_EQ(0, ScopedRefPtrCountDerived::destructor_count());

            p1 = std::move(p2);
            EXPECT_EQ(2, ScopedRefPtrCountBase::constructor_count());
            EXPECT_EQ(1, ScopedRefPtrCountBase::destructor_count());
            EXPECT_EQ(1, ScopedRefPtrCountDerived::constructor_count());
            EXPECT_EQ(0, ScopedRefPtrCountDerived::destructor_count());
            EXPECT_EQ(raw2, p1.get());
            EXPECT_EQ(nullptr, p2.get());

            // p2 goes out of scope.
        }
        EXPECT_EQ(2, ScopedRefPtrCountBase::constructor_count());
        EXPECT_EQ(1, ScopedRefPtrCountBase::destructor_count());
        EXPECT_EQ(1, ScopedRefPtrCountDerived::constructor_count());
        EXPECT_EQ(0, ScopedRefPtrCountDerived::destructor_count());

        // p1 goes out of scope.
    }
    EXPECT_EQ(2, ScopedRefPtrCountBase::constructor_count());
    EXPECT_EQ(2, ScopedRefPtrCountBase::destructor_count());
    EXPECT_EQ(1, ScopedRefPtrCountDerived::constructor_count());
    EXPECT_EQ(1, ScopedRefPtrCountDerived::destructor_count());
}

TEST(RefCountedUnitTest, MoveConstructor)
{
    ScopedRefPtrCountBase::reset_count();

    {
        ScopedRefPtrCountBase *raw = new ScopedRefPtrCountBase();
        dsn::ref_ptr<ScopedRefPtrCountBase> p1(raw);
        EXPECT_EQ(1, ScopedRefPtrCountBase::constructor_count());
        EXPECT_EQ(0, ScopedRefPtrCountBase::destructor_count());

        {
            dsn::ref_ptr<ScopedRefPtrCountBase> p2(std::move(p1));
            EXPECT_EQ(1, ScopedRefPtrCountBase::constructor_count());
            EXPECT_EQ(0, ScopedRefPtrCountBase::destructor_count());
            EXPECT_EQ(nullptr, p1.get());
            EXPECT_EQ(raw, p2.get());

            // p2 goes out of scope.
        }
        EXPECT_EQ(1, ScopedRefPtrCountBase::constructor_count());
        EXPECT_EQ(1, ScopedRefPtrCountBase::destructor_count());

        // p1 goes out of scope.
    }
    EXPECT_EQ(1, ScopedRefPtrCountBase::constructor_count());
    EXPECT_EQ(1, ScopedRefPtrCountBase::destructor_count());
}

TEST(RefCountedUnitTest, MoveConstructorDerived)
{
    ScopedRefPtrCountBase::reset_count();
    ScopedRefPtrCountDerived::reset_count();

    {
        ScopedRefPtrCountDerived *raw1 = new ScopedRefPtrCountDerived();
        dsn::ref_ptr<ScopedRefPtrCountDerived> p1(raw1);
        EXPECT_EQ(1, ScopedRefPtrCountBase::constructor_count());
        EXPECT_EQ(0, ScopedRefPtrCountBase::destructor_count());
        EXPECT_EQ(1, ScopedRefPtrCountDerived::constructor_count());
        EXPECT_EQ(0, ScopedRefPtrCountDerived::destructor_count());

        {
            dsn::ref_ptr<ScopedRefPtrCountBase> p2(std::move(p1));
            EXPECT_EQ(1, ScopedRefPtrCountBase::constructor_count());
            EXPECT_EQ(0, ScopedRefPtrCountBase::destructor_count());
            EXPECT_EQ(1, ScopedRefPtrCountDerived::constructor_count());
            EXPECT_EQ(0, ScopedRefPtrCountDerived::destructor_count());
            EXPECT_EQ(nullptr, p1.get());
            EXPECT_EQ(raw1, p2.get());

            // p2 goes out of scope.
        }
        EXPECT_EQ(1, ScopedRefPtrCountBase::constructor_count());
        EXPECT_EQ(1, ScopedRefPtrCountBase::destructor_count());
        EXPECT_EQ(1, ScopedRefPtrCountDerived::constructor_count());
        EXPECT_EQ(1, ScopedRefPtrCountDerived::destructor_count());

        // p1 goes out of scope.
    }
    EXPECT_EQ(1, ScopedRefPtrCountBase::constructor_count());
    EXPECT_EQ(1, ScopedRefPtrCountBase::destructor_count());
    EXPECT_EQ(1, ScopedRefPtrCountDerived::constructor_count());
    EXPECT_EQ(1, ScopedRefPtrCountDerived::destructor_count());
}

TEST(RefCountedUnitTest, TestOverloadResolutionCopy)
{
    const dsn::ref_ptr<Derived> derived(new Derived);
    const dsn::ref_ptr<SelfAssign> expected(derived);
    EXPECT_EQ(expected, Overloaded((dsn::ref_ptr<SelfAssign>)(derived)));

    const dsn::ref_ptr<Other> other(new Other);
    EXPECT_EQ(other, Overloaded((dsn::ref_ptr<Other>)other));
}

TEST(RefCountedUnitTest, TestOverloadResolutionMove)
{
    dsn::ref_ptr<Derived> derived(new Derived);
    const dsn::ref_ptr<SelfAssign> expected(derived);
    EXPECT_EQ(expected, Overloaded((dsn::ref_ptr<SelfAssign>)(std::move(derived))));

    dsn::ref_ptr<Other> other(new Other);
    const dsn::ref_ptr<Other> other2(other);
    EXPECT_EQ(other2, Overloaded((dsn::ref_ptr<Other>)(std::move(other))));
}

TEST(RefCountedUnitTest, TestMakeRefCounted)
{
    dsn::ref_ptr<Derived> derived = new Derived;
    EXPECT_TRUE(derived->get_count() == 1);
    derived = nullptr;

    dsn::ref_ptr<Derived> derived2(new Derived());
    EXPECT_TRUE(derived2->get_count() == 1);
    derived2 = nullptr;
}

TEST(RefCountedUnitTest, TestInitialRefCountIsOne)
{
    dsn::ref_ptr<InitialRefCountIsOne> obj(new InitialRefCountIsOne());
    EXPECT_TRUE(obj->get_count() == 1);
    obj = nullptr;

    dsn::ref_ptr<InitialRefCountIsOne> obj2(new InitialRefCountIsOne);
    EXPECT_TRUE(obj2->get_count() == 1);
    obj2 = nullptr;

    dsn::ref_ptr<Other> obj3(new Other());
    EXPECT_TRUE(obj3->get_count() == 1);
    obj3 = nullptr;
}
