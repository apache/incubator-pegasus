/*
* Copyright (c) 2012 Ariane van der Steldt <ariane@stack.nl>
*
* Permission to use, copy, modify, and distribute this software for any
* purpose with or without fee is hereby granted, provided that the above
* copyright notice and this permission notice appear in all copies.
*
* THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
* WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
* MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
* ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
* WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
* ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
* OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
*/

#include <dsn/internal/lf.ll.h>
#include <assert.h>
#include <stdint.h>


/*
* Invariants.
* Since this is a lock-free algorithm, these invariants must hold at all time.
*
* Definitions:
* S		-- the set of elements in the list
* q_head(s)	-- the head of the list (LL_HEAD)
* n		-- the node we are observing
* n->succ	-- the successor value of n
* n->pred	-- the predecessor value of n
* succ(n)	-- the actual successor of n
* pred(n)	-- the actual predecessor of n
* tail(n)	-- succ(n) union tail(succ(n))
* deleted(n)	-- predicate: n is deleted from S, but still traversable
* n not in S	-- n is not on S and not being inserted/removed right now
*
* [1]	successor is always up-to-date
* foreach(n  :  n in S  :  n->succ == succ(n))
* [2]	each node is reachable from its predecessors
* foreach(n  :  n in S  :  n in tail(n->pred))
* [3]	first element description
* q_head(n)->succ is the first element in the list
* [4]	last element description
* foreach(n  :  n in S  :  n->succ == q_head(S)  ==>  n is the last element)
* [5]	empty list constraint
* q_head(S)->succ == q_head(S)  ==>  S is the empty list
* [6]	head of the list may never be deleted
* n == q_head(S)  ==>  !deleted(n)
* [7]	deleted node is still traversable while referenced
* deleted(n)  ==>  n->pred == pred(n)
* deleted(n)  ==>  n->succ == succ(n)
* [8]	successor constraint on deleted nodes
* define: p = pred(n)
* !deleted(n)  ==>  p->succ == n
* deleted(n)  ==>  n->pred == p
* [9]	nodes not on the list have null pointers
* n not in S  ==>  n->succ == nil and n->pred == nil
* [10]	deletion mark
* deleted(n)  ==>  n->pred | FLAGGED
* [11]	insert in progress mark
* n is being inserted  ==>  n->succ | FLAGGED
* [12]	can we insert n?
* n can be inserted  ==>  n->succ = nil and !(n->suc | FLAGGED)
* [13]	can we unlink n?
* n can be unlinked  ==>  n->pred != nil and !(n->pred | FLAGGED)
*/


/*
* Spinwait instruction.
* On hyperthread archs, we don't want to hog the cpu if we are spinning.
*/
#ifndef SPINWAIT

/* Spinwait asm for gnu-compatible compilers on intel-like platforms
* (using PAUSE instruction). */
#if (defined(__GNUC__) || defined(__clang__)) &&			\
    (defined(__amd64__) || defined(__x86_64__) || \
    defined(__i386__) || defined(__ia64__))

#define SPINWAIT() do { __asm __volatile("pause":::"memory"); } while (0)

/* Spinwait asm for MS compiler on i386/amd64. */
#elif defined(_MSC_VER) && (defined(_M_IX86) || defined(_M_X64))

#define SPINWAIT() do { __asm { __asm pause }; } while (0)

#else

/* No pause instruction on other platforms/compilers. */
#define SPINWAIT() do { /* nothing */ } while (0)

#endif /* SPINWAIT platform selector. */

#endif /* SPINWAIT */


/*
* Flag bits.
*
* The elem_ptr_t values must be aligned to at least 4 bytes and a power of 2.
* The lower 2 bits are used to flag states.
*
* The DEREF bit is set during dereference of a pointer.  While it is set, no
* changes may happen to the pointer.  These pointer derefences (usually held
* for ~3 instructions) are the only locks in the algorithm, required to jump
* between nodes in the linked list.  Due to the deref bit, the algorithm
* cannot qualify as wait-free, but I expect performance in most cases will be
* near identical.
*
* The flagged bit marks the state of the element.  If pred is flagged, the
* node is marked for deletion and traversal functions should skip it.  The
* reference count on a deleted node will go to zero, at which point the
* algorithm will release the node.
*/
#define DEREF	((uintptr_t)0x1U)
#define FLAGGED	((uintptr_t)0x2U)
#define MASK	(DEREF | FLAGGED)

using namespace std;


/*
* Primitive operations.
* Inline, so they can be optimized by the compiler.
*/


/* Strip a pointer of its bit flags. */
static __inline struct ll_elem*
ptr_clear(struct ll_elem *e)
{
    return (struct ll_elem*)((uintptr_t)e & ~MASK);
}
/* Add the deref bit to a pointer. */
static __inline struct ll_elem*
ptr_ref(struct ll_elem *e)
{
    return (struct ll_elem*)((uintptr_t)e | DEREF);
}
/* Acquire (additional) references on element. */
static __inline void
deref_acquire(struct ll_elem *e, size_t count)
{
    atomic_fetch_add_explicit(&ptr_clear(e)->refcnt, count,
        memory_order_acquire);
}
/*
* Release references on element.
* Returns the updated value of the reference counter.
*/
static __inline void
deref_release(struct ll_head *q_head, struct ll_elem *e, size_t count)
{
    size_t old;

    old = atomic_fetch_sub_explicit(&ptr_clear(e)->refcnt, count,
        memory_order_release);
    assert(old >= count);
}

/*
* Dereference the atomic elem_ptr_t,
* acquiring a reference to the dereferenced value.
*
* The dereferenced pointer is returned with flag bits intact
* (note that the DEREF bit can never be set, since the function
* spins during that time).
*/
static __inline struct ll_elem*
deref(elem_ptr_t *ptr)
{
    uintptr_t e, old;

    while ((e = atomic_fetch_or_explicit(ptr, DEREF,
        memory_order_consume)) & DEREF)
        SPINWAIT();
    if (ptr_clear((struct ll_elem*)e) != NULL)
        deref_acquire((struct ll_elem*)e, 1);
    old = atomic_fetch_and_explicit(ptr, ~DEREF, memory_order_release);
    assert(old & DEREF);
    return (struct ll_elem*)e;
}
/*
* Combine the address in ptr with the flags in fl.
*/
static __inline struct ll_elem*
flag_combine(struct ll_elem *ptr, struct ll_elem *fl)
{
    return (struct ll_elem*)((uintptr_t)ptr_clear(ptr) |
        ((uintptr_t)fl & FLAGGED));
}

/*
* Test if the given pred pointer has the deletion mark set.
*/
static __inline int
deleted_ptr(struct ll_elem *e)
{
    return ((uintptr_t)e & FLAGGED) != 0;
}
/*
* Test if element is marked for deletion.
*/
static __inline int
deleted(struct ll_elem *e)
{
    return deleted_ptr((struct ll_elem*)atomic_load_explicit(
        &ptr_clear(e)->pred, memory_order_relaxed));
}
/*
* CAS pointer rewrite.
*
* On succes, ptr has the DEREF bit set.
* Note that this call does not alter the reference pointers.
*/
static __inline int
ptr_cas(elem_ptr_t *ptr, struct ll_elem **expect, struct ll_elem *set)
{
    struct ll_elem	*old = *expect;
    int		 succes;
    uintptr_t	 set_;

    assert(!((uintptr_t)old & DEREF));
    set_ = (uintptr_t)ptr_ref(flag_combine(set, old));

    for (;;) {
        succes = atomic_compare_exchange_weak_explicit(ptr,
            (uintptr_t*)expect, set_,
            memory_order_acq_rel, memory_order_relaxed);
        if (succes)
            break;
        if (((uintptr_t)*expect & ~DEREF) != (uintptr_t)old)
            break;
        *expect = old;
        SPINWAIT();
    }

    return succes;
}
/*
* Clear deref lock on ptr.
*/
static __inline void
ptr_clear_deref(elem_ptr_t *ptr)
{
    uintptr_t	 p;

    p = atomic_fetch_and_explicit(ptr, ~DEREF, memory_order_release);
    assert(p & DEREF);
}


/*
* Complex operations.
* These are the actual algorithm parts.
*/


/*
* Find the successor of n.
*
* Will update successor pointers to skip deleted elements
* during traversal.
*
* Returns q if n has no successor.
* Does not clear flag bits.
*/
static struct ll_elem*
succ(struct ll_head *q_head, struct ll_elem *n)
{
    struct ll_elem	*q, *s, *s_, *ss;

    q = &q_head->q;
    assert(q == ptr_clear(q));
    n = ptr_clear(n);
    assert(n != NULL);

    s = deref(&n->succ);
    /* If s is null, n is not on the queue. */
    if (ptr_clear(s) == NULL)
        return s;

    while (deleted(s)) {
        ss = flag_combine(deref(&ptr_clear(s)->succ), s);
        s_ = s;
        if (ptr_cas(&n->succ, &s_, ss)) {
            /* cas succeeded */
            deref_acquire(ss, 1);
            ptr_clear_deref(&n->succ);

            /* Release s:
            * - once for n->succ
            * - once because we no longer claim s.
            */
            deref_release(q_head, s, 2);
            s = ss;
        }
        else {
            /* cas failed */
            deref_release(q_head, s, 1);
            deref_release(q_head, ss, 1);

            /*
            * Cannot do s = s_, since we do not have the lock on
            * n->succ at the moment and thus cannot acquire the
            * reference counter on s_.
            */
            s = deref(&n->succ);
        }
    }

    return s;
}

/*
* Find the predecessor of n.
*
* Will update predecessor pointer of n to skip deleted elements
* during traversal.  Will update successor pointers of elements
* [p .. n) for forward traversal (if n is deleted during this call,
* it may update successor pointers past n).
*
* Returns q if n has no predecessor.
* Does not clear flag bits.
*/
static struct ll_elem*
pred(struct ll_head *q_head, struct ll_elem *n)
{
    struct ll_elem	*q, *p, *p_, *ps, *pp;

    q = &q_head->q;
    assert(q == ptr_clear(q));
    n = ptr_clear(n);
    assert(n != NULL);

    p = deref(&n->pred);
    if (ptr_clear(p) == NULL)
        return p;

    for (;;) {
        /*
        * Predecessor cannot be null,
        * since it is on the queue and we have a reference to n,
        * preventing it from leaving the queue.
        */
        assert(ptr_clear(p) != NULL);

        /*
        * Search forward to reach the direct predecessor of
        * n between p and n.  Note that we cannot search for
        * a better predecessor when p holds the deletion mark
        * for n.
        */
        if (!deleted_ptr(p)) {
            ps = succ(q_head, p);
            assert(ptr_clear(ps) != NULL);
            while (ptr_clear(ps) != n && !deleted_ptr(p)) {
                p_ = p;
                if (ptr_cas(&n->pred, &p_, ps)) {
                    /* cas succeeded */
                    deref_acquire(ps, 1);
                    ptr_clear_deref(&n->pred);
                    deref_release(q_head, p, 2);
                    p = flag_combine(ps, p_);
                }
                else {
                    /* cas failed */
                    deref_release(q_head, ps, 1);
                    deref_release(q_head, p, 1);

                    /*
                    * Cannot do p = p_, since we do not
                    * have the lock on n->pred at the
                    * moment and thus cannot acquire the
                    * reference counter on p_.
                    */
                    p = deref(&n->pred);
                }

                ps = succ(q_head, p);
            }
            deref_release(q_head, ps, 1);
        }

        /*
        * p is the best predecessor, provided it is not a deleted value.
        */
        if (!deleted(p))
            break;		/* GUARD */

        /*
        * Skip to the predecessor of p, since p is deleted.
        */
        pp = deref(&ptr_clear(p)->pred);
        p_ = p;
        if (ptr_cas(&n->pred, &p_, pp)) {
            /* cas succeeded */
            deref_acquire(pp, 1);
            ptr_clear_deref(&n->pred);
            deref_release(q_head, p, 2);
            p = pp;
        }
        else {
            /* cas failed */
            deref_release(q_head, p, 1);
            deref_release(q_head, pp, 1);
            p = deref(&n->pred);
        }
    }

    return flag_combine(p, (struct ll_elem*)atomic_load_explicit(&n->pred,
        memory_order_relaxed));
}

/*
* Mark n as being inserted.
* Will prevent multiple inserts from running at the same time.
*
* Returns 1 on succes, 0 on failure.
* Only fails if n is already inserted or being inserted.
*/
static __inline int
insert_lock(struct ll_elem *n)
{
    uintptr_t zero = 0;

    /* Mark succ as flagged, preserving the DEREF bit. */
    while (!atomic_compare_exchange_weak_explicit(&n->succ, &zero,
        FLAGGED | (zero & DEREF),
        memory_order_acquire, memory_order_relaxed)) {
        if (zero & ~DEREF)
            return 0;
    }
    /* Wait for pred to fall to zero. */
    while (atomic_load_explicit(&n->pred, memory_order_relaxed) & ~DEREF)
        SPINWAIT();

    return 1;
}

/*
* Insert n between p and s.
*
* If s is not a direct successor of p, the insert will fail.
* The insert may also fail if p or s becomes deleted.
*
* No smarts regarding fixing p or s is done: the call is used by
* ll_insert_before() and ll_insert_after(), which have very strict
* rules regarding recovery which this function has no access to.
*
* Returns true on success, false on failure.
*/
static int
insert_between(struct ll_head *q_head, struct ll_elem *n,
struct ll_elem *p, struct ll_elem *s)
{
    struct ll_elem	*q, *ps = NULL, *ps_, *old;
    int		 cas;

    q = &q_head->q;
    /* Check arguments. */
    assert(q == ptr_clear(q));
    assert(n == ptr_clear(n));
    assert(p == ptr_clear(p));
    assert(s == ptr_clear(s));
    /* Check initial state of n. */
    assert(ptr_clear((struct ll_elem*)atomic_load_explicit(&n->pred,
        memory_order_relaxed)) == NULL);
    assert(atomic_load_explicit(&n->succ, memory_order_relaxed) == FLAGGED);
    assert(atomic_load_explicit(&n->refcnt, memory_order_relaxed) > 0);
    /*
    * Give unlink_release() time to update n->pred.
    * XXX try and make this an aid function?
    */
    while (atomic_load_explicit(&n->pred, memory_order_relaxed) != 0)
        SPINWAIT();

    /*
    * Assign n->pred, n->succ.
    *
    * We add a flag to s, to prevent deletion operations from starting
    * before we are ready.
    */
    deref_acquire(p, 1);	/* Because n->pred = p. */
    deref_acquire(s, 1);	/* Because n->succ = s. */

    old = (struct ll_elem*)FLAGGED;
    cas = ptr_cas(&n->succ, &old,
        (struct ll_elem*)((uintptr_t)s | FLAGGED));
    assert(cas);
    ptr_clear_deref(&n->succ);

    old = NULL;
    cas = ptr_cas(&n->pred, &old, p);
    assert(cas);
    ptr_clear_deref(&n->pred);

    /* Load bits in p->succ. */
    ps = deref(&p->succ);
    if (ptr_clear(ps) != ptr_clear(s) || deleted(p))
        goto fail;

    /* Exchange p->succ from s to n. */
    ps_ = ps;
    if (!ptr_cas(&p->succ, &ps_, n))
        goto fail;
    /*
    * If p is not deleted at this moment, the link is succesful
    * (since a non-deleted p means p is reachable from q and
    * our update changed n to be reachable from p,
    * hence it's reachable from q).
    *
    * Note that we hold the DEREF on ps->succ, so it cannot change.
    *
    * Also, note that we do not care if s has the deleted bit set:
    * if it is being deleted, its deletor would have to update
    * p->succ as well; we simply won the race to do that.
    */
    if (deleted(p)) {
        uintptr_t	 tmp;

        /*
        * Restore old value, note that this operation also clears
        * the deref bit, since ps_ will not have that set.
        */
        tmp = atomic_exchange_explicit(&p->succ, (uintptr_t)ps_,
            memory_order_release);
        assert((tmp & ~(FLAGGED | DEREF)) == (uintptr_t)n);
        assert(tmp & DEREF);
        goto fail;
    }

    /* Update succesful.  Update deref counter on n and release p->succ. */
    deref_acquire(n, 1);
    ptr_clear_deref(&p->succ);
    /* Forget ps. */
    deref_release(q_head, ps, 2);	/* Once for ps, once for p->succ. */
    ps = NULL;

    /* Fix pred pointer of s. */
    deref_release(q_head, pred(q_head, s), 1);

    /* Update list size. */
    atomic_fetch_add_explicit(&q_head->size, 1, memory_order_relaxed);
    /* Clear delete block. */
    atomic_fetch_and_explicit(&n->succ, ~FLAGGED, memory_order_release);

    return 1;

fail:
    /* Undo setting n->succ. */
    old = (struct ll_elem*)((uintptr_t)s | FLAGGED);
    cas = ptr_cas(&n->succ, &old, (struct ll_elem*)FLAGGED);
    assert(cas);
    ptr_clear_deref(&n->succ);

    /* Undo setting n->pred. */
    old = p;
    cas = ptr_cas(&n->pred, &old, NULL);
    assert(cas);
    ptr_clear_deref(&n->pred);

    deref_release(q_head, p, 1);
    deref_release(q_head, s, 1);
    deref_release(q_head, ps, 1);
    return 0;
}

/*
* Acquire the lock on p->succ.
*
* Succeeds once the DEREF bit is set on p->succ and
* ptr_clear(p->succ) == expect_s.
*/
static int
unlink_ps_lock(struct ll_head *q_head, struct ll_elem *p,
struct ll_elem *expect_s)
{
    struct ll_elem	*ps;
    int		 clear_deref = 0;

    /* Clear arguments. */
    p = ptr_clear(p);
    expect_s = ptr_clear(expect_s);

    /* Acquire deref, canceling once p->succ changes. */
    do {
        ps = (struct ll_elem*)atomic_fetch_or_explicit(&p->succ, DEREF,
            memory_order_acquire);
        if ((uintptr_t)ps & DEREF)
            SPINWAIT();
        else
            clear_deref = 1;
    } while (!clear_deref && ptr_clear(ps) == expect_s);

    /* Test reason for cancelation. */
    if (ptr_clear(ps) == expect_s) {
        assert(clear_deref);
        return 1;
    }

    /* Undo deref acquisition. */
    if (clear_deref)
        ptr_clear_deref(&p->succ);

    return 0;
}

/*
* Unlink n from the queue.
*
* Returns true if the deletion succeeded.  Fails if another thread is
* unlinking the element.
*/
static int
unlink(struct ll_head *q_head, struct ll_elem *n)
{
    struct ll_elem	*q, *p, *p_, *i, *i_;

    q = &q_head->q;
    /* Argument validation. */
    assert(q == ptr_clear(q));
    n = ptr_clear(n);
    assert(n != NULL);

    /*
    * Since we hold the reference to n, it cannot be removed completely
    * before we return.
    * However it can be that this function was called while n is not on
    * the queue, hence test that now and fail if that is the case.
    */
    if (ptr_clear((struct ll_elem*)atomic_load_explicit(&n->succ,
        memory_order_relaxed)) == NULL)
        return 0;

    /* Ensure n is not halfway an insert. */
    while (atomic_load_explicit(&n->succ, memory_order_relaxed) & FLAGGED)
        SPINWAIT();

restart:
    /*
    * Update n->pred to the deleted state, taking care n->pred->succ
    * points at n.
    * Fails if n->pred is already marked as deleted.
    */
    for (;;) {
        p = pred(q_head, n);
        /*
        * p can become null, if we acquired our reference between
        * another unlink_release detecting a refcnt of 1 and
        * it clearing its pred/succ pointers.
        */
        if (ptr_clear(p) == NULL)
            return 0;

        if (deleted_ptr(p)) {
            /* Another thread marked n->pred with deletion. */
            deref_release(q_head, p, 1);
            return 0;
        }

        /* Lock down p->succ and ensure it points at n. */
        if (unlink_ps_lock(q_head, p, n)) {
            assert(atomic_load_explicit(&ptr_clear(p)->succ,
                memory_order_relaxed) & DEREF);
            assert(ptr_clear((struct ll_elem*)atomic_load_explicit(
                &ptr_clear(p)->succ, memory_order_relaxed)) == n);
            break;			/* GUARD */
        }

        /*
        * p no longer points at n.  Re-resolve p.
        */
        deref_release(q_head, p, 1);
    }
    /*
    * p - direct predecessor of n.
    * p->succ - locked, pointing at n.
    *
    * Update n->prev to point at p and mark it as deleted.
    * Note that another thread can have acquired the deletion mark,
    * between our looking up of the predecessor and locking down p->succ.
    */
    assert(ptr_clear((struct ll_elem*)atomic_load_explicit(
        &ptr_clear(p)->succ, memory_order_relaxed)) == n);
    assert(!((uintptr_t)p & DEREF));
    assert(!((uintptr_t)p & FLAGGED));

    /* Acquire deletion lock. */
    p_ = p;
    p = ptr_clear(p);
    while (!atomic_compare_exchange_weak_explicit(&n->pred,
        (uintptr_t*)&p_, (uintptr_t)p | FLAGGED,
        memory_order_acq_rel, memory_order_relaxed)) {
        if (((uintptr_t)p_ & FLAGGED) || ptr_clear(p_) != p) {
            /* We have to restart or abort. */
            ptr_clear_deref(&p->succ);
            deref_release(q_head, p, 1);

            /* Another thread deleted n. */
            if ((uintptr_t)p_ & FLAGGED)
                return 0;
            /*
            * n->pred changed from under us (indicates p is being
            * deleted as well).
            */
            goto restart;
        }

        assert(ptr_clear(p_) == p);
        p_ = p;
        SPINWAIT();
    }
    ptr_clear_deref(&p->succ);	/* Unlock p->succ. */

    /*
    * n is succesfully marked as deleted,
    * with the correct value for n->pred
    * (the immediate predecessor, as required by the invariants).
    *
    * Update our predecessor p to skip n.
    */
    deref_release(q_head, succ(q_head, p), 1);
    deref_release(q_head, p, 1);
    atomic_fetch_sub_explicit(&q_head->size, 1, memory_order_relaxed);

    /*
    * Loop the list forward, clearing up references until our refcount
    * becomes 1 or the list is exhausted.
    */
    i = ptr_clear(succ(q_head, n));
    while (atomic_load_explicit(&n->refcnt, memory_order_relaxed) > 1) {
        /* If i points at n, update its pred pointer. */
        if (ptr_clear((struct ll_elem*)atomic_load_explicit(&i->pred,
            memory_order_relaxed)) == n)
            deref_release(q_head, pred(q_head, i), 1);

        /* GUARD: stop at end of q. */
        if (i == q)
            break;

        /* Skip to next element. */
        i_ = i;
        i = ptr_clear(succ(q_head, i));
        deref_release(q_head, i_, 1);
    }
    deref_release(q_head, i, 1);
    return 1;
}
/*
* Wait until the unlinked node is unreferenced.
*
* If inslock is set, this function will atomically acquire the insert lock
* on the node.
* If inslock is set, the reference held by this function will not be released.
*/
static void
unlink_release(struct ll_head *q_head, struct ll_elem *n, int inslock)
{
    struct ll_elem	*p, *s;
    int		 p_cas, s_cas;

    assert(n == ptr_clear(n));

    /* Wait until the last reference to n, not held by us, goes away. */
    while (atomic_load_explicit(&n->refcnt, memory_order_relaxed) > 1)
        SPINWAIT();

    s = ptr_clear((struct ll_elem*)atomic_load_explicit(&n->succ,
        memory_order_acquire));
    p = flag_combine((struct ll_elem*)atomic_load_explicit(&n->pred,
        memory_order_acquire), (struct ll_elem*)FLAGGED);
    assert(s != NULL && p != NULL);

    /*
    * Clear out the pred and succ pointers.
    * Set succ to 0 before setting pred to 0, since the former is used to
    * detect if the element is on the queue.
    *
    * Since ptr_cas maintains the FLAGGED bit, clear it manually
    * afterwards.
    */
    if (inslock) {
        uintptr_t ns;

        ns = atomic_fetch_or_explicit(&n->succ, FLAGGED,
            memory_order_acq_rel);
        assert(!(ns & FLAGGED));
        s = flag_combine(s, (struct ll_elem*)FLAGGED);
    }
    s_cas = ptr_cas(&n->succ, &s, NULL);
    assert(s_cas);
    ptr_clear_deref(&n->succ);
    deref_release(q_head, s, 1);

    p_cas = ptr_cas(&n->pred, &p, NULL);
    assert(p_cas);
    atomic_fetch_and_explicit(&n->pred, ~FLAGGED, memory_order_release);
    ptr_clear_deref(&n->pred);
    deref_release(q_head, p, 1);

    /* Release our reference. */
    if (!inslock)
        deref_release(q_head, n, 1);
}


/*
* Public interface.
*/


/*
* Remove the given element from the queue.
*
* Returns the removed element, if the delete succeeds.
* If the delete fails, the node is being removed by another thread.
*
* If wait is set, the function will not return until the last reference to n
* is removed.
*
* If wait is 0, the function will not wait until n is fully unreferenced.
* The ll_unlink_release() function should be called on n to complete the
* wait stage prior to freeing n.
*/
struct ll_elem*
    ll_unlink(struct ll_head *q_head, struct ll_elem *n, int wait)
{
        if (!unlink(q_head, n))
            return NULL;
        if (wait)
            unlink_release(q_head, n, 0);
        return n;
    }
/*
* Release n after unlinking it.  Should only be called if wait==0.
*/
void
ll_unlink_release(struct ll_head *q_head, struct ll_elem *n)
{
    unlink_release(q_head, n, 0);
}
/*
* Ensure the element is unlinked and will never again be linked.
*
* Returns n if the element was removed from the list as part of this operation.
* Returns null if the element was not on the list.
*/
struct ll_elem*
    ll_unlink_robust(struct ll_head *q_head, struct ll_elem *n)
{
        for (;;) {
            if (insert_lock(n))
                return NULL;

            if (unlink(q_head, n)) {
                unlink_release(q_head, n, 1);
                return n;
            }
            SPINWAIT();
        }
    }

/*
* Find the successor of n.
* This is the public facing interface for the queue.
*/
struct ll_elem*
    ll_succ(struct ll_head *q_head, struct ll_elem *n)
{
        struct ll_elem	*s;

        s = succ(q_head, n);

        /* Clean away the flag bits. */
        s = ptr_clear(s);
        if (s == &q_head->q) {
            /* Don't return q: it is not an element of the list. */
            deref_release(q_head, s, 1);
            s = NULL;
        }
        return s;
    }

/*
* Find the predecessor of n.
* This is the public facing interface for the queue.
*/
struct ll_elem*
    ll_pred(struct ll_head *q_head, struct ll_elem *n)
{
        struct ll_elem	*p;

        p = pred(q_head, n);

        /* Clean away the flag bits. */
        p = ptr_clear(p);
        if (p == &q_head->q) {
            /* Don't return q: it is not an element of the list. */
            deref_release(q_head, p, 1);
            p = NULL;
        }
        return p;
    }

/*
* Increment the reference on n.
* n must be in the queue or be referenced elsewhere (this is not checked).
*/
void
ll_ref(struct ll_head *q_head, struct ll_elem *n)
{
    assert(n != &q_head->q);
    deref_acquire(n, 1);
}

/*
* Release reference to n.
*/
void
ll_release(struct ll_head *q_head, struct ll_elem *n)
{
    assert(n != &q_head->q);
    deref_release(q_head, n, 1);
}

/*
* Test if the given queue is empty.
*
* May yield false negatives while an element is in the process
* of being deleted.  Note that that is not a bug, since it can
* only happen if another thread is in the process of deleting a
* node, in which case the observable behaviour is that the delete
* operation happens after the call to ll_empty().
*/
int
ll_empty(struct ll_head *q_head)
{
    return ptr_clear((struct ll_elem*)atomic_load_explicit(&q_head->q.succ,
        memory_order_relaxed)) == &q_head->q;
}

/*
* Insert n before rel.
*
* If rel gets deleted, the insert will happen before succ(rel).
* This ensures relative ordering between calls to
* insert_before(q,n,rel) and insert_after(q,n,pred(rel)).
*/
int
ll_insert_before(struct ll_head *q_head, struct ll_elem *n,
struct ll_elem *rel, int wait)
{
    struct ll_elem	*q, *s, *s_, *p;

    q = &q_head->q;
    assert(q == ptr_clear(q));
    assert(n == ptr_clear(n));
    assert(rel == ptr_clear(rel));

    /*
    * Mark n for insert.  If this fails, n is either on a queue or
    * being inserted/deleted.
    *
    * Required that n is properly initialized.
    */
    if (wait)
        unlink_release(q_head, n, 1);
    else if (!insert_lock(n))
        return 0;
    deref_acquire(n, 1);

    /* This is insert_before, so rel is the successor. */
    s = rel;
    p = NULL;
    deref_acquire(s, 1);	/* Our local reference. */

    /* Lookup predecessor. */
    p = ptr_clear(pred(q_head, s));

    /*
    * We now have:
    * - p -- the predecessor of the insert position
    * - s -- the successor of the insert position
    * - n -- the node we need to insert between p and s
    *
    * Note that both p and s may be deleted by the time we read this
    * comment.
    */
    while (!insert_between(q_head, n, p, s)) {
        /*
        * Insert failed.
        * This means at least one of p and s is no longer suitable.
        * Forget p, fix s and re-resolve p.
        */

        /* Forget p. */
        deref_release(q_head, p, 1);

        /* Fix s:
        * if it is deleted, we need to insert before its successor.
        */
        if (deleted(s)) {
            s_ = s;
            s = ptr_clear(succ(q_head, s));
            deref_release(q_head, s_, 1);
        }

        /* Find correct s. */
        p = ptr_clear(pred(q_head, s));
    }

    /*
    * We have succesfully inserted n.
    * Release our references on n, p and s.
    */
    deref_release(q_head, p, 1);
    deref_release(q_head, s, 1);
    deref_release(q_head, n, 1);
    return 1;
}

/*
* Insert n after rel.
*
* If rel gets deleted, the insert will happen after pred(rel).
* This ensures relative ordering between calls to
* insert_before(q,n,rel) and insert_after(q,n,pred(rel)).
*/
int
ll_insert_after(struct ll_head *q_head, struct ll_elem *n,
struct ll_elem *rel, int wait)
{
    struct ll_elem	*q, *s, *p, *p_;

    q = &q_head->q;
    assert(q == ptr_clear(q));
    assert(n == ptr_clear(n));
    assert(rel == ptr_clear(rel));

    /*
    * Mark n for insert.  If this fails, n is either on a queue or
    * being inserted/deleted.
    *
    * Required that n is properly initialized.
    */
    if (wait)
        unlink_release(q_head, n, 1);
    else if (!insert_lock(n))
        return 0;
    deref_acquire(n, 1);

    /* This is insert_after, so rel is the predecessor. */
    s = NULL;
    p = rel;
    deref_acquire(p, 1);	/* Our local reference. */

    /* Lookup successor. */
    s = ptr_clear(succ(q_head, p));

    /*
    * We now have:
    * - p -- the predecessor of the insert position
    * - s -- the successor of the insert position
    * - n -- the node we need to insert between p and s
    *
    * Note that both p and s may be deleted by the time we read this
    * comment.
    */
    while (!insert_between(q_head, n, p, s)) {
        /*
        * Insert failed.
        * This means at least one of p and s is no longer suitable.
        * Forget s, fix p and re-resolve s.
        */

        /* Forget s. */
        deref_release(q_head, s, 1);

        /* Fix p:
        * if it is deleted, we need to insert after its predecessor.
        */
        if (deleted(p)) {
            p_ = p;
            p = ptr_clear(pred(q_head, p));
            deref_release(q_head, p_, 1);
        }

        /* Find correct s. */
        s = ptr_clear(succ(q_head, p));
    }

    /*
    * We have succesfully inserted n.
    * Release our references on n, p and s.
    */
    deref_release(q_head, p, 1);
    deref_release(q_head, s, 1);
    deref_release(q_head, n, 1);
    return 1;
}

/*
* Insert n at the head of the list.
*/
int
ll_insert_head(struct ll_head *q_head, struct ll_elem *n, int wait)
{
    struct ll_elem	*q, *s;

    q = &q_head->q;
    assert(q == ptr_clear(q));
    assert(n == ptr_clear(n));

    /*
    * Mark n for insert.  If this fails, n is either on a queue or
    * being inserted/deleted.
    *
    * Required that n is properly initialized.
    */
    if (wait)
        unlink_release(q_head, n, 1);
    else if (!insert_lock(n))
        return 0;
    deref_acquire(n, 1);

    s = ptr_clear(succ(q_head, q));

    /*
    * We now have:
    * - q -- the predecessor of the insert position
    * - s -- the successor of the insert position
    * - n -- the node we need to insert between p and s
    *
    * Note that both s may be deleted by the time we read this
    * comment.
    */
    while (!insert_between(q_head, n, q, s)) {
        /*
        * Insert failed.
        * This means s is no longer suitable.
        */

        /* Re-resolve s. */
        deref_release(q_head, s, 1);
        s = ptr_clear(succ(q_head, q));
    }

    /*
    * We have succesfully inserted n.  Release our reference on n and s.
    */
    deref_release(q_head, s, 1);
    deref_release(q_head, n, 1);
    return 1;
}

/*
* Insert n at the tail of the list.
*/
int
ll_insert_tail(struct ll_head *q_head, struct ll_elem *n, int wait)
{
    struct ll_elem	*q, *p;

    q = &q_head->q;
    assert(q == ptr_clear(q));
    assert(n == ptr_clear(n));

    /*
    * Mark n for insert.  If this fails, n is either on a queue or
    * being inserted/deleted.
    *
    * Required that n is properly initialized.
    */
    if (wait)
        unlink_release(q_head, n, 1);
    else if (!insert_lock(n))
        return 0;
    deref_acquire(n, 1);

    p = ptr_clear(pred(q_head, q));

    /*
    * We now have:
    * - p -- the predecessor of the insert position
    * - q -- the successor of the insert position
    * - n -- the node we need to insert between p and q
    *
    * Note that p may be deleted by the time we read this
    * comment.
    */
    while (!insert_between(q_head, n, p, q)) {
        /*
        * Insert failed.
        * This means p is no longer suitable.
        */

        /* Re-resolve s. */
        deref_release(q_head, p, 1);
        p = ptr_clear(pred(q_head, q));
    }

    /*
    * We have succesfully inserted n.  Release our reference on n and p.
    */
    deref_release(q_head, p, 1);
    deref_release(q_head, n, 1);
    return 1;
}

/*
* Unlink and return the first node in the list.
*/
struct ll_elem*
    ll_pop_front(struct ll_head *q_head, int wait)
{
        struct ll_elem	*q, *n, *n_;

        q = &q_head->q;
        while ((n = ptr_clear(succ(q_head, q))) != q) {
            do {
                if (ll_unlink(q_head, n, wait))
                    return n;
                n_ = n;
                n = ptr_clear(succ(q_head, n_));
                deref_release(q_head, n_, 1);
            } while (n != q);
            deref_release(q_head, n, 1);
        }
        deref_release(q_head, n, 1);
        return NULL;
    }

/*
* Unlink and return the first node in the list.
*/
struct ll_elem*
    ll_pop_back(struct ll_head *q_head, int wait)
{
        struct ll_elem	*q, *n, *n_;

        q = &q_head->q;
        while ((n = ptr_clear(pred(q_head, q))) != q) {
            do {
                if (ll_unlink(q_head, n, wait))
                    return n;
                n_ = n;
                n = ptr_clear(pred(q_head, n_));
                deref_release(q_head, n_, 1);
            } while (n != q);
            deref_release(q_head, n, 1);
        }
        deref_release(q_head, n, 1);
        return NULL;
    }

/*
* Return the number of nodes in the list.
*/
size_t
ll_size(struct ll_head *q_head)
{
    return atomic_load_explicit(&q_head->size, memory_order_relaxed);
}

