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
#pragma once

#include <atomic>
#include <stddef.h>
#include <stdint.h>

typedef std::atomic<uintptr_t> elem_ptr_t;
struct ll_elem {
	elem_ptr_t	succ, pred;
	std::atomic<size_t>	refcnt;
};

struct ll_head {
	struct ll_elem	    q;
    std::atomic<size_t>	size;
};


struct ll_elem	*ll_unlink(struct ll_head*, struct ll_elem*, int);
void		 ll_unlink_release(struct ll_head*, struct ll_elem*);
struct ll_elem	*ll_unlink_robust(struct ll_head*, struct ll_elem*);
struct ll_elem	*ll_succ(struct ll_head*, struct ll_elem*);
struct ll_elem	*ll_pred(struct ll_head*, struct ll_elem*);
void		 ll_ref(struct ll_head*, struct ll_elem*);
void		 ll_release(struct ll_head*, struct ll_elem*);
int		 ll_empty(struct ll_head*);
int		 ll_insert_before(struct ll_head*, struct ll_elem*,
		    struct ll_elem*, int);
int		 ll_insert_after(struct ll_head*, struct ll_elem*,
		    struct ll_elem*, int);
int		 ll_insert_head(struct ll_head*, struct ll_elem*, int);
int		 ll_insert_tail(struct ll_head*, struct ll_elem*, int);
struct ll_elem	*ll_pop_front(struct ll_head*, int);
struct ll_elem	*ll_pop_back(struct ll_head*, int);
size_t		 ll_size(struct ll_head*);


#define LL_HEAD(name, type)						\
	struct name {							\
		struct ll_head	ll_head;				\
	}
#define LL_ENTRY(type)							\
	struct ll_elem

#define LL_HEAD_INITIALIZER(head)					\
	{ LL_HEAD_INITIALIZER__HEAD(head.ll_head) }

#define LL_HEAD_INITIALIZER__HEAD(ll_head)				\
{									\
	{								\
		ATOMIC_VAR_INIT((uintptr_t)&ll_head),			\
		ATOMIC_VAR_INIT((uintptr_t)&ll_head),			\
		ATOMIC_VAR_INIT((size_t)2)				\
	},								\
	ATOMIC_VAR_INIT((size_t)0)					\
}

#define LL_INIT(head)							\
	LL_INIT__LL_HEAD(&(head)->ll_head)

#define LL_INIT__LL_HEAD(ll_head)					\
do {									\
	atomic_init(&(ll_head)->q.succ, (uintptr_t)(ll_head));		\
    atomic_init(&(ll_head)->q.pred, (uintptr_t)(ll_head));		\
	atomic_init(&(ll_head)->q.refcnt, 2);				\
	atomic_init(&(ll_head)->size, 0);				\
} while (0)

#define LL_ENTRY_INITIALIZER(entry)					\
{									\
	ATOMIC_VAR_INIT((uintptr_t)0),					\
	ATOMIC_VAR_INIT((uintptr_t)0),					\
	ATOMIC_VAR_INIT((size_t)0),					\
}

#define LL_INIT_ENTRY(entry)						\
do {									\
	atomic_init(&(entry)->succ, (uintptr_t)0);			\
	atomic_init(&(entry)->pred, (uintptr_t)0);			\
	atomic_init(&(entry)->refcnt, 0);				\
} while (0)

#define LL_NEXT(name, head, node)	ll_succ_##name(head, node)
#define LL_PREV(name, head, node)	ll_pred_##name(head, node)
#define LL_FIRST(name, head)		ll_first_##name(head)
#define LL_LAST(name, head)		ll_last_##name(head)
#define LL_EMPTY(name, head)		ll_empty_##name(head)
#define LL_SIZE(name, head)		ll_size_##name(head)
#define LL_REF(name, head, node)	ll_ref_##name(head, node)
#define LL_RELEASE(name, head, node)	ll_release_##name(head, node)

#define LL_INSERT_AFTER(name, head, node, rel)				\
	ll_insert_after_##name(head, node, rel)
#define LL_INSERT_BEFORE(name, head, node, rel)				\
	ll_insert_before_##name(head, node, rel)
#define LL_INSERT_HEAD(name, head, node)				\
	ll_insert_head_##name(head, node)
#define LL_INSERT_TAIL(name, head, node)				\
	ll_insert_tail_##name(head, node)
#define LL_UNLINK(name, head, node)					\
	ll_unlink_##name(head, node)
#define LL_UNLINK_NOWAIT(name, head, node)				\
	ll_unlink_nowait_##name(head, node)
#define LL_UNLINK_WAIT(name, head, node)				\
	ll_unlink_wait_##name(head, node)
#define LL_UNLINK_ROBUST(name, head, node)				\
	ll_unlink_robust_##name(head, node)
#define LL_POP_FRONT(name, head)					\
	ll_pop_front_##name(head)
#define LL_POP_FRONT_NOWAIT(name, head)					\
	ll_pop_front_nowait_##name(head)
#define LL_POP_BACK(name, head)						\
	ll_pop_back_##name(head)
#define LL_POP_BACK_NOWAIT(name, head)					\
	ll_pop_back_nowait_##name(head)
#define LL_UNLINK_WAIT_INSERT_BEFORE(name, head, node, rel)		\
	ll_unlink_wait_insert_before_##name(head, node, rel)
#define LL_UNLINK_WAIT_INSERT_AFTER(name, head, node, rel)		\
	ll_unlink_wait_insert_after_##name(head, node, rel)
#define LL_UNLINK_WAIT_INSERT_HEAD(name, head, node)			\
	ll_unlink_wait_insert_head_##name(head, node)
#define LL_UNLINK_WAIT_INSERT_TAIL(name, head, node)			\
	ll_unlink_wait_insert_tail_##name(head, node)

#define LL_FOREACH(var, name, head)					\
	for ((var) = ll_first_##name(head);				\
	    (var) != NULL;						\
	    (var) = ll_foreach_succ_##name((head), (var)))
#define LL_FOREACH_REVERSE(var, name, head)				\
	for ((var) = ll_last_##name(head);				\
	    (var) != NULL;						\
	    (var) = ll_foreach_pred_##name((head), (var)))

#define LL_PUSH_FRONT(name, head, node)					\
	ll_insert_head_##name(head, node)
#define LL_PUSH_BACK(name, head, node)					\
	ll_insert_tail_##name(head, node)


#define LL_GENERATE(name, type, member)					\
static __inline type*						\
ll_elem_##name(struct ll_elem *e)					\
{									\
	return (e == NULL ? NULL :					\
	    (type*)((uintptr_t)e -				\
	    offsetof(type, member)));				\
}									\
static __inline type*						\
ll_succ_##name(struct name *q, type *n)				\
{									\
	return ll_elem_##name(ll_succ(&q->ll_head, &n->member));	\
}									\
static __inline type*						\
ll_pred_##name(struct name *q, type *n)				\
{									\
	return ll_elem_##name(ll_pred(&q->ll_head, &n->member));	\
}									\
static __inline type*						\
ll_first_##name(struct name *q)						\
{									\
	return ll_elem_##name(ll_succ(&q->ll_head, &q->ll_head.q));	\
}									\
static __inline type*						\
ll_last_##name(struct name *q)						\
{									\
	return ll_elem_##name(ll_pred(&q->ll_head, &q->ll_head.q));	\
}									\
static __inline void							\
ll_ref_##name(struct name *q, type *n)				\
{									\
	ll_ref(&q->ll_head, &n->member);				\
}									\
static __inline void							\
ll_release_##name(struct name *q, type *n)			\
{									\
	ll_release(&q->ll_head, &n->member);				\
}									\
static __inline int							\
ll_empty_##name(struct name *q)						\
{									\
	return ll_empty(&q->ll_head);					\
}									\
static __inline type*						\
ll_foreach_succ_##name(struct name *q, type *n)			\
{									\
	type	*s;						\
									\
	/* Lookup successor. */						\
	s = ll_succ_##name(q, n);					\
	/* Release n. */						\
	ll_release_##name(q, n);					\
	/* Return successor. */						\
	return s;							\
}									\
static __inline type*						\
ll_foreach_pred_##name(struct name *q, type *n)			\
{									\
	type	*p;						\
									\
	/* Lookup predecessor. */					\
	p = ll_pred_##name(q, n);					\
	/* Release n. */						\
	ll_release_##name(q, n);					\
	/* Return successor. */						\
	return p;							\
}									\
static __inline int							\
ll_insert_after_##name(struct name *q, type *n,			\
    type *rel)							\
{									\
	return ll_insert_after(&q->ll_head, &n->member,			\
	    &rel->member, 0);						\
}									\
static __inline int							\
ll_insert_before_##name(struct name *q, type *n,			\
    type *rel)							\
{									\
	return ll_insert_before(&q->ll_head, &n->member,		\
	    &rel->member, 0);						\
}									\
static __inline int							\
ll_insert_head_##name(struct name *q, type *n)			\
{									\
	return ll_insert_head(&q->ll_head, &n->member, 0);		\
}									\
static __inline int							\
ll_insert_tail_##name(struct name *q, type *n)			\
{									\
	return ll_insert_tail(&q->ll_head, &n->member, 0);		\
}									\
static __inline type*						\
ll_unlink_##name(struct name *q, type *n)			\
{									\
	return ll_elem_##name(ll_unlink(&q->ll_head, &n->member, 1));	\
}									\
static __inline type*						\
ll_unlink_robust_##name(struct name *q, type *n)			\
{									\
	return ll_elem_##name(						\
	    ll_unlink_robust(&q->ll_head, &n->member));			\
}									\
static __inline type*						\
ll_unlink_nowait_##name(struct name *q, type *n)			\
{									\
	return ll_elem_##name(ll_unlink(&q->ll_head, &n->member, 0));	\
}									\
static __inline void							\
ll_unlink_wait_##name(struct name *q, type *n)			\
{									\
	ll_unlink_release(&q->ll_head, &n->member);			\
}									\
static __inline void							\
ll_unlink_wait_insert_tail_##name(struct name *q, type *n)	\
{									\
	/* Never fails. */						\
	ll_insert_tail(&q->ll_head, &n->member, 1);			\
}									\
static __inline void							\
ll_unlink_wait_insert_head_##name(struct name *q, type *n)	\
{									\
	/* Never fails. */						\
	ll_insert_head(&q->ll_head, &n->member, 1);			\
}									\
static __inline void							\
ll_unlink_wait_insert_after_##name(struct name *q, type *n,	\
    type *rel)							\
{									\
	/* Never fails. */						\
	ll_insert_after(&q->ll_head, &n->member,			\
	    &rel->member, 1);						\
}									\
static __inline void							\
ll_unlink_wait_insert_before_##name(struct name *q, type *n,	\
    type *rel)							\
{									\
	/* Never fails. */						\
	ll_insert_before(&q->ll_head, &n->member,			\
	    &rel->member, 1);						\
}									\
static __inline type*						\
ll_pop_front_##name(struct name *q)					\
{									\
	return ll_elem_##name(ll_pop_front(&q->ll_head, 1));		\
}									\
static __inline type*						\
ll_pop_back_##name(struct name *q)					\
{									\
	return ll_elem_##name(ll_pop_back(&q->ll_head, 1));		\
}									\
static __inline type*						\
ll_pop_front_nowait_##name(struct name *q)				\
{									\
	return ll_elem_##name(ll_pop_front(&q->ll_head, 0));		\
}									\
static __inline type*						\
ll_pop_back_nowait_##name(struct name *q)				\
{									\
	return ll_elem_##name(ll_pop_back(&q->ll_head, 0));		\
}									\
static __inline size_t							\
ll_size_##name(struct name *q)						\
{									\
	return ll_size(&q->ll_head);					\
}
/* End of LL_GENERATE macro. */

