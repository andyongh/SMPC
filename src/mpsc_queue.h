/*
 * mpsc_queue.h — Production lock-free MPSC queue for Linux (C11)
 * ================================================================
 *
 * Two self-contained, header-only implementations:
 *
 *   ┌──────────────────────────────────────────────────────────────────┐
 *   │ 1. mpsc_lf_*   Unbounded intrusive linked-list (Vyukov)          │
 *   │                Producers: wait-free  (1 atomic exchange/push)     │
 *   │                Consumer : lock-free  (may observe transient NULL) │
 *   ├──────────────────────────────────────────────────────────────────┤
 *   │ 2. mpsc_rb_*   Bounded ring buffer, power-of-2 capacity          │
 *   │                Sequence-number slots, cache-line isolated         │
 *   │                Producers: wait-free when queue is not full;       │
 *   │                           spin (progress-guaranteed) when full    │
 *   │                Consumer : wait-free                               │
 *   └──────────────────────────────────────────────────────────────────┘
 *
 * Both structures are safe for any number of concurrent producers and
 * exactly ONE consumer thread.
 *
 * Requires: C11, Linux/glibc, x86-64 or ARMv8 (any TSO/release-acquire arch).
 *           Compile with -std=c11 -O2 -pthread.
 *
 * SPDX-License-Identifier: MIT
 */

#pragma once

#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdalign.h>
#include <stdbool.h>
#include <assert.h>
#include <errno.h>
#include <stdlib.h>
#include <sched.h>   /* sched_yield() */

/* -----------------------------------------------------------------------
 * Platform helpers
 * --------------------------------------------------------------------- */

#define MPSC_CACHE_LINE  64u

/** Portable container_of. */
#define mpsc_container_of(ptr, type, member) \
    ((type *)((char *)(uintptr_t)(ptr) - offsetof(type, member)))

/** CPU-level spin-loop hint (prevents pipeline stalls in tight loops). */
#if defined(__x86_64__) || defined(__i386__)
#  define mpsc_cpu_relax()  __asm__ volatile("pause" ::: "memory")
#elif defined(__aarch64__) || defined(__arm__)
#  define mpsc_cpu_relax()  __asm__ volatile("yield" ::: "memory")
#else
#  define mpsc_cpu_relax()  atomic_thread_fence(memory_order_seq_cst)
#endif

/** Backoff: alternate between PAUSE and sched_yield for long waits. */
static inline void mpsc_backoff(unsigned *step)
{
    if ((*step)++ < 16) {
        mpsc_cpu_relax();
    } else {
        *step = 0;
        sched_yield();
    }
}

/* =========================================================================
 * IMPLEMENTATION 1: Unbounded Intrusive Linked-List MPSC Queue
 *
 * Algorithm
 * ---------
 * Dmitry Vyukov's MPSC non-intrusive queue, adapted to be fully intrusive
 * (no internal node allocation).
 *
 *   push(node):
 *     node->next = NULL                        // relaxed: node not yet shared
 *     prev = XCHG(&tail, node)                 // acq_rel: serialise producers
 *     STORE(&prev->next, node)                 // release: hand node to consumer
 *
 *   pop():
 *     head = q->head                           // single consumer: plain load
 *     next = LOAD(&head->next)                 // acquire: see producer's data
 *     if next == NULL → return NULL (empty or mid-enqueue)
 *     q->head = next
 *     return next
 *
 * Memory ordering
 * ---------------
 *   push acq_rel XCHG  → establishes a total order among all producers.
 *   push release STORE → makes the node visible to the consumer.
 *   pop  acquire LOAD  → pairs with the producer's release store above,
 *                        guaranteeing all writes done before STORE are
 *                        visible to the consumer.
 *
 * Caveats
 * -------
 *   • pop() can return NULL even when a concurrent push() is in flight
 *     (producer completed XCHG but has not yet stored prev->next).  This
 *     is inherent to the algorithm and is fine for try-pop semantics.
 *   • The stub node lives inside the queue struct and must not be freed.
 *   • Nodes must not be reused until pop() has returned them.
 *
 * Embed mpsc_lf_node_t in your own struct and use mpsc_container_of()
 * to recover the outer pointer.
 * ========================================================================= */

typedef struct mpsc_lf_node {
    _Atomic(struct mpsc_lf_node *) next;
} mpsc_lf_node_t;

typedef struct {
    /* Producer side — written by every push().  Isolated on its own cache line
     * to avoid false sharing with the consumer's head pointer.              */
    alignas(MPSC_CACHE_LINE) _Atomic(mpsc_lf_node_t *) tail;

    /* Consumer side — written only by pop().                                */
    alignas(MPSC_CACHE_LINE) mpsc_lf_node_t *head;

    /* Sentinel node: always present, never returned to the caller.          */
    mpsc_lf_node_t stub;
} mpsc_lf_queue_t;

/**
 * mpsc_lf_init() — Initialise queue.  Call once before any push/pop.
 */
static inline void mpsc_lf_init(mpsc_lf_queue_t *q)
{
    atomic_store_explicit(&q->stub.next, NULL, memory_order_relaxed);
    atomic_store_explicit(&q->tail, &q->stub, memory_order_relaxed);
    q->head = &q->stub;
}

/**
 * mpsc_lf_push() — Enqueue a node.  Wait-free.  Thread-safe for N producers.
 *
 * @node  Caller-allocated node embedded in the item to enqueue.
 *        Must remain valid until returned by mpsc_lf_pop().
 */
static inline void mpsc_lf_push(mpsc_lf_queue_t *q, mpsc_lf_node_t *node)
{
    /* Node is brand-new from the producer's perspective; relaxed is enough. */
    atomic_store_explicit(&node->next, NULL, memory_order_relaxed);

    /*
     * Atomically swing the tail to the new node and get the previous tail.
     * acq_rel: the acquire half ensures we see any prior state of prev;
     *          the release half makes our node's payload visible once
     *          prev->next is stored below.
     */
    mpsc_lf_node_t *prev =
        atomic_exchange_explicit(&q->tail, node, memory_order_acq_rel);

    /*
     * Link the previous tail to our node.  This is the only store that
     * makes the node reachable by the consumer, so it must be a release.
     *
     * Between the exchange above and this store, the consumer may load
     * prev->next and observe NULL — correctly treating the queue as
     * transiently empty.  That is documented behaviour.
     */
    atomic_store_explicit(&prev->next, node, memory_order_release);
}

/**
 * mpsc_lf_pop() — Dequeue a node.  Lock-free.  Must be called from one
 * consumer thread only.
 *
 * @return  Pointer to the dequeued node, or NULL if the queue is empty
 *          (or transiently appears empty due to a concurrent in-flight push).
 *
 * The returned node is always the *next* field of the old head (sentinel),
 * not the head node itself.  Use mpsc_container_of() to retrieve your struct.
 */
static inline mpsc_lf_node_t *mpsc_lf_pop(mpsc_lf_queue_t *q)
{
    mpsc_lf_node_t *head = q->head; /* single consumer: plain load is safe */

    /*
     * Acquire: if next != NULL, all writes the producer did before its
     * release-store of prev->next are now visible to us.
     */
    mpsc_lf_node_t *next =
        atomic_load_explicit(&head->next, memory_order_acquire);

    if (next == NULL) {
        return NULL; /* empty or a push() is mid-flight */
    }

    /*
     * Advance the sentinel.  'next' becomes the new sentinel; it is also
     * the node we return to the caller.  The old head (prev sentinel) is
     * now logically consumed and must not be touched by the consumer again
     * (but the caller should not free stub — it's owned by the queue).
     */
    q->head = next;
    return next;
}

/**
 * mpsc_lf_pop_wait() — Like mpsc_lf_pop() but spins until an item is
 * available.  Useful when the caller knows items are incoming.
 */
static inline mpsc_lf_node_t *mpsc_lf_pop_wait(mpsc_lf_queue_t *q)
{
    unsigned step = 0;
    mpsc_lf_node_t *node;
    while ((node = mpsc_lf_pop(q)) == NULL) {
        mpsc_backoff(&step);
    }
    return node;
}

/**
 * mpsc_lf_empty() — Non-authoritative emptiness check (single consumer only).
 * A push() currently in flight may cause a false "empty" reading.
 */
static inline bool mpsc_lf_empty(const mpsc_lf_queue_t *q)
{
    return atomic_load_explicit(
               &q->head->next, memory_order_acquire) == NULL;
}


/* =========================================================================
 * IMPLEMENTATION 2: Bounded Ring-Buffer MPSC Queue
 *
 * Algorithm
 * ---------
 * Each slot carries a sequence number that acts as a generation counter.
 * Producers claim a slot with an atomic fetch_add on a shared head index;
 * the consumer advances a private tail index (no sharing required).
 *
 *   Slot lifecycle (capacity = N, mask = N-1):
 *
 *     Initial         : slot[i].seq = i           (i = 0 … N-1)
 *     After enqueue   : slot[i].seq = pos + 1     (signals consumer)
 *     After dequeue   : slot[i].seq = pos + N     (signals producers for
 *                                                   the next generation)
 *
 *   push(data):
 *     pos = fetch_add(&head, 1)                   // acq_rel: claim slot
 *     slot = &slots[pos & mask]
 *     spin until slot->seq == pos                 // acquire: wait for slot
 *     slot->data = data                           // plain: guarded by seq
 *     STORE(&slot->seq, pos + 1)                  // release: signal consumer
 *
 *   pop(out):
 *     pos = consumer_tail                         // private: no atomic needed
 *     slot = &slots[pos & mask]
 *     LOAD(&slot->seq) acquire; if != pos+1 → empty, return false
 *     *out = slot->data                           // plain: guarded by seq
 *     STORE(&slot->seq, pos + N)                  // release: recycle slot
 *     consumer_tail++
 *     return true
 *
 * Memory ordering
 * ---------------
 *   push: release store of seq  → pairs with consumer's acquire load.
 *   pop:  acquire load of seq   → sees all writes before push's release.
 *   pop:  release store of seq  → pairs with producer's acquire load when
 *                                 the slot is recycled in the next round.
 *
 * Capacity
 * --------
 *   Must be a power of two, ≥ 2.  Recommended: 2× peak outstanding items
 *   so producers never spin on a full queue in the common case.
 * ========================================================================= */

typedef struct {
    alignas(MPSC_CACHE_LINE) _Atomic(uint64_t) sequence;
    void *data;
    /* Pad so each slot occupies exactly one cache line. */
    char _pad[MPSC_CACHE_LINE
              - sizeof(_Atomic(uint64_t))
              - sizeof(void *)];
} mpsc_rb_slot_t;

static_assert(sizeof(mpsc_rb_slot_t) == MPSC_CACHE_LINE,
              "mpsc_rb_slot_t must be exactly one cache line");

typedef struct {
    /* Producers contend on this counter to claim slots.                     */
    alignas(MPSC_CACHE_LINE) _Atomic(uint64_t) enqueue_pos;

    /* Consumer's private tail — never written by producers.                 */
    alignas(MPSC_CACHE_LINE) uint64_t dequeue_pos;

    /* Queue geometry.                                                        */
    alignas(MPSC_CACHE_LINE) uint64_t capacity; /* always a power of 2       */
    uint64_t mask;                              /* capacity - 1              */

    /* Flexible array of slots.  Allocated by mpsc_rb_create().              */
    mpsc_rb_slot_t slots[];
} mpsc_rb_queue_t;

/**
 * mpsc_rb_create() — Allocate and initialise a ring-buffer queue.
 *
 * @capacity  Number of slots.  Must be a power of two and ≥ 2.
 * @return    Heap-allocated queue, or NULL on allocation failure.
 *            Free with mpsc_rb_destroy().
 */
static inline mpsc_rb_queue_t *mpsc_rb_create(uint64_t capacity)
{
    if (capacity < 2 || (capacity & (capacity - 1)) != 0) {
        /* Not a power of two. */
        errno = EINVAL;
        return NULL;
    }

    size_t sz = sizeof(mpsc_rb_queue_t) + capacity * sizeof(mpsc_rb_slot_t);
    mpsc_rb_queue_t *q = (mpsc_rb_queue_t *)aligned_alloc(MPSC_CACHE_LINE, sz);
    if (!q) return NULL;

    atomic_store_explicit(&q->enqueue_pos, 0, memory_order_relaxed);
    q->dequeue_pos = 0;
    q->capacity    = capacity;
    q->mask        = capacity - 1;

    /* Initialise each slot's generation counter to its index. */
    for (uint64_t i = 0; i < capacity; i++) {
        atomic_store_explicit(&q->slots[i].sequence, i, memory_order_relaxed);
        q->slots[i].data = NULL;
    }

    /* Ensure all initialisation is visible before any push/pop. */
    atomic_thread_fence(memory_order_seq_cst);
    return q;
}

/**
 * mpsc_rb_destroy() — Free a queue created by mpsc_rb_create().
 * Must not be called while producers or consumer are active.
 */
static inline void mpsc_rb_destroy(mpsc_rb_queue_t *q)
{
    free(q);
}

/**
 * mpsc_rb_push() — Enqueue a pointer.  Wait-free unless queue is full.
 * Thread-safe for N concurrent producers.
 *
 * If the queue is full (all slots occupied by unconsumed items), this
 * function spins until a slot is freed by the consumer.  Size the queue
 * to make this rare.
 *
 * @data  Arbitrary pointer (must not be NULL if you rely on NULL as sentinel).
 */
static inline void mpsc_rb_push(mpsc_rb_queue_t *q, void *data)
{
    /*
     * Claim a slot position with an atomic increment.  Even if multiple
     * producers race, each gets a unique pos and therefore a unique slot.
     * acq_rel: the acquire half synchronises with the consumer's release
     *          store when it recycles a slot; the release half is unused
     *          here but enforces a total modification order on enqueue_pos.
     */
    uint64_t pos =
        atomic_fetch_add_explicit(&q->enqueue_pos, 1, memory_order_acq_rel);

    mpsc_rb_slot_t *slot = &q->slots[pos & q->mask];

    /*
     * Wait until the consumer has recycled this slot for us (seq == pos).
     * Initially seq[i] = i, so the very first enqueue at each slot does
     * not spin at all.  In subsequent generations, we spin only if the
     * consumer hasn't caught up yet.
     */
    unsigned step = 0;
    for (;;) {
        uint64_t seq =
            atomic_load_explicit(&slot->sequence, memory_order_acquire);
        if (seq == pos) break;           /* slot is ours */
        /* seq < pos: consumer hasn't freed it yet — backoff and retry. */
        mpsc_backoff(&step);
    }

    slot->data = data; /* plain store: protected by the sequence handshake */

    /*
     * Publish the slot to the consumer by advancing the sequence to pos+1.
     * release: guarantees slot->data is visible when consumer acquires seq.
     */
    atomic_store_explicit(&slot->sequence, pos + 1, memory_order_release);
}

/**
 * mpsc_rb_try_push() — Non-blocking enqueue.  Returns false immediately
 * if the queue appears full rather than spinning.
 *
 * Unlike mpsc_rb_push(), this attempts a CAS on enqueue_pos so that the
 * counter is not incremented if the queue is full.
 *
 * Note: this promotes the push from wait-free to lock-free because of the
 * CAS retry loop on enqueue_pos contention.  Use mpsc_rb_push() when
 * throughput matters and mpsc_rb_try_push() when you cannot afford to block.
 */
static inline bool mpsc_rb_try_push(mpsc_rb_queue_t *q, void *data)
{
    uint64_t pos = atomic_load_explicit(&q->enqueue_pos, memory_order_relaxed);

    for (;;) {
        mpsc_rb_slot_t *slot = &q->slots[pos & q->mask];
        uint64_t seq =
            atomic_load_explicit(&slot->sequence, memory_order_acquire);

        int64_t diff = (int64_t)seq - (int64_t)pos;

        if (diff == 0) {
            /* Slot is free.  Race to claim it. */
            if (atomic_compare_exchange_weak_explicit(
                    &q->enqueue_pos, &pos, pos + 1,
                    memory_order_acq_rel, memory_order_relaxed)) {
                /* We won the slot. */
                slot->data = data;
                atomic_store_explicit(&slot->sequence, pos + 1,
                                      memory_order_release);
                return true;
            }
            /* Another producer won; reload and retry. */
            continue;
        }

        if (diff < 0) {
            /* seq < pos: slot still occupied by consumer — queue full. */
            return false;
        }

        /* diff > 0: slot already consumed, reload enqueue_pos and retry. */
        pos = atomic_load_explicit(&q->enqueue_pos, memory_order_relaxed);
    }
}

/**
 * mpsc_rb_pop() — Dequeue a pointer.  Wait-free.
 * Must be called from exactly ONE consumer thread.
 *
 * @out   Set to the dequeued pointer on success.
 * @return true if an item was dequeued, false if the queue is empty.
 */
static inline bool mpsc_rb_pop(mpsc_rb_queue_t *q, void **out)
{
    uint64_t pos  = q->dequeue_pos; /* private: no atomic needed            */
    mpsc_rb_slot_t *slot = &q->slots[pos & q->mask];

    /*
     * Check whether the producer has finished writing this slot.
     * acquire: if seq == pos+1, all writes before the producer's release
     *          store of seq are visible to us (including slot->data).
     */
    uint64_t seq =
        atomic_load_explicit(&slot->sequence, memory_order_acquire);

    if (seq != pos + 1) {
        return false; /* not ready yet — queue appears empty */
    }

    *out = slot->data;

    /*
     * Recycle the slot for the next generation of producers.
     * The next enqueue into this slot will be at pos + capacity, so set
     * seq = pos + capacity to signal that the slot is free.
     * release: pairs with the producer's acquire load of slot->sequence
     *          in mpsc_rb_push() / mpsc_rb_try_push().
     */
    atomic_store_explicit(&slot->sequence, pos + q->capacity,
                          memory_order_release);

    q->dequeue_pos = pos + 1;
    return true;
}

/**
 * mpsc_rb_pop_wait() — Like mpsc_rb_pop() but spins until an item is ready.
 */
static inline void *mpsc_rb_pop_wait(mpsc_rb_queue_t *q)
{
    unsigned step = 0;
    void *out;
    while (!mpsc_rb_pop(q, &out)) {
        mpsc_backoff(&step);
    }
    return out;
}

/**
 * mpsc_rb_size() — Approximate number of items currently in the queue.
 * Non-authoritative: producers may be mid-flight.
 */
static inline uint64_t mpsc_rb_size(const mpsc_rb_queue_t *q)
{
    uint64_t enq =
        atomic_load_explicit(&q->enqueue_pos, memory_order_acquire);
    uint64_t deq = q->dequeue_pos;
    return enq > deq ? enq - deq : 0;
}

/**
 * mpsc_rb_empty() — Non-authoritative emptiness check (consumer only).
 */
static inline bool mpsc_rb_empty(const mpsc_rb_queue_t *q)
{
    return mpsc_rb_size(q) == 0;
}
