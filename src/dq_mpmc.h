/**
 * dq_mpmc.h — Multi-Producer Multi-Consumer lock-free queue
 *
 * Algorithm
 *   Classic Dmitry Vyukov bounded MPMC queue.  Both ends use a CAS on
 *   their respective index.  The slot seq counter serialises producers
 *   and consumers independently; there is no global lock anywhere.
 *
 *   seq state machine (identical across all four variants):
 *     seq == pos          → empty, any producer may claim via CAS(head)
 *     seq == pos + 1      → full,  any consumer may claim via CAS(tail)
 *     seq == pos + cap    → recycled for the next production lap
 *
 * Producer flow
 *   1. Relaxed-load head → pos.
 *   2. Acquire-load slot seq.
 *      diff = seq - pos
 *      diff == 0 → slot is free at pos; try CAS(head, pos, pos+1).
 *      diff  < 0 → queue full; return DQ_FULL.
 *      diff  > 0 → another producer stole pos; reload head, retry.
 *   3. On CAS win: write data, release-store seq = pos + 1.
 *
 * Consumer flow (mirror of producer)
 *   1. Relaxed-load tail → pos.
 *   2. Acquire-load slot seq.
 *      diff = seq - (pos+1)
 *      diff == 0 → slot is full at pos; try CAS(tail, pos, pos+1).
 *      diff  < 0 → queue empty; return DQ_EMPTY.
 *      diff  > 0 → another consumer stole pos; reload tail, retry.
 *   3. On CAS win: read data, release-store seq = pos + cap.
 *
 * Thread safety
 *   Any number of threads may call mpmc_enqueue / mpmc_enqueue_spin.
 *   Any number of threads may call mpmc_dequeue / mpmc_dequeue_spin.
 */
#pragma once
#include "dq_platform.h"
#include <sched.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── struct ─────────────────────────────────────────────────────────────── */

typedef struct {
    /* Both lines are hot: multiple producers and consumers hammer them. */
    DQ_ALIGNED _Atomic(uint64_t) head;
    char _ph[DQ_CACHE_LINE - sizeof(_Atomic(uint64_t))];

    DQ_ALIGNED _Atomic(uint64_t) tail;
    char _pt[DQ_CACHE_LINE - sizeof(_Atomic(uint64_t))];

    DQ_ALIGNED uint64_t capacity;
    uint64_t            mask;
    dq_slot_t          *slots;
} mpmc_queue_t;

/* ── lifecycle ──────────────────────────────────────────────────────────── */

static inline mpmc_queue_t *mpmc_queue_create(uint64_t cap)
{
    if (!dq_is_pow2(cap)) return NULL;
    mpmc_queue_t *q = dq_aligned_alloc(DQ_CACHE_LINE, sizeof(*q));
    if (!q) return NULL;
    memset(q, 0, sizeof(*q));
    q->slots = dq_aligned_alloc(DQ_CACHE_LINE, cap * sizeof(dq_slot_t));
    if (!q->slots) { free(q); return NULL; }
    for (uint64_t i = 0; i < cap; i++)
        atomic_store_explicit(&q->slots[i].seq, i, memory_order_relaxed);
    atomic_store_explicit(&q->head, 0, memory_order_relaxed);
    atomic_store_explicit(&q->tail, 0, memory_order_relaxed);
    q->capacity = cap;
    q->mask     = cap - 1;
    atomic_thread_fence(memory_order_seq_cst);
    return q;
}

static inline void mpmc_queue_destroy(mpmc_queue_t *q)
{
    if (!q) return;
    free(q->slots);
    free(q);
}

/* ── producer ───────────────────────────────────────────────────────────── */

static inline dq_err_t mpmc_enqueue(mpmc_queue_t *q, void *data)
{
    if (DQ_UNLIKELY(!q)) return DQ_INVALID;

    uint64_t pos = atomic_load_explicit(&q->head, memory_order_relaxed);
    for (;;) {
        dq_slot_t *slot = &q->slots[pos & q->mask];
        uint64_t   seq  = atomic_load_explicit(&slot->seq, memory_order_acquire);
        int64_t    diff = (int64_t)seq - (int64_t)pos;

        if (diff == 0) {
            if (atomic_compare_exchange_weak_explicit(
                    &q->head, &pos, pos + 1,
                    memory_order_relaxed, memory_order_relaxed))
            {
                slot->data = data;
                atomic_store_explicit(&slot->seq, pos + 1, memory_order_release);
                return DQ_OK;
            }
            continue;   /* CAS failed — pos refreshed, retry immediately */
        }
        if (diff < 0) return DQ_FULL;
        pos = atomic_load_explicit(&q->head, memory_order_relaxed);
    }
}

static inline dq_err_t mpmc_enqueue_spin(mpmc_queue_t *q, void *data)
{
    if (DQ_UNLIKELY(!q)) return DQ_INVALID;
    for (uint32_t i = 0; i < DQ_MAX_SPIN; i++) {
        dq_err_t e = mpmc_enqueue(q, data);
        if (e != DQ_FULL) return e;
        if (i < DQ_SPIN_YIELD_AT) DQ_CPU_RELAX(); else sched_yield();
    }
    return DQ_FULL;
}

/* ── consumer ───────────────────────────────────────────────────────────── */

static inline dq_err_t mpmc_dequeue(mpmc_queue_t *q, void **out)
{
    if (DQ_UNLIKELY(!q || !out)) return DQ_INVALID;

    uint64_t pos = atomic_load_explicit(&q->tail, memory_order_relaxed);
    for (;;) {
        dq_slot_t *slot = &q->slots[pos & q->mask];
        uint64_t   seq  = atomic_load_explicit(&slot->seq, memory_order_acquire);
        int64_t    diff = (int64_t)seq - (int64_t)(pos + 1);

        if (diff == 0) {
            if (atomic_compare_exchange_weak_explicit(
                    &q->tail, &pos, pos + 1,
                    memory_order_relaxed, memory_order_relaxed))
            {
                *out = slot->data;
                atomic_store_explicit(&slot->seq,
                                      pos + q->capacity,
                                      memory_order_release);
                return DQ_OK;
            }
            continue;   /* lost race — pos refreshed, retry */
        }
        if (diff < 0) return DQ_EMPTY;
        pos = atomic_load_explicit(&q->tail, memory_order_relaxed);
    }
}

static inline dq_err_t mpmc_dequeue_spin(mpmc_queue_t *q, void **out)
{
    if (DQ_UNLIKELY(!q || !out)) return DQ_INVALID;
    for (uint32_t i = 0; i < DQ_MAX_SPIN; i++) {
        dq_err_t e = mpmc_dequeue(q, out);
        if (e != DQ_EMPTY) return e;
        if (i < DQ_SPIN_YIELD_AT) DQ_CPU_RELAX(); else sched_yield();
    }
    return DQ_EMPTY;
}

/* ── introspection ──────────────────────────────────────────────────────── */

static inline uint64_t mpmc_size(const mpmc_queue_t *q) {
    if (!q) return 0;
    uint64_t h = atomic_load_explicit(&q->head, memory_order_acquire);
    uint64_t t = atomic_load_explicit(&q->tail, memory_order_acquire);
    return h >= t ? h - t : 0;
}
static inline bool     mpmc_is_empty(const mpmc_queue_t *q) { return mpmc_size(q) == 0; }
static inline uint64_t mpmc_capacity(const mpmc_queue_t *q) { return q ? q->capacity : 0; }

#ifdef __cplusplus
}
#endif
