/**
 * dq_mpsc.h — Multi-Producer Single-Consumer lock-free queue
 *
 * Properties
 *   Producers  : wait-free fast path — one CAS on `head`, then private write.
 *   Consumer   : fully wait-free — no CAS, only seq acquire-load.
 *
 * Thread safety
 *   Any number of threads may call mpsc_enqueue / mpsc_enqueue_spin.
 *   Exactly ONE thread may call mpsc_dequeue.
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
    /* Hot producer line — multiple threads compete here. */
    DQ_ALIGNED _Atomic(uint64_t) head;
    char _ph[DQ_CACHE_LINE - sizeof(_Atomic(uint64_t))];

    /* Hot consumer line — owned by one thread, no sharing. */
    DQ_ALIGNED _Atomic(uint64_t) tail;
    char _pt[DQ_CACHE_LINE - sizeof(_Atomic(uint64_t))];

    DQ_ALIGNED uint64_t capacity;
    uint64_t            mask;
    dq_slot_t          *slots;
} mpsc_queue_t;

/* ── lifecycle ──────────────────────────────────────────────────────────── */

static inline mpsc_queue_t *mpsc_queue_create(uint64_t cap)
{
    if (!dq_is_pow2(cap)) return NULL;
    mpsc_queue_t *q = dq_aligned_alloc(DQ_CACHE_LINE, sizeof(*q));
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

static inline void mpsc_queue_destroy(mpsc_queue_t *q)
{
    if (!q) return;
    free(q->slots);
    free(q);
}

/* ── producer ───────────────────────────────────────────────────────────── */

static inline dq_err_t mpsc_enqueue(mpsc_queue_t *q, void *data)
{
    if (DQ_UNLIKELY(!q)) return DQ_INVALID;

    uint64_t pos = atomic_load_explicit(&q->head, memory_order_relaxed);
    for (;;) {
        dq_slot_t *slot = &q->slots[pos & q->mask];
        uint64_t   seq  = atomic_load_explicit(&slot->seq, memory_order_acquire);
        int64_t    diff = (int64_t)seq - (int64_t)pos;

        if (diff == 0) {
            /* Slot is free at our position — try to claim head. */
            if (atomic_compare_exchange_weak_explicit(
                    &q->head, &pos, pos + 1,
                    memory_order_relaxed, memory_order_relaxed))
            {
                slot->data = data;
                atomic_store_explicit(&slot->seq, pos + 1, memory_order_release);
                return DQ_OK;
            }
            /* Lost CAS — pos was updated by acq_fail, retry. */
            continue;
        }
        if (diff < 0) return DQ_FULL;   /* consumer hasn't caught up */
        /* diff > 0: another producer advanced head; reload and retry. */
        pos = atomic_load_explicit(&q->head, memory_order_relaxed);
    }
}

static inline dq_err_t mpsc_enqueue_spin(mpsc_queue_t *q, void *data)
{
    if (DQ_UNLIKELY(!q)) return DQ_INVALID;
    for (uint32_t i = 0; i < DQ_MAX_SPIN; i++) {
        dq_err_t e = mpsc_enqueue(q, data);
        if (e != DQ_FULL) return e;
        if (i < DQ_SPIN_YIELD_AT) DQ_CPU_RELAX(); else sched_yield();
    }
    return DQ_FULL;
}

/* ── consumer ───────────────────────────────────────────────────────────── */

static inline dq_err_t mpsc_dequeue(mpsc_queue_t *q, void **out)
{
    if (DQ_UNLIKELY(!q || !out)) return DQ_INVALID;

    uint64_t   pos  = atomic_load_explicit(&q->tail, memory_order_relaxed);
    dq_slot_t *slot = &q->slots[pos & q->mask];
    uint64_t   seq  = atomic_load_explicit(&slot->seq, memory_order_acquire);

    if ((int64_t)seq - (int64_t)(pos + 1) < 0) return DQ_EMPTY;

    *out = slot->data;
    atomic_store_explicit(&q->tail, pos + 1, memory_order_relaxed);
    atomic_store_explicit(&slot->seq, pos + q->capacity, memory_order_release);
    return DQ_OK;
}

/* ── introspection ──────────────────────────────────────────────────────── */

static inline uint64_t mpsc_size(const mpsc_queue_t *q) {
    if (!q) return 0;
    uint64_t h = atomic_load_explicit(&q->head, memory_order_acquire);
    uint64_t t = atomic_load_explicit(&q->tail, memory_order_acquire);
    return h >= t ? h - t : 0;
}
static inline bool     mpsc_is_empty(const mpsc_queue_t *q) { return mpsc_size(q) == 0; }
static inline uint64_t mpsc_capacity(const mpsc_queue_t *q) { return q ? q->capacity : 0; }

#ifdef __cplusplus
}
#endif
