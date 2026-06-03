/**
 * dq_spmc.h — Single-Producer Multi-Consumer lock-free queue
 *
 * Algorithm
 *   SPMC is the structural mirror of MPSC.  Where MPSC puts a CAS on
 *   `head` (producer side) and leaves the consumer wait-free, SPMC puts
 *   the CAS on `tail` (consumer side) and leaves the producer wait-free.
 *
 *   seq state machine (same as every other variant):
 *     seq == pos          → empty, producer may write (no CAS needed here)
 *     seq == pos + 1      → full,  consumer may claim via CAS on tail
 *     seq == pos + cap    → recycled, available next lap
 *
 * Producer flow (wait-free, no CAS)
 *   1. Read head (own private counter, relaxed).
 *   2. Acquire-load slot seq; if seq != head → full.
 *   3. Write data.
 *   4. Release-store seq = head + 1.   ← linearisation point
 *   5. Increment local head.
 *
 * Consumer flow (each consumer competes for tail)
 *   1. Acquire-load head (shared position pointer, relaxed initial).
 *   2. Acquire-load slot seq; if seq != pos+1 → empty.
 *   3. CAS tail from pos → pos+1.      ← linearisation point
 *   4. Read data from slot.
 *   5. Release-store seq = pos + cap.  ← recycle slot for producer
 *
 * Thread safety
 *   Exactly ONE thread may call spmc_enqueue / spmc_enqueue_spin.
 *   Any number of threads may call spmc_dequeue / spmc_dequeue_spin.
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
    /* Hot consumer line — multiple threads compete here. */
    DQ_ALIGNED _Atomic(uint64_t) tail;
    char _pt[DQ_CACHE_LINE - sizeof(_Atomic(uint64_t))];

    /* Hot producer line — owned by one thread, no sharing. */
    DQ_ALIGNED _Atomic(uint64_t) head;
    char _ph[DQ_CACHE_LINE - sizeof(_Atomic(uint64_t))];

    DQ_ALIGNED uint64_t capacity;
    uint64_t            mask;
    dq_slot_t          *slots;
} spmc_queue_t;

/* ── lifecycle ──────────────────────────────────────────────────────────── */

static inline spmc_queue_t *spmc_queue_create(uint64_t cap)
{
    if (!dq_is_pow2(cap)) return NULL;
    spmc_queue_t *q = dq_aligned_alloc(DQ_CACHE_LINE, sizeof(*q));
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

static inline void spmc_queue_destroy(spmc_queue_t *q)
{
    if (!q) return;
    free(q->slots);
    free(q);
}

/* ── producer ───────────────────────────────────────────────────────────── */

/**
 * spmc_enqueue — non-blocking enqueue (producer thread only, no CAS).
 *
 * Because only one thread ever writes head, we keep it as a plain
 * uint64_t in the struct and update it without any atomic RMW.
 * The publish barrier is the release-store of seq.
 */
static inline dq_err_t spmc_enqueue(spmc_queue_t *q, void *data)
{
    if (DQ_UNLIKELY(!q)) return DQ_INVALID;

    uint64_t   pos  = atomic_load_explicit(&q->head, memory_order_relaxed);
    dq_slot_t *slot = &q->slots[pos & q->mask];
    uint64_t   seq  = atomic_load_explicit(&slot->seq, memory_order_acquire);

    /*
     * seq == pos  : slot is empty, we can write.
     * seq  < pos  : slot not yet recycled by consumers → full.
     * seq  > pos  : impossible (only one producer).
     */
    if (DQ_UNLIKELY((int64_t)seq - (int64_t)pos != 0))
        return DQ_FULL;

    slot->data = data;
    atomic_store_explicit(&slot->seq, pos + 1, memory_order_release);
    atomic_store_explicit(&q->head, pos + 1, memory_order_relaxed);
    return DQ_OK;
}

static inline dq_err_t spmc_enqueue_spin(spmc_queue_t *q, void *data)
{
    if (DQ_UNLIKELY(!q)) return DQ_INVALID;
    for (uint32_t i = 0; i < DQ_MAX_SPIN; i++) {
        dq_err_t e = spmc_enqueue(q, data);
        if (e != DQ_FULL) return e;
        if (i < DQ_SPIN_YIELD_AT) DQ_CPU_RELAX(); else sched_yield();
    }
    return DQ_FULL;
}

/* ── consumer ───────────────────────────────────────────────────────────── */

/**
 * spmc_dequeue — non-blocking dequeue (any consumer thread).
 *
 * Multiple consumers compete for the same tail via CAS.
 * After winning the CAS the consumer owns that slot privately:
 * no other consumer can read the same position.
 *
 * A subtle ordering requirement: the release-store of seq (step 5)
 * must happen AFTER reading data (step 4).  This is guaranteed by
 * sequenced-before in C11: the store comes later in program order.
 */
static inline dq_err_t spmc_dequeue(spmc_queue_t *q, void **out)
{
    if (DQ_UNLIKELY(!q || !out)) return DQ_INVALID;

    uint64_t pos = atomic_load_explicit(&q->tail, memory_order_relaxed);
    for (;;) {
        dq_slot_t *slot = &q->slots[pos & q->mask];
        uint64_t   seq  = atomic_load_explicit(&slot->seq, memory_order_acquire);
        int64_t    diff = (int64_t)seq - (int64_t)(pos + 1);

        if (diff == 0) {
            /*
             * Slot is full at our position — race other consumers for it.
             * CAS tail: pos → pos+1.
             */
            if (atomic_compare_exchange_weak_explicit(
                    &q->tail, &pos, pos + 1,
                    memory_order_relaxed, memory_order_relaxed))
            {
                /* Won the slot.  Read data, then recycle. */
                *out = slot->data;
                atomic_store_explicit(&slot->seq,
                                      pos + q->capacity,
                                      memory_order_release);
                return DQ_OK;
            }
            /* Lost race — pos updated by CAS failure, reload seq and retry. */
            continue;
        }
        if (diff < 0) return DQ_EMPTY;  /* producer hasn't published yet */
        /* diff > 0: another consumer just claimed this slot; reload pos. */
        pos = atomic_load_explicit(&q->tail, memory_order_relaxed);
    }
}

static inline dq_err_t spmc_dequeue_spin(spmc_queue_t *q, void **out)
{
    if (DQ_UNLIKELY(!q || !out)) return DQ_INVALID;
    for (uint32_t i = 0; i < DQ_MAX_SPIN; i++) {
        dq_err_t e = spmc_dequeue(q, out);
        if (e != DQ_EMPTY) return e;
        if (i < DQ_SPIN_YIELD_AT) DQ_CPU_RELAX(); else sched_yield();
    }
    return DQ_EMPTY;
}

/* ── introspection ──────────────────────────────────────────────────────── */

static inline uint64_t spmc_size(const spmc_queue_t *q) {
    if (!q) return 0;
    uint64_t h = atomic_load_explicit(&q->head, memory_order_acquire);
    uint64_t t = atomic_load_explicit(&q->tail, memory_order_acquire);
    return h >= t ? h - t : 0;
}
static inline bool     spmc_is_empty(const spmc_queue_t *q) { return spmc_size(q) == 0; }
static inline uint64_t spmc_capacity(const spmc_queue_t *q) { return q ? q->capacity : 0; }

#ifdef __cplusplus
}
#endif
