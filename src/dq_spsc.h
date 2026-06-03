/**
 * dq_spsc.h — Single-Producer Single-Consumer lock-free queue
 *
 * Properties
 *   Zero CAS.  Both producer and consumer use only seq load/store
 *   (acquire/release).  This is the fastest possible ring-buffer queue.
 *
 *   The producer owns `head` exclusively; it reads it with relaxed load
 *   because no other thread ever writes it.  Same for the consumer and
 *   `tail`.  The only cross-thread synchronisation is through the slot's
 *   seq counter: producer release-stores seq after writing, consumer
 *   acquire-loads seq before reading.
 *
 * Thread safety
 *   Exactly ONE thread may call spsc_enqueue / spsc_enqueue_spin.
 *   Exactly ONE (different) thread may call spsc_dequeue.
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
    /* Producer cache line: head is written only by the producer. */
    DQ_ALIGNED uint64_t head;
    char _ph[DQ_CACHE_LINE - sizeof(uint64_t)];

    /* Consumer cache line: tail is written only by the consumer. */
    DQ_ALIGNED uint64_t tail;
    char _pt[DQ_CACHE_LINE - sizeof(uint64_t)];

    /* Cold metadata. */
    DQ_ALIGNED uint64_t capacity;
    uint64_t            mask;

    /* The ring. */
    dq_slot_t          *slots;
} spsc_queue_t;

/* ── lifecycle ──────────────────────────────────────────────────────────── */

static inline spsc_queue_t *spsc_queue_create(uint64_t cap)
{
    if (!dq_is_pow2(cap)) return NULL;

    spsc_queue_t *q = dq_aligned_alloc(DQ_CACHE_LINE, sizeof(*q));
    if (!q) return NULL;
    memset(q, 0, sizeof(*q));

    q->slots = dq_aligned_alloc(DQ_CACHE_LINE, cap * sizeof(dq_slot_t));
    if (!q->slots) { free(q); return NULL; }

    for (uint64_t i = 0; i < cap; i++)
        atomic_store_explicit(&q->slots[i].seq, i, memory_order_relaxed);

    q->head = q->tail = 0;
    q->capacity = cap;
    q->mask     = cap - 1;

    atomic_thread_fence(memory_order_seq_cst);
    return q;
}

static inline void spsc_queue_destroy(spsc_queue_t *q)
{
    if (!q) return;
    free(q->slots);
    free(q);
}

/* ── producer ───────────────────────────────────────────────────────────── */

/**
 * spsc_enqueue — non-blocking enqueue (producer thread only).
 *
 * No CAS.  The producer reads head from its own cache line (relaxed),
 * checks seq with acquire, writes data, then publishes with release.
 */
static inline dq_err_t spsc_enqueue(spsc_queue_t *q, void *data)
{
    if (DQ_UNLIKELY(!q)) return DQ_INVALID;

    uint64_t   pos  = q->head;           /* private to producer — no atomic needed */
    dq_slot_t *slot = &q->slots[pos & q->mask];
    uint64_t   seq  = atomic_load_explicit(&slot->seq, memory_order_acquire);

    if (DQ_UNLIKELY(seq != pos))         /* seq < pos: full; seq > pos: impossible */
        return DQ_FULL;

    slot->data = data;
    atomic_store_explicit(&slot->seq, pos + 1, memory_order_release);
    q->head = pos + 1;
    return DQ_OK;
}

static inline dq_err_t spsc_enqueue_spin(spsc_queue_t *q, void *data)
{
    if (DQ_UNLIKELY(!q)) return DQ_INVALID;
    for (uint32_t i = 0; i < DQ_MAX_SPIN; i++) {
        if (spsc_enqueue(q, data) == DQ_OK) return DQ_OK;
        if (i < DQ_SPIN_YIELD_AT) DQ_CPU_RELAX(); else sched_yield();
    }
    return DQ_FULL;
}

/* ── consumer ───────────────────────────────────────────────────────────── */

/**
 * spsc_dequeue — non-blocking dequeue (consumer thread only).
 *
 * No CAS.  Consumer reads tail from its own cache line (relaxed),
 * acquire-loads seq, reads data, then release-stores the recycled seq.
 */
static inline dq_err_t spsc_dequeue(spsc_queue_t *q, void **out)
{
    if (DQ_UNLIKELY(!q || !out)) return DQ_INVALID;

    uint64_t   pos  = q->tail;           /* private to consumer */
    dq_slot_t *slot = &q->slots[pos & q->mask];
    uint64_t   seq  = atomic_load_explicit(&slot->seq, memory_order_acquire);

    if (DQ_UNLIKELY(seq != pos + 1))     /* not yet published */
        return DQ_EMPTY;

    *out = slot->data;
    atomic_store_explicit(&slot->seq, pos + q->capacity, memory_order_release);
    q->tail = pos + 1;
    return DQ_OK;
}

/* ── introspection ──────────────────────────────────────────────────────── */

static inline uint64_t spsc_size(const spsc_queue_t *q)
{
    if (!q) return 0;
    /* head/tail are plain uint64_t; use compiler barrier for a snapshot. */
    uint64_t h = q->head, t = q->tail;
    return h >= t ? h - t : 0;
}
static inline bool     spsc_is_empty(const spsc_queue_t *q) { return spsc_size(q) == 0; }
static inline uint64_t spsc_capacity(const spsc_queue_t *q) { return q ? q->capacity : 0; }

#ifdef __cplusplus
}
#endif
