/**
 * dq_platform.h — Shared platform definitions for all daking queue variants
 *
 * Included by every queue header; never included directly by application code.
 */
#pragma once

#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* ── cache line ─────────────────────────────────────────────────────────── */

#define DQ_CACHE_LINE   64u   /* bytes; correct for x86-64 and arm64        */

#define DQ_ALIGNED      __attribute__((aligned(DQ_CACHE_LINE)))
#define DQ_LIKELY(x)    __builtin_expect(!!(x), 1)
#define DQ_UNLIKELY(x)  __builtin_expect(!!(x), 0)

/* ── CPU pause / yield ──────────────────────────────────────────────────── */

#if defined(__x86_64__) || defined(__i386__)
#  define DQ_CPU_RELAX()  __asm__ volatile("pause" ::: "memory")
#elif defined(__aarch64__) || defined(__arm__)
#  define DQ_CPU_RELAX()  __asm__ volatile("yield" ::: "memory")
#else
#  define DQ_CPU_RELAX()  ((void)0)
#endif

/* ── spin back-off limits ───────────────────────────────────────────────── */

#define DQ_MAX_SPIN        512u
#define DQ_SPIN_YIELD_AT   64u

/* ── common error codes ─────────────────────────────────────────────────── */

typedef enum {
    DQ_OK      =  0,
    DQ_FULL    = -1,
    DQ_EMPTY   = -2,
    DQ_INVALID = -3,
    DQ_NOMEM   = -4,
} dq_err_t;

static inline const char *dq_strerror(dq_err_t e)
{
    switch (e) {
    case DQ_OK:      return "ok";
    case DQ_FULL:    return "full";
    case DQ_EMPTY:   return "empty";
    case DQ_INVALID: return "invalid argument";
    case DQ_NOMEM:   return "out of memory";
    default:         return "unknown";
    }
}

/* ── slot (shared by all four variants) ─────────────────────────────────── */

/**
 * dq_slot_t — one ring-buffer slot, exactly one cache line wide.
 *
 * The sequence counter drives the state machine for every variant:
 *
 *   seq == pos          producer may claim  (slot is empty)
 *   seq == pos + 1      consumer may read   (slot is full)
 *   seq == pos + cap    slot recycled       (available next lap)
 *
 * data  : the payload pointer; written by producer, read by consumer.
 * _pad  : fills the remaining bytes so sizeof(dq_slot_t) == DQ_CACHE_LINE.
 *         This prevents false sharing between adjacent slots.
 */
typedef struct {
    _Atomic(uint64_t)  seq;
    void              *data;
    char               _pad[DQ_CACHE_LINE
                            - sizeof(_Atomic(uint64_t))
                            - sizeof(void *)];
} DQ_ALIGNED dq_slot_t;

_Static_assert(sizeof(dq_slot_t) == DQ_CACHE_LINE,
               "dq_slot_t must be exactly one cache line");

/* ── aligned allocation (Linux + macOS) ─────────────────────────────────── */

#include <stdlib.h>

static inline void *dq_aligned_alloc(size_t alignment, size_t size)
{
    void *p = NULL;
#if defined(__APPLE__)
    if (posix_memalign(&p, alignment, size) != 0) p = NULL;
#else
    p = aligned_alloc(alignment, size);
#endif
    return p;
}

/* ── power-of-two validation ─────────────────────────────────────────────── */

static inline bool dq_is_pow2(uint64_t n)
{
    return n && n <= ((uint64_t)1 << 30) && (n & (n - 1)) == 0;
}
