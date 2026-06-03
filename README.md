# Queue

Production-grade lock-free ring-buffer queues for Linux and macOS, written in C11.
Four variants — **SPSC, MPSC, SPMC, MPMC** — sharing one slot layout and one seq-counter state machine.

---

## Design

Every variant is a bounded ring buffer where each slot is padded to exactly one cache line (64 bytes).  A sequence counter inside each slot drives a small state machine that lets producers and consumers synchronise without ever touching a mutex:

```
seq == pos          →  empty   (producer may write)
seq == pos + 1      →  full    (consumer may read)
seq == pos + cap    →  recycled (available next lap)
```

The only difference between variants is *which end* uses a CAS:

| Variant | Producer CAS | Consumer CAS | Notes |
|---------|:---:|:---:|-------|
| SPSC | — | — | Zero atomics on hot path; pure acquire/release |
| MPSC | `head` | — | Consumer is fully wait-free |
| SPMC | — | `tail` | Producer is fully wait-free |
| MPMC | `head` | `tail` | Both ends scale independently |

Because the ring is pre-allocated at creation time, there are no `malloc`/`free` calls on the hot path, no ABA problem, and no hazard pointers.

### False-sharing elimination

`head`, `tail`, and every slot each occupy a dedicated cache line.  A producer writing `head` and a consumer reading `tail` never share a cache line, so there is no ping-pong between cores.

```
 ┌─────────────────────┐  ← 64-byte aligned
 │ head  (8 B + 56 pad)│  producers only
 ├─────────────────────┤  ← 64-byte aligned
 │ tail  (8 B + 56 pad)│  consumers only
 ├─────────────────────┤  ← 64-byte aligned
 │ capacity, mask      │  read-only after init
 ├─────────────────────┤  ← 64-byte aligned
 │ slot[0]  seq | data │  64 bytes
 │ slot[1]  seq | data │  64 bytes
 │   …                 │
 └─────────────────────┘
```

---

## Files

```
src/
  dq_platform.h   — shared types, macros, slot definition
  dq_spsc.h       — SPSC implementation (header-only)
  dq_mpsc.h       — MPSC implementation (header-only)
  dq_spmc.h       — SPMC implementation (header-only)
  dq_mpmc.h       — MPMC implementation (header-only)
  smpc.h          — single include for all four variants

test/
  test_all.c      — correctness + throughput tests for all variants

Makefile
README.md
```

All implementations are **header-only** (`static inline`).  There is no `.c` file to compile and no library to link.

---

## Requirements

| | Minimum |
|---|---|
| C standard | C11 |
| Compiler | GCC 5+ or Clang 3.6+ |
| OS | Linux, macOS |
| Architecture | x86-64, arm64 (CPU pause/yield auto-selected) |
| Dependencies | none (only `<stdatomic.h>`, `<pthread.h>`, `<sched.h>`) |

---

## Quick start

```c
#include "smpc.h"

/* Create a queue with 1024 slots (must be a power of two, max 2^30). */
mpsc_queue_t *q = mpsc_queue_create(1024);

/* Producer threads — any number. */
mpsc_enqueue(q, my_pointer);          /* non-blocking: returns DQ_FULL if full */
mpsc_enqueue_spin(q, my_pointer);     /* spins up to DQ_MAX_SPIN iterations    */

/* Consumer thread — exactly one. */
void *item;
if (mpsc_dequeue(q, &item) == DQ_OK) {
    /* process item */
}

mpsc_queue_destroy(q);
```

Replace `mpsc_` with `spsc_`, `spmc_`, or `mpmc_` to switch variants.  The API is identical across all four.

---

## API reference

### Lifecycle

```c
T *XY_queue_create(uint64_t capacity);   // capacity must be a non-zero power of two
void XY_queue_destroy(T *q);             // safe to call with NULL
```

`capacity` must be a power of two between 1 and 2³⁰.  The queue struct and the slot array are each allocated with cache-line alignment.

### Producer

```c
dq_err_t XY_enqueue(T *q, void *data);         // non-blocking
dq_err_t XY_enqueue_spin(T *q, void *data);    // spins on DQ_FULL, then yields
```

`spmc_enqueue` and `spsc_enqueue` may only be called from **one thread at a time**.
`mpsc_enqueue` and `mpmc_enqueue` may be called from **any number of threads simultaneously**.

### Consumer

```c
dq_err_t XY_dequeue(T *q, void **out);         // non-blocking
dq_err_t XY_dequeue_spin(T *q, void **out);    // spins on DQ_EMPTY, then yields
```

`mpsc_dequeue` and `spsc_dequeue` may only be called from **one thread at a time**.
`spmc_dequeue` and `mpmc_dequeue` may be called from **any number of threads simultaneously**.

`_spin` variants are available on all four queue types for both enqueue and dequeue.

### Introspection

```c
uint64_t XY_size(const T *q);       // approximate item count (snapshot)
bool     XY_is_empty(const T *q);   // true if queue appears empty
uint64_t XY_capacity(const T *q);   // total slot count
```

These are snapshots.  In a concurrent program the value may be stale by the time the caller acts on it.  Use them for monitoring and back-pressure heuristics, not for correctness decisions.

### Error codes

```c
typedef enum {
    DQ_OK      =  0,   // success
    DQ_FULL    = -1,   // queue is full  (enqueue)
    DQ_EMPTY   = -2,   // queue is empty (dequeue)
    DQ_INVALID = -3,   // NULL argument or bad capacity
    DQ_NOMEM   = -4,   // allocation failure
} dq_err_t;

const char *dq_strerror(dq_err_t e);
```

---

## Building and testing

```bash
# Build and run all tests
make test

# With ThreadSanitizer (recommended for porting to a new platform)
make SANITIZE=1 test

# Manual build — no Makefile needed
gcc -O2 -std=c11 -pthread -Isrc -o test_all test/test_all.c
```

### Test output

```
DAKING QUEUE — all-variants test suite
  dq_slot_t  : 64 bytes
  spsc_queue : 192 bytes
  mpsc_queue : 192 bytes
  spmc_queue : 192 bytes
  mpmc_queue : 192 bytes

[SPSC]
  spsc: create / enqueue / dequeue ...              PASS
  spsc: full detection and wrap-around ...          PASS
  spsc: threaded 1P-1C 2M items + throughput ...    (1.6 Mops/s) PASS

[MPSC]
  mpsc: create / enqueue / dequeue ...              PASS
  mpsc: 8P-1C stress + per-producer ordering ...    (1.6 Mops/s) PASS

[SPMC]
  spmc: create / enqueue / dequeue ...              PASS
  spmc: 1P-8C stress + total-count verification ... (0.6 Mops/s) PASS

[MPMC]
  mpmc: create / enqueue / dequeue ...              PASS
  mpmc: 4P-4C stress + total-count verification ... (1.0 Mops/s) PASS

[Shared]
  all: cache-line alignment of structs and slots ...PASS
  all: NULL / invalid argument guards ...           PASS

═══════════════════════════════════════════════════
  PASSED: 11   FAILED: 0
```

Numbers above are from a single-core virtual machine.  On a physical multi-core machine expect 50–200 Mops/s for SPSC and 20–80 Mops/s for MPMC depending on core count and cache topology.

---

## Choosing a variant

**SPSC** — one producer thread, one consumer thread.  No CAS on either side; only acquire/release stores and loads.  Use this whenever the topology permits it (audio pipelines, network I/O loops, per-thread work queues drained by a single poller).

**MPSC** — many producer threads, one consumer thread.  The consumer is fully wait-free (zero CAS).  Classic topology for a logger, a metrics sink, or a single I/O thread receiving work from a pool.

**SPMC** — one producer thread, many consumer threads (thread pool).  The producer is fully wait-free.  A dispatcher or task injector that feeds a fixed pool of workers.

**MPMC** — general case.  Both ends scale.  Use when neither the producer nor the consumer side can be constrained to a single thread, or when you need to change the topology at runtime.

The rule of thumb: always use the most constrained variant your topology allows.  Every constraint you express removes a CAS from the hot path.

---

## Correctness properties

**Linearisability.** Each enqueue and dequeue is atomic with respect to all other operations.  The linearisation point for enqueue is the release-store of `seq = pos + 1`; for dequeue it is the CAS on `tail` (SPMC/MPMC) or the relaxed store of `tail` (MPSC/SPSC).

**Per-producer FIFO ordering.** Items enqueued by the same thread are dequeued in the order they were enqueued.  There is no global FIFO guarantee across multiple producers — that would require a global lock.

**No spurious failures.** `DQ_FULL` means the queue was full at the moment of the call.  `DQ_EMPTY` means no item was visible at that moment.  Neither is a transient error that will resolve by itself without external progress (a producer must enqueue or a consumer must dequeue).

**Memory safety.** The queue stores `void *` pointers.  It does not own the memory pointed to.  The caller is responsible for ensuring that the pointed-to objects remain valid until they are dequeued and that they are freed after use.  `XY_queue_destroy` does not free any stored items.

---

## Limitations

- **Capacity is fixed at creation time.** There is no dynamic resize.  Choose a capacity large enough for your peak back-pressure budget.  Powers of two between 256 and 65536 are typical.
- **`void *` items only.** To pass structs by value, allocate them on the heap and enqueue the pointer, or use a separate ring buffer of fixed-size structs (requires a custom slot type).
- **No blocking wait.** There is no built-in condition variable or eventfd.  If you need a consumer to sleep when the queue is empty, wrap `XY_dequeue` in a loop with a `sem_wait` or `eventfd_read` triggered by the producer after each enqueue.
- **Single consumer for SPSC and MPSC.** Calling `spsc_dequeue` or `mpsc_dequeue` from more than one thread concurrently is undefined behaviour.

---

## References

- Dmitry Vyukov, *"Bounded MPMC queue"*, 2010 — the foundational algorithm all four variants derive from.
- Paul McKenney, *"Memory Ordering in Modern Microprocessors"*, Linux Journal, 2005.
- C11 `<stdatomic.h>` — ISO/IEC 9899:2011 §7.17.
