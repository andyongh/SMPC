/**
 * test_all.c — Correctness + throughput tests for all four queue variants
 *
 * Build:
 *   gcc -O2 -std=c11 -pthread -Isrc -o test_all test/test_all.c
 *   clang -O2 -std=c11 -pthread -Isrc -o test_all test/test_all.c
 *
 * With sanitizers:
 *   gcc -O1 -std=c11 -pthread -fsanitize=thread -g -Isrc -o test_all test/test_all.c
 */

#define _POSIX_C_SOURCE 200809L

#include "smpc.h"

#include <assert.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/* ── timing ─────────────────────────────────────────────────────────────── */

static double now_sec(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec * 1e-9;
}

/* ── test framework ─────────────────────────────────────────────────────── */

static int g_pass = 0, g_fail = 0;

#define SUITE(name) printf("\n[" name "]\n")

#define TEST(label) \
    printf("  %-50s", label " ..."); fflush(stdout);

#define PASS() do { printf("PASS\n"); g_pass++; } while(0)

#define FAIL(fmt, ...) do { \
    printf("FAIL\n"); \
    fprintf(stderr, "    " fmt "\n    %s:%d\n", ##__VA_ARGS__, __FILE__, __LINE__); \
    g_fail++; return; \
} while(0)

#define ASSERT(cond, ...) \
    if (!(cond)) FAIL(__VA_ARGS__)

/* ── helpers ─────────────────────────────────────────────────────────────── */

/* Encode (producer_id:32, seq:32) into a void*. */
static inline void *encode(uint32_t pid, uint32_t seq) {
    return (void *)(uintptr_t)(((uint64_t)pid << 32) | seq);
}
static inline uint32_t decode_pid(void *v) { return (uint32_t)((uintptr_t)v >> 32); }
static inline uint32_t decode_seq(void *v) { return (uint32_t)((uintptr_t)v & 0xFFFFFFFF); }

/* ═══════════════════════════════════════════════════════════════════════════
 * SPSC tests
 * ══════════════════════════════════════════════════════════════════════════*/

static void test_spsc_basic(void)
{
    TEST("spsc: create / enqueue / dequeue");
    spsc_queue_t *q = spsc_queue_create(8);
    ASSERT(q && spsc_capacity(q) == 8 && spsc_is_empty(q), "create");

    int vals[4] = {10,20,30,40};
    for (int i = 0; i < 4; i++)
        ASSERT(spsc_enqueue(q, &vals[i]) == DQ_OK, "enqueue %d", i);
    ASSERT(spsc_size(q) == 4, "size == 4");

    for (int i = 0; i < 4; i++) {
        void *p;
        ASSERT(spsc_dequeue(q, &p) == DQ_OK, "dequeue %d", i);
        ASSERT(p == &vals[i], "wrong ptr %d", i);
    }
    ASSERT(spsc_is_empty(q), "empty after drain");
    void *p; ASSERT(spsc_dequeue(q, &p) == DQ_EMPTY, "extra dequeue → EMPTY");
    spsc_queue_destroy(q);
    PASS();
}

static void test_spsc_full(void)
{
    TEST("spsc: full detection and wrap-around");
    spsc_queue_t *q = spsc_queue_create(4);
    int d = 0;
    for (int i = 0; i < 4; i++) ASSERT(spsc_enqueue(q, &d) == DQ_OK, "fill %d", i);
    ASSERT(spsc_enqueue(q, &d) == DQ_FULL, "5th enqueue → FULL");

    for (int lap = 0; lap < 8; lap++) {
        void *p;
        ASSERT(spsc_dequeue(q, &p) == DQ_OK, "drain lap=%d", lap);
        ASSERT(spsc_enqueue(q, &d) == DQ_OK, "refill lap=%d", lap);
    }
    spsc_queue_destroy(q);
    PASS();
}

#define SPSC_N  2000000

typedef struct { spsc_queue_t *q; uint64_t n; } spsc_arg_t;

static void *spsc_producer(void *arg)
{
    spsc_arg_t *a = arg;
    for (uint64_t i = 0; i < a->n; i++) {
        while (spsc_enqueue_spin(a->q, encode(0, (uint32_t)i)) != DQ_OK)
            sched_yield();
    }
    return NULL;
}

static void test_spsc_threaded(void)
{
    TEST("spsc: threaded 1P-1C 2M items + throughput");
    spsc_queue_t *q = spsc_queue_create(4096);
    spsc_arg_t arg = { q, SPSC_N };

    double t0 = now_sec();
    pthread_t pt;
    pthread_create(&pt, NULL, spsc_producer, &arg);

    uint64_t expected = 0;
    for (uint64_t got = 0; got < SPSC_N; ) {
        void *p;
        if (spsc_dequeue(q, &p) == DQ_OK) {
            ASSERT(decode_pid(p) == 0,          "corrupt pid");
            ASSERT(decode_seq(p) == (uint32_t)expected, "out-of-order");
            expected++; got++;
        } else DQ_CPU_RELAX();
    }
    double mops = SPSC_N / (now_sec() - t0) / 1e6;
    pthread_join(pt, NULL);
    spsc_queue_destroy(q);
    printf("(%.1f Mops/s) ", mops);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════════════
 * MPSC tests
 * ══════════════════════════════════════════════════════════════════════════*/

static void test_mpsc_basic(void)
{
    TEST("mpsc: create / enqueue / dequeue");
    mpsc_queue_t *q = mpsc_queue_create(16);
    ASSERT(q && mpsc_capacity(q) == 16 && mpsc_is_empty(q), "create");
    int vals[8];
    for (int i = 0; i < 8; i++) { vals[i]=i; ASSERT(mpsc_enqueue(q,&vals[i])==DQ_OK,"enqueue %d",i); }
    for (int i = 0; i < 8; i++) { void *p; ASSERT(mpsc_dequeue(q,&p)==DQ_OK,"dequeue %d",i); ASSERT(p==&vals[i],"ptr %d",i); }
    void *p; ASSERT(mpsc_dequeue(q,&p)==DQ_EMPTY,"extra → EMPTY");
    mpsc_queue_destroy(q);
    PASS();
}

#define MPSC_PRODS  8
#define MPSC_N      200000

typedef struct { mpsc_queue_t *q; uint64_t id, n; } mpsc_arg_t;
static void *mpsc_producer(void *arg)
{
    mpsc_arg_t *a = arg;
    for (uint64_t i = 0; i < a->n; i++)
        while (mpsc_enqueue_spin(a->q, encode((uint32_t)a->id,(uint32_t)i)) != DQ_OK)
            sched_yield();
    return NULL;
}

static void test_mpsc_stress(void)
{
    TEST("mpsc: 8P-1C stress + per-producer ordering");
    mpsc_queue_t *q = mpsc_queue_create(4096);
    pthread_t threads[MPSC_PRODS];
    mpsc_arg_t args[MPSC_PRODS];
    for (int i = 0; i < MPSC_PRODS; i++) {
        args[i] = (mpsc_arg_t){ q, (uint64_t)i, MPSC_N };
        pthread_create(&threads[i], NULL, mpsc_producer, &args[i]);
    }
    uint64_t total = (uint64_t)MPSC_PRODS * MPSC_N;
    uint64_t recv = 0;
    uint64_t expect[MPSC_PRODS] = {0};
    double t0 = now_sec();
    while (recv < total) {
        void *p;
        if (mpsc_dequeue(q, &p) == DQ_OK) {
            uint32_t pid = decode_pid(p), seq = decode_seq(p);
            ASSERT(pid < MPSC_PRODS, "corrupt pid %" PRIu32, pid);
            ASSERT(seq == (uint32_t)expect[pid], "out-of-order pid=%" PRIu32 " got=%" PRIu32 " want=%" PRIu64, pid,seq,expect[pid]);
            expect[pid]++; recv++;
        } else DQ_CPU_RELAX();
    }
    double mops = total / (now_sec() - t0) / 1e6;
    for (int i = 0; i < MPSC_PRODS; i++) pthread_join(threads[i], NULL);
    mpsc_queue_destroy(q);
    printf("(%.1f Mops/s) ", mops);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════════════
 * SPMC tests
 * ══════════════════════════════════════════════════════════════════════════*/

static void test_spmc_basic(void)
{
    TEST("spmc: create / enqueue / dequeue");
    spmc_queue_t *q = spmc_queue_create(16);
    ASSERT(q && spmc_capacity(q) == 16 && spmc_is_empty(q), "create");
    int vals[8];
    for (int i=0;i<8;i++){vals[i]=i; ASSERT(spmc_enqueue(q,&vals[i])==DQ_OK,"enqueue %d",i);}
    for (int i=0;i<8;i++){void *p; ASSERT(spmc_dequeue(q,&p)==DQ_OK,"dequeue %d",i); ASSERT(p==&vals[i],"ptr %d",i);}
    void *p; ASSERT(spmc_dequeue(q,&p)==DQ_EMPTY,"extra → EMPTY");
    spmc_queue_destroy(q);
    PASS();
}

#define SPMC_CONS   8
#define SPMC_N      1600000   /* must be divisible by SPMC_CONS */

typedef struct { spmc_queue_t *q; } spmc_carg_t;
typedef struct {
    _Atomic(uint64_t) recv;   /* items received by this consumer */
    _Atomic(uint64_t) sum;    /* sum of encoded values           */
} spmc_result_t;



static void *spmc_consumer(void *arg)
{
    spmc_carg_t   *a  = arg;
    int            id = (int)(a - (spmc_carg_t *)arg); /* computed below */
    (void)id;
    uint64_t local_recv = 0, local_sum = 0;
    for (;;) {
        void *p;
        dq_err_t e = spmc_dequeue(a->q, &p);
        if (e == DQ_OK) {
            local_sum += (uintptr_t)p;
            local_recv++;
            if (p == (void *)~(uintptr_t)0) break;  /* sentinel */
        } else DQ_CPU_RELAX();
    }
    /* store results; index discovered via pointer arithmetic in main */
    (void)local_recv; (void)local_sum;
    return (void *)(uintptr_t)local_recv;
}

static void test_spmc_stress(void)
{
    TEST("spmc: 1P-8C stress + total-count verification");
    spmc_queue_t *q = spmc_queue_create(4096);

    /* We use a simpler strategy: producer sends SPMC_N numbered items,
     * then SPMC_CONS sentinels.  Each consumer stops on its first sentinel.
     * We verify total received == SPMC_N + SPMC_CONS (sentinels included). */

    pthread_t threads[SPMC_CONS];
    spmc_carg_t cargs[SPMC_CONS];
    uint64_t   counts[SPMC_CONS];

    for (int i = 0; i < SPMC_CONS; i++) { cargs[i].q = q; }
    for (int i = 0; i < SPMC_CONS; i++)
        pthread_create(&threads[i], NULL, spmc_consumer, &cargs[i]);

    double t0 = now_sec();

    /* Producer: send SPMC_N items. */
    for (uint64_t i = 0; i < SPMC_N; i++)
        while (spmc_enqueue_spin(q, (void *)(uintptr_t)(i+1)) != DQ_OK)
            sched_yield();

    /* Send SPMC_CONS sentinels so each consumer exits. */
    for (int i = 0; i < SPMC_CONS; i++)
        while (spmc_enqueue_spin(q, (void *)~(uintptr_t)0) != DQ_OK)
            sched_yield();

    uint64_t total_recv = 0;
    for (int i = 0; i < SPMC_CONS; i++) {
        void *ret;
        pthread_join(threads[i], &ret);
        counts[i] = (uint64_t)(uintptr_t)ret;
        total_recv += counts[i];
    }
    double mops = (SPMC_N + SPMC_CONS) / (now_sec() - t0) / 1e6;

    /* Each consumer received at least 1 item (its sentinel). */
    for (int i = 0; i < SPMC_CONS; i++)
        ASSERT(counts[i] >= 1, "consumer %d got nothing", i);

    ASSERT(total_recv == (uint64_t)(SPMC_N + SPMC_CONS),
           "total recv %" PRIu64 " want %" PRIu64,
           total_recv, (uint64_t)(SPMC_N + SPMC_CONS));

    spmc_queue_destroy(q);
    printf("(%.1f Mops/s) ", mops);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════════════
 * MPMC tests
 * ══════════════════════════════════════════════════════════════════════════*/

static void test_mpmc_basic(void)
{
    TEST("mpmc: create / enqueue / dequeue");
    mpmc_queue_t *q = mpmc_queue_create(16);
    ASSERT(q && mpmc_capacity(q) == 16 && mpmc_is_empty(q), "create");
    int vals[8];
    for (int i=0;i<8;i++){vals[i]=i; ASSERT(mpmc_enqueue(q,&vals[i])==DQ_OK,"enqueue %d",i);}
    for (int i=0;i<8;i++){void *p; ASSERT(mpmc_dequeue(q,&p)==DQ_OK,"dequeue %d",i); ASSERT(p==&vals[i],"ptr %d",i);}
    void *p; ASSERT(mpmc_dequeue(q,&p)==DQ_EMPTY,"extra → EMPTY");
    mpmc_queue_destroy(q);
    PASS();
}

#define MPMC_PRODS  4
#define MPMC_CONS   4
#define MPMC_N      400000   /* per producer */

typedef struct { mpmc_queue_t *q; uint64_t id, n; } mpmc_parg_t;
typedef struct { mpmc_queue_t *q; uint64_t total; } mpmc_carg_t;

static void *mpmc_producer_fn(void *arg)
{
    mpmc_parg_t *a = arg;
    for (uint64_t i = 0; i < a->n; i++)
        while (mpmc_enqueue_spin(a->q, encode((uint32_t)a->id,(uint32_t)i)) != DQ_OK)
            sched_yield();
    return NULL;
}

static void *mpmc_consumer_fn(void *arg)
{
    mpmc_carg_t *a = arg;
    uint64_t recv = 0;
    while (recv < a->total) {
        void *p;
        if (mpmc_dequeue(a->q, &p) == DQ_OK) recv++;
        else DQ_CPU_RELAX();
    }
    return (void *)(uintptr_t)recv;
}

static void test_mpmc_stress(void)
{
    TEST("mpmc: 4P-4C stress + total-count verification");
    mpmc_queue_t *q = mpmc_queue_create(4096);

    pthread_t pthreads[MPMC_PRODS], cthreads[MPMC_CONS];
    mpmc_parg_t pargs[MPMC_PRODS];
    mpmc_carg_t cargs[MPMC_CONS];

    uint64_t total    = (uint64_t)MPMC_PRODS * MPMC_N;
    uint64_t per_cons = total / MPMC_CONS;

    for (int i = 0; i < MPMC_CONS; i++) { cargs[i] = (mpmc_carg_t){ q, per_cons }; }
    for (int i = 0; i < MPMC_CONS; i++) pthread_create(&cthreads[i], NULL, mpmc_consumer_fn, &cargs[i]);

    double t0 = now_sec();
    for (int i = 0; i < MPMC_PRODS; i++) {
        pargs[i] = (mpmc_parg_t){ q, (uint64_t)i, MPMC_N };
        pthread_create(&pthreads[i], NULL, mpmc_producer_fn, &pargs[i]);
    }
    for (int i = 0; i < MPMC_PRODS; i++) pthread_join(pthreads[i], NULL);

    uint64_t total_recv = 0;
    for (int i = 0; i < MPMC_CONS; i++) {
        void *ret; pthread_join(cthreads[i], &ret);
        total_recv += (uint64_t)(uintptr_t)ret;
    }
    double mops = total / (now_sec() - t0) / 1e6;
    ASSERT(total_recv == total, "recv %" PRIu64 " want %" PRIu64, total_recv, total);
    mpmc_queue_destroy(q);
    printf("(%.1f Mops/s) ", mops);
    PASS();
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Shared / cross-variant tests
 * ══════════════════════════════════════════════════════════════════════════*/

static void test_alignment_all(void)
{
    TEST("all: cache-line alignment of structs and slots");
#define CHECK_ALIGN(q, cap) do { \
    ASSERT(((uintptr_t)(q) & (DQ_CACHE_LINE-1)) == 0, "struct not aligned"); \
    ASSERT(((uintptr_t)(q)->slots & (DQ_CACHE_LINE-1)) == 0, "slots not aligned"); \
    for (uint64_t _i = 0; _i < (cap); _i++) \
        ASSERT(((uintptr_t)&(q)->slots[_i] & (DQ_CACHE_LINE-1)) == 0, \
               "slot[%" PRIu64 "] not aligned", _i); \
} while(0)

    { spsc_queue_t *q = spsc_queue_create(8); CHECK_ALIGN(q,8); spsc_queue_destroy(q); }
    { mpsc_queue_t *q = mpsc_queue_create(8); CHECK_ALIGN(q,8); mpsc_queue_destroy(q); }
    { spmc_queue_t *q = spmc_queue_create(8); CHECK_ALIGN(q,8); spmc_queue_destroy(q); }
    { mpmc_queue_t *q = mpmc_queue_create(8); CHECK_ALIGN(q,8); mpmc_queue_destroy(q); }
#undef CHECK_ALIGN
    PASS();
}

static void test_null_guards_all(void)
{
    TEST("all: NULL / invalid argument guards");
    void *p;
    /* SPSC */
    ASSERT(spsc_queue_create(0) == NULL && spsc_queue_create(3) == NULL, "spsc bad cap");
    ASSERT(spsc_enqueue(NULL,NULL) == DQ_INVALID, "spsc enqueue NULL");
    ASSERT(spsc_dequeue(NULL,&p)   == DQ_INVALID, "spsc dequeue NULL q");
    { spsc_queue_t *q = spsc_queue_create(4);
      ASSERT(spsc_dequeue(q,NULL) == DQ_INVALID, "spsc dequeue NULL out");
      spsc_queue_destroy(q); }
    /* MPSC */
    ASSERT(mpsc_queue_create(0) == NULL, "mpsc bad cap");
    ASSERT(mpsc_enqueue(NULL,NULL) == DQ_INVALID, "mpsc enqueue NULL");
    ASSERT(mpsc_dequeue(NULL,&p)   == DQ_INVALID, "mpsc dequeue NULL q");
    /* SPMC */
    ASSERT(spmc_queue_create(0) == NULL, "spmc bad cap");
    ASSERT(spmc_enqueue(NULL,NULL) == DQ_INVALID, "spmc enqueue NULL");
    ASSERT(spmc_dequeue(NULL,&p)   == DQ_INVALID, "spmc dequeue NULL q");
    /* MPMC */
    ASSERT(mpmc_queue_create(0) == NULL, "mpmc bad cap");
    ASSERT(mpmc_enqueue(NULL,NULL) == DQ_INVALID, "mpmc enqueue NULL");
    ASSERT(mpmc_dequeue(NULL,&p)   == DQ_INVALID, "mpmc dequeue NULL q");
    /* destroy(NULL) must not crash */
    spsc_queue_destroy(NULL); mpsc_queue_destroy(NULL);
    spmc_queue_destroy(NULL); mpmc_queue_destroy(NULL);
    PASS();
}

/* ── main ────────────────────────────────────────────────────────────────── */

int main(void)
{
    printf("DAKING QUEUE — all-variants test suite\n");
    printf("  dq_slot_t  : %zu bytes\n", sizeof(dq_slot_t));
    printf("  spsc_queue : %zu bytes\n", sizeof(spsc_queue_t));
    printf("  mpsc_queue : %zu bytes\n", sizeof(mpsc_queue_t));
    printf("  spmc_queue : %zu bytes\n", sizeof(spmc_queue_t));
    printf("  mpmc_queue : %zu bytes\n", sizeof(mpmc_queue_t));

    SUITE("SPSC");
    test_spsc_basic();
    test_spsc_full();
    test_spsc_threaded();

    SUITE("MPSC");
    test_mpsc_basic();
    test_mpsc_stress();

    SUITE("SPMC");
    test_spmc_basic();
    test_spmc_stress();

    SUITE("MPMC");
    test_mpmc_basic();
    test_mpmc_stress();

    SUITE("Shared");
    test_alignment_all();
    test_null_guards_all();

    printf("\n═══════════════════════════════════════════════════\n");
    printf("  PASSED: %d   FAILED: %d\n", g_pass, g_fail);
    return g_fail ? EXIT_FAILURE : EXIT_SUCCESS;
}
