/*
 * mpsc_test.c — Stress-test & micro-benchmark for mpsc_queue.h
 *
 * Tests:
 *   1. Single-threaded correctness (both queues)
 *   2. Multi-producer stress test with integrity checks
 *   3. Throughput benchmark
 *
 * Build:
 *   make              — optimised build
 *   make asan         — AddressSanitizer + UBSan
 *   make tsan         — ThreadSanitizer
 *
 * Run:
 *   ./mpsc_test [num_producers] [items_per_producer]
 */
//////////////////////////////makefile
CC      := gcc
CFLAGS  := -std=c11 -Wall -Wextra -Wpedantic -pthread
SRC     := mpsc_test.c
HDR     := mpsc_queue.h

.PHONY: all release asan tsan clean run

# Default: optimised build
all: release

release: $(SRC) $(HDR)
	$(CC) $(CFLAGS) -O2 -march=native -o mpsc_test $(SRC)
	@echo "Built: ./mpsc_test"

# AddressSanitizer + UndefinedBehaviourSanitizer
asan: $(SRC) $(HDR)
	$(CC) $(CFLAGS) -O1 -g \
	      -fsanitize=address,undefined \
	      -fno-omit-frame-pointer \
	      -o mpsc_test_asan $(SRC)
	@echo "Built: ./mpsc_test_asan"

# ThreadSanitizer (use a modest workload to keep runtime reasonable)
tsan: $(SRC) $(HDR)
	$(CC) $(CFLAGS) -O1 -g \
	      -fsanitize=thread \
	      -fno-omit-frame-pointer \
	      -o mpsc_test_tsan $(SRC)
	@echo "Built: ./mpsc_test_tsan  (run: ./mpsc_test_tsan 4 20000)"

run: release
	./mpsc_test

# Run with ThreadSanitizer and a small load that completes quickly
run-tsan: tsan
	./mpsc_test_tsan 4 20000

# Run with AddressSanitizer
run-asan: asan
	./mpsc_test_asan 4 20000

clean:
	rm -f mpsc_test mpsc_test_asan mpsc_test_tsan

///////////////end

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <inttypes.h>
#include <time.h>
#include <pthread.h>
#include <unistd.h>
#include <errno.h>
#include <sched.h>
#include <assert.h>

#include "mpsc_queue.h"

/* -----------------------------------------------------------------------
 * Portable monotonic clock
 * --------------------------------------------------------------------- */
static inline uint64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

/* -----------------------------------------------------------------------
 * Helpers
 * --------------------------------------------------------------------- */
#define CHECK(cond, ...)                                                 \
    do {                                                                  \
        if (!(cond)) {                                                    \
            fprintf(stderr, "[FAIL] %s:%d: ", __FILE__, __LINE__);       \
            fprintf(stderr, __VA_ARGS__);                                 \
            fprintf(stderr, "\n");                                        \
            abort();                                                      \
        }                                                                 \
    } while (0)

static inline void ok(const char *msg) { printf("  [OK] %s\n", msg); }

/* =========================================================================
 * PART 1: Linked-list (mpsc_lf) correctness
 * ========================================================================= */

typedef struct {
    int          value;
    mpsc_lf_node_t node;
} lf_item_t;

static void test_lf_single_thread(void)
{
    printf("\n── Linked-list single-thread correctness ──\n");

    mpsc_lf_queue_t q;
    mpsc_lf_init(&q);

    /* Empty pop */
    CHECK(mpsc_lf_pop(&q) == NULL, "empty queue must return NULL");
    ok("empty pop returns NULL");

    /* Push one, pop one */
    lf_item_t a = {.value = 42};
    mpsc_lf_push(&q, &a.node);
    mpsc_lf_node_t *n = mpsc_lf_pop(&q);
    CHECK(n != NULL, "pop after one push must not be NULL");
    lf_item_t *got = mpsc_container_of(n, lf_item_t, node);
    CHECK(got->value == 42, "value mismatch: %d", got->value);
    ok("push/pop single item, value preserved");

    /* FIFO order over N items */
    const int N = 10000;
    lf_item_t *items = calloc((size_t)N, sizeof(*items));
    CHECK(items, "calloc");
    for (int i = 0; i < N; i++) {
        items[i].value = i;
        mpsc_lf_push(&q, &items[i].node);
    }
    for (int i = 0; i < N; i++) {
        mpsc_lf_node_t *nn = mpsc_lf_pop(&q);
        CHECK(nn != NULL, "unexpected NULL at i=%d", i);
        lf_item_t *it = mpsc_container_of(nn, lf_item_t, node);
        CHECK(it->value == i, "FIFO violated: expected %d got %d", i, it->value);
    }
    CHECK(mpsc_lf_pop(&q) == NULL, "queue must be empty after draining");
    ok("FIFO order verified over 10 000 items");

    /* Interleaved push/pop */
    for (int i = 0; i < N; i++) {
        items[i].value = i * 2;
        mpsc_lf_push(&q, &items[i].node);
        mpsc_lf_node_t *nn = mpsc_lf_pop(&q);
        CHECK(nn != NULL, "interleaved: unexpected NULL");
        lf_item_t *it = mpsc_container_of(nn, lf_item_t, node);
        CHECK(it->value == i * 2, "interleaved value mismatch");
    }
    ok("interleaved push/pop correctness");

    free(items);
}

/* =========================================================================
 * PART 2: Ring-buffer (mpsc_rb) single-thread correctness
 * ========================================================================= */

static void test_rb_single_thread(void)
{
    printf("\n── Ring-buffer single-thread correctness ──\n");

    /* Invalid capacity */
    mpsc_rb_queue_t *bad = mpsc_rb_create(3); /* not power of two */
    CHECK(bad == NULL, "non-power-of-two should fail");
    ok("rejects non-power-of-two capacity");

    mpsc_rb_queue_t *q = mpsc_rb_create(64);
    CHECK(q, "mpsc_rb_create");

    /* Empty pop */
    void *out = NULL;
    CHECK(!mpsc_rb_pop(q, &out), "empty queue must return false");
    ok("empty pop returns false");

    /* Basic round-trip */
    int x = 99;
    mpsc_rb_push(q, &x);
    CHECK(mpsc_rb_pop(q, &out), "should dequeue item");
    CHECK(out == &x, "pointer identity mismatch");
    ok("push/pop pointer identity preserved");

    /* FIFO order */
    const int N = 60;
    int vals[60];
    for (int i = 0; i < N; i++) {
        vals[i] = i * 3;
        mpsc_rb_push(q, &vals[i]);
    }
    for (int i = 0; i < N; i++) {
        CHECK(mpsc_rb_pop(q, &out), "unexpected empty at i=%d", i);
        CHECK(out == &vals[i], "FIFO violated at i=%d", i);
    }
    CHECK(!mpsc_rb_pop(q, &out), "must be empty");
    ok("FIFO order over 60 items");

    /* Wrap-around: fill, drain, fill again */
    for (int round = 0; round < 3; round++) {
        for (int i = 0; i < N; i++) mpsc_rb_push(q, &vals[i % N]);
        for (int i = 0; i < N; i++) {
            CHECK(mpsc_rb_pop(q, &out), "wrap round %d, i %d", round, i);
        }
    }
    ok("wrap-around correctness (3 rounds)");

    /* try_push: fill queue to capacity and verify it returns false */
    mpsc_rb_queue_t *small = mpsc_rb_create(8);
    CHECK(small, "mpsc_rb_create small");
    int dummy[8];
    for (int i = 0; i < 8; i++)
        CHECK(mpsc_rb_try_push(small, &dummy[i]), "try_push %d should succeed", i);
    CHECK(!mpsc_rb_try_push(small, &dummy[0]),
          "try_push on full queue must return false");
    ok("try_push returns false when full");
    mpsc_rb_destroy(small);

    mpsc_rb_destroy(q);
}

/* =========================================================================
 * PART 3: Multi-producer stress test
 * ========================================================================= */

#define MAX_PRODUCERS 32

/* ── Linked-list ── */

typedef struct {
    mpsc_lf_queue_t *q;
    uint64_t         n_items;
    uint64_t         producer_id;
    lf_item_t       *pool; /* pre-allocated pool; valid until after join */
} lf_prod_arg_t;

static void *lf_producer(void *arg)
{
    lf_prod_arg_t *a = arg;
    for (uint64_t i = 0; i < a->n_items; i++) {
        /* Pack producer_id and sequence into value for integrity check. */
        a->pool[i].value = (int)((a->producer_id << 20) | (i & 0xFFFFFu));
        mpsc_lf_push(a->q, &a->pool[i].node);
    }
    return NULL;
}

static void test_lf_multiproducer(int n_producers, uint64_t items_each)
{
    printf("\n── Linked-list multi-producer stress: %d producers × %"PRIu64" items ──\n",
           n_producers, items_each);

    mpsc_lf_queue_t    q;
    pthread_t          threads[MAX_PRODUCERS];
    lf_prod_arg_t      args[MAX_PRODUCERS];

    mpsc_lf_init(&q);

    for (int p = 0; p < n_producers; p++) {
        args[p].q           = &q;
        args[p].n_items     = items_each;
        args[p].producer_id = (uint64_t)p;
        args[p].pool        = calloc(items_each, sizeof(lf_item_t));
        CHECK(args[p].pool, "calloc producer pool");
        CHECK(!pthread_create(&threads[p], NULL, lf_producer, &args[p]),
              "pthread_create");
    }

    uint64_t total    = (uint64_t)n_producers * items_each;
    uint64_t consumed = 0;
    uint64_t counts[MAX_PRODUCERS];
    memset(counts, 0, sizeof(counts));
    uint64_t start_ns = now_ns();

    /* Drain all items while producers are running. */
    while (consumed < total) {
        mpsc_lf_node_t *node = mpsc_lf_pop(&q);
        if (!node) { mpsc_cpu_relax(); continue; }
        lf_item_t *item = mpsc_container_of(node, lf_item_t, node);
        int pid = (item->value >> 20) & 0xFFF;
        CHECK(pid >= 0 && pid < n_producers, "bad producer id %d", pid);
        counts[pid]++;
        consumed++;
    }
    uint64_t elapsed = now_ns() - start_ns;

    /*
     * Join producers *after* consuming all items.  At this point all
     * items have already been dequeued, so no pool memory is accessed
     * after the join + free below.
     */
    for (int p = 0; p < n_producers; p++) {
        pthread_join(threads[p], NULL);
        CHECK(counts[p] == items_each,
              "producer %d delivered %"PRIu64" of %"PRIu64,
              p, counts[p], items_each);
        free(args[p].pool); /* safe: pool nodes are no longer in the queue */
    }

    printf("  [OK] All %"PRIu64" items received, no loss, no duplication\n", total);
    printf("  [OK] Throughput: %.2f M items/s  (%.1f ns/item)\n",
           (double)total / (double)elapsed * 1e3,
           (double)elapsed / (double)total);
}

/* ── Ring-buffer ── */

typedef struct {
    mpsc_rb_queue_t *q;
    uint64_t         n_items;
    uint64_t         producer_id;
    uintptr_t       *pool; /* packed (pid<<32|seq) values */
} rb_prod_arg_t;

static void *rb_producer(void *arg)
{
    rb_prod_arg_t *a = arg;
    for (uint64_t i = 0; i < a->n_items; i++) {
        a->pool[i] = (a->producer_id << 32) | (uintptr_t)(i + 1);
        mpsc_rb_push(a->q, (void *)a->pool[i]);
    }
    return NULL;
}

static void test_rb_multiproducer(int n_producers, uint64_t items_each)
{
    printf("\n── Ring-buffer multi-producer stress: %d producers × %"PRIu64" items ──\n",
           n_producers, items_each);

    /* Capacity: next power of 2 >= 4× producer count.  Items are pointers
     * (not pool nodes), so we can free pools immediately after join.       */
    /* Size ring buffer large enough that producers rarely spin on full.
     * On machines with few CPUs the consumer may lag; 16K slots gives
     * ample headroom for 4–32 producers pushing at OS scheduling granularity. */
    uint64_t cap = 1u << 14; /* 16 K slots; tune up for deeper pipelines */
    while (cap < (uint64_t)n_producers * 4) cap <<= 1;

    mpsc_rb_queue_t *q = mpsc_rb_create(cap);
    CHECK(q, "mpsc_rb_create");

    pthread_t     threads[MAX_PRODUCERS];
    rb_prod_arg_t args[MAX_PRODUCERS];

    for (int p = 0; p < n_producers; p++) {
        args[p].q           = q;
        args[p].n_items     = items_each;
        args[p].producer_id = (uint64_t)p;
        args[p].pool        = calloc(items_each, sizeof(uintptr_t));
        CHECK(args[p].pool, "calloc");
        CHECK(!pthread_create(&threads[p], NULL, rb_producer, &args[p]),
              "pthread_create");
    }

    uint64_t total    = (uint64_t)n_producers * items_each;
    uint64_t consumed = 0;
    uint64_t last_seq[MAX_PRODUCERS];
    memset(last_seq, 0, sizeof(last_seq));
    uint64_t start_ns = now_ns();

    while (consumed < total) {
        void *data;
        if (!mpsc_rb_pop(q, &data)) { mpsc_cpu_relax(); continue; }
        uintptr_t v   = (uintptr_t)data;
        int       pid = (int)(v >> 32);
        uint64_t  seq = v & 0xFFFFFFFFULL;
        CHECK(pid >= 0 && pid < n_producers, "bad pid %d", pid);
        /* Per-producer FIFO: sequence numbers must be strictly increasing. */
        CHECK(seq > last_seq[pid],
              "producer %d: seq %"PRIu64" <= last %"PRIu64, pid, seq, last_seq[pid]);
        last_seq[pid] = seq;
        consumed++;
    }
    uint64_t elapsed = now_ns() - start_ns;

    for (int p = 0; p < n_producers; p++) {
        pthread_join(threads[p], NULL);
        CHECK(last_seq[p] == items_each,
              "producer %d: only %"PRIu64" of %"PRIu64" received",
              p, last_seq[p], items_each);
        free(args[p].pool);
    }

    printf("  [OK] All %"PRIu64" items received, per-producer FIFO verified\n", total);
    printf("  [OK] Throughput: %.2f M items/s  (%.1f ns/item)\n",
           (double)total / (double)elapsed * 1e3,
           (double)elapsed / (double)total);

    mpsc_rb_destroy(q);
}

/* =========================================================================
 * PART 4: Single-producer micro-benchmark
 * ========================================================================= */

static void bench_lf(uint64_t n)
{
    printf("\n── Linked-list single-producer baseline (%"PRIu64" items) ──\n", n);

    mpsc_lf_queue_t q;
    mpsc_lf_init(&q);

    lf_item_t *pool = calloc(n, sizeof(*pool));
    CHECK(pool, "calloc");

    uint64_t t0 = now_ns();
    for (uint64_t i = 0; i < n; i++)
        mpsc_lf_push(&q, &pool[i].node);
    uint64_t t1 = now_ns();
    for (uint64_t i = 0; i < n; i++) {
        mpsc_lf_node_t *node;
        while (!(node = mpsc_lf_pop(&q))) mpsc_cpu_relax();
        (void)node;
    }
    uint64_t t2 = now_ns();

    printf("  Push: %5.1f ns/op  (%5.2f M/s)\n",
           (double)(t1 - t0) / (double)n,
           (double)n / (double)(t1 - t0) * 1e3);
    printf("  Pop:  %5.1f ns/op  (%5.2f M/s)\n",
           (double)(t2 - t1) / (double)n,
           (double)n / (double)(t2 - t1) * 1e3);

    free(pool);
}

static void bench_rb(uint64_t n)
{
    printf("\n── Ring-buffer single-producer baseline (%"PRIu64" items) ──\n", n);

    uint64_t cap = 1; while (cap < n) cap <<= 1; mpsc_rb_queue_t *q = mpsc_rb_create(cap); /* 256 K slots */
    CHECK(q, "mpsc_rb_create");

    uint64_t t0 = now_ns();
    for (uint64_t i = 0; i < n; i++)
        mpsc_rb_push(q, (void *)(uintptr_t)(i + 1));
    uint64_t t1 = now_ns();

    void *out;
    for (uint64_t i = 0; i < n; i++) {
        while (!mpsc_rb_pop(q, &out)) mpsc_cpu_relax();
        (void)out;
    }
    uint64_t t2 = now_ns();

    printf("  Push: %5.1f ns/op  (%5.2f M/s)\n",
           (double)(t1 - t0) / (double)n,
           (double)n / (double)(t1 - t0) * 1e3);
    printf("  Pop:  %5.1f ns/op  (%5.2f M/s)\n",
           (double)(t2 - t1) / (double)n,
           (double)n / (double)(t2 - t1) * 1e3);

    mpsc_rb_destroy(q);
}

/* =========================================================================
 * main
 * ========================================================================= */

int main(int argc, char **argv)
{
    int      n_producers = (argc > 1) ? atoi(argv[1]) : 4;
    uint64_t items_each  = (argc > 2) ? (uint64_t)atoll(argv[2]) : 50000ULL;

    if (n_producers < 1 || n_producers > MAX_PRODUCERS) {
        fprintf(stderr, "n_producers must be 1..%d\n", MAX_PRODUCERS);
        return 1;
    }

    long ncpu = sysconf(_SC_NPROCESSORS_ONLN);
    printf("System : %ld CPU(s) online\n", ncpu);
    printf("Config : %d producer(s), %"PRIu64" items each (%"PRIu64" total)\n",
           n_producers, items_each, (uint64_t)n_producers * items_each);

    /* ── Single-thread correctness ── */
    test_lf_single_thread();
    test_rb_single_thread();

    /* ── Multi-producer stress ── */
    test_lf_multiproducer(n_producers, items_each);
    test_rb_multiproducer(n_producers, items_each);

    /* ── Micro-benchmarks ── */
    bench_lf(500000);
    bench_rb(500000);

    printf("\n══ All tests passed ══\n");
    return 0;
}
