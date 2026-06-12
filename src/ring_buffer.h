/**
 * ring_buffer.h  –  zero-malloc receive ring buffer
 *
 * The ring is embedded by VALUE inside hs_conn_t – no heap allocation.
 * readv() fills it with up to two iovecs handling the wrap transparently.
 *
 *   Physical layout:
 *
 *     data: [0 ........ r_idx ..... w_idx ........ RING_SIZE-1]
 *                        ^           ^
 *                    consume here  write here
 *
 *   Logical positions r and w are monotonically increasing uint32_t.
 *   Physical index = pos & RING_MASK.
 *   Invariant:  w - r  <=  RING_SIZE  (ring never over-filled).
 *
 * HS_RING_SIZE must be a power of two.  Override via CMake:
 *   -DHS_RING_SIZE=32768
 */
#pragma once

#include <stdint.h>
#include <string.h>
#include <sys/uio.h>
#include <unistd.h>
#include <errno.h>

/* ── compile-time knob ────────────────────────────────────────────────────── */
#ifndef HS_RING_SIZE
#  define HS_RING_SIZE  (8u * 1024u)   /* 8 KiB per connection */
#endif

#if (HS_RING_SIZE & (HS_RING_SIZE - 1)) != 0
#  error "HS_RING_SIZE must be a power of two"
#endif

#define HS_RING_MASK   (HS_RING_SIZE - 1u)

/* ── type ─────────────────────────────────────────────────────────────────── */
typedef struct {
    char     data[HS_RING_SIZE];
    uint32_t r;   /* consumer logical position */
    uint32_t w;   /* producer logical position */
} hs_ring_t;

/* ── basic queries ────────────────────────────────────────────────────────── */
static inline uint32_t hs_ring_len (const hs_ring_t *b){ return b->w - b->r; }
static inline uint32_t hs_ring_free(const hs_ring_t *b){ return HS_RING_SIZE - (b->w - b->r); }
static inline int      hs_ring_full (const hs_ring_t *b){ return hs_ring_free(b) == 0; }
static inline int      hs_ring_empty(const hs_ring_t *b){ return b->w == b->r; }

static inline void hs_ring_init (hs_ring_t *b){ b->r = b->w = 0; }
static inline void hs_ring_reset(hs_ring_t *b){ b->r = b->w = 0; }

/* ── physical index helpers ───────────────────────────────────────────────── */
static inline uint32_t  hs_ring_r_idx(const hs_ring_t *b){ return b->r & HS_RING_MASK; }
static inline uint32_t  hs_ring_w_idx(const hs_ring_t *b){ return b->w & HS_RING_MASK; }
static inline const char *hs_ring_r_ptr(const hs_ring_t *b){ return b->data + hs_ring_r_idx(b); }
static inline       char *hs_ring_w_ptr(      hs_ring_t *b){ return b->data + hs_ring_w_idx(b); }

/**
 * hs_ring_recv  –  fill ring from fd with readv().
 *
 *  Returns  > 0  : bytes received (ring->w advanced)
 *           = 0  : EOF – peer closed
 *           =-1  : EAGAIN / EWOULDBLOCK  (retry later)
 *           =-2  : hard error           (errno set, close connection)
 *           =-3  : ring is full         (caller must drain first)
 */
static inline ssize_t hs_ring_recv(hs_ring_t *ring, int fd)
{
    uint32_t free_bytes = hs_ring_free(ring);
    if (!free_bytes) return -3;

    uint32_t w_idx  = hs_ring_w_idx(ring);
    uint32_t to_end = HS_RING_SIZE - w_idx;   /* bytes until physical end */

    struct iovec iov[2];
    int niov;

    if (free_bytes <= to_end) {
        /* Free space is entirely before the wrap – one iovec suffices */
        iov[0].iov_base = ring->data + w_idx;
        iov[0].iov_len  = free_bytes;
        niov = 1;
    } else {
        /* Free space spans the physical end: use two iovecs */
        iov[0].iov_base = ring->data + w_idx;
        iov[0].iov_len  = to_end;
        iov[1].iov_base = ring->data;           /* wrap-around head */
        iov[1].iov_len  = free_bytes - to_end;
        niov = 2;
    }

    ssize_t n = readv(fd, iov, niov);
    if (n > 0) {
        ring->w += (uint32_t)n;
        return n;
    }
    if (n == 0) return 0; /* EOF */
    /* n < 0 */
    return (errno == EAGAIN || errno == EWOULDBLOCK) ? -1 : -2;
}

/**
 * hs_ring_peek  –  view contiguous readable data without consuming.
 *
 * *ptr  → pointer into ring->data at the current read position
 * *len  → contiguous byte count (may be < ring_len if data wraps)
 *
 * Call hs_ring_drain(n) after consuming n bytes.
 * Call again to get the second segment if the data wraps.
 */
static inline void hs_ring_peek(const hs_ring_t *ring,
                                 const char **ptr, uint32_t *len)
{
    uint32_t avail  = hs_ring_len(ring);
    if (!avail) { *ptr = NULL; *len = 0; return; }

    uint32_t r_idx  = hs_ring_r_idx(ring);
    uint32_t to_end = HS_RING_SIZE - r_idx;

    *ptr = ring->data + r_idx;
    *len = (avail < to_end) ? avail : to_end;
}

/** Advance consumer by n bytes. */
static inline void hs_ring_drain(hs_ring_t *ring, uint32_t n)
{
    ring->r += n;
}

/**
 * hs_ring_is_contiguous  –  true if 'len' bytes starting at absolute logical
 * position 'abs_pos' do NOT wrap around the physical end.
 */
static inline int hs_ring_is_contiguous(uint32_t abs_pos, uint32_t len)
{
    return ((abs_pos & HS_RING_MASK) + len) <= HS_RING_SIZE;
}

/** Pointer to the byte at absolute logical position 'pos'. */
static inline const char *hs_ring_abs_ptr(const hs_ring_t *ring, uint32_t pos)
{
    return ring->data + (pos & HS_RING_MASK);
}

/**
 * hs_ring_linearise  –  copy [abs_pos, abs_pos+len) into dst.
 * Handles the physical wrap transparently.
 * dst must have at least 'len' bytes of space.
 */
static inline void hs_ring_linearise(const hs_ring_t *ring,
                                      uint32_t abs_pos, uint32_t len,
                                      char *dst)
{
    uint32_t idx    = abs_pos & HS_RING_MASK;
    uint32_t to_end = HS_RING_SIZE - idx;

    if (len <= to_end) {
        memcpy(dst, ring->data + idx, len);
    } else {
        memcpy(dst, ring->data + idx, to_end);
        memcpy(dst + to_end, ring->data, len - to_end);
    }
}
