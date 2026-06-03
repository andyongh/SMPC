/**
 * smpc.h — single include for all four queue variants
 *
 *   #include "smpc.h"
 *
 *   spsc_queue_t  *q = spsc_queue_create(1024);
 *   mpsc_queue_t  *q = mpsc_queue_create(1024);
 *   spmc_queue_t  *q = spmc_queue_create(1024);
 *   mpmc_queue_t  *q = mpmc_queue_create(1024);
 */
#pragma once
#include "dq_spsc.h"
#include "dq_mpsc.h"
#include "dq_spmc.h"
#include "dq_mpmc.h"
