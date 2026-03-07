/*-------------------------------------------------------------------------
 *
 * freelist.c
 *	  routines for managing the buffer pool's replacement strategy.
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/buffer/freelist.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pgstat.h"
#include "port/atomics.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/proc.h"

#define INT_ACCESS_ONCE(var)	((int)(*((volatile int *)&(var))))

/* GUC variable: buffer eviction strategy, default is clock sweep */
int			buffer_eviction_strategy = EVICTION_CLOCK;


/*
 * The shared freelist control information.
 */
typedef struct
{
	/* Spinlock: protects the values below */
	slock_t		buffer_strategy_lock;

	/*
	 * Clock sweep hand: index of next buffer to consider grabbing. Note that
	 * this isn't a concrete buffer - we only ever increase the value. So, to
	 * get an actual buffer, it needs to be used modulo NBuffers.
	 */
	pg_atomic_uint32 nextVictimBuffer;

	int			firstFreeBuffer;	/* Head of list of unused buffers */
	int			lastFreeBuffer; /* Tail of list of unused buffers */

	/*
	 * NOTE: lastFreeBuffer is undefined when firstFreeBuffer is -1 (that is,
	 * when the list is empty)
	 */

	/*
	 * Statistics.  These counters should be wide enough that they can't
	 * overflow during a single bgwriter cycle.
	 */
	uint32		completePasses; /* Complete cycles of the clock sweep */
	pg_atomic_uint32 numBufferAllocs;	/* Buffers allocated since last reset */

	/*
	 * Bgworker process to be notified upon activity or -1 if none. See
	 * StrategyNotifyBgWriter.
	 */
	int			bgwprocno;
} BufferStrategyControl;

/* Pointers to shared state */
static BufferStrategyControl *StrategyControl = NULL;

/* ----------------------------------------------------------------
 *		LIRS (Low Inter-reference Recency Set) data structures
 *
 * The LIRS algorithm (Jiang & Zhang, SIGMETRICS 2002) classifies
 * buffer pages into two sets:
 *
 *   LIR (Low Inter-reference Recency) - pages with small reuse distance.
 *       These are the "hot" pages that should be kept resident.
 *
 *   HIR (High Inter-reference Recency) - pages with large reuse distance.
 *       A small fraction of HIR pages are kept resident (HIR-resident),
 *       and the rest are non-resident (tracked only for metadata).
 *
 * Two main structures:
 *   1. LIRS Stack (S) - ordered by recency, implemented as a doubly-linked
 *      list.  Contains all LIR blocks and some HIR blocks.
 *   2. HIR Queue (Q) - FIFO queue of resident HIR blocks, used for
 *      victim selection.
 *
 * Eviction: the victim is always taken from the tail of the HIR queue.
 * ----------------------------------------------------------------
 */

/*
 * LirsStackEntry - a node in the LIRS stack (doubly-linked list).
 * Stored in shared memory as an array indexed by buf_id.
 */
typedef struct LirsStackEntry
{
	int			prev;		/* index into LirsStack[], or -1 */
	int			next;		/* index into LirsStack[], or -1 */
	bool		in_stack;	/* is this entry currently in the stack? */
} LirsStackEntry;

/*
 * LirsQueueEntry - a node in the HIR resident queue (doubly-linked list).
 * Stored in shared memory as an array indexed by buf_id.
 */
typedef struct LirsQueueEntry
{
	int			prev;		/* index into LirsQueue[], or -1 */
	int			next;		/* index into LirsQueue[], or -1 */
	bool		in_queue;	/* is this entry currently in the queue? */
} LirsQueueEntry;

/*
 * LirsControl - shared control structure for LIRS strategy.
 * Protected by StrategyControl->buffer_strategy_lock (the existing spinlock).
 */
typedef struct LirsControl
{
	/* LIRS stack (S): doubly-linked list ordered by recency */
	int			stack_top;		/* most recently accessed (MRU end), or -1 */
	int			stack_bottom;	/* least recently accessed (LRU end), or -1 */
	int			stack_count;	/* number of entries in stack */

	/* HIR queue (Q): FIFO of resident HIR blocks */
	int			queue_head;		/* front of queue (eviction end), or -1 */
	int			queue_tail;		/* back of queue (insertion end), or -1 */
	int			queue_count;	/* number of entries in queue */

	/* Size limits */
	int			lir_limit;		/* max number of LIR blocks (~99% of NBuffers) */
	int			hir_limit;		/* max number of resident HIR blocks (~1%) */
	int			lir_count;		/* current number of LIR blocks */
} LirsControl;

/* Pointers to shared LIRS state */
static LirsControl *LirsCtl = NULL;
static LirsStackEntry *LirsStack = NULL;
static LirsQueueEntry *LirsQueue = NULL;

/* Shared array of per-buffer LIRS status, indexed by buf_id */
LirsBufferInfo *LirsBufferInfoArray = NULL;

/*
 * Private (non-shared) state for managing a ring of shared buffers to re-use.
 * This is currently the only kind of BufferAccessStrategy object, but someday
 * we might have more kinds.
 */
typedef struct BufferAccessStrategyData
{
	/* Overall strategy type */
	BufferAccessStrategyType btype;
	/* Number of elements in buffers[] array */
	int			nbuffers;

	/*
	 * Index of the "current" slot in the ring, ie, the one most recently
	 * returned by GetBufferFromRing.
	 */
	int			current;

	/*
	 * Array of buffer numbers.  InvalidBuffer (that is, zero) indicates we
	 * have not yet selected a buffer for this ring slot.  For allocation
	 * simplicity this is palloc'd together with the fixed fields of the
	 * struct.
	 */
	Buffer		buffers[FLEXIBLE_ARRAY_MEMBER];
}			BufferAccessStrategyData;


/* Prototypes for internal functions */
static BufferDesc *GetBufferFromRing(BufferAccessStrategy strategy,
									 uint32 *buf_state);
static void AddBufferToRing(BufferAccessStrategy strategy,
							BufferDesc *buf);

/* LIRS internal function prototypes */
static BufferDesc *LirsStrategyGetBuffer(BufferAccessStrategy strategy,
										 uint32 *buf_state, bool *from_ring);
static void LirsStackPush(int buf_id);
static void LirsStackRemove(int buf_id);
static void LirsStackPrune(void);
static void LirsQueueAppend(int buf_id);
static void LirsQueueRemove(int buf_id);
static int	LirsQueuePopHead(void);
static void LirsInitStrategy(void);

/*
 * ClockSweepTick - Helper routine for StrategyGetBuffer()
 *
 * Move the clock hand one buffer ahead of its current position and return the
 * id of the buffer now under the hand.
 */
static inline uint32
ClockSweepTick(void)
{
	uint32		victim;

	/*
	 * Atomically move hand ahead one buffer - if there's several processes
	 * doing this, this can lead to buffers being returned slightly out of
	 * apparent order.
	 */
	victim =
		pg_atomic_fetch_add_u32(&StrategyControl->nextVictimBuffer, 1);

	if (victim >= NBuffers)
	{
		uint32		originalVictim = victim;

		/* always wrap what we look up in BufferDescriptors */
		victim = victim % NBuffers;

		/*
		 * If we're the one that just caused a wraparound, force
		 * completePasses to be incremented while holding the spinlock. We
		 * need the spinlock so StrategySyncStart() can return a consistent
		 * value consisting of nextVictimBuffer and completePasses.
		 */
		if (victim == 0)
		{
			uint32		expected;
			uint32		wrapped;
			bool		success = false;

			expected = originalVictim + 1;

			while (!success)
			{
				/*
				 * Acquire the spinlock while increasing completePasses. That
				 * allows other readers to read nextVictimBuffer and
				 * completePasses in a consistent manner which is required for
				 * StrategySyncStart().  In theory delaying the increment
				 * could lead to an overflow of nextVictimBuffers, but that's
				 * highly unlikely and wouldn't be particularly harmful.
				 */
				SpinLockAcquire(&StrategyControl->buffer_strategy_lock);

				wrapped = expected % NBuffers;

				success = pg_atomic_compare_exchange_u32(&StrategyControl->nextVictimBuffer,
														 &expected, wrapped);
				if (success)
					StrategyControl->completePasses++;
				SpinLockRelease(&StrategyControl->buffer_strategy_lock);
			}
		}
	}
	return victim;
}

/*
 * have_free_buffer -- a lockless check to see if there is a free buffer in
 *					   buffer pool.
 *
 * If the result is true that will become stale once free buffers are moved out
 * by other operations, so the caller who strictly want to use a free buffer
 * should not call this.
 */
bool
have_free_buffer(void)
{
	if (StrategyControl->firstFreeBuffer >= 0)
		return true;
	else
		return false;
}

/*
 * StrategyGetBuffer
 *
 *	Called by the bufmgr to get the next candidate buffer to use in
 *	BufferAlloc(). The only hard requirement BufferAlloc() has is that
 *	the selected buffer must not currently be pinned by anyone.
 *
 *	strategy is a BufferAccessStrategy object, or NULL for default strategy.
 *
 *	To ensure that no one else can pin the buffer before we do, we must
 *	return the buffer with the buffer header spinlock still held.
 */
BufferDesc *
StrategyGetBuffer(BufferAccessStrategy strategy, uint32 *buf_state, bool *from_ring)
{
	BufferDesc *buf;
	int			bgwprocno;
	int			trycounter;
	uint32		local_buf_state;	/* to avoid repeated (de-)referencing */

	*from_ring = false;

	/*
	 * If given a strategy object, see whether it can select a buffer. We
	 * assume strategy objects don't need buffer_strategy_lock.
	 */
	if (strategy != NULL)
	{
		buf = GetBufferFromRing(strategy, buf_state);
		if (buf != NULL)
		{
			*from_ring = true;
			return buf;
		}
	}

	/*
	 * If asked, we need to waken the bgwriter. Since we don't want to rely on
	 * a spinlock for this we force a read from shared memory once, and then
	 * set the latch based on that value. We need to go through that length
	 * because otherwise bgwprocno might be reset while/after we check because
	 * the compiler might just reread from memory.
	 *
	 * This can possibly set the latch of the wrong process if the bgwriter
	 * dies in the wrong moment. But since PGPROC->procLatch is never
	 * deallocated the worst consequence of that is that we set the latch of
	 * some arbitrary process.
	 */
	bgwprocno = INT_ACCESS_ONCE(StrategyControl->bgwprocno);
	if (bgwprocno != -1)
	{
		/* reset bgwprocno first, before setting the latch */
		StrategyControl->bgwprocno = -1;

		/*
		 * Not acquiring ProcArrayLock here which is slightly icky. It's
		 * actually fine because procLatch isn't ever freed, so we just can
		 * potentially set the wrong process' (or no process') latch.
		 */
		SetLatch(&ProcGlobal->allProcs[bgwprocno].procLatch);
	}

	/*
	 * We count buffer allocation requests so that the bgwriter can estimate
	 * the rate of buffer consumption.  Note that buffers recycled by a
	 * strategy object are intentionally not counted here.
	 */
	pg_atomic_fetch_add_u32(&StrategyControl->numBufferAllocs, 1);

	/*
	 * Dispatch to the configured eviction strategy.
	 *
	 * When LIRS is enabled, delegate victim selection to LirsStrategyGetBuffer
	 * after the freelist is exhausted.  The freelist check is common to both
	 * strategies: newly-initialized buffers should always be consumed first.
	 */

	/*
	 * First check, without acquiring the lock, whether there's buffers in the
	 * freelist. Since we otherwise don't require the spinlock in every
	 * StrategyGetBuffer() invocation, it'd be sad to acquire it here -
	 * uselessly in most cases. That obviously leaves a race where a buffer is
	 * put on the freelist but we don't see the store yet - but that's pretty
	 * harmless, it'll just get used during the next buffer acquisition.
	 *
	 * If there's buffers on the freelist, acquire the spinlock to pop one
	 * buffer of the freelist. Then check whether that buffer is usable and
	 * repeat if not.
	 *
	 * Note that the freeNext fields are considered to be protected by the
	 * buffer_strategy_lock not the individual buffer spinlocks, so it's OK to
	 * manipulate them without holding the spinlock.
	 */
	if (StrategyControl->firstFreeBuffer >= 0)
	{
		while (true)
		{
			/* Acquire the spinlock to remove element from the freelist */
			SpinLockAcquire(&StrategyControl->buffer_strategy_lock);

			if (StrategyControl->firstFreeBuffer < 0)
			{
				SpinLockRelease(&StrategyControl->buffer_strategy_lock);
				break;
			}

			buf = GetBufferDescriptor(StrategyControl->firstFreeBuffer);
			Assert(buf->freeNext != FREENEXT_NOT_IN_LIST);

			/* Unconditionally remove buffer from freelist */
			StrategyControl->firstFreeBuffer = buf->freeNext;
			buf->freeNext = FREENEXT_NOT_IN_LIST;

			/*
			 * Release the lock so someone else can access the freelist while
			 * we check out this buffer.
			 */
			SpinLockRelease(&StrategyControl->buffer_strategy_lock);

			/*
			 * If the buffer is pinned or has a nonzero usage_count, we cannot
			 * use it; discard it and retry.  (This can only happen if VACUUM
			 * put a valid buffer in the freelist and then someone else used
			 * it before we got to it.  It's probably impossible altogether as
			 * of 8.3, but we'd better check anyway.)
			 */
			local_buf_state = LockBufHdr(buf);
			if (BUF_STATE_GET_REFCOUNT(local_buf_state) == 0
				&& BUF_STATE_GET_USAGECOUNT(local_buf_state) == 0)
			{
				if (strategy != NULL)
					AddBufferToRing(strategy, buf);
				*buf_state = local_buf_state;
				return buf;
			}
			UnlockBufHdr(buf, local_buf_state);
		}
	}

	/*
	 * Nothing on the freelist.  Dispatch to the appropriate eviction
	 * algorithm based on the GUC setting.
	 */
	if (buffer_eviction_strategy == EVICTION_LIRS)
		return LirsStrategyGetBuffer(strategy, buf_state, from_ring);

	/* Default: run the "clock sweep" algorithm */
	trycounter = NBuffers;
	for (;;)
	{
		buf = GetBufferDescriptor(ClockSweepTick());

		/*
		 * If the buffer is pinned or has a nonzero usage_count, we cannot use
		 * it; decrement the usage_count (unless pinned) and keep scanning.
		 */
		local_buf_state = LockBufHdr(buf);

		if (BUF_STATE_GET_REFCOUNT(local_buf_state) == 0)
		{
			if (BUF_STATE_GET_USAGECOUNT(local_buf_state) != 0)
			{
				local_buf_state -= BUF_USAGECOUNT_ONE;

				trycounter = NBuffers;
			}
			else
			{
				/* Found a usable buffer */
				if (strategy != NULL)
					AddBufferToRing(strategy, buf);
				*buf_state = local_buf_state;
				return buf;
			}
		}
		else if (--trycounter == 0)
		{
			/*
			 * We've scanned all the buffers without making any state changes,
			 * so all the buffers are pinned (or were when we looked at them).
			 * We could hope that someone will free one eventually, but it's
			 * probably better to fail than to risk getting stuck in an
			 * infinite loop.
			 */
			UnlockBufHdr(buf, local_buf_state);
			elog(ERROR, "no unpinned buffers available");
		}
		UnlockBufHdr(buf, local_buf_state);
	}
}

/*
 * StrategyFreeBuffer: put a buffer on the freelist
 */
void
StrategyFreeBuffer(BufferDesc *buf)
{
	SpinLockAcquire(&StrategyControl->buffer_strategy_lock);

	/*
	 * It is possible that we are told to put something in the freelist that
	 * is already in it; don't screw up the list if so.
	 */
	if (buf->freeNext == FREENEXT_NOT_IN_LIST)
	{
		buf->freeNext = StrategyControl->firstFreeBuffer;
		if (buf->freeNext < 0)
			StrategyControl->lastFreeBuffer = buf->buf_id;
		StrategyControl->firstFreeBuffer = buf->buf_id;
	}

	/*
	 * If LIRS is active, remove the buffer from LIRS data structures
	 * and reset its status.  When re-allocated from the freelist, it
	 * will be treated as a new buffer by LirsAccessBuffer().
	 */
	if (buffer_eviction_strategy == EVICTION_LIRS && LirsCtl != NULL)
	{
		int			buf_id = buf->buf_id;

		if (LirsBufferInfoArray[buf_id].lirs_status == LIRS_STATUS_LIR)
			LirsCtl->lir_count--;

		LirsStackRemove(buf_id);
		LirsQueueRemove(buf_id);
		LirsBufferInfoArray[buf_id].lirs_status = LIRS_STATUS_LIR;

		/* Prune stack bottom in case we removed the bottom LIR entry */
		LirsStackPrune();
	}

	SpinLockRelease(&StrategyControl->buffer_strategy_lock);
}

/*
 * StrategySyncStart -- tell BgBufferSync where to start syncing
 *
 * The result is the buffer index of the best buffer to sync first.
 * BgBufferSync() will proceed circularly around the buffer array from there.
 *
 * In addition, we return the completed-pass count (which is effectively
 * the higher-order bits of nextVictimBuffer) and the count of recent buffer
 * allocs if non-NULL pointers are passed.  The alloc count is reset after
 * being read.
 */
int
StrategySyncStart(uint32 *complete_passes, uint32 *num_buf_alloc)
{
	uint32		nextVictimBuffer;
	int			result;

	SpinLockAcquire(&StrategyControl->buffer_strategy_lock);
	nextVictimBuffer = pg_atomic_read_u32(&StrategyControl->nextVictimBuffer);
	result = nextVictimBuffer % NBuffers;

	if (complete_passes)
	{
		*complete_passes = StrategyControl->completePasses;

		/*
		 * Additionally add the number of wraparounds that happened before
		 * completePasses could be incremented. C.f. ClockSweepTick().
		 */
		*complete_passes += nextVictimBuffer / NBuffers;
	}

	if (num_buf_alloc)
	{
		*num_buf_alloc = pg_atomic_exchange_u32(&StrategyControl->numBufferAllocs, 0);
	}
	SpinLockRelease(&StrategyControl->buffer_strategy_lock);
	return result;
}

/*
 * StrategyNotifyBgWriter -- set or clear allocation notification latch
 *
 * If bgwprocno isn't -1, the next invocation of StrategyGetBuffer will
 * set that latch.  Pass -1 to clear the pending notification before it
 * happens.  This feature is used by the bgwriter process to wake itself up
 * from hibernation, and is not meant for anybody else to use.
 */
void
StrategyNotifyBgWriter(int bgwprocno)
{
	/*
	 * We acquire buffer_strategy_lock just to ensure that the store appears
	 * atomic to StrategyGetBuffer.  The bgwriter should call this rather
	 * infrequently, so there's no performance penalty from being safe.
	 */
	SpinLockAcquire(&StrategyControl->buffer_strategy_lock);
	StrategyControl->bgwprocno = bgwprocno;
	SpinLockRelease(&StrategyControl->buffer_strategy_lock);
}


/*
 * StrategyShmemSize
 *
 * estimate the size of shared memory used by the freelist-related structures.
 *
 * Note: for somewhat historical reasons, the buffer lookup hashtable size
 * is also determined here.
 */
Size
StrategyShmemSize(void)
{
	Size		size = 0;

	/* size of lookup hash table ... see comment in StrategyInitialize */
	size = add_size(size, BufTableShmemSize(NBuffers + NUM_BUFFER_PARTITIONS));

	/* size of the shared replacement strategy control block */
	size = add_size(size, MAXALIGN(sizeof(BufferStrategyControl)));

	/*
	 * Always allocate LIRS shared memory so that the strategy can be
	 * switched without requiring a restart.
	 */
	size = add_size(size, MAXALIGN(sizeof(LirsControl)));
	size = add_size(size, mul_size(NBuffers, MAXALIGN(sizeof(LirsStackEntry))));
	size = add_size(size, mul_size(NBuffers, MAXALIGN(sizeof(LirsQueueEntry))));
	size = add_size(size, mul_size(NBuffers, MAXALIGN(sizeof(LirsBufferInfo))));

	return size;
}

/*
 * StrategyInitialize -- initialize the buffer cache replacement
 *		strategy.
 *
 * Assumes: All of the buffers are already built into a linked list.
 *		Only called by postmaster and only during initialization.
 */
void
StrategyInitialize(bool init)
{
	bool		found;

	/*
	 * Initialize the shared buffer lookup hashtable.
	 *
	 * Since we can't tolerate running out of lookup table entries, we must be
	 * sure to specify an adequate table size here.  The maximum steady-state
	 * usage is of course NBuffers entries, but BufferAlloc() tries to insert
	 * a new entry before deleting the old.  In principle this could be
	 * happening in each partition concurrently, so we could need as many as
	 * NBuffers + NUM_BUFFER_PARTITIONS entries.
	 */
	InitBufTable(NBuffers + NUM_BUFFER_PARTITIONS);

	/*
	 * Get or create the shared strategy control block
	 */
	StrategyControl = (BufferStrategyControl *)
		ShmemInitStruct("Buffer Strategy Status",
						sizeof(BufferStrategyControl),
						&found);

	if (!found)
	{
		/*
		 * Only done once, usually in postmaster
		 */
		Assert(init);

		SpinLockInit(&StrategyControl->buffer_strategy_lock);

		/*
		 * Grab the whole linked list of free buffers for our strategy. We
		 * assume it was previously set up by BufferManagerShmemInit().
		 */
		StrategyControl->firstFreeBuffer = 0;
		StrategyControl->lastFreeBuffer = NBuffers - 1;

		/* Initialize the clock sweep pointer */
		pg_atomic_init_u32(&StrategyControl->nextVictimBuffer, 0);

		/* Clear statistics */
		StrategyControl->completePasses = 0;
		pg_atomic_init_u32(&StrategyControl->numBufferAllocs, 0);

		/* No pending notification */
		StrategyControl->bgwprocno = -1;
	}
	else
		Assert(!init);

	/*
	 * Initialize LIRS shared memory structures.  We always allocate these
	 * regardless of the current buffer_eviction_strategy setting, so that
	 * the strategy can be switched dynamically via SIGHUP.
	 */
	LirsInitStrategy();
}


/* ----------------------------------------------------------------
 *				Backend-private buffer ring management
 * ----------------------------------------------------------------
 */


/*
 * GetAccessStrategy -- create a BufferAccessStrategy object
 *
 * The object is allocated in the current memory context.
 */
BufferAccessStrategy
GetAccessStrategy(BufferAccessStrategyType btype)
{
	int			ring_size_kb;

	/*
	 * Select ring size to use.  See buffer/README for rationales.
	 *
	 * Note: if you change the ring size for BAS_BULKREAD, see also
	 * SYNC_SCAN_REPORT_INTERVAL in access/heap/syncscan.c.
	 */
	switch (btype)
	{
		case BAS_NORMAL:
			/* if someone asks for NORMAL, just give 'em a "default" object */
			return NULL;

		case BAS_BULKREAD:
			{
				int			ring_max_kb;

				/*
				 * The ring always needs to be large enough to allow some
				 * separation in time between providing a buffer to the user
				 * of the strategy and that buffer being reused. Otherwise the
				 * user's pin will prevent reuse of the buffer, even without
				 * concurrent activity.
				 *
				 * We also need to ensure the ring always is large enough for
				 * SYNC_SCAN_REPORT_INTERVAL, as noted above.
				 *
				 * Thus we start out a minimal size and increase the size
				 * further if appropriate.
				 */
				ring_size_kb = 256;

				/*
				 * There's no point in a larger ring if we won't be allowed to
				 * pin sufficiently many buffers.  But we never limit to less
				 * than the minimal size above.
				 */
				ring_max_kb = GetPinLimit() * (BLCKSZ / 1024);
				ring_max_kb = Max(ring_size_kb, ring_max_kb);

				/*
				 * We would like the ring to additionally have space for the
				 * configured degree of IO concurrency. While being read in,
				 * buffers can obviously not yet be reused.
				 *
				 * Each IO can be up to io_combine_limit blocks large, and we
				 * want to start up to effective_io_concurrency IOs.
				 *
				 * Note that effective_io_concurrency may be 0, which disables
				 * AIO.
				 */
				ring_size_kb += (BLCKSZ / 1024) *
					io_combine_limit * effective_io_concurrency;

				if (ring_size_kb > ring_max_kb)
					ring_size_kb = ring_max_kb;
				break;
			}
		case BAS_BULKWRITE:
			ring_size_kb = 16 * 1024;
			break;
		case BAS_VACUUM:
			ring_size_kb = 2048;
			break;

		default:
			elog(ERROR, "unrecognized buffer access strategy: %d",
				 (int) btype);
			return NULL;		/* keep compiler quiet */
	}

	return GetAccessStrategyWithSize(btype, ring_size_kb);
}

/*
 * GetAccessStrategyWithSize -- create a BufferAccessStrategy object with a
 *		number of buffers equivalent to the passed in size.
 *
 * If the given ring size is 0, no BufferAccessStrategy will be created and
 * the function will return NULL.  ring_size_kb must not be negative.
 */
BufferAccessStrategy
GetAccessStrategyWithSize(BufferAccessStrategyType btype, int ring_size_kb)
{
	int			ring_buffers;
	BufferAccessStrategy strategy;

	Assert(ring_size_kb >= 0);

	/* Figure out how many buffers ring_size_kb is */
	ring_buffers = ring_size_kb / (BLCKSZ / 1024);

	/* 0 means unlimited, so no BufferAccessStrategy required */
	if (ring_buffers == 0)
		return NULL;

	/* Cap to 1/8th of shared_buffers */
	ring_buffers = Min(NBuffers / 8, ring_buffers);

	/* NBuffers should never be less than 16, so this shouldn't happen */
	Assert(ring_buffers > 0);

	/* Allocate the object and initialize all elements to zeroes */
	strategy = (BufferAccessStrategy)
		palloc0(offsetof(BufferAccessStrategyData, buffers) +
				ring_buffers * sizeof(Buffer));

	/* Set fields that don't start out zero */
	strategy->btype = btype;
	strategy->nbuffers = ring_buffers;

	return strategy;
}

/*
 * GetAccessStrategyBufferCount -- an accessor for the number of buffers in
 *		the ring
 *
 * Returns 0 on NULL input to match behavior of GetAccessStrategyWithSize()
 * returning NULL with 0 size.
 */
int
GetAccessStrategyBufferCount(BufferAccessStrategy strategy)
{
	if (strategy == NULL)
		return 0;

	return strategy->nbuffers;
}

/*
 * GetAccessStrategyPinLimit -- get cap of number of buffers that should be pinned
 *
 * When pinning extra buffers to look ahead, users of a ring-based strategy are
 * in danger of pinning too much of the ring at once while performing look-ahead.
 * For some strategies, that means "escaping" from the ring, and in others it
 * means forcing dirty data to disk very frequently with associated WAL
 * flushing.  Since external code has no insight into any of that, allow
 * individual strategy types to expose a clamp that should be applied when
 * deciding on a maximum number of buffers to pin at once.
 *
 * Callers should combine this number with other relevant limits and take the
 * minimum.
 */
int
GetAccessStrategyPinLimit(BufferAccessStrategy strategy)
{
	if (strategy == NULL)
		return NBuffers;

	switch (strategy->btype)
	{
		case BAS_BULKREAD:

			/*
			 * Since BAS_BULKREAD uses StrategyRejectBuffer(), dirty buffers
			 * shouldn't be a problem and the caller is free to pin up to the
			 * entire ring at once.
			 */
			return strategy->nbuffers;

		default:

			/*
			 * Tell caller not to pin more than half the buffers in the ring.
			 * This is a trade-off between look ahead distance and deferring
			 * writeback and associated WAL traffic.
			 */
			return strategy->nbuffers / 2;
	}
}

/*
 * FreeAccessStrategy -- release a BufferAccessStrategy object
 *
 * A simple pfree would do at the moment, but we would prefer that callers
 * don't assume that much about the representation of BufferAccessStrategy.
 */
void
FreeAccessStrategy(BufferAccessStrategy strategy)
{
	/* don't crash if called on a "default" strategy */
	if (strategy != NULL)
		pfree(strategy);
}

/*
 * GetBufferFromRing -- returns a buffer from the ring, or NULL if the
 *		ring is empty / not usable.
 *
 * The bufhdr spin lock is held on the returned buffer.
 */
static BufferDesc *
GetBufferFromRing(BufferAccessStrategy strategy, uint32 *buf_state)
{
	BufferDesc *buf;
	Buffer		bufnum;
	uint32		local_buf_state;	/* to avoid repeated (de-)referencing */


	/* Advance to next ring slot */
	if (++strategy->current >= strategy->nbuffers)
		strategy->current = 0;

	/*
	 * If the slot hasn't been filled yet, tell the caller to allocate a new
	 * buffer with the normal allocation strategy.  He will then fill this
	 * slot by calling AddBufferToRing with the new buffer.
	 */
	bufnum = strategy->buffers[strategy->current];
	if (bufnum == InvalidBuffer)
		return NULL;

	/*
	 * If the buffer is pinned we cannot use it under any circumstances.
	 *
	 * If usage_count is 0 or 1 then the buffer is fair game (we expect 1,
	 * since our own previous usage of the ring element would have left it
	 * there, but it might've been decremented by clock sweep since then). A
	 * higher usage_count indicates someone else has touched the buffer, so we
	 * shouldn't re-use it.
	 */
	buf = GetBufferDescriptor(bufnum - 1);
	local_buf_state = LockBufHdr(buf);
	if (BUF_STATE_GET_REFCOUNT(local_buf_state) == 0
		&& BUF_STATE_GET_USAGECOUNT(local_buf_state) <= 1)
	{
		*buf_state = local_buf_state;
		return buf;
	}
	UnlockBufHdr(buf, local_buf_state);

	/*
	 * Tell caller to allocate a new buffer with the normal allocation
	 * strategy.  He'll then replace this ring element via AddBufferToRing.
	 */
	return NULL;
}

/*
 * AddBufferToRing -- add a buffer to the buffer ring
 *
 * Caller must hold the buffer header spinlock on the buffer.  Since this
 * is called with the spinlock held, it had better be quite cheap.
 */
static void
AddBufferToRing(BufferAccessStrategy strategy, BufferDesc *buf)
{
	strategy->buffers[strategy->current] = BufferDescriptorGetBuffer(buf);
}

/*
 * Utility function returning the IOContext of a given BufferAccessStrategy's
 * strategy ring.
 */
IOContext
IOContextForStrategy(BufferAccessStrategy strategy)
{
	if (!strategy)
		return IOCONTEXT_NORMAL;

	switch (strategy->btype)
	{
		case BAS_NORMAL:

			/*
			 * Currently, GetAccessStrategy() returns NULL for
			 * BufferAccessStrategyType BAS_NORMAL, so this case is
			 * unreachable.
			 */
			pg_unreachable();
			return IOCONTEXT_NORMAL;
		case BAS_BULKREAD:
			return IOCONTEXT_BULKREAD;
		case BAS_BULKWRITE:
			return IOCONTEXT_BULKWRITE;
		case BAS_VACUUM:
			return IOCONTEXT_VACUUM;
	}

	elog(ERROR, "unrecognized BufferAccessStrategyType: %d", strategy->btype);
	pg_unreachable();
}

/*
 * StrategyRejectBuffer -- consider rejecting a dirty buffer
 *
 * When a nondefault strategy is used, the buffer manager calls this function
 * when it turns out that the buffer selected by StrategyGetBuffer needs to
 * be written out and doing so would require flushing WAL too.  This gives us
 * a chance to choose a different victim.
 *
 * Returns true if buffer manager should ask for a new victim, and false
 * if this buffer should be written and re-used.
 */
bool
StrategyRejectBuffer(BufferAccessStrategy strategy, BufferDesc *buf, bool from_ring)
{
	/* We only do this in bulkread mode */
	if (strategy->btype != BAS_BULKREAD)
		return false;

	/* Don't muck with behavior of normal buffer-replacement strategy */
	if (!from_ring ||
		strategy->buffers[strategy->current] != BufferDescriptorGetBuffer(buf))
		return false;

	/*
	 * Remove the dirty buffer from the ring; necessary to prevent infinite
	 * loop if all ring members are dirty.
	 */
	strategy->buffers[strategy->current] = InvalidBuffer;

	return true;
}


/* ----------------------------------------------------------------
 *		LIRS (Low Inter-reference Recency Set) implementation
 *
 * Reference: Jiang & Zhang, "LIRS: An Efficient Low Inter-reference
 * Recency Set Replacement Policy to Improve Buffer Cache Performance",
 * ACM SIGMETRICS 2002.
 *
 * All LIRS functions below that manipulate the stack or queue assume
 * the caller holds StrategyControl->buffer_strategy_lock.
 * ----------------------------------------------------------------
 */

/*
 * LirsInitStrategy -- initialize LIRS shared memory data structures.
 *
 * Called from StrategyInitialize().  Allocates and initializes the
 * shared LirsControl, LirsStack, and LirsQueue arrays.
 */
static void
LirsInitStrategy(void)
{
	bool		found;
	int			i;

	LirsCtl = (LirsControl *)
		ShmemInitStruct("LIRS Control",
						sizeof(LirsControl),
						&found);

	if (!found)
	{
		LirsCtl->stack_top = -1;
		LirsCtl->stack_bottom = -1;
		LirsCtl->stack_count = 0;
		LirsCtl->queue_head = -1;
		LirsCtl->queue_tail = -1;
		LirsCtl->queue_count = 0;

		/*
		 * LIR set is ~99% of buffer pool, HIR resident set is ~1%.
		 * Ensure at least 1 HIR slot so eviction can work.
		 */
		LirsCtl->hir_limit = Max(NBuffers / 100, 1);
		LirsCtl->lir_limit = NBuffers - LirsCtl->hir_limit;
		LirsCtl->lir_count = 0;
	}

	LirsStack = (LirsStackEntry *)
		ShmemInitStruct("LIRS Stack",
						NBuffers * sizeof(LirsStackEntry),
						&found);

	if (!found)
	{
		for (i = 0; i < NBuffers; i++)
		{
			LirsStack[i].prev = -1;
			LirsStack[i].next = -1;
			LirsStack[i].in_stack = false;
		}
	}

	LirsQueue = (LirsQueueEntry *)
		ShmemInitStruct("LIRS Queue",
						NBuffers * sizeof(LirsQueueEntry),
						&found);

	if (!found)
	{
		for (i = 0; i < NBuffers; i++)
		{
			LirsQueue[i].prev = -1;
			LirsQueue[i].next = -1;
			LirsQueue[i].in_queue = false;
		}
	}

	LirsBufferInfoArray = (LirsBufferInfo *)
		ShmemInitStruct("LIRS Buffer Info",
						NBuffers * sizeof(LirsBufferInfo),
						&found);

	if (!found)
	{
		for (i = 0; i < NBuffers; i++)
		{
			LirsBufferInfoArray[i].lirs_status = LIRS_STATUS_LIR;
		}
	}
}

/*
 * LirsStackPush -- move or insert buf_id to the top (MRU end) of the
 *                  LIRS stack.
 *
 * If the entry is already in the stack, it is first removed from its
 * current position and then re-inserted at the top.
 *
 * Caller must hold buffer_strategy_lock.
 */
static void
LirsStackPush(int buf_id)
{
	Assert(buf_id >= 0 && buf_id < NBuffers);

	/* If already in the stack, remove it first */
	if (LirsStack[buf_id].in_stack)
		LirsStackRemove(buf_id);

	/* Insert at the top of the stack */
	LirsStack[buf_id].prev = -1;
	LirsStack[buf_id].next = LirsCtl->stack_top;

	if (LirsCtl->stack_top >= 0)
		LirsStack[LirsCtl->stack_top].prev = buf_id;
	else
		LirsCtl->stack_bottom = buf_id;	/* stack was empty */

	LirsCtl->stack_top = buf_id;
	LirsStack[buf_id].in_stack = true;
	LirsCtl->stack_count++;
}

/*
 * LirsStackRemove -- remove buf_id from the LIRS stack.
 *
 * Caller must hold buffer_strategy_lock.
 */
static void
LirsStackRemove(int buf_id)
{
	int			p,
				n;

	Assert(buf_id >= 0 && buf_id < NBuffers);

	if (!LirsStack[buf_id].in_stack)
		return;

	p = LirsStack[buf_id].prev;
	n = LirsStack[buf_id].next;

	if (p >= 0)
		LirsStack[p].next = n;
	else
		LirsCtl->stack_top = n;	/* was the top */

	if (n >= 0)
		LirsStack[n].prev = p;
	else
		LirsCtl->stack_bottom = p;	/* was the bottom */

	LirsStack[buf_id].prev = -1;
	LirsStack[buf_id].next = -1;
	LirsStack[buf_id].in_stack = false;
	LirsCtl->stack_count--;
}

/*
 * LirsStackPrune -- prune the bottom of the LIRS stack.
 *
 * The LIRS invariant requires that the bottom of the stack is always a
 * LIR block.  After any operation that might place a HIR block at the
 * bottom (e.g., after a LIR block is demoted), we must remove HIR
 * blocks from the stack bottom until a LIR block is at the bottom.
 *
 * Caller must hold buffer_strategy_lock.
 */
static void
LirsStackPrune(void)
{
	while (LirsCtl->stack_bottom >= 0)
	{
		int			bot = LirsCtl->stack_bottom;

		/* Stop when we reach a LIR block at the bottom */
		if (LirsBufferInfoArray[bot].lirs_status == LIRS_STATUS_LIR)
			break;

		/* Remove this HIR block from the stack */
		LirsStackRemove(bot);
	}
}

/*
 * LirsQueueAppend -- append buf_id to the tail (insertion end) of the
 *                    HIR resident queue.
 *
 * If the entry is already in the queue, it is first removed.
 *
 * Caller must hold buffer_strategy_lock.
 */
static void
LirsQueueAppend(int buf_id)
{
	Assert(buf_id >= 0 && buf_id < NBuffers);

	/* If already in queue, remove first */
	if (LirsQueue[buf_id].in_queue)
		LirsQueueRemove(buf_id);

	/* Append at the tail */
	LirsQueue[buf_id].next = -1;
	LirsQueue[buf_id].prev = LirsCtl->queue_tail;

	if (LirsCtl->queue_tail >= 0)
		LirsQueue[LirsCtl->queue_tail].next = buf_id;
	else
		LirsCtl->queue_head = buf_id;	/* queue was empty */

	LirsCtl->queue_tail = buf_id;
	LirsQueue[buf_id].in_queue = true;
	LirsCtl->queue_count++;
}

/*
 * LirsQueueRemove -- remove buf_id from the HIR resident queue.
 *
 * Caller must hold buffer_strategy_lock.
 */
static void
LirsQueueRemove(int buf_id)
{
	int			p,
				n;

	Assert(buf_id >= 0 && buf_id < NBuffers);

	if (!LirsQueue[buf_id].in_queue)
		return;

	p = LirsQueue[buf_id].prev;
	n = LirsQueue[buf_id].next;

	if (p >= 0)
		LirsQueue[p].next = n;
	else
		LirsCtl->queue_head = n;

	if (n >= 0)
		LirsQueue[n].prev = p;
	else
		LirsCtl->queue_tail = p;

	LirsQueue[buf_id].prev = -1;
	LirsQueue[buf_id].next = -1;
	LirsQueue[buf_id].in_queue = false;
	LirsCtl->queue_count--;
}

/*
 * LirsQueuePopHead -- remove and return the buf_id at the head
 *                     (eviction end) of the HIR queue, or -1 if empty.
 *
 * Caller must hold buffer_strategy_lock.
 */
static int
LirsQueuePopHead(void)
{
	int			head;

	head = LirsCtl->queue_head;
	if (head < 0)
		return -1;

	LirsQueueRemove(head);
	return head;
}

/*
 * LirsStrategyGetBuffer -- LIRS victim selection.
 *
 * This is called from StrategyGetBuffer() when the freelist is empty
 * and buffer_eviction_strategy == EVICTION_LIRS.
 *
 * The LIRS algorithm selects victims from the tail of the HIR resident
 * queue.  The returned buffer has its header spinlock held, as required
 * by the StrategyGetBuffer() contract.
 */
static BufferDesc *
LirsStrategyGetBuffer(BufferAccessStrategy strategy,
					  uint32 *buf_state, bool *from_ring)
{
	BufferDesc *buf;
	int			victim_id;
	uint32		local_buf_state;
	int			trycounter;

	trycounter = NBuffers;

	for (;;)
	{
		SpinLockAcquire(&StrategyControl->buffer_strategy_lock);

		victim_id = LirsQueuePopHead();

		SpinLockRelease(&StrategyControl->buffer_strategy_lock);

		/*
		 * If the HIR queue is empty, fall back to clock sweep.  This can
		 * happen during startup when not enough buffers have been accessed
		 * to populate the LIRS structures.
		 */
		if (victim_id < 0)
		{
			/* Fall back to clock sweep for this allocation */
			int			fallback_tries = NBuffers;

			for (;;)
			{
				buf = GetBufferDescriptor(ClockSweepTick());

				local_buf_state = LockBufHdr(buf);

				if (BUF_STATE_GET_REFCOUNT(local_buf_state) == 0)
				{
					if (BUF_STATE_GET_USAGECOUNT(local_buf_state) != 0)
					{
						local_buf_state -= BUF_USAGECOUNT_ONE;
						fallback_tries = NBuffers;
					}
					else
					{
						if (strategy != NULL)
							AddBufferToRing(strategy, buf);
						*buf_state = local_buf_state;
						return buf;
					}
				}
				else if (--fallback_tries == 0)
				{
					UnlockBufHdr(buf, local_buf_state);
					elog(ERROR, "no unpinned buffers available");
				}
				UnlockBufHdr(buf, local_buf_state);
			}
		}

		buf = GetBufferDescriptor(victim_id);

		/*
		 * Lock the buffer header and check if it's usable (not pinned).
		 */
		local_buf_state = LockBufHdr(buf);

		if (BUF_STATE_GET_REFCOUNT(local_buf_state) == 0)
		{
			/*
			 * We have a valid victim.  Mark it as HIR non-resident in the
			 * LIRS metadata (it's being evicted; if it's re-accessed later,
			 * it will be recognized as a returning non-resident HIR block).
			 *
			 * We need the strategy lock to update LIRS metadata.
			 * We already hold the buffer header lock, and we'll acquire
			 * the strategy spinlock briefly just to update the status.
			 * The buffer header lock is a lightweight spinlock embedded
			 * in the state word, so this nesting order is safe.
			 */
			SpinLockAcquire(&StrategyControl->buffer_strategy_lock);
			LirsBufferInfoArray[victim_id].lirs_status = LIRS_STATUS_HIR_NONRESIDENT;
			LirsStackRemove(victim_id);
			SpinLockRelease(&StrategyControl->buffer_strategy_lock);

			if (strategy != NULL)
				AddBufferToRing(strategy, buf);
			*buf_state = local_buf_state;
			return buf;
		}

		/*
		 * Buffer is pinned, cannot use it.  Put it back at the end of the
		 * HIR queue and try again.
		 */
		UnlockBufHdr(buf, local_buf_state);

		SpinLockAcquire(&StrategyControl->buffer_strategy_lock);
		LirsQueueAppend(victim_id);
		LirsBufferInfoArray[victim_id].lirs_status = LIRS_STATUS_HIR_RESIDENT;
		SpinLockRelease(&StrategyControl->buffer_strategy_lock);

		if (--trycounter == 0)
			elog(ERROR, "no unpinned buffers available");
	}
}

/*
 * LirsAccessBuffer -- update LIRS state when a buffer is accessed (pinned).
 *
 * This is called from PinBuffer() (via the extern declaration in
 * buf_internals.h) whenever a shared buffer is pinned using the default
 * access strategy and LIRS eviction is enabled.
 *
 * The function implements the core LIRS state transitions:
 *
 * Case 1: LIR block accessed
 *   - Move to top of LIRS stack
 *   - Prune stack bottom (ensure bottom is LIR)
 *
 * Case 2: HIR resident block accessed, and it is in the LIRS stack
 *   - Promote to LIR status
 *   - Move to top of stack
 *   - Remove from HIR queue
 *   - Demote bottom-of-stack LIR block to HIR resident
 *   - Move demoted block to end of HIR queue
 *   - Prune stack bottom
 *
 * Case 3: HIR resident block accessed, not in LIRS stack
 *   - Move to top of LIRS stack (still HIR)
 *   - Move to end of HIR queue
 *
 * Case 4: HIR non-resident block accessed (returning after eviction)
 *   - If in LIRS stack: promote to LIR, demote bottom LIR to HIR
 *   - Otherwise: mark as HIR resident, add to stack and queue
 *
 * Case 5: New buffer (never seen before)
 *   - If LIR set not full: add as LIR to stack
 *   - Otherwise: add as HIR resident to stack and queue
 */
void
LirsAccessBuffer(BufferDesc *buf)
{
	int			buf_id;

	/* Only active when LIRS eviction is enabled */
	if (buffer_eviction_strategy != EVICTION_LIRS)
		return;

	/* Only for shared buffers */
	if (BufferDescriptorGetBuffer(buf) <= 0)
		return;

	buf_id = buf->buf_id;

	SpinLockAcquire(&StrategyControl->buffer_strategy_lock);

	switch (LirsBufferInfoArray[buf_id].lirs_status)
	{
		case LIRS_STATUS_LIR:
			/*
			 * Case 1: LIR block hit.
			 * Move to top of stack and prune bottom.
			 */
			if (LirsStack[buf_id].in_stack)
			{
				/* Normal LIR hit: move to top of stack */
				LirsStackPush(buf_id);
				LirsStackPrune();
			}
			else
			{
				/*
				 * LIR block not in the stack.  This happens for freshly
				 * allocated buffers whose lirs_status is initialized to
				 * LIRS_STATUS_LIR (0) but have never been tracked by LIRS.
				 *
				 * If the LIR set is not full, add as LIR.
				 * If LIR set is full, demote to HIR resident.
				 */
				if (LirsCtl->lir_count < LirsCtl->lir_limit)
				{
					LirsCtl->lir_count++;
					LirsStackPush(buf_id);
				}
				else
				{
					LirsBufferInfoArray[buf_id].lirs_status = LIRS_STATUS_HIR_RESIDENT;
					LirsStackPush(buf_id);
					LirsQueueAppend(buf_id);
				}
			}
			break;

		case LIRS_STATUS_HIR_RESIDENT:
			/*
			 * Case 2/3: HIR resident block hit.
			 */
			if (LirsStack[buf_id].in_stack)
			{
				/*
				 * Case 2: In the stack -- promote to LIR.
				 */
				int			demoted;

				/* Promote this block to LIR */
				LirsBufferInfoArray[buf_id].lirs_status = LIRS_STATUS_LIR;
				LirsCtl->lir_count++;

				/* Remove from HIR queue */
				LirsQueueRemove(buf_id);

				/* Move to top of stack */
				LirsStackPush(buf_id);

				/*
				 * If LIR set is over limit, demote the bottom-of-stack LIR
				 * block to HIR resident.
				 */
				if (LirsCtl->lir_count > LirsCtl->lir_limit)
				{
					demoted = LirsCtl->stack_bottom;
					if (demoted >= 0 && demoted != buf_id)
					{
						LirsBufferInfoArray[demoted].lirs_status = LIRS_STATUS_HIR_RESIDENT;
						LirsCtl->lir_count--;

						/* Remove from stack bottom */
						LirsStackRemove(demoted);

						/* Add to end of HIR queue */
						LirsQueueAppend(demoted);
					}
				}

				/* Prune stack bottom */
				LirsStackPrune();
			}
			else
			{
				/*
				 * Case 3: Not in the stack -- keep as HIR resident,
				 * move to top of stack and end of HIR queue.
				 */
				LirsStackPush(buf_id);
				LirsQueueAppend(buf_id);
			}
			break;

		case LIRS_STATUS_HIR_NONRESIDENT:
			/*
			 * Case 4: HIR non-resident block accessed (re-accessed after
			 * eviction; the buffer has been re-loaded into a buffer slot).
			 */
			if (LirsStack[buf_id].in_stack)
			{
				int			demoted;

				/*
				 * Was in stack: promote to LIR.
				 */
				LirsBufferInfoArray[buf_id].lirs_status = LIRS_STATUS_LIR;
				LirsCtl->lir_count++;

				LirsStackPush(buf_id);

				/* Demote bottom LIR if over limit */
				if (LirsCtl->lir_count > LirsCtl->lir_limit)
				{
					demoted = LirsCtl->stack_bottom;
					if (demoted >= 0 && demoted != buf_id)
					{
						LirsBufferInfoArray[demoted].lirs_status = LIRS_STATUS_HIR_RESIDENT;
						LirsCtl->lir_count--;

						LirsStackRemove(demoted);
						LirsQueueAppend(demoted);
					}
				}

				LirsStackPrune();
			}
			else
			{
				/*
				 * Not in stack: add as HIR resident.
				 */
				LirsBufferInfoArray[buf_id].lirs_status = LIRS_STATUS_HIR_RESIDENT;
				LirsStackPush(buf_id);
				LirsQueueAppend(buf_id);
			}
			break;

		default:
			/*
			 * Should not happen: all valid LIRS status values are handled
			 * above.  Treat unexpected values as new HIR-resident buffers.
			 */
			LirsBufferInfoArray[buf_id].lirs_status = LIRS_STATUS_HIR_RESIDENT;
			LirsStackPush(buf_id);
			LirsQueueAppend(buf_id);
			break;
	}

	SpinLockRelease(&StrategyControl->buffer_strategy_lock);
}
