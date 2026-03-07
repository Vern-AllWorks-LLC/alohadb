/*-------------------------------------------------------------------------
 *
 * raft_log.c
 *	  Raft log storage backed by WAL segments.
 *
 * In our design the WAL *is* the Raft log.  Each WAL record corresponds
 * to a Raft log entry; the Raft log index is the WAL LSN.  This file
 * provides helper routines that map between Raft concepts (log index,
 * term, committed/replicated) and WAL primitives.
 *
 * Term tracking:  we keep a small in-memory cache that maps LSN ranges
 * to Raft terms.  Because terms change infrequently (only on leader
 * election), the cache is tiny.
 *
 * Portions Copyright (c) 2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/backend/replication/raft/raft_log.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "replication/raft.h"
#include "storage/spin.h"

/*
 * A small ring-buffer that maps consecutive term ranges to LSN boundaries.
 * Each slot says: "from startLSN onwards, the Raft term is <term>".
 * The latest slot covers all LSNs >= startLSN up to the current write head.
 *
 * We keep this inside the RaftState shared memory block conceptually,
 * but to avoid bloating that struct we maintain a private static table
 * here -- it's only ever accessed by the WAL-writing process (leader)
 * and needs no locking beyond what RaftCtl->mutex already provides.
 */
#define RAFT_TERM_MAP_SIZE		64

typedef struct RaftTermMapEntry
{
	XLogRecPtr	startLSN;	/* first LSN belonging to this term */
	uint64		term;		/* the Raft term */
} RaftTermMapEntry;

static RaftTermMapEntry TermMap[RAFT_TERM_MAP_SIZE];
static int	TermMapCount = 0;

/*
 * RaftLogGetLastLSN
 *		Return the LSN of the last WAL record flushed to disk.
 *
 * This is the Raft "last log index".  We use GetFlushRecPtr() which
 * gives us the end of the last record that has been flushed.
 */
XLogRecPtr
RaftLogGetLastLSN(void)
{
	return GetFlushRecPtr(NULL);
}

/*
 * RaftLogGetTermForLSN
 *		Return the Raft term in effect when the given LSN was written.
 *
 * Walks the term map backwards to find the entry whose startLSN is
 * <= the requested LSN.  If we have no mapping (e.g. before Raft was
 * initialised) we return the current term as a safe default.
 */
uint64
RaftLogGetTermForLSN(XLogRecPtr lsn)
{
	int			i;

	for (i = TermMapCount - 1; i >= 0; i--)
	{
		if (TermMap[i].startLSN <= lsn)
			return TermMap[i].term;
	}

	/* Fallback: return the current term */
	if (RaftCtl != NULL)
	{
		uint64		term;

		SpinLockAcquire(&RaftCtl->mutex);
		term = RaftCtl->currentTerm;
		SpinLockRelease(&RaftCtl->mutex);
		return term;
	}

	return 0;
}

/*
 * RaftLogRecordTermStart
 *		Record that a new Raft term begins at the given LSN.
 *
 * Called when this node becomes leader so that subsequent WAL writes
 * are tagged with the new term.
 */
void
RaftLogRecordTermStart(XLogRecPtr lsn, uint64 term)
{
	if (TermMapCount < RAFT_TERM_MAP_SIZE)
	{
		TermMap[TermMapCount].startLSN = lsn;
		TermMap[TermMapCount].term = term;
		TermMapCount++;
	}
	else
	{
		/*
		 * Ring buffer full -- shift entries left, discarding the oldest.
		 * This is fine because we only need recent history.
		 */
		memmove(&TermMap[0], &TermMap[1],
				sizeof(RaftTermMapEntry) * (RAFT_TERM_MAP_SIZE - 1));
		TermMap[RAFT_TERM_MAP_SIZE - 1].startLSN = lsn;
		TermMap[RAFT_TERM_MAP_SIZE - 1].term = term;
	}
}

/*
 * RaftLogIsCommitted
 *		Return true if the given LSN has been committed (i.e. replicated
 *		to a majority).
 */
bool
RaftLogIsCommitted(XLogRecPtr lsn)
{
	XLogRecPtr	ci;

	if (RaftCtl == NULL)
		return false;

	SpinLockAcquire(&RaftCtl->mutex);
	ci = RaftCtl->commitIndex;
	SpinLockRelease(&RaftCtl->mutex);

	return (lsn <= ci);
}

/*
 * RaftLogMarkCommitted
 *		Set the Raft commit index to at least the given LSN.
 *
 * On followers this is called when an AppendEntries RPC carries a
 * leaderCommit value that exceeds our current commitIndex.
 */
void
RaftLogMarkCommitted(XLogRecPtr lsn)
{
	if (RaftCtl == NULL)
		return;

	SpinLockAcquire(&RaftCtl->mutex);
	if (lsn > RaftCtl->commitIndex)
		RaftCtl->commitIndex = lsn;
	SpinLockRelease(&RaftCtl->mutex);

	ConditionVariableBroadcast(&RaftCtl->commitCV);
}

/*
 * RaftLogUpdateMatchIndex
 *		Update the matchIndex for a given peer (leader-side).
 */
void
RaftLogUpdateMatchIndex(int peerId, XLogRecPtr lsn)
{
	if (RaftCtl == NULL)
		return;

	if (peerId < 0 || peerId >= RAFT_MAX_NODES)
		return;

	SpinLockAcquire(&RaftCtl->mutex);
	if (lsn > RaftCtl->peers[peerId].matchIndex)
	{
		RaftCtl->peers[peerId].matchIndex = lsn;
		RaftCtl->peers[peerId].nextIndex = lsn;
	}
	SpinLockRelease(&RaftCtl->mutex);
}

/*
 * RaftLogGetMatchIndex
 *		Retrieve the current matchIndex for a given peer.
 */
XLogRecPtr
RaftLogGetMatchIndex(int peerId)
{
	XLogRecPtr	result;

	if (RaftCtl == NULL || peerId < 0 || peerId >= RAFT_MAX_NODES)
		return InvalidXLogRecPtr;

	SpinLockAcquire(&RaftCtl->mutex);
	result = RaftCtl->peers[peerId].matchIndex;
	SpinLockRelease(&RaftCtl->mutex);

	return result;
}
