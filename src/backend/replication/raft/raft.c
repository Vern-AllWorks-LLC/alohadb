/*-------------------------------------------------------------------------
 *
 * raft.c
 *	  Core Raft consensus state machine for synchronous WAL replication.
 *
 * This module manages the Raft state machine whose log entries are WAL
 * records.  Leader election uses randomised timeouts, and log commitment
 * requires a majority of cluster members to have flushed the WAL up to
 * the relevant LSN.
 *
 * All persistent Raft state (currentTerm, votedFor) is kept in shared
 * memory allocated via ShmemInitStruct.  Hot-path fields are protected
 * by a spinlock; heavier operations acquire the RaftLock LWLock.
 *
 * Portions Copyright (c) 2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/backend/replication/raft/raft.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "access/xlog.h"
#include "miscadmin.h"
#include "replication/raft.h"
#include "replication/walsender_private.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/guc.h"
#include "utils/timestamp.h"

/* Global pointer to shared-memory Raft state */
RaftState  *RaftCtl = NULL;

/* GUC variables -- defined here, declared extern in raft.h */
int			replication_mode = REPLICATION_MODE_STREAMING;
char	   *raft_members = NULL;
int			raft_election_timeout_ms = 1000;
int			raft_heartbeat_interval_ms = 150;

/*
 * RaftShmemSize
 *		Compute the amount of shared memory required for Raft state.
 */
Size
RaftShmemSize(void)
{
	return MAXALIGN(sizeof(RaftState));
}

/*
 * RaftShmemInit
 *		Allocate and initialise the shared-memory Raft state block.
 *
 * Called from CreateSharedMemoryAndSemaphores().
 */
void
RaftShmemInit(void)
{
	bool		found;

	RaftCtl = (RaftState *)
		ShmemInitStruct("Raft Ctl", RaftShmemSize(), &found);

	if (!found)
	{
		/* First time through -- zero everything */
		MemSet(RaftCtl, 0, RaftShmemSize());

		RaftCtl->currentTerm = 0;
		RaftCtl->votedFor = -1;
		RaftCtl->commitIndex = InvalidXLogRecPtr;
		RaftCtl->lastApplied = InvalidXLogRecPtr;
		RaftCtl->role = RAFT_ROLE_FOLLOWER;
		RaftCtl->leaderId = -1;
		RaftCtl->selfNodeId = -1;
		RaftCtl->numNodes = 0;
		RaftCtl->lastHeartbeat = 0;
		RaftCtl->electionTimeoutMs = 0;
		RaftCtl->initialized = false;

		SpinLockInit(&RaftCtl->mutex);
		ConditionVariableInit(&RaftCtl->commitCV);
	}
}

/*
 * RaftInit
 *		One-time initialisation after GUCs have been loaded and shared
 *		memory is ready.  Parses raft_members to populate the node list
 *		and picks our own node id.
 */
void
RaftInit(void)
{
	if (replication_mode != REPLICATION_MODE_RAFT)
		return;

	if (RaftCtl->initialized)
		return;

	/* Parse the membership list from the GUC */
	if (raft_members != NULL && raft_members[0] != '\0')
		RaftParseMembers(raft_members);

	/* Determine our own node_id */
	RaftCtl->selfNodeId = RaftGetNodeId();

	/* Randomise the initial election timeout */
	RaftResetElectionTimer();

	RaftCtl->initialized = true;

	elog(LOG, "raft: initialised node %d, cluster size %d",
		 RaftCtl->selfNodeId, RaftCtl->numNodes);
}

/*
 * RaftBecomeFollower
 *		Transition to follower state in the given term.
 */
void
RaftBecomeFollower(uint64 term, int leaderId)
{
	SpinLockAcquire(&RaftCtl->mutex);
	RaftCtl->currentTerm = term;
	RaftCtl->role = RAFT_ROLE_FOLLOWER;
	RaftCtl->votedFor = -1;
	RaftCtl->leaderId = leaderId;
	SpinLockRelease(&RaftCtl->mutex);

	RaftResetElectionTimer();

	elog(LOG, "raft: became follower for term " UINT64_FORMAT ", leader %d",
		 term, leaderId);
}

/*
 * RaftBecomeCandidate
 *		Start a new election: increment term, vote for self, reset timer.
 */
void
RaftBecomeCandidate(void)
{
	int			selfId;
	uint64		newTerm;
	int			i;

	SpinLockAcquire(&RaftCtl->mutex);
	RaftCtl->currentTerm++;
	newTerm = RaftCtl->currentTerm;
	selfId = RaftCtl->selfNodeId;
	RaftCtl->votedFor = selfId;
	RaftCtl->role = RAFT_ROLE_CANDIDATE;
	RaftCtl->leaderId = -1;

	/* Reset vote tracking */
	for (i = 0; i < RAFT_MAX_NODES; i++)
		RaftCtl->peers[i].voteGranted = false;

	/* Vote for ourselves */
	if (selfId >= 0 && selfId < RAFT_MAX_NODES)
		RaftCtl->peers[selfId].voteGranted = true;

	SpinLockRelease(&RaftCtl->mutex);

	RaftResetElectionTimer();

	elog(LOG, "raft: became candidate for term " UINT64_FORMAT, newTerm);

	/* Send RequestVote RPCs to all other nodes */
	for (i = 0; i < RaftCtl->numNodes; i++)
	{
		if (RaftCtl->nodes[i].node_id != selfId)
			RaftSendRequestVote(RaftCtl->nodes[i].node_id);
	}
}

/*
 * RaftBecomeLeader
 *		Transition to leader after winning an election.
 *
 * Initialise nextIndex and matchIndex for every peer per the Raft paper.
 */
void
RaftBecomeLeader(void)
{
	XLogRecPtr	lastLSN;
	int			i;
	uint64		term;

	lastLSN = RaftLogGetLastLSN();

	SpinLockAcquire(&RaftCtl->mutex);
	RaftCtl->role = RAFT_ROLE_LEADER;
	RaftCtl->leaderId = RaftCtl->selfNodeId;
	term = RaftCtl->currentTerm;

	for (i = 0; i < RAFT_MAX_NODES; i++)
	{
		/* nextIndex initialised to leader's last log index + 1 */
		RaftCtl->peers[i].nextIndex = lastLSN;
		RaftCtl->peers[i].matchIndex = InvalidXLogRecPtr;
	}
	SpinLockRelease(&RaftCtl->mutex);

	elog(LOG, "raft: became leader for term " UINT64_FORMAT, term);

	/* Immediately send heartbeats to establish authority */
	RaftBroadcastHeartbeat();
}

/*
 * RaftIsLeader
 *		Quick check whether this node is currently the Raft leader.
 */
bool
RaftIsLeader(void)
{
	bool		result;

	SpinLockAcquire(&RaftCtl->mutex);
	result = (RaftCtl->role == RAFT_ROLE_LEADER);
	SpinLockRelease(&RaftCtl->mutex);

	return result;
}

/*
 * RaftQuorumSize
 *		Return the minimum number of nodes needed for a majority.
 */
int
RaftQuorumSize(void)
{
	return (RaftCtl->numNodes / 2) + 1;
}

/*
 * RaftAdvanceCommitIndex
 *		Called on the leader to check whether the commit index can be
 *		advanced.  A log entry is committed when it has been replicated
 *		to a majority of servers and its term matches the current term
 *		(per section 5.4.2 of the Raft paper).
 */
void
RaftAdvanceCommitIndex(void)
{
	XLogRecPtr	matchLSNs[RAFT_MAX_NODES];
	int			npeers = 0;
	int			i;
	int			quorum;
	XLogRecPtr	newCommit;

	if (!RaftIsLeader())
		return;

	/* Gather match indices from all peers plus ourselves */
	SpinLockAcquire(&RaftCtl->mutex);
	for (i = 0; i < RaftCtl->numNodes; i++)
	{
		int			nid = RaftCtl->nodes[i].node_id;

		if (nid == RaftCtl->selfNodeId)
		{
			/* Leader has all local WAL flushed */
			matchLSNs[npeers++] = RaftLogGetLastLSN();
		}
		else if (nid >= 0 && nid < RAFT_MAX_NODES)
		{
			matchLSNs[npeers++] = RaftCtl->peers[nid].matchIndex;
		}
	}
	SpinLockRelease(&RaftCtl->mutex);

	if (npeers == 0)
		return;

	/* Sort LSNs in descending order */
	for (i = 0; i < npeers - 1; i++)
	{
		int			j;

		for (j = i + 1; j < npeers; j++)
		{
			if (matchLSNs[j] > matchLSNs[i])
			{
				XLogRecPtr	tmp = matchLSNs[i];

				matchLSNs[i] = matchLSNs[j];
				matchLSNs[j] = tmp;
			}
		}
	}

	/* The quorum-th largest value is the new commit point */
	quorum = RaftQuorumSize();
	if (quorum > npeers)
		return;

	newCommit = matchLSNs[quorum - 1];

	/* Only advance if the entry's term matches our current term */
	SpinLockAcquire(&RaftCtl->mutex);
	if (newCommit > RaftCtl->commitIndex)
	{
		uint64		entryTerm = RaftLogGetTermForLSN(newCommit);

		if (entryTerm == RaftCtl->currentTerm)
		{
			RaftCtl->commitIndex = newCommit;
			SpinLockRelease(&RaftCtl->mutex);

			/* Wake up any backends waiting for commit confirmation */
			ConditionVariableBroadcast(&RaftCtl->commitCV);

			elog(DEBUG2, "raft: commitIndex advanced to %X/%X",
				 LSN_FORMAT_ARGS(newCommit));
			return;
		}
	}
	SpinLockRelease(&RaftCtl->mutex);
}

/*
 * RaftResetElectionTimer
 *		Randomise the election timeout and record the current time.
 *
 * The timeout is uniformly distributed in [T, 2T] where T is
 * raft_election_timeout_ms, following the Raft paper's recommendation.
 */
void
RaftResetElectionTimer(void)
{
	int			jitter;

	/* pg_prng_uint32 is not available in all contexts; use random() */
	jitter = (int) (random() % (raft_election_timeout_ms + 1));

	SpinLockAcquire(&RaftCtl->mutex);
	RaftCtl->electionTimeoutMs = raft_election_timeout_ms + jitter;
	RaftCtl->lastHeartbeat = GetCurrentTimestamp();
	SpinLockRelease(&RaftCtl->mutex);
}

/*
 * RaftElectionTimedOut
 *		Return true if the election timer has expired.
 */
bool
RaftElectionTimedOut(void)
{
	TimestampTz now = GetCurrentTimestamp();
	TimestampTz lastHB;
	int			timeoutMs;
	long		secs;
	int			usecs;

	SpinLockAcquire(&RaftCtl->mutex);
	lastHB = RaftCtl->lastHeartbeat;
	timeoutMs = RaftCtl->electionTimeoutMs;
	SpinLockRelease(&RaftCtl->mutex);

	if (lastHB == 0)
		return false;

	TimestampDifference(lastHB, now, &secs, &usecs);

	return (secs * 1000 + usecs / 1000) >= timeoutMs;
}

/*
 * RaftTick
 *		Periodic driver called from WalSndLoop / WalReceiverMain.
 *
 *		- Followers / candidates: check election timeout and start a
 *		  new election if necessary.
 *		- Leaders: send periodic heartbeats and advance the commit index.
 */
void
RaftTick(void)
{
	RaftRole	role;

	if (replication_mode != REPLICATION_MODE_RAFT)
		return;

	if (!RaftCtl || !RaftCtl->initialized)
		return;

	SpinLockAcquire(&RaftCtl->mutex);
	role = RaftCtl->role;
	SpinLockRelease(&RaftCtl->mutex);

	switch (role)
	{
		case RAFT_ROLE_FOLLOWER:
		case RAFT_ROLE_CANDIDATE:
			if (RaftElectionTimedOut())
				RaftBecomeCandidate();
			break;

		case RAFT_ROLE_LEADER:
			{
				TimestampTz now = GetCurrentTimestamp();
				TimestampTz lastHB;
				long		secs;
				int			usecs;

				SpinLockAcquire(&RaftCtl->mutex);
				lastHB = RaftCtl->lastHeartbeat;
				SpinLockRelease(&RaftCtl->mutex);

				TimestampDifference(lastHB, now, &secs, &usecs);

				if ((secs * 1000 + usecs / 1000) >= raft_heartbeat_interval_ms)
				{
					RaftBroadcastHeartbeat();

					SpinLockAcquire(&RaftCtl->mutex);
					RaftCtl->lastHeartbeat = now;
					SpinLockRelease(&RaftCtl->mutex);
				}

				RaftAdvanceCommitIndex();
			}
			break;
	}
}
