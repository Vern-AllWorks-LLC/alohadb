/*-------------------------------------------------------------------------
 *
 * raft.h
 *	  Raft consensus protocol for synchronous WAL replication.
 *
 * This module implements Raft-based synchronous replication on top of
 * PostgreSQL's existing WAL infrastructure.  WAL records serve as the
 * Raft log entries; commitment requires a majority of cluster members
 * to have flushed a given LSN before the leader acknowledges the write.
 *
 * Portions Copyright (c) 2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/replication/raft.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _RAFT_H
#define _RAFT_H

#include "access/xlogdefs.h"
#include "storage/condition_variable.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/guc.h"
#include "utils/timestamp.h"

/*
 * Maximum number of nodes in a Raft cluster.
 * This sets an upper bound on the shared-memory array dimensions.
 */
#define RAFT_MAX_NODES			7

/*
 * Maximum length of a host string in a Raft node configuration.
 */
#define RAFT_MAX_HOST_LEN		256

/*
 * Raft RPC message types, sent as the first byte in a CopyData message
 * when replication_mode = 'raft'.
 */
#define RAFT_MSG_APPEND_ENTRIES		'A'
#define RAFT_MSG_APPEND_ENTRIES_REPLY	'a'
#define RAFT_MSG_REQUEST_VOTE		'V'
#define RAFT_MSG_REQUEST_VOTE_REPLY	'v'
#define RAFT_MSG_HEARTBEAT			'H'

/* ----------------------------------------------------------------
 *		Raft role enumeration
 * ----------------------------------------------------------------
 */
typedef enum RaftRole
{
	RAFT_ROLE_FOLLOWER = 0,
	RAFT_ROLE_CANDIDATE,
	RAFT_ROLE_LEADER
} RaftRole;

/* ----------------------------------------------------------------
 *		Replication mode enumeration (for the GUC)
 * ----------------------------------------------------------------
 */
typedef enum ReplicationMode
{
	REPLICATION_MODE_STREAMING = 0,
	REPLICATION_MODE_RAFT
} ReplicationMode;

/* ----------------------------------------------------------------
 *		Per-node configuration
 * ----------------------------------------------------------------
 */
typedef struct RaftNodeConfig
{
	int			node_id;					/* unique node identifier (1-based) */
	char		host[RAFT_MAX_HOST_LEN];	/* hostname or IP */
	int			port;						/* port number */
} RaftNodeConfig;

/* ----------------------------------------------------------------
 *		Per-peer tracking (leader-side)
 *
 * The leader maintains one of these for each peer.  nextIndex and
 * matchIndex follow the Raft paper exactly.
 * ----------------------------------------------------------------
 */
typedef struct RaftPeerState
{
	XLogRecPtr	nextIndex;		/* next log entry to send to this peer */
	XLogRecPtr	matchIndex;		/* highest LSN known replicated on peer */
	bool		voteGranted;	/* did this peer grant us its vote? */
} RaftPeerState;

/* ----------------------------------------------------------------
 *		Raft shared-memory state
 *
 * One instance in shared memory, protected by RaftLock (an LWLock)
 * and the embedded spinlock for hot-path fields.
 * ----------------------------------------------------------------
 */
typedef struct RaftState
{
	/* ------- persistent state (on all servers) ------- */
	uint64		currentTerm;		/* latest term this server has seen */
	int			votedFor;			/* node_id voted for in current term,
									 * or -1 if none */

	/* ------- volatile state (on all servers) ------- */
	XLogRecPtr	commitIndex;		/* highest LSN known to be committed */
	XLogRecPtr	lastApplied;		/* highest LSN applied to state machine */
	RaftRole	role;				/* current role */
	int			leaderId;			/* node_id of current leader, or -1 */

	/* ------- volatile state (on leaders) ------- */
	RaftPeerState peers[RAFT_MAX_NODES];

	/* ------- cluster membership ------- */
	int			selfNodeId;			/* this node's id */
	int			numNodes;			/* number of nodes in cluster */
	RaftNodeConfig nodes[RAFT_MAX_NODES];

	/* ------- election timer ------- */
	TimestampTz lastHeartbeat;		/* last time we received a heartbeat or
									 * granted a vote */
	int			electionTimeoutMs;	/* randomised election timeout */

	/* ------- synchronisation primitives ------- */
	slock_t		mutex;				/* protects hot-path fields */
	ConditionVariable commitCV;		/* signalled when commitIndex advances */

	/* ------- state flags ------- */
	bool		initialized;		/* set after first-time init */
} RaftState;

/* ----------------------------------------------------------------
 *		Global pointer (set during shmem init)
 * ----------------------------------------------------------------
 */
extern PGDLLIMPORT RaftState *RaftCtl;

/* ----------------------------------------------------------------
 *		GUC variables
 * ----------------------------------------------------------------
 */
extern PGDLLIMPORT int replication_mode;
extern PGDLLIMPORT char *raft_members;
extern PGDLLIMPORT int raft_election_timeout_ms;
extern PGDLLIMPORT int raft_heartbeat_interval_ms;

/* ----------------------------------------------------------------
 *		raft.c  --  core state machine
 * ----------------------------------------------------------------
 */
extern Size RaftShmemSize(void);
extern void RaftShmemInit(void);
extern void RaftInit(void);
extern void RaftBecomeFollower(uint64 term, int leaderId);
extern void RaftBecomeCandidate(void);
extern void RaftBecomeLeader(void);
extern bool RaftIsLeader(void);
extern void RaftAdvanceCommitIndex(void);
extern int	RaftQuorumSize(void);
extern void RaftResetElectionTimer(void);
extern bool RaftElectionTimedOut(void);
extern void RaftTick(void);

/* ----------------------------------------------------------------
 *		raft_log.c  --  Raft log backed by WAL
 * ----------------------------------------------------------------
 */
extern XLogRecPtr RaftLogGetLastLSN(void);
extern uint64 RaftLogGetTermForLSN(XLogRecPtr lsn);
extern void RaftLogRecordTermStart(XLogRecPtr lsn, uint64 term);
extern bool RaftLogIsCommitted(XLogRecPtr lsn);
extern void RaftLogMarkCommitted(XLogRecPtr lsn);
extern void RaftLogUpdateMatchIndex(int peerId, XLogRecPtr lsn);
extern XLogRecPtr RaftLogGetMatchIndex(int peerId);

/* ----------------------------------------------------------------
 *		raft_rpc.c  --  Raft RPC message handling
 * ----------------------------------------------------------------
 */

/*
 * AppendEntries RPC (leader -> follower)
 */
typedef struct RaftAppendEntriesMsg
{
	uint64		term;			/* leader's term */
	int			leaderId;		/* so follower can redirect clients */
	XLogRecPtr	prevLogLSN;		/* LSN of log entry immediately preceding
								 * new ones */
	uint64		prevLogTerm;	/* term of prevLogLSN entry */
	XLogRecPtr	leaderCommit;	/* leader's commitIndex */
	XLogRecPtr	entriesEndLSN;	/* end LSN of WAL data included */
} RaftAppendEntriesMsg;

/*
 * AppendEntries RPC reply (follower -> leader)
 */
typedef struct RaftAppendEntriesReply
{
	uint64		term;			/* follower's currentTerm */
	bool		success;		/* true if follower contained entry matching
								 * prevLogLSN and prevLogTerm */
	XLogRecPtr	matchIndex;		/* highest LSN stored by follower */
} RaftAppendEntriesReply;

/*
 * RequestVote RPC (candidate -> all)
 */
typedef struct RaftRequestVoteMsg
{
	uint64		term;			/* candidate's term */
	int			candidateId;	/* candidate requesting vote */
	XLogRecPtr	lastLogLSN;		/* LSN of candidate's last log entry */
	uint64		lastLogTerm;	/* term of candidate's last log entry */
} RaftRequestVoteMsg;

/*
 * RequestVote RPC reply
 */
typedef struct RaftRequestVoteReply
{
	uint64		term;			/* voter's currentTerm */
	bool		voteGranted;	/* true if candidate received vote */
} RaftRequestVoteReply;

extern void RaftSendAppendEntries(int peerId);
extern void RaftSendRequestVote(int peerId);
extern void RaftHandleAppendEntries(RaftAppendEntriesMsg *msg);
extern void RaftHandleAppendEntriesReply(int peerId,
										 RaftAppendEntriesReply *reply);
extern void RaftHandleRequestVote(RaftRequestVoteMsg *msg);
extern void RaftHandleRequestVoteReply(int peerId,
									   RaftRequestVoteReply *reply);
extern void RaftProcessMessage(const char *buf, int len);
extern void RaftBroadcastHeartbeat(void);

/* ----------------------------------------------------------------
 *		raft_config.c  --  cluster membership
 * ----------------------------------------------------------------
 */
extern void RaftParseMembers(const char *memberstr);
extern bool RaftCheckMembersGUC(char **newval, void **extra, GucSource source);
extern void RaftAssignMembersGUC(const char *newval, void *extra);
extern int	RaftGetNodeId(void);
extern const RaftNodeConfig *RaftGetNodeConfig(int nodeId);

#endif							/* _RAFT_H */
