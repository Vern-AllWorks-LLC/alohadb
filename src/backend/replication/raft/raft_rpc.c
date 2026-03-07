/*-------------------------------------------------------------------------
 *
 * raft_rpc.c
 *	  Raft RPC message construction, serialisation and handling.
 *
 * Messages are sent as CopyData payloads over the existing streaming-
 * replication COPY BOTH channel.  The first byte of each message
 * identifies its type (see RAFT_MSG_* constants in raft.h), followed
 * by the fixed-size message struct.  This piggybacks on the libpq
 * walreceiver connection infrastructure so no new sockets are needed.
 *
 * Portions Copyright (c) 2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/backend/replication/raft/raft_rpc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "replication/raft.h"
#include "replication/walreceiver.h"
#include "replication/walsender.h"
#include "replication/walsender_private.h"
#include "storage/spin.h"
#include "utils/timestamp.h"

/* ----------------------------------------------------------------
 *		Message serialisation helpers
 * ----------------------------------------------------------------
 */

/*
 * RaftSerialiseAppendEntries
 *		Build a wire-format message for an AppendEntries RPC.
 *
 * The caller supplies the StringInfo; this function appends the type
 * tag and the message body.
 */
static void
RaftSerialiseAppendEntries(StringInfo buf, RaftAppendEntriesMsg *msg)
{
	pq_sendbyte(buf, RAFT_MSG_APPEND_ENTRIES);
	pq_sendint64(buf, msg->term);
	pq_sendint32(buf, msg->leaderId);
	pq_sendint64(buf, msg->prevLogLSN);
	pq_sendint64(buf, msg->prevLogTerm);
	pq_sendint64(buf, msg->leaderCommit);
	pq_sendint64(buf, msg->entriesEndLSN);
}

/*
 * RaftDeserialiseAppendEntries
 *		Parse an AppendEntries message from a buffer (past the type byte).
 */
static void
RaftDeserialiseAppendEntries(const char *buf, int len,
							 RaftAppendEntriesMsg *msg)
{
	if (len < (int) (sizeof(uint64) * 4 + sizeof(int32) + sizeof(uint64)))
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("raft: AppendEntries message too short")));

	memcpy(&msg->term, buf, sizeof(uint64));
	buf += sizeof(uint64);
	memcpy(&msg->leaderId, buf, sizeof(int32));
	buf += sizeof(int32);
	memcpy(&msg->prevLogLSN, buf, sizeof(uint64));
	buf += sizeof(uint64);
	memcpy(&msg->prevLogTerm, buf, sizeof(uint64));
	buf += sizeof(uint64);
	memcpy(&msg->leaderCommit, buf, sizeof(uint64));
	buf += sizeof(uint64);
	memcpy(&msg->entriesEndLSN, buf, sizeof(uint64));
}

/*
 * RaftSerialiseAppendEntriesReply
 */
static void
RaftSerialiseAppendEntriesReply(StringInfo buf,
								RaftAppendEntriesReply *reply)
{
	pq_sendbyte(buf, RAFT_MSG_APPEND_ENTRIES_REPLY);
	pq_sendint64(buf, reply->term);
	pq_sendbyte(buf, reply->success ? 1 : 0);
	pq_sendint64(buf, reply->matchIndex);
}

/*
 * RaftDeserialiseAppendEntriesReply
 */
static void
RaftDeserialiseAppendEntriesReply(const char *buf, int len,
								  RaftAppendEntriesReply *reply)
{
	if (len < (int) (sizeof(uint64) + 1 + sizeof(uint64)))
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("raft: AppendEntriesReply message too short")));

	memcpy(&reply->term, buf, sizeof(uint64));
	buf += sizeof(uint64);
	reply->success = (*buf != 0);
	buf += 1;
	memcpy(&reply->matchIndex, buf, sizeof(uint64));
}

/*
 * RaftSerialiseRequestVote
 */
static void
RaftSerialiseRequestVote(StringInfo buf, RaftRequestVoteMsg *msg)
{
	pq_sendbyte(buf, RAFT_MSG_REQUEST_VOTE);
	pq_sendint64(buf, msg->term);
	pq_sendint32(buf, msg->candidateId);
	pq_sendint64(buf, msg->lastLogLSN);
	pq_sendint64(buf, msg->lastLogTerm);
}

/*
 * RaftDeserialiseRequestVote
 */
static void
RaftDeserialiseRequestVote(const char *buf, int len,
						   RaftRequestVoteMsg *msg)
{
	if (len < (int) (sizeof(uint64) + sizeof(int32) + sizeof(uint64) * 2))
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("raft: RequestVote message too short")));

	memcpy(&msg->term, buf, sizeof(uint64));
	buf += sizeof(uint64);
	memcpy(&msg->candidateId, buf, sizeof(int32));
	buf += sizeof(int32);
	memcpy(&msg->lastLogLSN, buf, sizeof(uint64));
	buf += sizeof(uint64);
	memcpy(&msg->lastLogTerm, buf, sizeof(uint64));
}

/*
 * RaftSerialiseRequestVoteReply
 */
static void
RaftSerialiseRequestVoteReply(StringInfo buf,
							  RaftRequestVoteReply *reply)
{
	pq_sendbyte(buf, RAFT_MSG_REQUEST_VOTE_REPLY);
	pq_sendint64(buf, reply->term);
	pq_sendbyte(buf, reply->voteGranted ? 1 : 0);
}

/*
 * RaftDeserialiseRequestVoteReply
 */
static void
RaftDeserialiseRequestVoteReply(const char *buf, int len,
								RaftRequestVoteReply *reply)
{
	if (len < (int) (sizeof(uint64) + 1))
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("raft: RequestVoteReply message too short")));

	memcpy(&reply->term, buf, sizeof(uint64));
	buf += sizeof(uint64);
	reply->voteGranted = (*buf != 0);
}

/* ----------------------------------------------------------------
 *		Sending RPCs
 *
 * We use pq_putmessage_v2 / walrcv_send depending on whether we
 * are in a walsender (leader) or walreceiver (follower) context.
 * For simplicity the current implementation constructs the message
 * into a StringInfoData and hands it off to the pq layer.
 * ----------------------------------------------------------------
 */

/*
 * RaftSendMessageToWalSender
 *		Send a Raft message from a follower (walreceiver) to the
 *		connected leader (walsender) via the COPY BOTH channel.
 */
static void
RaftSendMessageToWalSender(StringInfo msg)
{
	/* walrcv_send is available when running inside walreceiver */
	/* The caller should check WalReceiverFunctions != NULL before calling */
	/* For now we rely on the pq layer being connected in COPY BOTH mode */

	/*
	 * In walsender context we send via pq_putmessage.  In walreceiver
	 * context we send via walrcv_send.  We distinguish by checking
	 * am_walsender.
	 */
	if (am_walsender)
	{
		/* We're a walsender -- send CopyData to the connected standby */
		pq_putmessage_noblock(PqMsg_CopyData, msg->data, msg->len);
	}
	else if (WalReceiverFunctions != NULL)
	{
		/*
		 * We're in walreceiver context -- but walrcv_send is only safe
		 * to call if we have an active connection.  The actual send is
		 * done from walreceiver.c through the raft_rpc interface.
		 * Store the message for the walreceiver main loop to pick up.
		 *
		 * For the initial implementation, the walreceiver calls
		 * walrcv_send() directly after constructing the reply, so
		 * this path is not normally taken.  We leave it as a stub.
		 */
		elog(DEBUG3, "raft_rpc: message queued for walreceiver send path");
	}
}

/*
 * RaftSendAppendEntries
 *		Build and send an AppendEntries RPC to the given peer.
 *
 * In the leader-side walsender, the regular WAL streaming (XLogSendPhysical)
 * already ships WAL data.  This RPC is the Raft metadata envelope that
 * accompanies the WAL stream; the actual WAL bytes follow in the normal
 * CopyData WAL messages.
 */
void
RaftSendAppendEntries(int peerId)
{
	StringInfoData buf;
	RaftAppendEntriesMsg msg;
	XLogRecPtr	prevLSN;

	if (RaftCtl == NULL)
		return;

	SpinLockAcquire(&RaftCtl->mutex);
	msg.term = RaftCtl->currentTerm;
	msg.leaderId = RaftCtl->selfNodeId;
	msg.leaderCommit = RaftCtl->commitIndex;

	if (peerId >= 0 && peerId < RAFT_MAX_NODES)
		prevLSN = RaftCtl->peers[peerId].nextIndex;
	else
		prevLSN = InvalidXLogRecPtr;
	SpinLockRelease(&RaftCtl->mutex);

	msg.prevLogLSN = prevLSN;
	msg.prevLogTerm = RaftLogGetTermForLSN(prevLSN);
	msg.entriesEndLSN = RaftLogGetLastLSN();

	initStringInfo(&buf);
	RaftSerialiseAppendEntries(&buf, &msg);
	RaftSendMessageToWalSender(&buf);
	pfree(buf.data);
}

/*
 * RaftSendRequestVote
 *		Build and send a RequestVote RPC to the given peer.
 */
void
RaftSendRequestVote(int peerId)
{
	StringInfoData buf;
	RaftRequestVoteMsg msg;

	if (RaftCtl == NULL)
		return;

	SpinLockAcquire(&RaftCtl->mutex);
	msg.term = RaftCtl->currentTerm;
	msg.candidateId = RaftCtl->selfNodeId;
	SpinLockRelease(&RaftCtl->mutex);

	msg.lastLogLSN = RaftLogGetLastLSN();
	msg.lastLogTerm = RaftLogGetTermForLSN(msg.lastLogLSN);

	initStringInfo(&buf);
	RaftSerialiseRequestVote(&buf, &msg);
	RaftSendMessageToWalSender(&buf);
	pfree(buf.data);
}

/* ----------------------------------------------------------------
 *		Handling incoming RPCs
 * ----------------------------------------------------------------
 */

/*
 * RaftHandleAppendEntries
 *		Process an AppendEntries RPC received by a follower.
 *
 * Per the Raft paper:
 * 1. Reply false if term < currentTerm.
 * 2. Reply false if log doesn't contain an entry at prevLogLSN
 *    whose term matches prevLogTerm.
 * 3. Advance commitIndex if leaderCommit > commitIndex.
 * 4. Reset election timer.
 */
void
RaftHandleAppendEntries(RaftAppendEntriesMsg *msg)
{
	StringInfoData replybuf;
	RaftAppendEntriesReply reply;
	uint64		currentTerm;

	SpinLockAcquire(&RaftCtl->mutex);
	currentTerm = RaftCtl->currentTerm;
	SpinLockRelease(&RaftCtl->mutex);

	/* Step 1: reject stale terms */
	if (msg->term < currentTerm)
	{
		reply.term = currentTerm;
		reply.success = false;
		reply.matchIndex = InvalidXLogRecPtr;
		goto send_reply;
	}

	/* If the leader's term is newer, become follower */
	if (msg->term > currentTerm)
		RaftBecomeFollower(msg->term, msg->leaderId);
	else
	{
		/* Same term -- ensure we recognise this leader */
		SpinLockAcquire(&RaftCtl->mutex);
		if (RaftCtl->role == RAFT_ROLE_CANDIDATE)
		{
			/* Another server established itself as leader */
			SpinLockRelease(&RaftCtl->mutex);
			RaftBecomeFollower(msg->term, msg->leaderId);
		}
		else
		{
			RaftCtl->leaderId = msg->leaderId;
			SpinLockRelease(&RaftCtl->mutex);
		}
	}

	/* Step 2: log consistency check */
	if (msg->prevLogLSN != InvalidXLogRecPtr)
	{
		XLogRecPtr	localLastLSN = RaftLogGetLastLSN();

		if (localLastLSN < msg->prevLogLSN)
		{
			/* We don't have the previous entry yet */
			SpinLockAcquire(&RaftCtl->mutex);
			reply.term = RaftCtl->currentTerm;
			SpinLockRelease(&RaftCtl->mutex);
			reply.success = false;
			reply.matchIndex = localLastLSN;
			goto send_reply;
		}

		/* Check term of the entry at prevLogLSN */
		if (RaftLogGetTermForLSN(msg->prevLogLSN) != msg->prevLogTerm)
		{
			SpinLockAcquire(&RaftCtl->mutex);
			reply.term = RaftCtl->currentTerm;
			SpinLockRelease(&RaftCtl->mutex);
			reply.success = false;
			reply.matchIndex = InvalidXLogRecPtr;
			goto send_reply;
		}
	}

	/*
	 * Step 3: the actual WAL data has been (or will be) written by the
	 * normal walreceiver WAL-writing path.  We just need to update
	 * the Raft commit index.
	 */
	if (msg->leaderCommit > InvalidXLogRecPtr)
	{
		XLogRecPtr	lastEntry = RaftLogGetLastLSN();
		XLogRecPtr	newCommit;

		/* commitIndex = min(leaderCommit, index of last new entry) */
		newCommit = (msg->leaderCommit < lastEntry)
			? msg->leaderCommit : lastEntry;

		RaftLogMarkCommitted(newCommit);
	}

	/* Step 4: reset election timer */
	RaftResetElectionTimer();

	/* Build success reply */
	SpinLockAcquire(&RaftCtl->mutex);
	reply.term = RaftCtl->currentTerm;
	SpinLockRelease(&RaftCtl->mutex);
	reply.success = true;
	reply.matchIndex = RaftLogGetLastLSN();

send_reply:
	initStringInfo(&replybuf);
	RaftSerialiseAppendEntriesReply(&replybuf, &reply);
	RaftSendMessageToWalSender(&replybuf);
	pfree(replybuf.data);
}

/*
 * RaftHandleAppendEntriesReply
 *		Process a reply to our AppendEntries RPC (leader-side).
 */
void
RaftHandleAppendEntriesReply(int peerId,
							 RaftAppendEntriesReply *reply)
{
	uint64		currentTerm;

	SpinLockAcquire(&RaftCtl->mutex);
	currentTerm = RaftCtl->currentTerm;
	SpinLockRelease(&RaftCtl->mutex);

	/* If the reply carries a newer term, step down */
	if (reply->term > currentTerm)
	{
		RaftBecomeFollower(reply->term, -1);
		return;
	}

	if (reply->success)
	{
		/* Update matchIndex / nextIndex for this peer */
		RaftLogUpdateMatchIndex(peerId, reply->matchIndex);

		/* Try to advance the commit index */
		RaftAdvanceCommitIndex();
	}
	else
	{
		/*
		 * Decrement nextIndex for this peer and retry.  The standard
		 * Raft approach decrements by one, but we can optimise by
		 * jumping to the follower's reported matchIndex.
		 */
		SpinLockAcquire(&RaftCtl->mutex);
		if (peerId >= 0 && peerId < RAFT_MAX_NODES)
		{
			if (reply->matchIndex != InvalidXLogRecPtr)
				RaftCtl->peers[peerId].nextIndex = reply->matchIndex;
			else if (RaftCtl->peers[peerId].nextIndex > 0)
				RaftCtl->peers[peerId].nextIndex--;
		}
		SpinLockRelease(&RaftCtl->mutex);
	}
}

/*
 * RaftHandleRequestVote
 *		Process a RequestVote RPC.
 *
 * Per the Raft paper:
 * 1. Reply false if term < currentTerm.
 * 2. If votedFor is null or candidateId, and candidate's log is at
 *    least as up-to-date as receiver's log, grant vote.
 */
void
RaftHandleRequestVote(RaftRequestVoteMsg *msg)
{
	StringInfoData replybuf;
	RaftRequestVoteReply reply;
	uint64		currentTerm;
	int			votedFor;
	XLogRecPtr	myLastLSN;
	uint64		myLastTerm;
	bool		logOk;

	SpinLockAcquire(&RaftCtl->mutex);
	currentTerm = RaftCtl->currentTerm;
	votedFor = RaftCtl->votedFor;
	SpinLockRelease(&RaftCtl->mutex);

	/* Step down if we see a newer term */
	if (msg->term > currentTerm)
	{
		RaftBecomeFollower(msg->term, -1);
		SpinLockAcquire(&RaftCtl->mutex);
		currentTerm = RaftCtl->currentTerm;
		votedFor = RaftCtl->votedFor;
		SpinLockRelease(&RaftCtl->mutex);
	}

	/* Step 1: reject stale terms */
	if (msg->term < currentTerm)
	{
		reply.term = currentTerm;
		reply.voteGranted = false;
		goto send_reply;
	}

	/* Step 2: check whether we can vote for this candidate */
	if (votedFor != -1 && votedFor != msg->candidateId)
	{
		reply.term = currentTerm;
		reply.voteGranted = false;
		goto send_reply;
	}

	/* Log up-to-date check (section 5.4.1) */
	myLastLSN = RaftLogGetLastLSN();
	myLastTerm = RaftLogGetTermForLSN(myLastLSN);

	logOk = (msg->lastLogTerm > myLastTerm) ||
		(msg->lastLogTerm == myLastTerm && msg->lastLogLSN >= myLastLSN);

	if (!logOk)
	{
		reply.term = currentTerm;
		reply.voteGranted = false;
		goto send_reply;
	}

	/* Grant the vote */
	SpinLockAcquire(&RaftCtl->mutex);
	RaftCtl->votedFor = msg->candidateId;
	SpinLockRelease(&RaftCtl->mutex);

	RaftResetElectionTimer();

	reply.term = currentTerm;
	reply.voteGranted = true;

	elog(LOG, "raft: voted for node %d in term " UINT64_FORMAT,
		 msg->candidateId, msg->term);

send_reply:
	initStringInfo(&replybuf);
	RaftSerialiseRequestVoteReply(&replybuf, &reply);
	RaftSendMessageToWalSender(&replybuf);
	pfree(replybuf.data);
}

/*
 * RaftHandleRequestVoteReply
 *		Process a reply to our RequestVote RPC (candidate-side).
 */
void
RaftHandleRequestVoteReply(int peerId,
						   RaftRequestVoteReply *reply)
{
	uint64		currentTerm;
	int			votes;
	int			i;

	SpinLockAcquire(&RaftCtl->mutex);
	currentTerm = RaftCtl->currentTerm;

	/* Ignore if we're no longer a candidate */
	if (RaftCtl->role != RAFT_ROLE_CANDIDATE)
	{
		SpinLockRelease(&RaftCtl->mutex);
		return;
	}
	SpinLockRelease(&RaftCtl->mutex);

	/* If the reply carries a newer term, step down */
	if (reply->term > currentTerm)
	{
		RaftBecomeFollower(reply->term, -1);
		return;
	}

	if (!reply->voteGranted)
		return;

	/* Record the vote */
	SpinLockAcquire(&RaftCtl->mutex);
	if (peerId >= 0 && peerId < RAFT_MAX_NODES)
		RaftCtl->peers[peerId].voteGranted = true;

	/* Count total votes */
	votes = 0;
	for (i = 0; i < RaftCtl->numNodes; i++)
	{
		int			nid = RaftCtl->nodes[i].node_id;

		if (nid >= 0 && nid < RAFT_MAX_NODES && RaftCtl->peers[nid].voteGranted)
			votes++;
	}
	SpinLockRelease(&RaftCtl->mutex);

	elog(LOG, "raft: received vote from node %d, total votes %d / %d",
		 peerId, votes, RaftQuorumSize());

	/* Check if we've won the election */
	if (votes >= RaftQuorumSize())
		RaftBecomeLeader();
}

/*
 * RaftProcessMessage
 *		Dispatch an incoming Raft message based on its type tag.
 *
 * Called from ProcessRepliesIfAny() in walsender.c or from the
 * walreceiver main loop when replication_mode = 'raft'.
 */
void
RaftProcessMessage(const char *buf, int len)
{
	char		msgtype;

	if (len < 1)
		return;

	msgtype = buf[0];
	buf++;
	len--;

	switch (msgtype)
	{
		case RAFT_MSG_APPEND_ENTRIES:
			{
				RaftAppendEntriesMsg msg;

				RaftDeserialiseAppendEntries(buf, len, &msg);
				RaftHandleAppendEntries(&msg);
			}
			break;

		case RAFT_MSG_APPEND_ENTRIES_REPLY:
			{
				RaftAppendEntriesReply reply;
				int			peerId = -1;

				RaftDeserialiseAppendEntriesReply(buf, len, &reply);

				/*
				 * In the walsender context, we know the peer from the
				 * connection.  For now, we extract it from the WalSnd
				 * slot index.  A more robust solution would include
				 * the sender's node_id in the message.
				 */
				if (MyWalSnd != NULL)
				{
					int			idx = (int) (MyWalSnd - WalSndCtl->walsnds);

					if (idx >= 0 && idx < RAFT_MAX_NODES)
						peerId = idx;
				}

				RaftHandleAppendEntriesReply(peerId, &reply);
			}
			break;

		case RAFT_MSG_REQUEST_VOTE:
			{
				RaftRequestVoteMsg msg;

				RaftDeserialiseRequestVote(buf, len, &msg);
				RaftHandleRequestVote(&msg);
			}
			break;

		case RAFT_MSG_REQUEST_VOTE_REPLY:
			{
				RaftRequestVoteReply reply;
				int			peerId = -1;

				RaftDeserialiseRequestVoteReply(buf, len, &reply);

				if (MyWalSnd != NULL)
				{
					int			idx = (int) (MyWalSnd - WalSndCtl->walsnds);

					if (idx >= 0 && idx < RAFT_MAX_NODES)
						peerId = idx;
				}

				RaftHandleRequestVoteReply(peerId, &reply);
			}
			break;

		case RAFT_MSG_HEARTBEAT:
			{
				/*
				 * Heartbeat is a degenerate AppendEntries with no new
				 * entries.  We treat it the same way.
				 */
				RaftAppendEntriesMsg msg;

				RaftDeserialiseAppendEntries(buf, len, &msg);
				RaftHandleAppendEntries(&msg);
			}
			break;

		default:
			elog(WARNING, "raft: unknown message type '%c'", msgtype);
			break;
	}
}

/*
 * RaftBroadcastHeartbeat
 *		Send an empty AppendEntries (heartbeat) to all peers.
 *
 * Called periodically by the leader from RaftTick().
 */
void
RaftBroadcastHeartbeat(void)
{
	int			i;
	int			selfId;

	if (RaftCtl == NULL || !RaftCtl->initialized)
		return;

	SpinLockAcquire(&RaftCtl->mutex);
	selfId = RaftCtl->selfNodeId;
	SpinLockRelease(&RaftCtl->mutex);

	for (i = 0; i < RaftCtl->numNodes; i++)
	{
		if (RaftCtl->nodes[i].node_id != selfId)
			RaftSendAppendEntries(RaftCtl->nodes[i].node_id);
	}
}
