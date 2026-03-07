/*-------------------------------------------------------------------------
 *
 * raft_config.c
 *	  Raft cluster membership configuration.
 *
 * Parses the raft_members GUC (a comma-separated list of host:port
 * pairs) and populates the shared-memory node configuration.  Also
 * provides GUC check/assign hooks and utility functions to resolve
 * the local node's identity.
 *
 * Portions Copyright (c) 2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/backend/replication/raft/raft_config.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>
#include <netdb.h>
#include <unistd.h>

#include "libpq/libpq.h"
#include "miscadmin.h"
#include "postmaster/postmaster.h"
#include "replication/raft.h"
#include "storage/spin.h"
#include "utils/guc.h"

/*
 * parse_one_member
 *		Parse a single "host:port" (or "[host]:port" for IPv6) token.
 *
 * On success, fills hostbuf (up to hostbuflen) and *port_out, returning true.
 * On failure, returns false.
 */
static bool
parse_one_member(const char *token, char *hostbuf, size_t hostbuflen,
				 int *port_out)
{
	const char *p = token;
	const char *host_start;
	size_t		host_len;
	const char *portstr = NULL;

	/* Skip leading whitespace */
	while (*p && isspace((unsigned char) *p))
		p++;

	if (*p == '\0')
		return false;

	if (*p == '[')
	{
		/* IPv6 bracketed notation: [addr]:port */
		const char *bracket_end;

		host_start = p + 1;
		bracket_end = strchr(host_start, ']');
		if (bracket_end == NULL)
			return false;

		host_len = bracket_end - host_start;
		p = bracket_end + 1;

		if (*p == ':')
			portstr = p + 1;
	}
	else
	{
		/* Regular hostname or IPv4: host:port */
		const char *lastcolon;

		host_start = p;
		lastcolon = strrchr(p, ':');
		if (lastcolon != NULL)
		{
			host_len = lastcolon - host_start;
			portstr = lastcolon + 1;
		}
		else
		{
			host_len = strlen(host_start);

			/* Trim trailing whitespace from host */
			while (host_len > 0 &&
				   isspace((unsigned char) host_start[host_len - 1]))
				host_len--;
		}
	}

	if (host_len == 0)
		return false;

	if (host_len >= hostbuflen)
		host_len = hostbuflen - 1;

	memcpy(hostbuf, host_start, host_len);
	hostbuf[host_len] = '\0';

	/* Trim trailing whitespace from host */
	{
		int			i = (int) host_len - 1;

		while (i >= 0 && isspace((unsigned char) hostbuf[i]))
			hostbuf[i--] = '\0';
	}

	if (portstr != NULL && *portstr != '\0')
	{
		int			port = atoi(portstr);

		if (port <= 0 || port > 65535)
			port = 5432;
		*port_out = port;
	}
	else
	{
		*port_out = 5432;		/* default PostgreSQL port */
	}

	return true;
}

/*
 * RaftParseMembers
 *		Parse a comma-separated list of "host:port" strings and populate
 *		the shared-memory node configuration array.
 *
 * Node IDs are assigned sequentially starting from 0.
 *
 * Example input: "node1:5432,node2:5432,node3:5432"
 */
void
RaftParseMembers(const char *memberstr)
{
	char	   *rawstr;
	char	   *token;
	char	   *saveptr;
	int			idx = 0;

	if (RaftCtl == NULL)
		return;

	if (memberstr == NULL || memberstr[0] == '\0')
	{
		SpinLockAcquire(&RaftCtl->mutex);
		RaftCtl->numNodes = 0;
		SpinLockRelease(&RaftCtl->mutex);
		return;
	}

	/* Reset node count before repopulating */
	SpinLockAcquire(&RaftCtl->mutex);
	RaftCtl->numNodes = 0;
	SpinLockRelease(&RaftCtl->mutex);

	/* Work on a mutable copy */
	rawstr = pstrdup(memberstr);

	token = strtok_r(rawstr, ",", &saveptr);
	while (token != NULL && idx < RAFT_MAX_NODES)
	{
		char		hostbuf[RAFT_MAX_HOST_LEN];
		int			port;

		if (parse_one_member(token, hostbuf, sizeof(hostbuf), &port))
		{
			SpinLockAcquire(&RaftCtl->mutex);
			RaftCtl->nodes[idx].node_id = idx;
			strlcpy(RaftCtl->nodes[idx].host, hostbuf, RAFT_MAX_HOST_LEN);
			RaftCtl->nodes[idx].port = port;
			RaftCtl->numNodes = idx + 1;
			SpinLockRelease(&RaftCtl->mutex);

			elog(LOG, "raft_config: node %d = %s:%d", idx, hostbuf, port);
			idx++;
		}
		else
		{
			elog(WARNING, "raft_config: skipping malformed member entry: %s",
				 token);
		}

		token = strtok_r(NULL, ",", &saveptr);
	}

	pfree(rawstr);
}

/*
 * RaftCheckMembersGUC
 *		GUC check_hook for raft_members.
 *
 * Validates that the string is parseable as a list of host:port pairs.
 */
bool
RaftCheckMembersGUC(char **newval, void **extra, GucSource source)
{
	char	   *rawstr;
	char	   *token;
	char	   *saveptr;
	int			count = 0;

	if (*newval == NULL || (*newval)[0] == '\0')
		return true;			/* empty is allowed */

	rawstr = pstrdup(*newval);
	token = strtok_r(rawstr, ",", &saveptr);
	while (token != NULL)
	{
		char		hostbuf[RAFT_MAX_HOST_LEN];
		int			port;

		/* Only count non-empty, well-formed tokens */
		if (parse_one_member(token, hostbuf, sizeof(hostbuf), &port))
			count++;

		if (count > RAFT_MAX_NODES)
		{
			GUC_check_errdetail("Too many Raft members (max %d).",
								RAFT_MAX_NODES);
			pfree(rawstr);
			return false;
		}

		token = strtok_r(NULL, ",", &saveptr);
	}

	pfree(rawstr);
	return true;
}

/*
 * RaftAssignMembersGUC
 *		GUC assign_hook for raft_members.
 *
 * Re-parses the membership list and updates shared memory.
 */
void
RaftAssignMembersGUC(const char *newval, void *extra)
{
	if (RaftCtl == NULL)
		return;

	if (newval != NULL && newval[0] != '\0')
		RaftParseMembers(newval);
}

/*
 * RaftGetNodeId
 *		Determine this node's node_id by matching the local hostname
 *		and port against the configured membership list.
 *
 * Returns the matching node_id, or -1 if no match is found.
 */
int
RaftGetNodeId(void)
{
	char		hostname[RAFT_MAX_HOST_LEN];
	int			myport;
	int			i;

	if (RaftCtl == NULL)
		return -1;

	/* Get our hostname */
	if (gethostname(hostname, sizeof(hostname)) != 0)
		strlcpy(hostname, "localhost", sizeof(hostname));

	/* Get our port from the PostPortNumber global */
	myport = PostPortNumber;

	SpinLockAcquire(&RaftCtl->mutex);
	for (i = 0; i < RaftCtl->numNodes; i++)
	{
		if (RaftCtl->nodes[i].port == myport)
		{
			/*
			 * Compare hostnames.  Accept "localhost", "127.0.0.1", or
			 * an exact match.
			 */
			if (strcmp(RaftCtl->nodes[i].host, hostname) == 0 ||
				strcmp(RaftCtl->nodes[i].host, "localhost") == 0 ||
				strcmp(RaftCtl->nodes[i].host, "127.0.0.1") == 0 ||
				strcmp(RaftCtl->nodes[i].host, "::1") == 0)
			{
				int			nid = RaftCtl->nodes[i].node_id;

				SpinLockRelease(&RaftCtl->mutex);
				return nid;
			}
		}
	}
	SpinLockRelease(&RaftCtl->mutex);

	/*
	 * If we couldn't match, default to node 0 and log a warning.
	 * This allows single-node testing.
	 */
	elog(WARNING, "raft_config: could not determine node_id for %s:%d, "
		 "defaulting to 0",
		 hostname, myport);
	return 0;
}

/*
 * RaftGetNodeConfig
 *		Return a pointer to the node configuration for the given node_id.
 *
 * Returns NULL if the node_id is not found.
 */
const RaftNodeConfig *
RaftGetNodeConfig(int nodeId)
{
	int			i;

	if (RaftCtl == NULL)
		return NULL;

	SpinLockAcquire(&RaftCtl->mutex);
	for (i = 0; i < RaftCtl->numNodes; i++)
	{
		if (RaftCtl->nodes[i].node_id == nodeId)
		{
			const RaftNodeConfig *cfg = &RaftCtl->nodes[i];

			SpinLockRelease(&RaftCtl->mutex);
			return cfg;
		}
	}
	SpinLockRelease(&RaftCtl->mutex);

	return NULL;
}
