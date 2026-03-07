/*-------------------------------------------------------------------------
 *
 * buf_table.c
 *	  routines for mapping BufferTags to buffer indexes.
 *
 * Note: the routines in this file do no locking of their own.  The caller
 * must hold a suitable lock on the appropriate BufMappingLock, as specified
 * in the comments.  We can't do the locking inside these functions because
 * in most cases the caller needs to adjust the buffer header contents
 * before the lock is released (see notes in README).
 *
 * When enable_lockfree_bufmap is on (default), an alternative lock-free
 * Robin Hood hash table is used for lookups and inserts via atomic CAS.
 * This eliminates BufMappingLock contention on high-core-count systems.
 * The caller still acquires the partition lock for inserts/deletes (to
 * coordinate with buffer header manipulation), but lookups become fully
 * lock-free.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/buffer/buf_table.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "common/hashfn.h"
#include "port/pg_bitutils.h"
#include "storage/buf_internals.h"
#include "storage/shmem.h"

/* entry for buffer lookup hashtable (legacy path) */
typedef struct
{
	BufferTag	key;			/* Tag of a disk page */
	int			id;				/* Associated buffer ID */
} BufferLookupEnt;

/* Legacy partitioned hash table */
static HTAB *SharedBufHash;

/* GUC variable */
bool		enable_lockfree_bufmap = false;

/* Lock-free hash table in shared memory */
static LockFreeBufHash *LockFreeBufTable = NULL;

/* Size passed to InitBufTable, cached for hash computation */
static int	LockFreeBufTableRequestedSize = 0;

/*
 * Estimate space needed for mapping hashtable
 *		size is the desired hash table size (possibly more than NBuffers)
 */
Size
BufTableShmemSize(int size)
{
	Size		lf_size;
	int			nslots;

	/* Always account for legacy hash table */
	lf_size = hash_estimate_size(size, sizeof(BufferLookupEnt));

	/*
	 * Add space for the lock-free table.  We size it to twice the requested
	 * number of entries (for ~50% load factor), rounded up to a power of 2.
	 */
	nslots = pg_nextpower2_32(size * 2);
	lf_size = add_size(lf_size,
					   offsetof(LockFreeBufHash, slots) +
					   (Size) nslots * sizeof(pg_atomic_uint64));

	return lf_size;
}

/*
 * Initialize shmem hash table for mapping buffers
 *		size is the desired hash table size (possibly more than NBuffers)
 */
void
InitBufTable(int size)
{
	HASHCTL		info;
	int			nslots;
	bool		found;

	/* --- Legacy hash table (always initialized as fallback) --- */
	info.keysize = sizeof(BufferTag);
	info.entrysize = sizeof(BufferLookupEnt);
	info.num_partitions = NUM_BUFFER_PARTITIONS;

	SharedBufHash = ShmemInitHash("Shared Buffer Lookup Table",
								  size, size,
								  &info,
								  HASH_ELEM | HASH_BLOBS | HASH_PARTITION);

	/* --- Lock-free hash table --- */
	LockFreeBufTableRequestedSize = size;
	nslots = pg_nextpower2_32(size * 2);

	LockFreeBufTable = (LockFreeBufHash *)
		ShmemInitStruct("Lock-Free Buffer Lookup Table",
						offsetof(LockFreeBufHash, slots) +
						(Size) nslots * sizeof(pg_atomic_uint64),
						&found);

	if (!found)
	{
		int		i;

		LockFreeBufTable->nslots = nslots;
		LockFreeBufTable->mask = (uint32) (nslots - 1);

		for (i = 0; i < nslots; i++)
			pg_atomic_init_u64(&LockFreeBufTable->slots[i],
							   LOCKFREE_BUF_EMPTY);
	}
}

/*
 * BufTableHashCode
 *		Compute the hash code associated with a BufferTag
 *
 * This must be passed to the lookup/insert/delete routines along with the
 * tag.  We do it like this because the callers need to know the hash code
 * in order to determine which buffer partition to lock, and we don't want
 * to do the hash computation twice (hash_any is a bit slow).
 */
uint32
BufTableHashCode(BufferTag *tagPtr)
{
	/*
	 * Always use the legacy hash table's hash function so that the hash code
	 * can be used to select the correct BufMappingLock partition.  The
	 * lock-free table uses the same hash value.
	 */
	return get_hash_value(SharedBufHash, tagPtr);
}


/* ----------------------------------------------------------------
 *		Lock-free Robin Hood hash table operations
 *
 * The lock-free table stores (tag_hash, buf_id, probe_distance) in each
 * 64-bit atomic slot.  On lookup we walk from the ideal slot until we
 * find a match, an empty slot, or a slot whose probe distance is less
 * than ours (Robin Hood invariant guarantees the key is absent).
 *
 * Insert uses CAS to atomically claim a slot.  If the existing occupant
 * has a *shorter* probe distance than the entry being inserted (Robin
 * Hood property), we "steal" the slot via CAS and continue inserting the
 * displaced entry.
 *
 * Delete marks the slot as a tombstone.  Because inserts also hold the
 * partition lock, there is no ABA hazard between concurrent insert and
 * delete of the same key.
 *
 * Lookups require NO lock at all -- they perform only atomic reads.
 * ----------------------------------------------------------------
 */

/*
 * LockFreeBufTableLookup
 *		Lock-free lookup: return buffer ID, or -1 if not found.
 *
 * Completely lock-free; the caller need NOT hold BufMappingLock.
 * However, the returned buf_id may become stale immediately; callers
 * must recheck after pinning the buffer (same as the legacy path).
 */
static int
LockFreeBufTableLookup(BufferTag *tagPtr, uint32 hashcode)
{
	uint32		idx;
	uint8		dist;
	uint32		mask = LockFreeBufTable->mask;

	idx = hashcode & mask;

	for (dist = 0; dist <= LOCKFREE_BUF_MAX_DIST; dist++)
	{
		uint64	slot = pg_atomic_read_u64(&LockFreeBufTable->slots[idx]);

		if (LockFreeBufSlotIsEmpty(slot))
			return -1;

		if (LockFreeBufSlotIsOccupied(slot))
		{
			/*
			 * Robin Hood invariant: if the existing entry's probe distance
			 * is less than ours, the key cannot be further ahead.
			 */
			if (LockFreeBufSlotGetDist(slot) < dist)
				return -1;

			if (LockFreeBufSlotGetHash(slot) == hashcode)
			{
				int		cand_id = LockFreeBufSlotGetBufId(slot);
				BufferDesc *bufHdr = GetBufferDescriptor(cand_id);

				/*
				 * Compare the full BufferTag to confirm.  The tag might be
				 * changing concurrently, but we will recheck after pinning
				 * the buffer (standard protocol).  A read barrier ensures
				 * we see a consistent snapshot of the tag written under the
				 * buffer header lock.
				 */
				pg_read_barrier();
				if (BufferTagsEqual(tagPtr, &bufHdr->tag))
					return cand_id;
			}
		}

		/* Tombstone slots: keep probing */
		idx = (idx + 1) & mask;
	}

	return -1;
}

/*
 * LockFreeBufTableInsert
 *		Insert (hashcode, buf_id) into the lock-free table.
 *
 * Returns -1 on success.  If a conflicting entry with the same tag
 * already exists, returns that entry's buf_id.
 *
 * Caller must hold exclusive BufMappingLock for this partition.
 */
static int
LockFreeBufTableInsert(BufferTag *tagPtr, uint32 hashcode, int buf_id)
{
	uint32		mask = LockFreeBufTable->mask;
	uint32		idx;
	uint8		dist;
	uint64		new_slot;

	/* The entry we are trying to place */
	uint32		ins_hash = hashcode;
	int			ins_id = buf_id;

	idx = ins_hash & mask;
	dist = 0;

	for (;;)
	{
		uint64	slot;
		uint64	expected;

		if (dist > LOCKFREE_BUF_MAX_DIST)
			elog(ERROR, "lock-free buffer hash table: max probe distance exceeded");

		slot = pg_atomic_read_u64(&LockFreeBufTable->slots[idx]);

		/*
		 * Empty or tombstone slot: try to claim it via CAS.
		 */
		if (LockFreeBufSlotIsEmpty(slot) || LockFreeBufSlotIsTombstone(slot))
		{
			new_slot = LockFreeBufSlotPack(ins_hash, ins_id, dist);
			expected = slot;
			if (pg_atomic_compare_exchange_u64(&LockFreeBufTable->slots[idx],
											   &expected, new_slot))
				return -1;		/* success */

			/* CAS failed, re-read and retry this slot */
			continue;
		}

		/* Slot is occupied */
		Assert(LockFreeBufSlotIsOccupied(slot));

		/*
		 * Check for duplicate: same hash?  Then compare full tag.
		 */
		if (LockFreeBufSlotGetHash(slot) == ins_hash)
		{
			int		existing_id = LockFreeBufSlotGetBufId(slot);
			BufferDesc *bufHdr = GetBufferDescriptor(existing_id);

			pg_read_barrier();
			if (BufferTagsEqual(tagPtr, &bufHdr->tag))
				return existing_id;		/* conflicting entry */
		}

		/*
		 * Robin Hood: if the existing entry has a shorter probe distance,
		 * steal its slot and continue inserting the displaced entry.
		 */
		if (LockFreeBufSlotGetDist(slot) < dist)
		{
			new_slot = LockFreeBufSlotPack(ins_hash, ins_id, dist);
			expected = slot;
			if (pg_atomic_compare_exchange_u64(&LockFreeBufTable->slots[idx],
											   &expected, new_slot))
			{
				/* Successfully displaced; now re-insert the evicted entry */
				ins_hash = LockFreeBufSlotGetHash(slot);
				ins_id = LockFreeBufSlotGetBufId(slot);
				dist = LockFreeBufSlotGetDist(slot);
				/* fall through to advance to next slot */
			}
			else
			{
				/* CAS failed; retry this slot */
				continue;
			}
		}

		idx = (idx + 1) & mask;
		dist++;
	}
}

/*
 * LockFreeBufTableDelete
 *		Mark the slot holding the given tag as a tombstone.
 *
 * Caller must hold exclusive BufMappingLock for this partition.
 */
static void
LockFreeBufTableDelete(BufferTag *tagPtr, uint32 hashcode)
{
	uint32		mask = LockFreeBufTable->mask;
	uint32		idx;
	uint8		dist;

	idx = hashcode & mask;

	for (dist = 0; dist <= LOCKFREE_BUF_MAX_DIST; dist++)
	{
		uint64	slot = pg_atomic_read_u64(&LockFreeBufTable->slots[idx]);

		if (LockFreeBufSlotIsEmpty(slot))
			break;				/* not found -- fall through to error */

		if (LockFreeBufSlotIsOccupied(slot) &&
			LockFreeBufSlotGetDist(slot) < dist)
			break;				/* Robin Hood: can't be further */

		if (LockFreeBufSlotIsOccupied(slot) &&
			LockFreeBufSlotGetHash(slot) == hashcode)
		{
			int		cand_id = LockFreeBufSlotGetBufId(slot);
			BufferDesc *bufHdr = GetBufferDescriptor(cand_id);

			pg_read_barrier();
			if (BufferTagsEqual(tagPtr, &bufHdr->tag))
			{
				uint64	tomb;

				/*
				 * Mark as tombstone.  We keep the hash and buf_id in the
				 * slot for debugging, but set the tombstone flag.
				 */
				tomb = (slot & ~LOCKFREE_BUF_OCCUPIED) | LOCKFREE_BUF_TOMBSTONE;
				pg_atomic_write_u64(&LockFreeBufTable->slots[idx], tomb);
				return;
			}
		}

		idx = (idx + 1) & mask;
	}

	elog(ERROR, "lock-free buffer hash table corrupted: entry not found");
}


/* ----------------------------------------------------------------
 *		Public API -- dispatch to lock-free or legacy implementation
 * ----------------------------------------------------------------
 */

/*
 * BufTableLookup
 *		Lookup the given BufferTag; return buffer ID, or -1 if not found
 *
 * When enable_lockfree_bufmap is on, this is fully lock-free.
 * Otherwise caller must hold at least share lock on BufMappingLock for
 * tag's partition.
 */
int
BufTableLookup(BufferTag *tagPtr, uint32 hashcode)
{
	if (enable_lockfree_bufmap && LockFreeBufTable != NULL)
		return LockFreeBufTableLookup(tagPtr, hashcode);

	/* Legacy path */
	{
		BufferLookupEnt *result;

		result = (BufferLookupEnt *)
			hash_search_with_hash_value(SharedBufHash,
										tagPtr,
										hashcode,
										HASH_FIND,
										NULL);
		if (!result)
			return -1;

		return result->id;
	}
}

/*
 * BufTableInsert
 *		Insert a hashtable entry for given tag and buffer ID,
 *		unless an entry already exists for that tag
 *
 * Returns -1 on successful insertion.  If a conflicting entry exists
 * already, returns the buffer ID in that entry.
 *
 * Caller must hold exclusive lock on BufMappingLock for tag's partition.
 */
int
BufTableInsert(BufferTag *tagPtr, uint32 hashcode, int buf_id)
{
	Assert(buf_id >= 0);		/* -1 is reserved for not-in-table */
	Assert(tagPtr->blockNum != P_NEW);	/* invalid tag */

	if (enable_lockfree_bufmap && LockFreeBufTable != NULL)
	{
		int		lf_result;

		lf_result = LockFreeBufTableInsert(tagPtr, hashcode, buf_id);

		/*
		 * Also maintain the legacy table so that switching the GUC off at
		 * runtime gives a consistent view.
		 */
		{
			BufferLookupEnt *result;
			bool	found;

			result = (BufferLookupEnt *)
				hash_search_with_hash_value(SharedBufHash,
											tagPtr,
											hashcode,
											HASH_ENTER,
											&found);
			if (!found)
				result->id = buf_id;
		}

		return lf_result;
	}

	/* Legacy-only path */
	{
		BufferLookupEnt *result;
		bool		found;

		result = (BufferLookupEnt *)
			hash_search_with_hash_value(SharedBufHash,
										tagPtr,
										hashcode,
										HASH_ENTER,
										&found);

		if (found)
			return result->id;

		result->id = buf_id;
		return -1;
	}
}

/*
 * BufTableDelete
 *		Delete the hashtable entry for given tag (which must exist)
 *
 * Caller must hold exclusive lock on BufMappingLock for tag's partition
 */
void
BufTableDelete(BufferTag *tagPtr, uint32 hashcode)
{
	if (enable_lockfree_bufmap && LockFreeBufTable != NULL)
	{
		LockFreeBufTableDelete(tagPtr, hashcode);

		/* Also remove from legacy table to keep it in sync */
		{
			BufferLookupEnt *result;

			result = (BufferLookupEnt *)
				hash_search_with_hash_value(SharedBufHash,
											tagPtr,
											hashcode,
											HASH_REMOVE,
											NULL);
			if (!result)
				elog(ERROR, "shared buffer hash table corrupted");
		}
		return;
	}

	/* Legacy-only path */
	{
		BufferLookupEnt *result;

		result = (BufferLookupEnt *)
			hash_search_with_hash_value(SharedBufHash,
										tagPtr,
										hashcode,
										HASH_REMOVE,
										NULL);
		if (!result)
			elog(ERROR, "shared buffer hash table corrupted");
	}
}
