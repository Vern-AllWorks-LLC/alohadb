/*
 * xxHash - Extremely Fast Hash algorithm
 * Header File
 * Copyright (C) 2012-2023 Yann Collet
 *
 * BSD 2-Clause License (https://www.opensource.org/licenses/bsd-license.php)
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *    * Redistributions of source code must retain the above copyright
 *      notice, this list of conditions and the following disclaimer.
 *    * Redistributions in binary form must reproduce the above
 *      copyright notice, this list of conditions and the following disclaimer
 *      in the documentation and/or other materials provided with the
 *      distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * You can contact the author at:
 *   - xxHash homepage: https://www.xxhash.com
 *   - xxHash source repository: https://github.com/Cyan4973/xxHash
 *
 * -----------------------------------------------------------------------
 *
 * This is a minimal, self-contained, single-header implementation of XXH3
 * (64-bit variant) for use in PostgreSQL page checksumming.  It provides:
 *
 *   XXH3_64bits(const void *data, size_t len)
 *   XXH3_64bits_withSeed(const void *data, size_t len, XXH64_hash_t seed)
 *
 * This file is designed to be included in XXH_INLINE_ALL mode: all
 * functions are defined as static inline for maximum performance and
 * zero link-time overhead.
 */

#ifndef PG_XXHASH_H
#define PG_XXHASH_H

#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* ======================================================================
 * Type definitions
 * ====================================================================== */

typedef uint64_t XXH64_hash_t;

/* ======================================================================
 * Compiler / platform helpers
 * ====================================================================== */

#if defined(__GNUC__) || defined(__clang__)
#define XXH_FORCE_INLINE static __inline__ __attribute__((always_inline, unused))
#elif defined(_MSC_VER)
#define XXH_FORCE_INLINE static __forceinline
#else
#define XXH_FORCE_INLINE static inline
#endif

/* ======================================================================
 * Unaligned memory access helpers
 * ====================================================================== */

XXH_FORCE_INLINE uint32_t
XXH_read32(const void *ptr)
{
	uint32_t val;
	memcpy(&val, ptr, sizeof(val));
	return val;
}

XXH_FORCE_INLINE uint64_t
XXH_read64(const void *ptr)
{
	uint64_t val;
	memcpy(&val, ptr, sizeof(val));
	return val;
}

/* ======================================================================
 * Bit manipulation helpers
 * ====================================================================== */

XXH_FORCE_INLINE uint64_t
XXH_rotl64(uint64_t x, int r)
{
	return (x << r) | (x >> (64 - r));
}

XXH_FORCE_INLINE uint32_t
XXH_swap32(uint32_t x)
{
	return ((x << 24) & 0xff000000) |
		   ((x <<  8) & 0x00ff0000) |
		   ((x >>  8) & 0x0000ff00) |
		   ((x >> 24) & 0x000000ff);
}

XXH_FORCE_INLINE uint64_t
XXH_swap64(uint64_t x)
{
	return ((x << 56) & 0xff00000000000000ULL) |
		   ((x << 40) & 0x00ff000000000000ULL) |
		   ((x << 24) & 0x0000ff0000000000ULL) |
		   ((x <<  8) & 0x000000ff00000000ULL) |
		   ((x >>  8) & 0x00000000ff000000ULL) |
		   ((x >> 24) & 0x0000000000ff0000ULL) |
		   ((x >> 40) & 0x000000000000ff00ULL) |
		   ((x >> 56) & 0x00000000000000ffULL);
}

/* ======================================================================
 * 128-bit multiply helper  (64x64 -> 128)
 * ====================================================================== */

typedef struct
{
	uint64_t low64;
	uint64_t high64;
} XXH128_hash_t;

XXH_FORCE_INLINE XXH128_hash_t
XXH_mult64to128(uint64_t lhs, uint64_t rhs)
{
	XXH128_hash_t r128;

#if defined(__SIZEOF_INT128__) || \
	(defined(_INTEGRAL_MAX_BITS) && _INTEGRAL_MAX_BITS >= 128)
	__uint128_t product = (__uint128_t) lhs * (__uint128_t) rhs;
	r128.low64  = (uint64_t) product;
	r128.high64 = (uint64_t) (product >> 64);
#else
	/* Portable 64x64 -> 128 multiply using 32-bit pieces */
	uint64_t lo_lo  = (uint64_t)(uint32_t) lhs * (uint64_t)(uint32_t) rhs;
	uint64_t hi_lo  = (lhs >> 32)                * (uint64_t)(uint32_t) rhs;
	uint64_t lo_hi  = (uint64_t)(uint32_t) lhs   * (rhs >> 32);
	uint64_t hi_hi  = (lhs >> 32)                * (rhs >> 32);

	/* accumulate, handling carry */
	uint64_t cross  = (lo_lo >> 32) + (uint32_t) hi_lo + lo_hi;
	uint64_t upper  = (hi_lo >> 32) + (cross >> 32) + hi_hi;
	uint64_t lower  = (cross << 32) | (uint32_t) lo_lo;

	r128.low64  = lower;
	r128.high64 = upper;
#endif
	return r128;
}

/*
 * xor the high and low 64 bits of a 128-bit multiply result together.
 * This is the "fold" operation central to XXH3.
 */
XXH_FORCE_INLINE uint64_t
XXH3_mul128_fold64(uint64_t lhs, uint64_t rhs)
{
	XXH128_hash_t product = XXH_mult64to128(lhs, rhs);
	return product.low64 ^ product.high64;
}

/* ======================================================================
 * XXH3 constants
 * ====================================================================== */

#define XXH_PRIME32_1  0x9E3779B1U
#define XXH_PRIME32_2  0x85EBCA77U
#define XXH_PRIME32_3  0xC2B2AE3DU

#define XXH_PRIME64_1  0x9E3779B185EBCA87ULL
#define XXH_PRIME64_2  0xC2B2AE3D27D4EB4FULL
#define XXH_PRIME64_3  0x165667B19E3779F9ULL
#define XXH_PRIME64_4  0x85EBCA77C2B2AE63ULL
#define XXH_PRIME64_5  0x27D4EB2F165667C5ULL

#define XXH3_MIDSIZE_MAX 240

/* Number of secret bytes consumed per stripe */
#define XXH3_SECRET_CONSUME_RATE   8
/* Number of stripes per block */
#define XXH_STRIPE_LEN            64
#define XXH_SECRET_DEFAULT_SIZE  192
#define XXH3_INTERNALBUFFER_STRIPES (XXH_SECRET_DEFAULT_SIZE / XXH3_SECRET_CONSUME_RATE)
#define XXH_ACC_NB                 8   /* = XXH_STRIPE_LEN / sizeof(uint64_t) */
#define XXH3_SECRET_SIZE_MIN     136
#define XXH_SECRET_MERGEACCS_START 11
#define XXH3_SECRET_LASTACC_START  7   /* not necessarily starting from beginning */
#define XXH_SEC_ALIGN             8

/*
 * Default secret for XXH3, used when no custom secret is provided.
 * Taken from the official xxHash implementation.
 */
static const uint8_t XXH3_kSecret[XXH_SECRET_DEFAULT_SIZE] = {
	0xb8, 0xfe, 0x6c, 0x39, 0x23, 0xa4, 0x4b, 0xbe,
	0x7c, 0x01, 0x81, 0x2c, 0xf7, 0x21, 0xad, 0x1c,
	0xde, 0xd4, 0x6d, 0xe9, 0x83, 0x90, 0x97, 0xdb,
	0x72, 0x40, 0xa4, 0xa4, 0xb7, 0xb3, 0x67, 0x1f,
	0xcb, 0x79, 0xe6, 0x4e, 0xcc, 0xc0, 0xe5, 0x78,
	0x82, 0x5a, 0xd0, 0x7d, 0xcc, 0xff, 0x72, 0x21,
	0xb8, 0x08, 0x46, 0x74, 0xf7, 0x43, 0x24, 0x8e,
	0xe0, 0x35, 0x90, 0xe6, 0x81, 0x3a, 0x26, 0x4c,
	0x3c, 0x28, 0x52, 0xbb, 0x91, 0xc3, 0x00, 0xcb,
	0x88, 0xd0, 0x65, 0x8b, 0x1b, 0x53, 0x2e, 0xa3,
	0x71, 0x64, 0x48, 0x97, 0xa2, 0x0d, 0xf9, 0x4e,
	0x38, 0x19, 0xef, 0x46, 0xa9, 0xde, 0xac, 0xd8,
	0xa8, 0xfa, 0x76, 0x3f, 0xe3, 0x9c, 0x34, 0x3f,
	0xf9, 0xdc, 0xbb, 0xc7, 0xc7, 0x0b, 0x4f, 0x1d,
	0x8a, 0x51, 0xe0, 0x4b, 0xcd, 0xb4, 0x59, 0x31,
	0xc8, 0x9f, 0x7e, 0xc9, 0xd9, 0x78, 0x73, 0x64,
	0xea, 0xc5, 0xac, 0x83, 0x34, 0xd3, 0xeb, 0xc3,
	0xc5, 0x81, 0xa0, 0xff, 0xfa, 0x13, 0x63, 0xeb,
	0x17, 0x0d, 0xdd, 0x51, 0xb7, 0xf0, 0xda, 0x49,
	0xd3, 0x16, 0xca, 0xce, 0x09, 0xb6, 0x4c, 0x39,
	0x9c, 0xfe, 0x30, 0x6a, 0x27, 0x8c, 0x07, 0x68,
	0x73, 0x14, 0x14, 0x37, 0x4f, 0x07, 0x78, 0x69,
	0x2a, 0x41, 0xb1, 0x9a, 0x3b, 0x5e, 0xcc, 0x2a,
	0x47, 0x1a, 0x26, 0x84, 0x4c, 0x78, 0x58, 0x08
};

/* ======================================================================
 * XXH3 internal mixing functions
 * ====================================================================== */

/*
 * XXH3_avalanche -- final avalanche / bit-mixing step.
 */
XXH_FORCE_INLINE uint64_t
XXH3_avalanche(uint64_t h64)
{
	h64 ^= h64 >> 37;
	h64 *= 0x165667919E3779F9ULL;
	h64 ^= h64 >> 32;
	return h64;
}

/*
 * Rrmxmx -- alternative finaliser used for short keys (len 4..8).
 */
XXH_FORCE_INLINE uint64_t
XXH3_rrmxmx(uint64_t h64, uint64_t len)
{
	h64 ^= XXH_rotl64(h64, 49) ^ XXH_rotl64(h64, 24);
	h64 *= 0x9FB21C651E98DF25ULL;
	h64 ^= (h64 >> 35) + len;
	h64 *= 0x9FB21C651E98DF25ULL;
	return h64 ^ (h64 >> 28);
}

XXH_FORCE_INLINE uint64_t
XXH3_mix16B(const uint8_t *input,
			const uint8_t *secret,
			uint64_t seed64)
{
	uint64_t input_lo = XXH_read64(input);
	uint64_t input_hi = XXH_read64(input + 8);
	return XXH3_mul128_fold64(
		input_lo ^ (XXH_read64(secret) + seed64),
		input_hi ^ (XXH_read64(secret + 8) - seed64));
}

/* ======================================================================
 * Short-key path: len 0..16
 * ====================================================================== */

XXH_FORCE_INLINE uint64_t
XXH3_len_0(const uint8_t *secret, uint64_t seed)
{
	return XXH3_avalanche(seed ^
						  (XXH_read64(secret + 56) ^
						   XXH_read64(secret + 64)));
}

XXH_FORCE_INLINE uint64_t
XXH3_len_1to3_64b(const uint8_t *input, size_t len,
				  const uint8_t *secret, uint64_t seed)
{
	uint8_t  c1 = input[0];
	uint8_t  c2 = input[len >> 1];
	uint8_t  c3 = input[len - 1];
	uint32_t combined = ((uint32_t) c1 << 16) |
						((uint32_t) c2 <<  24) |
						((uint32_t) c3 <<  0) |
						((uint32_t) len << 8);
	uint64_t bitflip  = (XXH_read32(secret) ^ XXH_read32(secret + 4))
						+ seed;
	uint64_t keyed    = (uint64_t) combined ^ bitflip;
	return XXH3_avalanche(keyed);
}

XXH_FORCE_INLINE uint64_t
XXH3_len_4to8_64b(const uint8_t *input, size_t len,
				  const uint8_t *secret, uint64_t seed)
{
	seed ^= (uint64_t) XXH_swap32((uint32_t) seed) << 32;
	{
		uint32_t input1 = XXH_read32(input);
		uint32_t input2 = XXH_read32(input + len - 4);
		uint64_t s64    = XXH_read64(secret + 8) ^ XXH_read64(secret + 16);
		uint64_t bitflip = s64 - seed;
		uint64_t input64 = input2 + ((uint64_t) input1 << 32);
		uint64_t keyed   = input64 ^ bitflip;
		return XXH3_rrmxmx(keyed, len);
	}
}

XXH_FORCE_INLINE uint64_t
XXH3_len_9to16_64b(const uint8_t *input, size_t len,
				   const uint8_t *secret, uint64_t seed)
{
	uint64_t bitflip1 = (XXH_read64(secret + 24) ^ XXH_read64(secret + 32)) + seed;
	uint64_t bitflip2 = (XXH_read64(secret + 40) ^ XXH_read64(secret + 48)) - seed;
	uint64_t input_lo = XXH_read64(input)           ^ bitflip1;
	uint64_t input_hi = XXH_read64(input + len - 8) ^ bitflip2;
	uint64_t acc      = len
						+ XXH_swap64(input_lo)
						+ input_hi
						+ XXH3_mul128_fold64(input_lo, input_hi);
	return XXH3_avalanche(acc);
}

XXH_FORCE_INLINE uint64_t
XXH3_len_0to16_64b(const uint8_t *input, size_t len,
				   const uint8_t *secret, uint64_t seed)
{
	if (len > 8)
		return XXH3_len_9to16_64b(input, len, secret, seed);
	if (len >= 4)
		return XXH3_len_4to8_64b(input, len, secret, seed);
	if (len > 0)
		return XXH3_len_1to3_64b(input, len, secret, seed);
	return XXH3_len_0(secret, seed);
}

/* ======================================================================
 * Mid-size path: len 17..128
 * ====================================================================== */

XXH_FORCE_INLINE uint64_t
XXH3_len_17to128_64b(const uint8_t *input, size_t len,
					 const uint8_t *secret, size_t secretSize,
					 uint64_t seed)
{
	uint64_t acc = len * XXH_PRIME64_1;
	size_t   i;

	(void) secretSize; /* used only in assert-like checks in reference impl */

	if (len > 32)
	{
		if (len > 64)
		{
			if (len > 96)
			{
				acc += XXH3_mix16B(input + 48, secret + 96, seed);
				acc += XXH3_mix16B(input + len - 64, secret + 112, seed);
			}
			acc += XXH3_mix16B(input + 32, secret + 64, seed);
			acc += XXH3_mix16B(input + len - 48, secret + 80, seed);
		}
		acc += XXH3_mix16B(input + 16, secret + 32, seed);
		acc += XXH3_mix16B(input + len - 32, secret + 48, seed);
	}
	acc += XXH3_mix16B(input + 0, secret + 0, seed);
	acc += XXH3_mix16B(input + len - 16, secret + 16, seed);

	return XXH3_avalanche(acc);
}

/* ======================================================================
 * Mid-size path: len 129..240
 * ====================================================================== */

#define XXH3_MIDSIZE_STARTOFFSET 3
#define XXH3_MIDSIZE_LASTOFFSET 17

XXH_FORCE_INLINE uint64_t
XXH3_len_129to240_64b(const uint8_t *input, size_t len,
					  const uint8_t *secret, size_t secretSize,
					  uint64_t seed)
{
	uint64_t acc = len * XXH_PRIME64_1;
	size_t   nbRounds = len / 16;
	size_t   i;

	(void) secretSize;

	for (i = 0; i < 8; i++)
		acc += XXH3_mix16B(input + (16 * i), secret + (16 * i), seed);

	acc = XXH3_avalanche(acc);

	for (i = 8; i < nbRounds; i++)
		acc += XXH3_mix16B(input + (16 * i),
						   secret + (16 * (i - 8)) + XXH3_MIDSIZE_STARTOFFSET,
						   seed);

	/* last bytes */
	acc += XXH3_mix16B(input + len - 16,
					   secret + XXH3_SECRET_SIZE_MIN - XXH3_MIDSIZE_LASTOFFSET,
					   seed);
	return XXH3_avalanche(acc);
}

/* ======================================================================
 * Long-key path: len > 240   (stripe-based accumulation)
 * ====================================================================== */

/*
 * XXH3_accumulate_512 -- process one 64-byte stripe.
 * Accumulates 8 x 64-bit lanes.
 */
XXH_FORCE_INLINE void
XXH3_accumulate_512(uint64_t *acc,
					const uint8_t *input,
					const uint8_t *secret)
{
	size_t i;
	for (i = 0; i < XXH_ACC_NB; i++)
	{
		uint64_t data_val   = XXH_read64(input  + 8 * i);
		uint64_t data_key   = data_val ^ XXH_read64(secret + 8 * i);
		acc[i ^ 1]         += data_val;            /* swap adjacent lanes */
		acc[i]             += (uint32_t) data_key * (data_key >> 32);
	}
}

/*
 * XXH3_scrambleAcc -- scramble the accumulators after every super-block
 * (a super-block is (secret_size - 64) / 8 stripes).
 */
XXH_FORCE_INLINE void
XXH3_scrambleAcc(uint64_t *acc, const uint8_t *secret)
{
	size_t i;
	for (i = 0; i < XXH_ACC_NB; i++)
	{
		uint64_t key64 = XXH_read64(secret + 8 * i);
		uint64_t acc64 = acc[i];
		acc64 = (acc64 ^ (acc64 >> 47)) ^ key64;
		acc64 *= XXH_PRIME32_1;
		acc[i] = acc64;
	}
}

/*
 * Process a full set of stripes in one block.
 */
XXH_FORCE_INLINE void
XXH3_accumulate(uint64_t *acc,
				const uint8_t *input,
				const uint8_t *secret,
				size_t nbStripes)
{
	size_t n;
	for (n = 0; n < nbStripes; n++)
	{
		const uint8_t *in = input + n * XXH_STRIPE_LEN;
		XXH3_accumulate_512(acc, in, secret + n * XXH3_SECRET_CONSUME_RATE);
	}
}

/*
 * Merge accumulators at the end to produce the final hash.
 */
XXH_FORCE_INLINE uint64_t
XXH3_mergeAccs(const uint64_t *acc,
			   const uint8_t *secret,
			   uint64_t start)
{
	uint64_t result64 = start;
	size_t i;
	for (i = 0; i < 4; i++)
	{
		result64 += XXH3_mul128_fold64(
			acc[2 * i] ^ XXH_read64(secret + 16 * i),
			acc[2 * i + 1] ^ XXH_read64(secret + 16 * i + 8));
	}
	return XXH3_avalanche(result64);
}

/*
 * Initialise the accumulator lanes with the default secret.
 */
#define XXH3_INIT_ACC { XXH_PRIME32_3, XXH_PRIME64_1, XXH_PRIME64_2, \
						XXH_PRIME64_3, XXH_PRIME64_4, XXH_PRIME32_2, \
						XXH_PRIME64_5, XXH_PRIME32_1 }

XXH_FORCE_INLINE uint64_t
XXH3_hashLong_64b_internal(const uint8_t *input, size_t len,
						   const uint8_t *secret, size_t secretSize)
{
	uint64_t acc[XXH_ACC_NB] = XXH3_INIT_ACC;

	size_t nbStripesPerBlock = (secretSize - XXH_STRIPE_LEN) / XXH3_SECRET_CONSUME_RATE;
	size_t block_len         = XXH_STRIPE_LEN * nbStripesPerBlock;
	size_t nb_blocks         = (len - 1) / block_len;
	size_t n;

	for (n = 0; n < nb_blocks; n++)
	{
		XXH3_accumulate(acc, input + n * block_len, secret, nbStripesPerBlock);
		XXH3_scrambleAcc(acc, secret + secretSize - XXH_STRIPE_LEN);
	}

	/* last partial block */
	{
		size_t nbStripes = ((len - 1) - (block_len * nb_blocks)) / XXH_STRIPE_LEN;
		XXH3_accumulate(acc, input + nb_blocks * block_len, secret, nbStripes);

		/* last stripe -- may overlap with the previous one */
		{
			const uint8_t *p = input + len - XXH_STRIPE_LEN;
			XXH3_accumulate_512(acc, p,
				secret + secretSize - XXH_STRIPE_LEN - XXH3_SECRET_CONSUME_RATE);
		}
	}

	/* merge all 8 accumulator lanes */
	return XXH3_mergeAccs(acc,
						  secret + XXH_SECRET_MERGEACCS_START,
						  (uint64_t) len * XXH_PRIME64_1);
}

/* ======================================================================
 * Secret-with-seed derivation (for seeded long inputs)
 * ====================================================================== */

XXH_FORCE_INLINE void
XXH3_initCustomSecret(uint8_t *customSecret, uint64_t seed64)
{
	size_t i;
	/*
	 * The default secret is 192 bytes (24 uint64s).  We derive a
	 * per-seed secret by adding the seed to each 64-bit pair
	 * (alternating +seed / -seed).
	 */
	for (i = 0; i < XXH_SECRET_DEFAULT_SIZE / 16; i++)
	{
		uint64_t lo = XXH_read64(XXH3_kSecret + 16 * i)     + seed64;
		uint64_t hi = XXH_read64(XXH3_kSecret + 16 * i + 8) - seed64;
		memcpy(customSecret + 16 * i,     &lo, sizeof(lo));
		memcpy(customSecret + 16 * i + 8, &hi, sizeof(hi));
	}
}

XXH_FORCE_INLINE uint64_t
XXH3_hashLong_64b_withSeed_internal(const uint8_t *input, size_t len,
									uint64_t seed)
{
	if (seed == 0)
		return XXH3_hashLong_64b_internal(input, len,
										  XXH3_kSecret,
										  sizeof(XXH3_kSecret));
	{
		uint8_t secret[XXH_SECRET_DEFAULT_SIZE];
		XXH3_initCustomSecret(secret, seed);
		return XXH3_hashLong_64b_internal(input, len,
										  secret, sizeof(secret));
	}
}

/* ======================================================================
 * Public API
 * ====================================================================== */

/*
 * XXH3_64bits -- compute 64-bit xxHash3 of the given buffer, no seed.
 */
XXH_FORCE_INLINE XXH64_hash_t
XXH3_64bits(const void *data, size_t len)
{
	const uint8_t *input = (const uint8_t *) data;

	if (len <= 16)
		return XXH3_len_0to16_64b(input, len, XXH3_kSecret, 0);
	if (len <= 128)
		return XXH3_len_17to128_64b(input, len,
									XXH3_kSecret, sizeof(XXH3_kSecret), 0);
	if (len <= XXH3_MIDSIZE_MAX)
		return XXH3_len_129to240_64b(input, len,
									 XXH3_kSecret, sizeof(XXH3_kSecret), 0);
	return XXH3_hashLong_64b_internal(input, len,
									  XXH3_kSecret, sizeof(XXH3_kSecret));
}

/*
 * XXH3_64bits_withSeed -- compute 64-bit xxHash3 with an explicit seed.
 */
XXH_FORCE_INLINE XXH64_hash_t
XXH3_64bits_withSeed(const void *data, size_t len, XXH64_hash_t seed)
{
	const uint8_t *input = (const uint8_t *) data;

	if (len <= 16)
		return XXH3_len_0to16_64b(input, len, XXH3_kSecret, seed);
	if (len <= 128)
		return XXH3_len_17to128_64b(input, len,
									XXH3_kSecret, sizeof(XXH3_kSecret), seed);
	if (len <= XXH3_MIDSIZE_MAX)
		return XXH3_len_129to240_64b(input, len,
									 XXH3_kSecret, sizeof(XXH3_kSecret), seed);
	return XXH3_hashLong_64b_withSeed_internal(input, len, seed);
}

#endif							/* PG_XXHASH_H */
