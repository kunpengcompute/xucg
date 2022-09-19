/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_MATH_H_
#define UCG_MATH_H_

#include <stddef.h>
#include <stdint.h>
#include <math.h>
#include "ucg/api/ucg.h"

#define UCG_KBYTE   (1ull << 10)
#define UCG_MBYTE   (1ull << 20)
#define UCG_GBYTE   (1ull << 30)
#define UCG_TBYTE   (1ull << 40)

#define ucg_min(_a, _b) \
({ \
    typeof(_a) _min_a = (_a); \
    typeof(_b) _min_b = (_b); \
    (_min_a < _min_b) ? _min_a : _min_b; \
})

#define ucg_max(_a, _b) \
({ \
    typeof(_a) _max_a = (_a); \
    typeof(_b) _max_b = (_b); \
    (_max_a > _max_b) ? _max_a : _max_b; \
})

#define DO_OP_MAX(_v1, _v2)     (_v1 > _v2 ? _v1 : _v2)
#define DO_OP_MIN(_v1, _v2)     (_v1 < _v2 ? _v1 : _v2)
#define DO_OP_SUM(_v1, _v2)     (_v1 + _v2)
#define DO_OP_PROD(_v1, _v2)    (_v1 * _v2)
#define DO_OP_LAND(_v1, _v2)    (_v1 && _v2)
#define DO_OP_BAND(_v1, _v2)    (_v1 & _v2)
#define DO_OP_LOR(_v1, _v2)     (_v1 || _v2)
#define DO_OP_BOR(_v1, _v2)     (_v1 | _v2)
#define DO_OP_LXOR(_v1, _v2)    ((!_v1) != (!_v2))
#define DO_OP_BXOR(_v1, _v2)    (_v1 ^ _v2)

#define ucg_is_pow2_or_zero(_n) \
    !((_n) & ((_n) - 1))

#define ucg_is_pow2(_n) \
    (((_n) > 0) && ucg_is_pow2_or_zero(_n))

#define ucg_padding(_n, _alignment) \
    (((_alignment) - (_n) % (_alignment)) % (_alignment))

#define ucg_align_down(_n, _alignment) \
    ((_n) - ((_n) % (_alignment)))

#define ucg_align_up(_n, _alignment) \
    ((_n) + ucg_padding(_n, _alignment))

#define ucg_align_down_pow2(_n, _alignment) \
    ((_n) & ~((_alignment) - 1))

#define ucg_align_up_pow2(_n, _alignment) \
    ucg_align_down_pow2((_n) + (_alignment) - 1, _alignment)

#define ucg_align_down_pow2_ptr(_ptr, _alignment) \
    ((typeof(_ptr))ucg_align_down_pow2((uintptr_t)(_ptr), (_alignment)))

#define ucg_align_up_pow2_ptr(_ptr, _alignment) \
    ((typeof(_ptr))ucg_align_up_pow2((uintptr_t)(_ptr), (_alignment)))

#define ucg_roundup_pow2(_n) \
    ({ \
        typeof(_n) pow2; \
        ucg_assert((_n) >= 1); \
        for (pow2 = 1; pow2 < (_n); pow2 <<= 1); \
        pow2; \
    })

#define ucg_rounddown_pow2(_n) (ucg_roundup_pow2(_n + 1) / 2)

#define ucg_signum(_n) \
    (((_n) > (typeof(_n))0) - ((_n) < (typeof(_n))0))

#define ucg_roundup_pow2_or0(_n) \
    (((_n) == 0) ? 0 : ucg_roundup_pow2(_n))

/* Return values: 0 - aligned, non-0 - unaligned */
#define ucg_check_if_align_pow2(_n, _p) ((_n) & ((_p) - 1))

/* Return values: off-set from the alignment */
#define ucg_padding_pow2(_n, _p) ucg_check_if_align_pow2(_n, _p)

#define UCG_MASK_SAFE(_i) \
    (((_i) >= 64) ? ((uint64_t)(-1)) : UCG_MASK(_i))

#define ucg_div_round_up(_n, _d) \
    (((_n) + (_d) - 1) / (_d))

static inline double ucg_log2(double x)
{
    return log(x) / log(2.0);
}

#define ucg_ilog2(_n)          \
(                              \
    (_n) < 1 ? 0 :             \
    (_n) & (1ULL << 63) ? 63 : \
    (_n) & (1ULL << 62) ? 62 : \
    (_n) & (1ULL << 61) ? 61 : \
    (_n) & (1ULL << 60) ? 60 : \
    (_n) & (1ULL << 59) ? 59 : \
    (_n) & (1ULL << 58) ? 58 : \
    (_n) & (1ULL << 57) ? 57 : \
    (_n) & (1ULL << 56) ? 56 : \
    (_n) & (1ULL << 55) ? 55 : \
    (_n) & (1ULL << 54) ? 54 : \
    (_n) & (1ULL << 53) ? 53 : \
    (_n) & (1ULL << 52) ? 52 : \
    (_n) & (1ULL << 51) ? 51 : \
    (_n) & (1ULL << 50) ? 50 : \
    (_n) & (1ULL << 49) ? 49 : \
    (_n) & (1ULL << 48) ? 48 : \
    (_n) & (1ULL << 47) ? 47 : \
    (_n) & (1ULL << 46) ? 46 : \
    (_n) & (1ULL << 45) ? 45 : \
    (_n) & (1ULL << 44) ? 44 : \
    (_n) & (1ULL << 43) ? 43 : \
    (_n) & (1ULL << 42) ? 42 : \
    (_n) & (1ULL << 41) ? 41 : \
    (_n) & (1ULL << 40) ? 40 : \
    (_n) & (1ULL << 39) ? 39 : \
    (_n) & (1ULL << 38) ? 38 : \
    (_n) & (1ULL << 37) ? 37 : \
    (_n) & (1ULL << 36) ? 36 : \
    (_n) & (1ULL << 35) ? 35 : \
    (_n) & (1ULL << 34) ? 34 : \
    (_n) & (1ULL << 33) ? 33 : \
    (_n) & (1ULL << 32) ? 32 : \
    (_n) & (1ULL << 31) ? 31 : \
    (_n) & (1ULL << 30) ? 30 : \
    (_n) & (1ULL << 29) ? 29 : \
    (_n) & (1ULL << 28) ? 28 : \
    (_n) & (1ULL << 27) ? 27 : \
    (_n) & (1ULL << 26) ? 26 : \
    (_n) & (1ULL << 25) ? 25 : \
    (_n) & (1ULL << 24) ? 24 : \
    (_n) & (1ULL << 23) ? 23 : \
    (_n) & (1ULL << 22) ? 22 : \
    (_n) & (1ULL << 21) ? 21 : \
    (_n) & (1ULL << 20) ? 20 : \
    (_n) & (1ULL << 19) ? 19 : \
    (_n) & (1ULL << 18) ? 18 : \
    (_n) & (1ULL << 17) ? 17 : \
    (_n) & (1ULL << 16) ? 16 : \
    (_n) & (1ULL << 15) ? 15 : \
    (_n) & (1ULL << 14) ? 14 : \
    (_n) & (1ULL << 13) ? 13 : \
    (_n) & (1ULL << 12) ? 12 : \
    (_n) & (1ULL << 11) ? 11 : \
    (_n) & (1ULL << 10) ? 10 : \
    (_n) & (1ULL <<  9) ?  9 : \
    (_n) & (1ULL <<  8) ?  8 : \
    (_n) & (1ULL <<  7) ?  7 : \
    (_n) & (1ULL <<  6) ?  6 : \
    (_n) & (1ULL <<  5) ?  5 : \
    (_n) & (1ULL <<  4) ?  4 : \
    (_n) & (1ULL <<  3) ?  3 : \
    (_n) & (1ULL <<  2) ?  2 : \
    (_n) & (1ULL <<  1) ?  1 : \
    (_n) & (1ULL <<  0) ?  0 : \
    0                          \
)

/* Returns the number of 1-bits in x */
#define ucg_popcount(_n) \
    ((sizeof(_n) <= 4) ? __builtin_popcount((uint32_t)(_n)) : \
                         __builtin_popcountl(_n))

/**
 * @brief Compare unsigned numbers which can wrap-around, assuming the wrap-around
 * distance can be at most the maximal value of the signed type.
 *
 * @param __a           First number
 * @param __op          Operator (e.g >=)
 * @param __b           Second number
 * @param __signed_type Signed type of __a/__b (e.g int32_t)
 *
 * @return value of the expression "__a __op __b".
 */
#define UCG_CIRCULAR_COMPARE(__a, __op, __b, __signed_type) \
    ((__signed_type)((__a) - (__b)) __op 0)

#define UCG_CIRCULAR_COMPARE8(__a, __op, __b)   UCG_CIRCULAR_COMPARE(__a, __op, __b, int8_t)
#define UCG_CIRCULAR_COMPARE16(__a, __op, __b)  UCG_CIRCULAR_COMPARE(__a, __op, __b, int16_t)
#define UCG_CIRCULAR_COMPARE32(__a, __op, __b)  UCG_CIRCULAR_COMPARE(__a, __op, __b, int32_t)
#define UCG_CIRCULAR_COMPARE64(__a, __op, __b)  UCG_CIRCULAR_COMPARE(__a, __op, __b, int64_t)

/**
 * @brief Generate all sub-masks of the given mask, from 0 to _mask inclusive.
 *
 * @param _submask  Variable to iterate over the sub-masks
 * @param _mask     Generate sub-masks of this value
 */
#define ucg_for_each_submask(_submask, _mask) \
    for (/* start with 0*/ \
         (_submask) = 0; \
         /* end when reaching _mask + 1 */ \
         (_submask) <= (_mask); \
         /* Increase _submask by 1. If it became larger than _mask, do nothing \
          * here, and next condition check will exit the loop. Otherwise, add \
          * ~mask to fast-forward the carry (from ++ operation) to the next \
          * valid bit in _mask, and then do "& _mask" to remove any bits which \
          * are not in the mask. \
          */ \
         (_submask)++, \
         ((_submask) <= (_mask)) ? \
                 ((_submask) = ((_submask) + ~(_mask)) & (_mask)) : 0)

#endif