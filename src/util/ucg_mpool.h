/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_MPOOL_H_
#define UCG_MPOOL_H_

#include "ucg/api/ucg.h"
#include "ucg_lock.h"

#include <ucs/datastruct/mpool.h>

#define UCG_ELEMS_PER_CHUNK 8

#ifdef UCG_ENABLE_MT
#define UCG_MPOOL_INIT(...) ucg_mpool_init_mt(__VA_ARGS__)
#else
#define UCG_MPOOL_INIT(...) ucg_mpool_init(__VA_ARGS__)
#endif

typedef struct ucg_mpool ucg_mpool_t;
typedef struct ucg_mpool_ops {
    /**
     * @brief Allocate a chunk of memory from @a mp mpool.
     *
     * @param [in]    mp        the memory pool
     * @param [inout] psize     the size of chunk want to alloc, this function may
     *                          modify it to actual allocated size (will greater or equal to input)
     * @param [out]   pchunk    the chunk
     */
    ucg_status_t (*chunk_alloc)(ucg_mpool_t *mp, size_t *psize, void **pchunk);

    /**
     * @brief Release the chunk to memory pool
     *
     * @param [in] mp           the memory pool
     * @param [in] chunk        the chunk alloc by chunk_alloc()
     */
    void (*chunk_release)(ucg_mpool_t *mp, void *chunk);

    /**
     * @brief Initialize an object when it is first be alloced from memory pool
     * @note  If set NULL then will not init the object
     *
     * @param [in] mp           the memory pool
     * @param [in] obj          the object user defined
     * @param [in] chunk        the chunk alloc by chunk_alloc()
     */
    void (*obj_init)(ucg_mpool_t *mp, void *obj, void *chunk);

    /**
     * @brief Cleanup an object before it is be freed to memory pool
     * @note  If set NULL then will not cleanup the object
     *
     * @param [in]  mp          the memory pool
     * @param [out] obj         the object
     */
    void (*obj_cleanup)(ucg_mpool_t *mp, void *obj);
} ucg_mpool_ops_t;

struct ucg_mpool {
    ucs_mpool_t super;          /**< UCS memory pool */
    ucg_mpool_ops_t *ops;       /**< UCG mpool ops */
    ucg_lock_t lock;
};

/**
 * @brief Create a memory pool.
 *
 * @param [in] mp               the memory pool
 * @param [in] priv_size        user defined private data length, can be 0
 * @param [in] elem_size        the size of an element alloc from mpool
 * @param [in] align_offset     offset in the element which should be aligned to the given boundary
 * @param [in] alignment        boundary to which align the given offset within the element
 * @param [in] elems_per_chunk  the max number of elements allocated from a single chunk
 * @param [in] max_elems        the max number of elements allocated from this mpool
 *                              UINT_MAX is for unlimited
 * @param [in] ops              the memory pool ops, if NULL will use the default mpool ops
 * @param [in] name             the name of this mpool
 *
 * @return the status of init
 */
ucg_status_t ucg_mpool_init(ucg_mpool_t *mp, size_t priv_size,
                            size_t elem_size, size_t align_offset, size_t alignment,
                            unsigned elems_per_chunk, unsigned max_elems,
                            ucg_mpool_ops_t *ops, const char *name);

/**
 * @brief Create a thread-safe memory pool.
 *
 * @param [in] mp               the memory pool
 * @param [in] priv_size        user defined private data length, can be 0
 * @param [in] elem_size        the size of an element alloc from mpool
 * @param [in] align_offset     offset in the element which should be aligned to the given boundary
 * @param [in] alignment        boundary to which align the given offset within the element
 * @param [in] elems_per_chunk  the max number of elements allocated from a single chunk
 * @param [in] max_elems        the max number of elements allocated from this mpool
 *                              UINT_MAX is for unlimited
 * @param [in] ops              the memory pool ops, if NULL will use the default mpool ops
 * @param [in] name             the name of this mpool
 *
 * @return the status of init
 */
ucg_status_t ucg_mpool_init_mt(ucg_mpool_t *mp, size_t priv_size,
                               size_t elem_size, size_t align_offset, size_t alignment,
                               unsigned elems_per_chunk, unsigned max_elems,
                               ucg_mpool_ops_t *ops, const char *name);

/**
 * @brief Cleanup the memory pool
 *
 * @param [in] mp               the memory pool
 * @param [in] check_leak       0 for not check, 1 for check
 */
void ucg_mpool_cleanup(ucg_mpool_t *mp, int check_leak);

/**
 * @brief Get an element from mpool
 *
 * @return the element, if failed then return NULL.
 */
void *ucg_mpool_get(ucg_mpool_t *mp);

/**
 * @brief Put an element to mpool
 */
void ucg_mpool_put(void *obj);

/**
 * @brief the default chunk alloc function
 */
ucg_status_t ucg_mpool_hugetlb_malloc(ucg_mpool_t *mp, size_t *psize, void **pchunk);

/**
 * @brief the default chunk release function
 */
void ucg_mpool_hugetlb_free(ucg_mpool_t *mp, void *chunk);

#endif