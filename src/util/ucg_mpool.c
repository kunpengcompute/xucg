/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2024. All rights reserved.
 */
#include "ucg_mpool.h"
#include "ucg_helper.h"
#include "ucg_malloc.h"
#include "ucg_shmem_segment.h"
#include "util/ucg_math.h"
#include "util/ucg_sys.h"

static ucg_mpool_ops_t ucg_default_mpool_ops[UCG_MPOOL_ALLOCATE_LAST] = {
    {
        .chunk_alloc = ucg_mpool_hugetlb_malloc,
        .chunk_release = ucg_mpool_hugetlb_free,
        .obj_init = NULL,
        .obj_cleanup = NULL
    },
    {
        .chunk_alloc = ucg_mpool_chunk_mmap,
        .chunk_release = ucg_mpool_chunk_munmap,
        .obj_init = ucg_mpool_chunk_obj_init,
        .obj_cleanup = NULL
    }
};

/**
 * @brief The wrapper functions registered to UCS_MPOOL, they will call ucg's
 *        mpool functions.
 */
static ucs_status_t ucg_mpool_chunk_alloc_wrapper(ucs_mpool_t *ucs_mp,
                                                  size_t *psize, void **pchunk)
{
    ucg_mpool_t *ucg_mp = ucg_derived_of(ucs_mp, ucg_mpool_t);
    ucg_status_t status = ucg_mp->ops->chunk_alloc(ucg_mp, psize, pchunk);

    return ucg_status_g2s(status);
}

static void ucg_mpool_chunk_release_wrapper(ucs_mpool_t *ucs_mp, void *chunk)
{
    ucg_mpool_t *ucg_mp = ucg_derived_of(ucs_mp, ucg_mpool_t);
    ucg_mp->ops->chunk_release(ucg_mp, chunk);
}

static void ucg_mpool_obj_init_wrapper(ucs_mpool_t *ucs_mp, void *obj, void *chunk)
{
    ucg_mpool_t *ucg_mp = ucg_derived_of(ucs_mp, ucg_mpool_t);
    ucg_mp->ops->obj_init(ucg_mp, obj, chunk);
}

static void ucg_mpool_obj_cleanup_wrapper(ucs_mpool_t *ucs_mp, void *obj)
{
    ucg_mpool_t *ucg_mp = ucg_derived_of(ucs_mp, ucg_mpool_t);
    ucg_mp->ops->obj_cleanup(ucg_mp, obj);
}

static ucs_mpool_ops_t *ucs_ops;

ucg_status_t ucg_mpool_init(ucg_mpool_t *mp, size_t priv_size,
                            size_t elem_size, size_t align_offset, size_t alignment,
                            unsigned elems_per_chunk, unsigned max_elems,
                            ucg_mpool_ops_t *ops, const char *name)
{
    ucg_status_t status;
    ucs_mpool_params_t mp_params;

    if (mp == NULL || name == NULL) {
        return UCG_ERR_INVALID_PARAM;
    }

    ucs_ops = ucg_calloc(1, sizeof(ucs_mpool_ops_t), "ucs_ops");
    if (ucs_ops == NULL) {
        return UCG_ERR_NO_MEMORY;
    }

    mp->ops = (ops == NULL) ? &ucg_default_mpool_ops[mp->allocate_type] : ops;
    if (mp->ops->obj_init == NULL) {
        ucs_ops->obj_init = NULL;
    }
    if (mp->ops->obj_cleanup == NULL) {
        ucs_ops->obj_cleanup = NULL;
    }

    ucs_mpool_params_reset(&mp_params);
    mp_params.priv_size         = priv_size;
    mp_params.elem_size         = elem_size;
    mp_params.align_offset      = align_offset;
    mp_params.alignment         = alignment;
    mp_params.elems_per_chunk   = elems_per_chunk;
    mp_params.max_elems         = max_elems;
    mp_params.ops               = ucs_ops;
    mp_params.name              = name;
    ucs_ops->chunk_alloc = ucg_mpool_chunk_alloc_wrapper;
    ucs_ops->chunk_release = ucg_mpool_chunk_release_wrapper;
    ucs_ops->obj_init = (mp->ops->obj_init == NULL) ? NULL : ucg_mpool_obj_init_wrapper;
    ucs_ops->obj_cleanup = (mp->ops->obj_cleanup == NULL) ? NULL : ucg_mpool_obj_cleanup_wrapper;

    status = ucg_status_s2g(ucs_mpool_init(&mp_params, &mp->super));
    if (status != UCG_OK) {
        return status;
    }
    status = ucg_lock_init(&mp->lock, UCG_LOCK_TYPE_NONE);
    return status;
}

ucg_status_t ucg_mpool_init_mt(ucg_mpool_t *mp, size_t priv_size,
                               size_t elem_size, size_t align_offset, size_t alignment,
                               unsigned elems_per_chunk, unsigned max_elems,
                               ucg_mpool_ops_t *ops, const char *name)
{
    ucg_status_t status;
    status = ucg_mpool_init(mp, priv_size, elem_size, align_offset, alignment,
                            elems_per_chunk, max_elems, ops, name);
    if (status != UCG_OK) {
        return status;
    }
    ucg_lock_destroy(&mp->lock);
    status = ucg_lock_init(&mp->lock, UCG_LOCK_TYPE_SPINLOCK);
    return status;
}

void ucg_mpool_cleanup(ucg_mpool_t *mp, int check_leak)
{
    if (mp == NULL) {
        return;
    }

    ucs_mpool_cleanup(&mp->super, check_leak);
    if (ucs_ops != NULL) {
        ucg_free(ucs_ops);
        ucs_ops = NULL;
    }
    ucg_lock_destroy(&mp->lock);
    return;
}

void *ucg_mpool_get(ucg_mpool_t *mp)
{
    if (mp == NULL) {
        return NULL;
    }
    void *obj = NULL;
    ucg_lock_enter(&mp->lock);
    obj = ucs_mpool_get(&mp->super);
    ucg_lock_leave(&mp->lock);
    return obj;
}

void ucg_mpool_put(void *obj)
{
    if (obj == NULL) {
        return;
    }
    /* depends on the implementation of ucs mpool. */
    ucs_mpool_elem_t *elem = (ucs_mpool_elem_t*)obj - 1;
    ucg_mpool_t *mp = ucg_derived_of(elem->mpool, ucg_mpool_t);
    ucg_lock_enter(&mp->lock);
    ucs_mpool_put(obj);
    ucg_lock_leave(&mp->lock);
    return;
}

int ucg_mpool_is_empty(ucg_mpool_t *mp)
{
    return (mp->super.freelist == NULL) && (mp->super.data->quota == 0);
}

ucg_status_t ucg_mpool_init_allocate_type(ucg_mpool_t *mp, ucg_mpool_allocate_type_t type)
{
    if (mp == NULL) {
        return UCG_ERR_INVALID_PARAM;
    }

    mp->allocate_type = type;
    return UCG_OK;
}

ucg_status_t ucg_mpool_chunk_mmap(ucg_mpool_t *mp, size_t *size_p, void **chunk_p)
{
    ucg_status_t status;
    ucg_mmap_mpool_chunk_hdr_t *chunk;

    ucg_shmem_segment_t shmem_segment;
    ucg_shmem_reset_ds(&shmem_segment);

    /**
     * An segment data structure:
     * +----------------------------+---------------------+-------------------+-------------+
     * | ucg_mmap_mpool_chunk_hdr_t | ucg_shmem_sgement_t | ucs_mpool_chunk_t | actual_Data |
     * +----------------------------+---------------------+-------------------+-------------+
     */
    size_t size_with_shmem_seg = sizeof(*chunk) + ucg_shmem_sizeof_ds(&shmem_segment) + (*size_p);
    status = ucg_shmem_segment_create(&shmem_segment, size_with_shmem_seg);
    if (status != UCG_OK) {
        return status;
    }
    chunk = shmem_segment.seg_base_addr;
    memcpy(chunk+1, &shmem_segment, sizeof(shmem_segment));

    chunk->size = shmem_segment.seg_size;
    *size_p = shmem_segment.seg_size - sizeof(*chunk) - ucg_shmem_sizeof_ds(&shmem_segment);
    *chunk_p = (void *)((char *)(chunk+1) + ucg_shmem_sizeof_ds(&shmem_segment));

    return UCG_OK;
}

void ucg_mpool_chunk_munmap(ucg_mpool_t *mp, void *chunk)
{
    ucg_shmem_segment_t shmem_segment;

    memcpy(&shmem_segment, (char *)chunk - sizeof(shmem_segment), sizeof(shmem_segment));
    ucg_shmem_segment_detach(&shmem_segment);
}

void ucg_mpool_chunk_obj_init(ucg_mpool_t *mp, void *obj, void *chunk)
{
    memcpy(obj, (char *)chunk - sizeof(ucg_shmem_segment_t), sizeof(ucg_shmem_segment_t));
}

ucg_status_t ucg_mpool_hugetlb_malloc(ucg_mpool_t *mp, size_t *psize, void **pchunk)
{
    ucs_status_t ucs_status = ucs_mpool_hugetlb_malloc(&mp->super, psize, pchunk);
    return ucg_status_s2g(ucs_status);
}

void ucg_mpool_hugetlb_free(ucg_mpool_t *mp, void *chunk)
{
    ucs_mpool_hugetlb_free(&mp->super, chunk);
}