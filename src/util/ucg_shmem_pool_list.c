/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
#include "ucg_shmem_pool_list.h"

static ucg_status_t ucg_shmem_pool_create(ucg_shmem_pool_t *shmem_pool, size_t length)
{
    ucg_status_t status;
    ucg_mpool_t *mp = &shmem_pool->super;
    shmem_pool->length = length;
    if (SIZE_MAX - UCG_MAX_CHUNK_PADDING < length) {
        ucg_error("shmem pool init length too lagre, length is %zu", length);
        return UCG_ERR_IO_ERROR;
    }
    /* set max_elems to be "1" to limit number of elems in shared memory chunk */
    status = ucg_mpool_chunk_init(mp, 0, sizeof(ucg_shmem_segment_t) + length, 0, 64, 1, 1,
                                  NULL, "shared memory mpool", length + UCG_MAX_CHUNK_PADDING);
    if (status != UCG_OK) {
        return status;
    }

    void *obj = ucg_mpool_get(mp);
    UCG_CHECK_NULL(UCG_ERR_NO_MEMORY, obj);
    ucg_mpool_put(obj);

    /* check the payload length to be within the valid range in chunk */
    ucg_shmem_segment_t *shmseg = (ucg_shmem_segment_t *)obj;
    size_t real_size = (char *) obj - (char *)shmseg->seg_base_addr + sizeof(ucg_shmem_segment_t) + length;
    if (real_size > shmseg->seg_size) {
        ucg_error("the actual length exceed the size of shared memory segment!");
        return UCG_ERR_NO_MEMORY;
    }

    return UCG_OK;
}

static ucg_status_t ucg_shmem_pool_cleanup(ucg_shmem_pool_t *shmem_pool)
{
    ucg_mpool_t *mp = &shmem_pool->super;
    ucg_shmem_segment_t shmem_segment;
    ucg_status_t status;

    void *obj = ucg_mpool_get(mp);
    UCG_CHECK_NULL(UCG_ERR_NO_MEMORY, obj);
    memcpy(&shmem_segment, obj, sizeof(ucg_shmem_segment_t));
    ucg_mpool_put(obj);

    ucg_mpool_cleanup(mp, 1);
    status = ucg_shmem_segment_unlink(&shmem_segment);
    return status;
}

int ucg_list_shmem_pool_is_empty(ucg_list_link_t *head)
{
    return ucg_list_is_empty(head);
}

unsigned long ucg_list_shmem_pool_length(ucg_list_link_t *head)
{
    return ucg_list_length(head);
}

ucg_status_t ucg_list_shmem_pool_init(ucg_list_link_t *head, const ucg_shmem_pool_params_t *params)
{
    ucg_shmem_pool_t *shmem_pool = (ucg_shmem_pool_t *)ucg_malloc(sizeof(ucg_shmem_pool_t), "shmem pool");
    if (shmem_pool == NULL) {
        ucg_error("malloc shmem pool error");
        return UCG_ERR_NO_MEMORY;
    }

    ucg_status_t status = ucg_shmem_pool_create(shmem_pool, params->length);
    if (status != UCG_OK) {
        return status;
    }

    ucg_list_head_init(head);
    ucg_list_add_tail(head, &shmem_pool->list);

    return UCG_OK;
}

ucg_shmem_pool_t *ucg_list_shmem_pool_get(ucg_list_link_t *head, const ucg_shmem_pool_params_t *params)
{
    size_t length = params->length;
    ucg_shmem_pool_t *iter, *temp;
    ucg_status_t status;

    ucg_list_for_each_safe(iter, temp, head, list) {
        if (length <= iter->length) {
            ucg_list_del(&iter->list);
            return iter;
        }
    }

    ucg_shmem_pool_t *shmem_pool = (ucg_shmem_pool_t *)ucg_malloc(sizeof(ucg_shmem_pool_t), "shmem pool");
    UCG_CHECK_NULL(NULL, shmem_pool);
    status = ucg_shmem_pool_create(shmem_pool, params->length);
    UCG_CHECK_STATUS(NULL, status);

    return shmem_pool;
}

ucg_status_t ucg_list_shmem_pool_put(ucg_list_link_t *head, ucg_shmem_pool_t *shmem_pool)
{
    size_t length = shmem_pool->length;
    ucg_shmem_pool_t *iter = NULL, *temp = NULL;
    unsigned long counter = 0;
    unsigned long list_length = ucg_list_shmem_pool_length(head);

    ucg_list_for_each_safe(iter, temp, head, list) {
        if (length < iter->length) {
            break;
        }
        counter++;
    }

    if (counter == list_length) {
        ucg_list_add_tail(head, &shmem_pool->list);
    } else {
        ucg_list_insert_before(&iter->list, &shmem_pool->list);
    }

    return UCG_OK;
}

ucg_status_t ucg_list_shmem_pool_del(ucg_list_link_t *head, ucg_shmem_pool_t *shmem_pool)
{
    ucg_status_t status;

    ucg_list_del(&shmem_pool->list);
    status = ucg_shmem_pool_cleanup(shmem_pool);
    return status;
}

ucg_status_t ucg_list_shmem_pool_cleanup(ucg_list_link_t *head)
{
    ucg_status_t status;
    ucg_shmem_pool_t *iter, *temp;

    ucg_list_for_each_safe(iter, temp, head, list) {
        status = ucg_list_shmem_pool_del(head, iter);
        if (status != UCG_OK) {
            return status;
        }
    }
    return UCG_OK;
}