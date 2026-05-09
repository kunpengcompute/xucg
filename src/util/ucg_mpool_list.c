/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
#include "util/ucg_cpu.h"
#include "ucg_mpool_list.h"

/**
 * structure of memory pool list:
 * head(mpool list)
 *   |
 * mpool0   --->    mpool1  --->    mpool2  --->    ...
 *   |                 |              |
 * elem (size = a)  elem (size = b) elem (size = c)
 */

/* create a new memory pool */
static ucg_status_t ucg_mpool_list_create_mpool(ucg_mpool_list_elem_t *mpool_list_elem, size_t length)
{
    ucg_status_t status;
    ucg_mpool_t *mp = &mpool_list_elem->super;

    mpool_list_elem->length = length;
    ucg_mpool_init_allocate_type(mp, UCG_MPOOL_ALLOCATE_BY_HUGETBL);
    status = ucg_mpool_init(mp, 0, length,
                            0, UCG_CACHE_LINE_SIZE, UCG_ELEMS_PER_CHUNK,
                            UINT_MAX, NULL, "mpool list elem");
    if (status != UCG_OK) {
        return status;
    }

    void *obj = ucg_mpool_get(mp);
    UCG_CHECK_NULL(UCG_ERR_NO_MEMORY, obj);
    ucg_mpool_put(obj);

    return UCG_OK;
}

static ucg_status_t ucg_mpool_list_mpool_cleanup(ucg_mpool_list_elem_t *mpool_list_elem)
{
    ucg_mpool_t *mp = &mpool_list_elem->super;
    ucg_mpool_cleanup(mp, 1);
    return UCG_OK;
}

/* initialized mpool list */
ucg_status_t ucg_mpool_list_init(ucg_list_link_t *head, const size_t length)
{
    ucg_mpool_list_elem_t *mpool_list_elem = (ucg_mpool_list_elem_t *)ucg_malloc(sizeof(ucg_mpool_list_elem_t), "mpool list elem");
    if (mpool_list_elem == NULL) {
        ucg_error("malloc mpool list elem error");
        return UCG_ERR_NO_MEMORY;
    }

    ucg_status_t status = ucg_mpool_list_create_mpool(mpool_list_elem, length);
    if (status != UCG_OK) {
        return status;
    }

    /* intialize the link of mpool list */
    ucg_list_head_init(head);
    ucg_list_add_tail(head, &mpool_list_elem->list);
    mpool_list_elem->head = head;

    return UCG_OK;
}

ucg_status_t ucg_mpool_list_grow(ucg_list_link_t *head, ucg_mpool_list_elem_t *mpool_list_elem)
{
    size_t length = mpool_list_elem->length;
    ucg_mpool_list_elem_t *iter = NULL, *temp = NULL;
    unsigned long counter = 0;
    unsigned long list_length = ucg_list_length(head);

    ucg_list_for_each_safe(iter, temp, head, list) {
        if (length < iter->length) {
            break;
        }
        counter++;
    }

    if (counter == list_length) {
        ucg_list_add_tail(head, &mpool_list_elem->list);
    } else {
        ucg_list_insert_before(&iter->list, &mpool_list_elem->list);
    }

    mpool_list_elem->head = head;

    return UCG_OK;
}

ucg_mpool_list_elem_t *ucg_mpool_list_mpool_get(ucg_list_link_t *head, const size_t length)
{
    ucg_mpool_list_elem_t *iter, *temp;
    ucg_status_t status;

    /* find existing mpool elem on the link */
    ucg_list_for_each_safe(iter, temp, head, list) {
        if (length <= iter->length) {
            return iter;
        }
    }

    /* create new mpool elem on the link */
    ucg_mpool_list_elem_t *mpool_list_elem = (ucg_mpool_list_elem_t *)ucg_malloc(sizeof(ucg_mpool_list_elem_t), "mpool list elem");
    UCG_CHECK_NULL(NULL, mpool_list_elem);
    status = ucg_mpool_list_create_mpool(mpool_list_elem, length);
    UCG_CHECK_STATUS(NULL, status);
    /* add new elem to the link */
    status = ucg_mpool_list_grow(head, mpool_list_elem);
    UCG_CHECK_STATUS(NULL, status);

    return mpool_list_elem;
}

void *ucg_mpool_list_get(ucg_list_link_t *head, const size_t length)
{
    ucg_mpool_list_elem_t *mpool_list_elem;

    mpool_list_elem = ucg_mpool_list_mpool_get(head, length);

    void *obj = NULL;
    obj = ucg_mpool_get(&mpool_list_elem->super);
    return obj;
}

ucg_status_t ucg_mpool_list_del(ucg_list_link_t *head, ucg_mpool_list_elem_t *mpool_list_elem)
{
    ucg_status_t status;

    ucg_list_del(&mpool_list_elem->list);
    status = ucg_mpool_list_mpool_cleanup(mpool_list_elem);
    return status;
}

ucg_status_t ucg_mpool_list_cleanup(ucg_list_link_t *head)
{
    ucg_status_t status;
    ucg_mpool_list_elem_t *iter, *temp;

    ucg_list_for_each_safe(iter, temp, head, list) {
        status = ucg_mpool_list_del(head, iter);
        if (status != UCG_OK) {
            return status;
        }
    }
    return UCG_OK;
}