/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

#ifndef UCG_SHM_MPOOL_LIST_H_
#define UCG_SHM_MPOOL_LIST_H_

#include "ucg/api/ucg.h"
#include "ucg_mpool.h"
#include "ucg_list.h"
#include "ucg_malloc.h"
#include "ucg_shmem_segment.h"
#include "ucg_log.h"

#include <limits.h>

typedef struct ucg_shmem_pool {
    ucg_mpool_t super;
    size_t length;
    ucg_list_link_t list;
} ucg_shmem_pool_t;

typedef struct ucg_shmem_pool_params {
    size_t length;
} ucg_shmem_pool_params_t;

int ucg_list_shmem_pool_is_empty(ucg_list_link_t *head);

unsigned long ucg_list_shmem_pool_length(ucg_list_link_t *head);

ucg_status_t ucg_list_shmem_pool_init(ucg_list_link_t *head, const ucg_shmem_pool_params_t *params);

ucg_shmem_pool_t *ucg_list_shmem_pool_get(ucg_list_link_t *head, const ucg_shmem_pool_params_t *params);

ucg_status_t ucg_list_shmem_pool_put(ucg_list_link_t *head, ucg_shmem_pool_t *shmem_mp);

ucg_status_t ucg_list_shmem_pool_del(ucg_list_link_t *head, ucg_shmem_pool_t *shmem_mp);

ucg_status_t ucg_list_shmem_pool_cleanup(ucg_list_link_t *head);

#endif