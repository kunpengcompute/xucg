/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

#ifndef UCG_MPOOL_LIST_H_
#define UCG_MPOOL_LIST_H_

#include "ucg/api/ucg.h"
#include "ucg_mpool.h"
#include "ucg_list.h"
#include "ucg_malloc.h"
#include "ucg_log.h"

#include <limits.h>

/* first size of mpool elem on the mpool list : 16KB */
#define UCG_MPOOL_LIST_INIT_SIZE 16384

typedef struct  ucg_mpool_list_elem {
    ucg_mpool_t super; 
    size_t length;
    ucg_list_link_t *head;
    ucg_list_link_t list;
} ucg_mpool_list_elem_t;

ucg_status_t ucg_mpool_list_init(ucg_list_link_t *head, const size_t length);

void *ucg_mpool_list_get(ucg_list_link_t *head, const size_t length);

ucg_status_t ucg_mpool_list_cleanup(ucg_list_link_t *head);

#endif