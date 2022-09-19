/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "ucg_ring.h"
#include "util/ucg_helper.h"

void ucg_algo_ring_iter_init(ucg_algo_ring_iter_t *iter, int size, ucg_rank_t myrank)
{
    ucg_assert(myrank != UCG_INVALID_RANK);
    iter->left = (myrank - 1 + size) % size;
    iter->right = (myrank + 1) % size;
    iter->idx = 0;
    iter->max_idx = size - 1;
    return;
}