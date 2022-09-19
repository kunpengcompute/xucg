/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "ucg_rh.h"
#include "util/ucg_helper.h"

/**
 * Recursive Halving Algorithm
 * rank_id         0     1     2     3     4     5
 * step pre        0<----------------------4
 *                       1<----------------------5
 *      recursive  0-----------2
 *                       1-----------3
 *                 0-----1     2-----3
 *      post       0---------------------->4
 *                       1---------------------->5
 * rank_type       B|P   B|P   B     B     E     E
 */
void ucg_algo_rh_iter_init(ucg_algo_rh_iterator_t *p, int group_size, ucg_rank_t my_rank)
{
    ucg_assert(my_rank != UCG_INVALID_RANK);
    p->my_rank = my_rank;
    p->iteration = 0;

    /* Determine nearest power of two less than or equal to size */
    int adjust_group_size = UCG_BIT(ucg_ilog2(group_size));
    p->adjust_group_size = adjust_group_size;
    p->max_iteration = ucg_ilog2(adjust_group_size);

    if (my_rank < adjust_group_size) {
        p->my_type = UCG_ALGO_RH_RANK_BASE;
        if (my_rank < group_size - adjust_group_size) {
            p->my_type |= UCG_ALGO_RH_RANK_PROXY;
        }
    } else {
        p->my_type = UCG_ALGO_RH_RANK_EXTRA;
    }
}

void ucg_algo_rh_get_extra(const ucg_algo_rh_iterator_t *p, ucg_rank_t *peer)
{
    if (!(p->my_type & UCG_ALGO_RH_RANK_PROXY)) {
        *peer = UCG_INVALID_RANK;
        return;
    }
    *peer = p->my_rank + p->adjust_group_size;
}

void ucg_algo_rh_get_proxy(const ucg_algo_rh_iterator_t *p, ucg_rank_t *peer)
{
    if (!(p->my_type & UCG_ALGO_RH_RANK_EXTRA)) {
        *peer = UCG_INVALID_RANK;
        return;
    }
    *peer = p->my_rank - p->adjust_group_size;
}

void ucg_algo_rh_get_next_base(ucg_algo_rh_iterator_t *p, ucg_rank_t *peer)
{
    if (!(p->my_type & UCG_ALGO_RH_RANK_BASE) || p->iteration >= p->max_iteration) {
        *peer = UCG_INVALID_RANK;
        return;
    }
    *peer = p->my_rank ^ UCG_BIT(p->max_iteration - p->iteration - 1);
    ++p->iteration;
}