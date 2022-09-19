/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "ucg_rd.h"
#include "util/ucg_helper.h"

static inline void ucg_algo_rd_iter_update(ucg_algo_rd_iter_t *iter)
{
    if (iter->idx == iter->max_idx) {
        iter->current = UCG_INVALID_RANK;
        return;
    }
    int new_current;
    switch (iter->type) {
        case UCG_ALGO_RD_ITER_PROXY:
            if (iter->idx == 0 || iter->idx == iter->max_idx - 1) {
                iter->current = iter->myrank - 1;
            } else {
                new_current = iter->new_rank ^ UCG_BIT(iter->idx - 1);
                iter->current = (new_current < iter->proxy_num) ?
                                (new_current * 2 + 1) : (new_current + iter->proxy_num);
            }
            break;
        case UCG_ALGO_RD_ITER_BASE:
            new_current = iter->new_rank ^ UCG_BIT(iter->idx);
            iter->current = (new_current < iter->proxy_num) ?
                            (new_current * 2 + 1) : (new_current + iter->proxy_num);
            break;
        case UCG_ALGO_RD_ITER_EXTRA:
            iter->current = iter->myrank + 1;
            break;
        default:
            /* never happen */
            ucg_assert(0);
            break;
    }
    return;
}

void ucg_algo_rd_iter_init(ucg_algo_rd_iter_t *iter, int size, ucg_rank_t myrank)
{
    ucg_assert(myrank != UCG_INVALID_RANK);

    int n_base = UCG_BIT(ucg_ilog2(size));
    int proxy_num = size - n_base;
    int max_idx = ucg_ilog2(n_base);
    if (myrank < proxy_num * 2) {
        if (myrank % 2 == 0) {
            iter->type = UCG_ALGO_RD_ITER_EXTRA;
            iter->max_idx = 2;
        } else {
            iter->type = UCG_ALGO_RD_ITER_PROXY;
            iter->max_idx = max_idx + 2;
            iter->new_rank = myrank >> 1;
        }
    } else {
        iter->type = UCG_ALGO_RD_ITER_BASE;
        iter->max_idx = max_idx;
        iter->new_rank = myrank - proxy_num;
    }
    iter->n_base = n_base;
    iter->myrank = myrank;
    iter->idx = 0;
    iter->proxy_num = proxy_num;
    ucg_algo_rd_iter_update(iter);
    return;
}

void ucg_algo_rd_iter_reset(ucg_algo_rd_iter_t *iter)
{
    iter->idx = 0;
    ucg_algo_rd_iter_update(iter);
    return;
}

void ucg_algo_rd_iter_inc(ucg_algo_rd_iter_t *iter)
{
    ++iter->idx;
    ucg_algo_rd_iter_update(iter);
    return;
}