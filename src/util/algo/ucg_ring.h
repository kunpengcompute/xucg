/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_ALGO_RING_H_
#define UCG_ALGO_RING_H_

#include "ucg/api/ucg.h"

/**
 * @brief Ring algorithm iterator
 */
typedef struct ucg_algo_ring_iter {
    ucg_rank_t left;
    ucg_rank_t right;
    int idx;
    int max_idx;
} ucg_algo_ring_iter_t;

/**
 * @brief Initialize iterator of ring algorithm
 *
 * At every step i (0 <= i < size), my left and right are not changed.
 */
void ucg_algo_ring_iter_init(ucg_algo_ring_iter_t *iter, int size, ucg_rank_t myrank);

/**
 * @brief Reset the iterator to the beginning.
 */
static inline void ucg_algo_ring_iter_reset(ucg_algo_ring_iter_t *iter)
{
    iter->idx = 0;
    return;
}

/**
 * @brief move to the next iteration.
 */
static inline void ucg_algo_ring_iter_inc(ucg_algo_ring_iter_t *iter)
{
    ++iter->idx;
    return;
}

/**
 * @brief Get the current step index.
 */
static inline int ucg_algo_ring_iter_idx(ucg_algo_ring_iter_t *iter)
{
    return iter->idx;
}

static inline int ucg_algo_ring_iter_end(ucg_algo_ring_iter_t *iter)
{
    if (iter->idx < iter->max_idx) {
        return 0;
    }
    return 1;
}

/**
 * @brief Get my left rank.
 */
static inline ucg_rank_t ucg_algo_ring_iter_left_value(ucg_algo_ring_iter_t *iter)
{
    if (iter->idx < iter->max_idx) {
        return iter->left;
    }
    return UCG_INVALID_RANK;
}

/**
 * @brief Get my right rank.
 */
static inline ucg_rank_t ucg_algo_ring_iter_right_value(ucg_algo_ring_iter_t *iter)
{
    if (iter->idx < iter->max_idx) {
        return iter->right;
    }
    return UCG_INVALID_RANK;
}

#endif