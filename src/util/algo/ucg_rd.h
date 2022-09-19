/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_ALGO_RECURSIVE_DOUBLING_H_
#define UCG_ALGO_RECURSIVE_DOUBLING_H_

#include "ucg/api/ucg.h"
#include "util/ucg_math.h"

typedef enum {
    UCG_ALGO_RD_ITER_BASE,
    UCG_ALGO_RD_ITER_PROXY,
    UCG_ALGO_RD_ITER_EXTRA,
} ucg_algo_rd_iter_type_t;

/**
 * @brief Recursive doubling algorithm iterator
 */
typedef struct ucg_algo_rd_iter {
    ucg_algo_rd_iter_type_t type;
    int max_idx;
    int n_base;
    ucg_rank_t myrank;
    int idx;
    ucg_rank_t current;
    int new_rank;
    int proxy_num;
} ucg_algo_rd_iter_t;

/**
 * @brief Initialize iterator of recursive doubling algorithm
 *
 * Depending on the input size, iterators are classified into three types:
 * - BASE:  values of this iterator are {BASE,...,BASE}.
 * - PROXY: Special BASE, values of this iterator are {EXTRA,BASE,...,BASE,EXTRA}.
 * - EXTRA: values of this iterator are {PROXY,PROXY}.
 * For example, if the input size is 5, the types of each rank are as follows:
 *      rank  0     1     2     3    4
 *      type  E     P     B     B    B
 * The values of iterator of rank 0 are {1, 1} in each iteration.
 * The values of iterator of rank 1 are {0, 2, 3, 0} in each iteration.
 * The values of iterator of rank 2 are {1, 4} in each iteration.
 * The values of iterator of rank 3 are {4, 1} in each iteration.
 * The values of iterator of rank 4 are {3, 2} in each iteration.
 */
void ucg_algo_rd_iter_init(ucg_algo_rd_iter_t *iter, int size, ucg_rank_t myrank);

/**
 * @brief Reset the iterator to the beginning.
 */
void ucg_algo_rd_iter_reset(ucg_algo_rd_iter_t *iter);

/**
 * @brief move to the next rank.
 */
void ucg_algo_rd_iter_inc(ucg_algo_rd_iter_t *iter);

/**
 * @brief Get the current value of iterator.
 * @retval Current rank.
 * @retval UCG_INVALID_RANK the end of iterator.
 */
static inline ucg_rank_t ucg_algo_rd_iter_value(ucg_algo_rd_iter_t *iter)
{
    return iter->current;
}

/**
 * @brief Get the current value of base iterator.
 * Treat the iterator type as UCG_ALGO_RD_ITER_BASE, which means this routine only
 * get BASE rank.
 * @retval Current rank.
 * @retval UCG_INVALID_RANK the end of iterator.
 */
static inline ucg_rank_t ucg_algo_rd_iter_base_value(ucg_algo_rd_iter_t *iter)
{
    if ((iter->type == UCG_ALGO_RD_ITER_PROXY &&
         (iter->idx == 0 || iter->idx == iter->max_idx - 1)) ||
        iter->type == UCG_ALGO_RD_ITER_EXTRA) {
        return UCG_INVALID_RANK;
    }
    return iter->current;
}

/**
 * @brief Get the current value of iterator and increase.
 * @retval Current rank.
 * @retval UCG_INVALID_RANK the end of iterator.
 */
static inline ucg_rank_t ucg_algo_rd_iter_value_inc(ucg_algo_rd_iter_t *iter)
{
    ucg_rank_t cur = iter->current;
    ucg_algo_rd_iter_inc(iter);
    return cur;
}

static inline ucg_algo_rd_iter_type_t ucg_algo_rd_iter_type(ucg_algo_rd_iter_t *iter)
{
    return iter->type;
}

#endif