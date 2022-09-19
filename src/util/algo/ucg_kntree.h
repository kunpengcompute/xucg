/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_ALGO_KNTREE_H_
#define UCG_ALGO_KNTREE_H_

#include "ucg/api/ucg.h"

typedef struct ucg_algo_kntree_iter {
    int size;
    int degree;
    ucg_rank_t root;
    ucg_rank_t myrank;
    uint8_t leftmost;
    int max_subsize;
    int subsize;
    int child_idx;
    ucg_rank_t parent;
    ucg_rank_t child;
} ucg_algo_kntree_iter_t;

/**
 * @brief Initialize iterator of k-nomial tree algorithm
 *
 * if the input size = 10, depending on degree, the left-most kntree is as follows:
 *      degree=2          degree=3             degree=4
 *         0                 0                    0
 *      / / \ \         / /  |  \ \          /   / \ \ \
 *     8 4   2 1       9 3   6   1 2        4   8   1 2 3
 *     | |\  |           |\  |\            /|\  |
 *     9 6 5 3           4 5 7 8          5 6 7 9
 *       |
 *       7
 * The right-most kntree is a mirror flip of left-most kntree. Take rank0 as an
 * example, its left-most children in each iteration are {8,4,2,1}, and its
 * right-most children in each iteration are {1,2,4,8}.
 *
 * Generally, left-most applies to fan-out scenarios, and right-most applies to
 * fan-in scenarios.
 */
void ucg_algo_kntree_iter_init(ucg_algo_kntree_iter_t *iter, int size, int degree,
                               int root, ucg_rank_t myrank, uint8_t leftmost);

/**
 * @brief Reset the iterator to the beginning.
 */
void ucg_algo_kntree_iter_reset(ucg_algo_kntree_iter_t *iter);

/**
 * @brief Get the root value of this iterator.
 */
static inline ucg_rank_t ucg_algo_kntree_iter_root_value(ucg_algo_kntree_iter_t *iter)
{
    return iter->root;
}

/**
 * @brief Get the current parent value of iterator.
 * @retval Current parent rank.
 * @retval UCG_INVALID_RANK no parent rank.
 */
static inline ucg_rank_t ucg_algo_kntree_iter_parent_value(ucg_algo_kntree_iter_t *iter)
{
    return iter->parent;
}

/**
 * @brief Get the current child value of iterator.
 * @retval Current child rank.
 * @retval UCG_INVALID_RANK the end of iterator.
 */
static inline ucg_rank_t ucg_algo_kntree_iter_child_value(ucg_algo_kntree_iter_t *iter)
{
    return iter->child;
}

/**
 * @brief move to the next child rank.
 */
void ucg_algo_kntree_iter_child_inc(ucg_algo_kntree_iter_t *iter);

/**
 * @brief Get total leaf size.
 * @retval Leaf size include input rank.
 */
int32_t ucg_algo_kntree_get_subtree_size(ucg_algo_kntree_iter_t *iter, ucg_rank_t rank);

#endif