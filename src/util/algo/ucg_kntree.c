/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "ucg_kntree.h"
#include "util/ucg_helper.h"
#include "util/ucg_math.h"
#include <stdio.h>

static void ucg_algo_kntree_iter_update_kntree_leftmost(ucg_algo_kntree_iter_t *iter)
{
    ucg_rank_t myrank = iter->myrank;
    int size = iter->size;
    int degree = iter->degree;
    int stride = iter->subsize / degree;
    while (stride > 0) {
        for (; iter->child_idx < degree; ++iter->child_idx) {
            int child = myrank + stride * iter->child_idx;
            if (child < size) {
                iter->child = (child + iter->root) % size;
                return;
            }
        }
        /* move to next smaller sub-kntree */
        iter->subsize = stride;
        iter->child_idx = 1;
        stride /= degree;
    }
    iter->child = UCG_INVALID_RANK;
    return;
}

static void ucg_algo_kntree_iter_update_kntree_rightmost(ucg_algo_kntree_iter_t *iter)
{
    ucg_rank_t myrank = iter->myrank;
    int size = iter->size;
    int degree = iter->degree;
    int max_subsize = iter->max_subsize;
    int stride = iter->subsize;
    while (stride < max_subsize) {
        for (int i = degree - iter->child_idx; i > 0; --i, ++iter->child_idx) {
            int child = myrank + stride * i;
            if (child < size) {
                iter->child = (child + iter->root) % size;
                return;
            }
        }
        stride *= degree;
        /* move to next bigger sub-kntree */
        iter->subsize = stride;
        iter->child_idx = 1;
    }
    iter->child = UCG_INVALID_RANK;
    return;
}

static void ucg_algo_kntree_iter_update(ucg_algo_kntree_iter_t *iter)
{
    if (iter->leftmost) {
        ucg_algo_kntree_iter_update_kntree_leftmost(iter);
    } else {
        ucg_algo_kntree_iter_update_kntree_rightmost(iter);
    }
    return;
}

void ucg_algo_kntree_iter_init(ucg_algo_kntree_iter_t *iter, int size, int degree,
                               int root, ucg_rank_t myrank, uint8_t leftmost)
{
    ucg_assert(myrank != UCG_INVALID_RANK);

    ucg_rank_t v_myrank = (myrank - root + size) % size;
    iter->size = size;
    iter->degree = degree;
    iter->root = root;
    iter->myrank = v_myrank;
    iter->leftmost = leftmost;
    /* 1. find my parent */
    iter->parent = UCG_INVALID_RANK;
    int subsize = 1; /* At first, I'm the only member of the sub-kntree and the root. */
    while (subsize < size) {
        /* I'm the root of this sub-kntree, find my parent in the bigger sub-kntree. */
        int next_subsize = subsize * degree;
        if (v_myrank % next_subsize != 0) {
            iter->parent = v_myrank / next_subsize * next_subsize;
            iter->parent = (iter->parent + root) %size;
            break;
        }
        /* I'm the root of the bigger sub-kntree, keep looking. */
        subsize = next_subsize;
    }
    iter->max_subsize = subsize;
    /* 2. update current child. */
    iter->subsize = iter->leftmost ? iter->max_subsize : 1;
    iter->child = UCG_INVALID_RANK;
    iter->child_idx = 1; /* 0 is myself, start from 1. */
    ucg_algo_kntree_iter_update(iter);
    return;
}

void ucg_algo_kntree_iter_reset(ucg_algo_kntree_iter_t *iter)
{
    iter->subsize = iter->leftmost ? iter->max_subsize : 1;
    iter->child = UCG_INVALID_RANK;
    iter->child_idx = 1; /* 0 is myself, start from 1. */
    ucg_algo_kntree_iter_update(iter);
    return;
}

void ucg_algo_kntree_iter_child_inc(ucg_algo_kntree_iter_t *iter)
{
    ++iter->child_idx;
    ucg_algo_kntree_iter_update(iter);
    return;
}

int32_t ucg_algo_kntree_get_subtree_size(ucg_algo_kntree_iter_t *iter, ucg_rank_t rank)
{
    ucg_algo_kntree_iter_t inner_iter;
    ucg_algo_kntree_iter_init(&inner_iter, iter->size, iter->degree,
                              iter->root, rank, 1);
    int32_t count = 1;
    ucg_rank_t peer;
    while ((peer = ucg_algo_kntree_iter_child_value(&inner_iter)) != UCG_INVALID_RANK) {
        count += ucg_algo_kntree_get_subtree_size(&inner_iter, peer);
        ucg_algo_kntree_iter_child_inc(&inner_iter);
    }

    return count;
}