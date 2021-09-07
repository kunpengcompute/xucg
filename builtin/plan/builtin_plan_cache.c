/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019-2021.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#include <ucs/debug/log.h>

#include "builtin_plan.h"
#include "builtin_plan_cache.h"

#define ROOT_NUMS 96

const static int cache_size[COLL_TYPE_NUMS] = {
    UCG_ALGORITHM_BARRIER_LAST - 1,
    (UCG_ALGORITHM_BCAST_LAST - 1) * ROOT_NUMS,
    UCG_ALGORITHM_ALLREDUCE_LAST - 1,
    UCG_ALGORITHM_ALLTOALLV_LAST - 1,
};

static inline unsigned ucg_collective_compare_basic_coll_params(const ucg_collective_params_t *left,
                                                                const ucg_collective_params_t *right)
{
    return !memcmp(left, right, sizeof(ucg_collective_params_t));
}

static inline unsigned ucg_collective_compare_full_coll_params(ucg_group_h group,
                                                               const ucg_collective_params_t *left,
                                                               const ucg_collective_params_t *right)
{
    ucg_group_member_index_t member_count = ucg_group_get_member_count(group);
    int send_counts_len = member_count * sizeof(int);

    unsigned is_same = ucg_collective_compare_basic_coll_params(left, right) &&
                       (!memcmp(left->send.counts, right->send.counts, send_counts_len)) &&
                       (!memcmp(left->send.displs, right->send.displs, send_counts_len)) &&
                       (!memcmp(left->recv.counts, right->recv.counts, send_counts_len)) &&
                       (!memcmp(left->recv.displs, right->recv.displs, send_counts_len));
    return is_same;
}

ucs_status_t ucg_builtin_pcache_init(ucg_group_h group)
{
    coll_type_t coll_type;
    size_t alloc_size;

    for (coll_type = 0; coll_type < COLL_TYPE_NUMS; coll_type++) {
        group->builtin_pcache[coll_type] = NULL;
        alloc_size = sizeof(ucg_plan_t *) * cache_size[coll_type];
        group->builtin_pcache[coll_type] = (ucg_plan_t **)UCS_ALLOC_CHECK(alloc_size, "builtin_pcache");
        memset(group->builtin_pcache[coll_type], 0, alloc_size);
    }
    return UCS_OK;
}

void ucg_builtin_pcache_destroy(ucg_group_h group)
{
    coll_type_t coll_type;
    for (coll_type = 0; coll_type < COLL_TYPE_NUMS; coll_type++) {
        if (group->builtin_pcache[coll_type]) {
            ucs_free(group->builtin_pcache[coll_type]);
            group->builtin_pcache[coll_type] = NULL;
        }
    }
}

static ucg_plan_t *ucg_builtin_alltoallv_pcache_find(const ucg_group_h group, int algo,
                                                     const ucg_collective_params_t *coll_params)
{
    return NULL;
}

ucg_plan_t *ucg_builtin_pcache_find(const ucg_group_h group, int algo,
                                    const ucg_collective_params_t *coll_params)
{
    coll_type_t coll_type = coll_params->coll_type;
    ucg_plan_t *plan = NULL;
    int pos;

    switch (coll_type) {
        case COLL_TYPE_BCAST:
            pos = (coll_params->type.root % ROOT_NUMS) * (UCG_ALGORITHM_BCAST_LAST - 1) + algo - 1;
            plan = group->builtin_pcache[coll_type][pos];
            return (plan != NULL && plan->type.root != coll_params->type.root) ? NULL : plan;

        case COLL_TYPE_ALLTOALLV:
            return ucg_builtin_alltoallv_pcache_find(group, algo, coll_params);

        default:
            return group->builtin_pcache[coll_type][algo - 1];
    }
}

void ucg_builtin_pcache_update(ucg_group_h group, ucg_plan_t *plan, int algo,
                                const ucg_collective_params_t *coll_params)
{
    coll_type_t coll_type = coll_params->coll_type;
    ucg_builtin_plan_t *builtin_plan = NULL;
    ucg_plan_t *plan_old = NULL;
    int pos;

    switch (coll_type) {
        case COLL_TYPE_BCAST:
            pos = (coll_params->type.root % ROOT_NUMS) * (UCG_ALGORITHM_BCAST_LAST - 1) + algo - 1;
            plan_old = group->builtin_pcache[coll_type][pos];
            group->builtin_pcache[coll_type][pos] = plan;
            break;
        default:
            plan_old = group->builtin_pcache[coll_type][algo - 1];
            group->builtin_pcache[coll_type][algo - 1] = plan;
            break;
    }
    if (plan_old) {
        builtin_plan = ucs_derived_of(plan_old, ucg_builtin_plan_t);
        ucg_builtin_destroy_plan(builtin_plan, group);
    }
}
