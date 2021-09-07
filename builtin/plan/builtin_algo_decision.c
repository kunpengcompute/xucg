/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019-2021.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#include <ucs/debug/log.h>
#include <ucs/debug/assert.h>
#include <ucg/api/ucg_mpi.h>
#include <ucg/builtin/ops/builtin_ops.h>

#include "builtin_algo_decision.h"

static const char *coll_type_str_array[COLL_TYPE_NUMS] = {
    "barrier",
    "bcast",
    "allreduce",
    "alltoallv",
};

typedef struct {
    int low;
    int up;
} boundary_t;

boundary_t boundary[COLL_TYPE_NUMS] = {
    {UCG_ALGORITHM_BARRIER_AUTO_DECISION, UCG_ALGORITHM_BARRIER_LAST},
    {UCG_ALGORITHM_BCAST_BMTREE, UCG_ALGORITHM_BCAST_LAST},
    {UCG_ALGORITHM_ALLREDUCE_AUTO_DECISION, UCG_ALGORITHM_ALLREDUCE_LAST},
    {UCG_ALGORITHM_ALLTOALLV_AUTO_DECISION, UCG_ALGORITHM_ALLTOALLV_LAST},
};

static inline int ucg_builtin_get_valid_algo(int algo, int lb, int ub)
{
    if (algo > lb && algo < ub) {
        return algo;
    }
    return 0;
}

static int ucg_builtin_get_custom_algo(coll_type_t coll_type)
{
    ucg_builtin_config_t *config = (ucg_builtin_config_t *)ucg_builtin_component.plan_config;
    int algo = 0;
    switch (coll_type) {
        case COLL_TYPE_BARRIER:
            algo = (int)config->barrier_algorithm;
            break;
        case COLL_TYPE_BCAST:
            algo = (int)config->bcast_algorithm;
            break;
        case COLL_TYPE_ALLREDUCE:
            algo = (int)config->allreduce_algorithm;
            break;
        case COLL_TYPE_ALLTOALLV:
            algo = (int)config->alltoallv_algorithm;
            break;
        default:
            break;
    }
    return ucg_builtin_get_valid_algo(algo, boundary[coll_type].low, boundary[coll_type].up);
}
STATIC_GTEST coll_type_t ucg_builtin_get_coll_type(const ucg_collective_type_t *coll_type)
{
    if (coll_type->modifiers == ucg_predefined_modifiers[UCG_PRIMITIVE_BARRIER]) {
        return COLL_TYPE_BARRIER;
    }
    if (coll_type->modifiers == ucg_predefined_modifiers[UCG_PRIMITIVE_BCAST]) {
        return COLL_TYPE_BCAST;
    }
    if (coll_type->modifiers == ucg_predefined_modifiers[UCG_PRIMITIVE_ALLREDUCE]) {
        return COLL_TYPE_ALLREDUCE;
    }
    if (coll_type->modifiers == ucg_predefined_modifiers[UCG_PRIMITIVE_ALLTOALLV]) {
        return COLL_TYPE_ALLTOALLV;
    }
    return COLL_TYPE_NUMS;
}

int ucg_builtin_algo_decision(const ucg_group_params_t *group_params, const ucg_collective_params_t *coll_params)
{
    int algo, algo_final;
    algo = ucg_builtin_get_custom_algo(coll_params->coll_type);
    ucs_info("current coll_type is %s", coll_type_str_array[coll_params->coll_type]);
    /* Algorithm auto select occurs only if the user does not provide a valid algorithm parameter */
    if (algo) {
        ucs_info("custom algorithm is %d", algo);
    } else {
        algo = ucg_builtin_algo_auto_select(group_params, coll_params);
        ucs_info("auto select algorithm is %d", algo);
    }
    /* Check whether this algo can use, fall back if not */
    algo_final = ucg_builtin_algo_check_fallback(group_params, coll_params, algo);
    ucs_info("final algorithm is %d", algo_final);
    return algo_final;
}
