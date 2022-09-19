/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_PLANC_UCX_PLAN_H_
#define UCG_PLANC_UCX_PLAN_H_

#include "planc_ucx_context.h"
#include "planc_ucx_group.h"
#include "planc_ucx_p2p.h"
#include "core/ucg_plan.h"
#include "core/ucg_dt.h"
#include "core/ucg_group.h"
#include "bcast/bcast.h"
#include "allreduce/allreduce.h"
#include "barrier/barrier.h"
#include "allgatherv/allgatherv.h"
#include "reduce/reduce.h"
#include "scatterv/scatterv.h"
#include "gatherv/gatherv.h"

#ifndef UCG_PLANC_UCX_DEFAULT_SCORE
    #define UCG_PLANC_UCX_DEFAULT_SCORE 90
#endif

#define UCG_PLANC_UCX_BUILTIN_ALGO_REGISTER(_coll_type, _config, _size) \
    UCG_PLANC_UCX_ALGO_REGISTER(_coll_type, UCX_BUILTIN, _config, _size)

#define UCG_PLANC_UCX_ALGO_REGISTER(_coll_type, _module_type, _config, _size) \
    UCG_STATIC_INIT { \
        ucx_algo_global_table[_coll_type][_module_type].config_table = _config; \
        ucx_algo_global_table[_coll_type][_module_type].size = _size; \
    } \
    UCG_STATIC_CLEANUP { \
        ucx_algo_global_table[_coll_type][_module_type].config_table = NULL; \
        ucx_algo_global_table[_coll_type][_module_type].size = 0; \
    }

typedef struct ucg_planc_ucx_op {
    ucg_plan_op_t super;
    ucg_planc_ucx_group_t *ucx_group;
    ucg_planc_ucx_p2p_state_t p2p_state;
    uint16_t tag;
    /* Abstracted fields, the concrete op determines how to use these. */
    uint64_t flags;
    void *staging_area;
    /* Fields related to collective operations. */
    union {
        ucg_planc_ucx_bcast_t bcast;
        ucg_planc_ucx_allreduce_t allreduce;
        ucg_planc_ucx_barrier_t barrier;
        ucg_planc_ucx_allgatherv_t allgatherv;
        ucg_planc_ucx_reduce_t reduce;
        ucg_planc_ucx_scatterv_t scatterv;
    };
} ucg_planc_ucx_op_t;

typedef struct ucg_planc_ucx_algo {
    int id; /**< algo id, starts from 1 and increases continuously */
    const char *desc;
    ucg_plan_prepare_func_t prepare;
} ucg_planc_ucx_algo_t;

typedef struct ucg_planc_ucx_algo_policy {
    int algo_id; /**< algo id, starts from 1 and increases continuously */
    ucg_plan_range_t range;
} ucg_planc_ucx_algo_policy_t;

typedef struct ucg_planc_ucx_algo_table {
    ucg_config_field_t *config_table;
    size_t size;
} ucg_planc_ucx_algo_table_t;

UCG_PLAN_ATTR_TABLE_DECLARE(ucg_planc_ucx);

static inline ucg_status_t ucg_planc_ucx_op_discard(ucg_plan_op_t *ucg_op)
{
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);

    if (op->staging_area != NULL) {
        ucg_free(op->staging_area);
    }
    UCG_CLASS_DESTRUCT(ucg_plan_op_t, &op->super);
    ucg_mpool_put(op);
    return UCG_OK;
}

static inline void ucg_planc_ucx_op_init(ucg_planc_ucx_op_t *op,
                                         ucg_planc_ucx_group_t *ucx_group)
{
    op->ucx_group = ucx_group;
    ucg_planc_ucx_p2p_state_reset(&op->p2p_state);
    op->flags = 0;
    op->staging_area = NULL;
    return;
}

static inline void ucg_planc_ucx_op_reset(ucg_planc_ucx_op_t *op)
{
    op->super.super.status = UCG_INPROGRESS;
    ucg_planc_ucx_p2p_state_reset(&op->p2p_state);
    /* The request ID is used as the tag to ensure that the messages
       in the same op can be correctly matched. */
    ucg_assert(op->super.super.id != UCG_GROUP_INVALID_REQ_ID);
    op->tag = op->super.super.id;
    op->flags = 0;
    return;
}

static inline void ucg_planc_ucx_op_set_p2p_params(ucg_planc_ucx_op_t *op,
                                                   ucg_planc_ucx_p2p_params_t *params)
{
    params->ucx_group = op->ucx_group;
    params->state = &op->p2p_state;
    params->request = NULL;
    return;
}

ucg_status_t ucg_planc_ucx_get_plans(ucg_planc_group_h planc_group, ucg_plans_t *plans);

#endif