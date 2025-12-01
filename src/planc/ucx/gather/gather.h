/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2024. All rights reserved.
 */

#ifndef UCG_PLANC_UCX_GATHER_H_
#define UCG_PLANC_UCX_GATHER_H_

#include "planc/ucx/planc_ucx_def.h"
#include "core/ucg_plan.h"
#include "util/algo/ucg_kntree.h"

typedef struct ucg_planc_ucx_gather_config {
    int kntree_degree;
    int na_kntree_inter_degree;
    int na_kntree_intra_degree;
} ucg_planc_ucx_gather_config_t;

typedef struct ucg_planc_ucx_gather {
    union {
        struct {
            ucg_planc_ucx_op_t *op;
            ucg_coll_args_t args;
            int32_t op_trigged;
        } gatherv;
    };
} ucg_planc_ucx_gather_t;

const ucg_plan_policy_t *ucg_planc_ucx_get_gather_plan_policy(ucg_planc_ucx_node_level_t node_level,
                                                               ucg_planc_ucx_ppn_level_t ppn_level);

ucg_status_t ucg_planc_ucx_gather_linear_op_progress(ucg_plan_op_t *ucg_op);

ucg_planc_ucx_op_t *ucg_planc_ucx_gather_linear_op_new(ucg_planc_ucx_group_t *ucx_group,
                                                        ucg_vgroup_t *vgroup,
                                                        const ucg_coll_args_t *args);

ucg_status_t ucg_planc_ucx_gather_linear_prepare(ucg_vgroup_t *vgroup,
                                                  const ucg_coll_args_t *args,
                                                  ucg_plan_op_t **op);

ucg_status_t ucg_planc_ucx_gather_na_linear_prepare(ucg_vgroup_t *vgroup,
                                                     const ucg_coll_args_t *args,
                                                     ucg_plan_op_t **op);

ucg_status_t ucg_planc_ucx_gather_kntree_prepare(ucg_vgroup_t *vgroup,
                                                  const ucg_coll_args_t *args,
                                                  ucg_plan_op_t **op);

ucg_planc_ucx_op_t *ucg_planc_ucx_gather_kntree_op_new(ucg_planc_ucx_group_t *ucx_group,
                                                        ucg_vgroup_t *vgroup,
                                                        const ucg_coll_args_t *args);

ucg_status_t ucg_planc_ucx_gather_na_kntree_prepare(ucg_vgroup_t *vgroup,
                                                     const ucg_coll_args_t *args,
                                                     ucg_plan_op_t **op);
#endif // UCG_PLANC_UCX_GATHER_H_