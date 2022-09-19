/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_PLANC_UCX_REDUCE_H_
#define UCG_PLANC_UCX_REDUCE_H_

#include "planc_ucx_def.h"
#include "planc_ucx_context.h"
#include "planc_ucx_group.h"
#include "planc_ucx_p2p.h"
#include "core/ucg_plan.h"
#include "util/algo/ucg_kntree.h"

typedef struct ucg_planc_ucx_reduce_config {
    int kntree_degree;
} ucg_planc_ucx_reduce_config_t;

typedef struct ucg_planc_ucx_reduce {
    ucg_algo_kntree_iter_t kntree_iter;
    ucg_planc_ucx_p2p_req_t **requests;
    int requests_count;
    uint64_t req_bitmap;
} ucg_planc_ucx_reduce_t;

void ucg_planc_ucx_reduce_set_plan_attr(ucg_vgroup_t *vgroup,
                                        ucg_plan_attr_t *default_plan_attr);

/* xxx_op_new routines are provided for internal algorithm combination */
ucg_planc_ucx_op_t *ucg_planc_ucx_reduce_kntree_op_new(ucg_planc_ucx_group_t *ucx_group,
                                                       ucg_vgroup_t *vgroup,
                                                       const ucg_coll_args_t *args,
                                                       ucg_planc_ucx_reduce_config_t *config);

/* xxx_prepare routines are provided for core layer to creat collective request */
ucg_status_t ucg_planc_ucx_reduce_kntree_prepare(ucg_vgroup_t *vgroup,
                                                 const ucg_coll_args_t *args,
                                                 ucg_plan_op_t **op);

#endif