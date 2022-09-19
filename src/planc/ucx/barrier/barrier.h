/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_PLANC_UCX_BARRIER_H_
#define UCG_PLANC_UCX_BARRIER_H_

#include "planc_ucx_def.h"
#include "planc_ucx_context.h"
#include "planc_ucx_group.h"
#include "core/ucg_plan.h"
#include "util/algo/ucg_kntree.h"
#include "util/algo/ucg_rd.h"
#include "util/algo/ucg_rh.h"
#include "util/algo/ucg_ring.h"

typedef struct ucg_planc_ucx_barrier_config {
    int fanin_inter_degree;
    int fanout_inter_degree;
    int fanin_intra_degree;
    int fanout_intra_degree;
} ucg_planc_ucx_barrier_config_t;

typedef struct ucg_planc_ucx_fanin_config {
    int kntree_degree;
} ucg_planc_ucx_fanin_config_t;

/**
 * @brief Barrier op auxiliary information
 *
 * Why union? One op corresponds to one basic algorithm.
 * For the combined algorithm, we can use multiple ops.
 */
typedef struct ucg_planc_ucx_barrier {
    union {
        ucg_algo_rd_iter_t rd_iter;
        ucg_algo_kntree_iter_t fanin_iter;
    };
} ucg_planc_ucx_barrier_t;

void ucg_planc_ucx_barrier_set_plan_attr(ucg_vgroup_t *vgroup,
                                         ucg_plan_attr_t *default_plan_attr);

ucg_planc_ucx_op_t *ucg_planc_ucx_barrier_rd_op_new(ucg_planc_ucx_group_t *ucx_group,
                                                    ucg_vgroup_t *vgroup,
                                                    const ucg_coll_args_t *args);

ucg_status_t ucg_planc_ucx_barrier_rd_prepare(ucg_vgroup_t *vgroup,
                                              const ucg_coll_args_t *args,
                                              ucg_plan_op_t **op);
ucg_status_t ucg_planc_ucx_barrier_na_rd_and_bntree_prepare(ucg_vgroup_t *vgroup,
                                                            const ucg_coll_args_t *args,
                                                            ucg_plan_op_t **op);
ucg_status_t ucg_planc_ucx_barrier_sa_rd_and_bntree_prepare(ucg_vgroup_t *vgroup,
                                                            const ucg_coll_args_t *args,
                                                            ucg_plan_op_t **op);
ucg_status_t ucg_planc_ucx_barrier_na_rd_and_kntree_prepare(ucg_vgroup_t *vgroup,
                                                            const ucg_coll_args_t *args,
                                                            ucg_plan_op_t **op);
ucg_status_t ucg_planc_ucx_barrier_sa_rd_and_kntree_prepare(ucg_vgroup_t *vgroup,
                                                            const ucg_coll_args_t *args,
                                                            ucg_plan_op_t **op);
ucg_status_t ucg_planc_ucx_barrier_na_kntree_prepare(ucg_vgroup_t *vgroup,
                                                       const ucg_coll_args_t *args,
                                                       ucg_plan_op_t **op);
ucg_status_t ucg_planc_ucx_barrier_sa_kntree_prepare(ucg_vgroup_t *vgroup,
                                                       const ucg_coll_args_t *args,
                                                       ucg_plan_op_t **op);
ucg_status_t ucg_planc_ucx_barrier_na_inc_prepare(ucg_vgroup_t *vgroup,
                                                    const ucg_coll_args_t *args,
                                                    ucg_plan_op_t **op);
ucg_status_t ucg_planc_ucx_barrier_sa_inc_prepare(ucg_vgroup_t *vgroup,
                                                    const ucg_coll_args_t *args,
                                                    ucg_plan_op_t **op);

#endif