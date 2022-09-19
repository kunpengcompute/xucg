/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_PLANC_UCX_BCAST_H_
#define UCG_PLANC_UCX_BCAST_H_

#include "planc_ucx_def.h"
#include "planc_ucx_context.h"
#include "planc_ucx_group.h"
#include "core/ucg_plan.h"
#include "core/ucg_topo.h"
#include "util/algo/ucg_kntree.h"
#include "util/algo/ucg_ring.h"


typedef struct ucg_planc_ucx_bcast_config {
    /* configuration of kntree bcast */
    int kntree_degree;
    uint8_t root_adjust;
    /* configuration of net-aware kntree bcast */
    int nta_kntree_inter_degree;
    int nta_kntree_intra_degree;
    /* configuration of node-aware kntree bcast */
    int na_kntree_inter_degree;
    int na_kntree_intra_degree;
} ucg_planc_ucx_bcast_config_t;

/**
 * @brief Bcast op auxiliary information
 */
typedef struct ucg_planc_ucx_bcast {
    union {
        ucg_algo_kntree_iter_t kntree_iter;
        ucg_algo_ring_iter_t ring_iter;
        struct {
            ucg_algo_kntree_iter_t kntree_iter;
            ucg_algo_ring_iter_t ring_iter;
            uint32_t curr_count;
            uint32_t quotient;
        } van_de_geijn;
    };
} ucg_planc_ucx_bcast_t;

void ucg_planc_ucx_bcast_set_plan_attr(ucg_vgroup_t *vgroup,
                                       ucg_plan_attr_t *default_plan_attr);

/* xxx_op_new routines are provided for internal algorithm combination */
ucg_planc_ucx_op_t *ucg_planc_ucx_bcast_kntree_op_new(ucg_planc_ucx_group_t *ucx_group,
                                                      ucg_vgroup_t *vgroup,
                                                      const ucg_coll_args_t *args,
                                                      const ucg_planc_ucx_bcast_config_t *config);
ucg_plan_meta_op_t *ucg_planc_ucx_bcast_na_kntree_op_new(ucg_planc_ucx_group_t *ucx_group,
                                                         ucg_vgroup_t *vgroup,
                                                         const ucg_coll_args_t *args,
                                                         const ucg_planc_ucx_bcast_config_t *config);

/* xxx_prepare routines are provided for core layer to create collective request */
ucg_status_t ucg_planc_ucx_bcast_bntree_prepare(ucg_vgroup_t *vgroup,
                                                const ucg_coll_args_t *args,
                                                ucg_plan_op_t **op);
ucg_status_t ucg_planc_ucx_bcast_kntree_prepare(ucg_vgroup_t *vgroup,
                                                const ucg_coll_args_t *args,
                                                ucg_plan_op_t **op);
ucg_status_t ucg_planc_ucx_bcast_na_bntree_prepare(ucg_vgroup_t *vgroup,
                                                   const ucg_coll_args_t *args,
                                                   ucg_plan_op_t **op);
ucg_status_t ucg_planc_ucx_bcast_na_kntree_and_bntree_prepare(ucg_vgroup_t *vgroup,
                                                              const ucg_coll_args_t *args,
                                                              ucg_plan_op_t **op);
ucg_status_t ucg_planc_ucx_bcast_na_kntree_prepare(ucg_vgroup_t *vgroup,
                                                   const ucg_coll_args_t *args,
                                                   ucg_plan_op_t **op);
ucg_status_t ucg_planc_ucx_bcast_na_inc_prepare(ucg_vgroup_t *vgroup,
                                                const ucg_coll_args_t *args,
                                                ucg_plan_op_t **op);
ucg_status_t ucg_planc_ucx_bcast_ring_prepare(ucg_vgroup_t *group,
                                              const ucg_coll_args_t *args,
                                              ucg_plan_op_t **op);
ucg_status_t ucg_planc_ucx_bcast_nta_kntree_prepare(ucg_vgroup_t *group,
                                                    const ucg_coll_args_t *args,
                                                    ucg_plan_op_t **op);
ucg_status_t ucg_planc_ucx_bcast_van_de_geijn_prepare(ucg_vgroup_t *vgroup,
                                                      const ucg_coll_args_t *args,
                                                      ucg_plan_op_t **op);

/* helper for adding op to meta op. */
ucg_status_t ucg_planc_ucx_bcast_add_adjust_root_op(ucg_plan_meta_op_t *meta_op,
                                                    ucg_planc_ucx_group_t *ucx_group,
                                                    ucg_vgroup_t *vgroup,
                                                    const ucg_coll_args_t *args,
                                                    const ucg_planc_ucx_bcast_config_t *config);
ucg_status_t ucg_planc_ucx_bcast_add_topo_group_kntree_op(ucg_plan_meta_op_t *meta_op,
                                                          ucg_planc_ucx_group_t *ucx_group,
                                                          ucg_vgroup_t *vgroup,
                                                          const ucg_coll_args_t *args,
                                                          const ucg_planc_ucx_bcast_config_t *config,
                                                          ucg_topo_group_type_t type);

#endif