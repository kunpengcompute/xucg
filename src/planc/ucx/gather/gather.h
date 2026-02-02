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
            ucg_planc_ucx_op_t *inter_op;
            ucg_planc_ucx_op_t *intra_op;
            ucg_planc_ucx_op_t *inter_sendcount_op;  // send recvcount and recvtype size
            int32_t is_adjust_root_op_trigged;
            int32_t is_inter_op_trigged;
            int32_t is_intra_op_trigged;
            int32_t inter_sendcount_op_trigged;
            int32_t is_node_leader;
            int32_t node_cnt;
            int32_t ppn;
            int32_t *intra_rbuf; // intra node leader recvbuf
            int32_t recvcount_type[2]; // recvcount and recvtype
        } topo_aware;
        struct {
            ucg_algo_kntree_iter_t kntree_iter;
            int32_t first_trigger;
            /**
             * staging_count indicates the number of rank data in staging area.
             * For example:
             *      degree=2
             *         0
             *      / / \ \
             *     8 4   2 1
             *     | |\  |
             *     9 6 5 3
             *       |
             *       7
             * The staging_count of rank 4 is 3, means staging area stores the data of
             * rank 5,6,7 (sequential increment).
             */
            uint32_t staging_count;
            /* recvcount of root rank*/
            int recvcount;
            /* recvtype true length of root rank*/
            int32_t rctype_size;
            int32_t *childlist;
            int32_t child_count;
        } kntree;
        
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
                                                        const ucg_coll_args_t *args,
                                                        const ucg_planc_ucx_gather_config_t *config);

ucg_status_t ucg_planc_ucx_gather_na_kntree_prepare(ucg_vgroup_t *vgroup,
                                                     const ucg_coll_args_t *args,
                                                     ucg_plan_op_t **op);
#endif // UCG_PLANC_UCX_GATHER_H_