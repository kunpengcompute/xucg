/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2024. All rights reserved.
 */

#ifndef UCG_PLANC_UCX_GATHERV_H_
#define UCG_PLANC_UCX_GATHERV_H_

#include "planc/ucx/planc_ucx_def.h"
#include "core/ucg_plan.h"
#include "util/algo/ucg_kntree.h"
#include "util/ucg_shmem_segment.h"
#include "util/ucg_shmem_pool_list.h"

typedef struct ucg_planc_ucx_gatherv_config {
    int kntree_degree;
    int na_kntree_inter_degree;
    int na_kntree_intra_degree;
} ucg_planc_ucx_gatherv_config_t;

typedef struct ucg_planc_ucx_gatherv_sm_args {
    ucg_coll_args_t origin_coll_args;
    ucg_coll_args_t phase_bcast;
    ucg_shmem_remote_fd_t shmem_remote_fd;
    ucg_shmem_pool_t *shmem_pool;
    ucg_shmem_segment_t *shmem_segment;
    uint32_t is_extern_mp;
    /* allgatherv-like: set all non-root peer's recvbuf from shared memory recvbuf of root */
    uint32_t is_allgatherv_like;
    uint32_t is_initialized;
    /* sm ddt related variables */
    const ucg_ddt_args_t *gatherw_ddt_args;
} ucg_planc_ucx_gatherv_sm_args_t;

typedef struct ucg_planc_ucx_gatherv {
    union {
        struct {
            ucg_planc_ucx_op_t *inter_op;
            ucg_planc_ucx_op_t *intra_op;
            ucg_planc_ucx_op_t *inter_sendcount_op;
            ucg_planc_ucx_op_t *intra_sendcount_op;
            int32_t is_adjust_root_op_trigged;
            int32_t is_inter_op_trigged;
            int32_t is_intra_op_trigged;
            int32_t intra_sendcount_op_trigged;
            int32_t inter_sendcount_op_trigged;
            int32_t is_node_leader;
            int32_t node_cnt;
            int32_t ppn;
            int32_t* intra_sendcounts;
            int32_t intra_total_count;
            ucg_coll_args_t intra_gatherv_sc_args;
            ucg_coll_args_t inter_gatherv_sc_args;
            ucg_coll_args_t intra_gatherv_args;
            ucg_coll_args_t inter_gatherv_args;
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
            /* staging_displs[] indicates the start address of each rank in staging area.*/
            int32_t *staging_displs;
            /* recvcounts[] of root rank*/
            int32_t *recvcounts;
            /* recvtype true length of root rank*/
            int32_t rctype_size;
            int32_t *childlist;
            int32_t child_count;
        } kntree;
        ucg_planc_ucx_gatherv_sm_args_t sm_args;
    };
} ucg_planc_ucx_gatherv_t;

const ucg_plan_policy_t *ucg_planc_ucx_get_gatherv_plan_policy(ucg_planc_ucx_node_level_t node_level,
                                                               ucg_planc_ucx_ppn_level_t ppn_level);

ucg_status_t ucg_planc_ucx_gatherv_linear_op_progress(ucg_plan_op_t *ucg_op);

ucg_planc_ucx_op_t *ucg_planc_ucx_gatherv_linear_op_new(ucg_planc_ucx_group_t *ucx_group,
                                                        ucg_vgroup_t *vgroup,
                                                        const ucg_coll_args_t *args);

ucg_status_t ucg_planc_ucx_gatherv_linear_prepare(ucg_vgroup_t *vgroup,
                                                  const ucg_coll_args_t *args,
                                                  ucg_plan_op_t **op);

ucg_status_t ucg_planc_ucx_gatherv_na_linear_prepare(ucg_vgroup_t *vgroup,
                                                     const ucg_coll_args_t *args,
                                                     ucg_plan_op_t **op);

ucg_status_t ucg_planc_ucx_gatherv_kntree_prepare(ucg_vgroup_t *vgroup,
                                                  const ucg_coll_args_t *args,
                                                  ucg_plan_op_t **op);

ucg_planc_ucx_op_t *ucg_planc_ucx_gatherv_kntree_op_new(ucg_planc_ucx_group_t *ucx_group,
                                                        ucg_vgroup_t *vgroup,
                                                        const ucg_coll_args_t *args,
                                                        const ucg_planc_ucx_gatherv_config_t *config);

ucg_status_t ucg_planc_ucx_gatherv_na_kntree_prepare(ucg_vgroup_t *vgroup,
                                                     const ucg_coll_args_t *args,
                                                     ucg_plan_op_t **op);

ucg_planc_ucx_op_t *ucg_planc_ucx_gatherv_na_kntree_op_new(ucg_planc_ucx_group_t *ucx_group,
                                                           ucg_vgroup_t *vgroup,
                                                           const ucg_coll_args_t *args,
                                                           ucg_planc_ucx_gatherv_config_t *config);

void *ucg_planc_ucx_gatherv_get_recvbuf_by_mp(ucg_shmem_pool_t *shmem_pool, int group_size);

ucg_status_t ucg_planc_ucx_gatherv_sm_op_progress(ucg_plan_op_t *ucg_op);

ucg_planc_ucx_op_t *ucg_planc_ucx_gatherv_sm_op_new(ucg_planc_ucx_group_t *ucx_group,
                                                    ucg_vgroup_t *vgroup,
                                                    const ucg_coll_args_t *args);

ucg_status_t ucg_planc_ucx_gatherv_sm_prepare(ucg_vgroup_t *vgroup,
                                              const ucg_coll_args_t *args,
                                              ucg_plan_op_t **op);

void ucg_planc_ucx_gatherv_sm_set_mp(ucg_planc_ucx_op_t *ucx_op,
                                     ucg_shmem_pool_t *shmem_pool,
                                     uint32_t is_extern_mp);

void ucg_planc_ucx_gatherv_sm_set_allgatherv_like(ucg_planc_ucx_op_t *ucx_op,
                                                  uint32_t is_allgatherv_like);

ucg_planc_ucx_op_t *ucg_planc_ucx_gatherw_ddt_sm_op_new(ucg_planc_ucx_group_t *ucx_group,
                                                        ucg_vgroup_t *vgroup,
                                                        const ucg_coll_args_t *coll_args,
                                                        const ucg_ddt_args_t *gatherw_ddt_args);

#endif // UCG_PLANC_UCX_GATHERV_H_