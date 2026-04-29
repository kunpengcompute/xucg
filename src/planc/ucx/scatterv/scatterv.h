/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2024. All rights reserved.
 */

#ifndef UCG_PLANC_UCX_SCATTERV_H_
#define UCG_PLANC_UCX_SCATTERV_H_

#include "planc/ucx/planc_ucx_def.h"
#include "planc/ucx/planc_ucx_context.h"
#include "util/algo/ucg_kntree.h"
#include "core/ucg_topo.h"
#include "core/ucg_plan.h"
#include "util/ucg_shmem_segment.h"
#include "util/ucg_shmem_pool_list.h"

#define UCG_PLANC_UCX_NA_KNTREE_CHECK_GOTO(_stmt, _label1, _label2) \
    do { \
        if ((_stmt) != UCG_OK) { \
            if ((_stmt) != UCG_INPROGRESS) { \
                goto _label2; \
            } else { \
                goto _label1; \
            } \
        } \
    } while (0)

typedef struct ucg_planc_ucx_scatterv_na_kntree_args {
    ucg_coll_args_t scatterv_origin;
    ucg_coll_args_t scatterv_inter;
    ucg_coll_args_t scatterv_intra;
    uint32_t global_nprocs;
    uint32_t group_nprocs;
    int8_t is_initialized;
    /* sendbuffer size of root rank */
    int32_t sendbuf_size;
    /* sendtype true length of root rank*/
    uint64_t sdtype_size;
    int32_t *displs;
    int32_t *sendcounts;
    ucg_topo_group_t *intra_node_group;
    ucg_topo_group_t *node_leader_group;
    ucg_vgroup_t *vgroup_origin;
    ucg_algo_kntree_iter_t kntree_iter;
} ucg_planc_ucx_scatterv_na_kntree_args_t;

typedef struct ucg_planc_ucx_scatterv_sm_args {
    ucg_coll_args_t origin_coll_args;
    ucg_coll_args_t phase_bcast;
    ucg_shmem_remote_fd_t shmem_remote_fd;
    ucg_shmem_pool_t *shmem_pool;
    ucg_shmem_segment_t *shmem_segment;
    uint32_t is_extern_mp;
    uint32_t is_initialized;
    /* sm ddt related variables */
    const ucg_ddt_args_t *scatterw_ddt_args;
} ucg_planc_ucx_scatterv_sm_args_t;

typedef struct ucg_planc_ucx_scatterv {
    union {
        ucg_planc_ucx_scatterv_sm_args_t sm_args;
        struct {
            int32_t idx;
            uint8_t send_type;
        } linear;
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
            /* sendcounts[] of root rank*/
            int32_t *sendcounts;
            /* sendtype true length of root rank*/
            int32_t sdtype_size;
        } kntree;
        ucg_planc_ucx_scatterv_na_kntree_args_t na_kntree;
    };
} ucg_planc_ucx_scatterv_t;

typedef struct ucg_planc_ucx_scatterv_config {
    size_t min_bsend;   /* for linear, closed boundary */
    size_t max_bsend;   /* for linear, closed boundary */
    int kntree_degree;  /* for kntree */
    /* configuration of node-aware kntree scatterv */
    int na_kntree_inter_degree;
    int na_kntree_intra_degree;
} ucg_planc_ucx_scatterv_config_t;

const ucg_plan_policy_t *ucg_planc_ucx_get_scatterv_plan_policy(ucg_planc_ucx_node_level_t node_level,
                                                                ucg_planc_ucx_ppn_level_t ppn_level);

ucg_planc_ucx_op_t *ucg_planc_ucx_scatterv_kntree_op_new(ucg_planc_ucx_group_t *ucx_group,
                                                         ucg_vgroup_t *vgroup,
                                                         const ucg_coll_args_t *args,
                                                         const ucg_planc_ucx_scatterv_config_t *config);

ucg_status_t ucg_planc_ucx_scatterv_kntree_prepare(ucg_vgroup_t *vgroup,
                                                   const ucg_coll_args_t *args,
                                                   ucg_plan_op_t **op);

ucg_status_t ucg_planc_ucx_scatterv_linear_op_progress(ucg_plan_op_t *ucg_op);

ucg_planc_ucx_op_t *ucg_planc_ucx_scatterv_linear_op_new(ucg_planc_ucx_group_t *ucx_group,
                                                         ucg_vgroup_t *vgroup,
                                                         const ucg_coll_args_t *args,
                                                         const ucg_planc_ucx_scatterv_config_t *config);

ucg_status_t ucg_planc_ucx_scatterv_linear_prepare(ucg_vgroup_t *vgroup,
                                                   const ucg_coll_args_t *args,
                                                   ucg_plan_op_t **op);

ucg_status_t ucg_planc_ucx_scatterv_na_kntree_prepare(ucg_vgroup_t *vgroup,
                                                      const ucg_coll_args_t *args,
                                                      ucg_plan_op_t **op);

void *ucg_planc_ucx_scatterv_get_sendbuf_by_mp(ucg_shmem_pool_t *shmem_pool, int group_size);

ucg_status_t ucg_planc_ucx_scatterv_sm_op_progress(ucg_plan_op_t *ucg_op);

ucg_planc_ucx_op_t *ucg_planc_ucx_scatterv_sm_op_new(ucg_planc_ucx_group_t *ucx_group,
                                                     ucg_vgroup_t *vgroup,
                                                     const ucg_coll_args_t *args);

ucg_status_t ucg_planc_ucx_scatterv_sm_prepare(ucg_vgroup_t *vgroup,
                                               const ucg_coll_args_t *args,
                                               ucg_plan_op_t **op);

void ucg_planc_ucx_scatterv_sm_set_mp(ucg_planc_ucx_op_t *ucx_op,
                                      ucg_shmem_pool_t *shmem_pool,
                                      uint32_t is_extern_mp);

ucg_planc_ucx_op_t *ucg_planc_ucx_scatterw_ddt_sm_op_new(ucg_planc_ucx_group_t *ucx_group,
                                                         ucg_vgroup_t *vgroup,
                                                         const ucg_coll_args_t *coll_args,
                                                         const ucg_ddt_args_t *scatterw_ddt_args);
#endif