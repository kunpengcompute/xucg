/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_PLANC_UCX_ALLREDUCE_H_
#define UCG_PLANC_UCX_ALLREDUCE_H_

#include "planc_ucx_def.h"
#include "planc_ucx_context.h"
#include "planc_ucx_group.h"
#include "core/ucg_plan.h"
#include "util/algo/ucg_kntree.h"
#include "util/algo/ucg_rd.h"
#include "util/algo/ucg_rh.h"
#include "util/algo/ucg_ring.h"
#include "core/ucg_topo.h"

typedef struct ucg_planc_ucx_allreduce_config {
    int fanin_inter_degree;
    int fanout_inter_degree;
    int fanin_intra_degree;
    int fanout_intra_degree;
    int nta_kntree_inter_degree;
    int nta_kntree_intra_degree;
} ucg_planc_ucx_allreduce_config_t;

typedef struct ucg_planc_ucx_allreduce_rabenseifner_args {
    int32_t mask;
    int32_t step_index;
    uint64_t rank_type;
    ucg_rank_t new_rank;
    int32_t window_size;
    int32_t *send_index;
    int32_t *recv_index;
    int32_t *send_count;
    int32_t *recv_count;
} ucg_planc_ucx_allreduce_rabenseifner_args_t;

/**
 * @brief Allreduce op auxiliary information
 *
 * Why union? One op corresponds to one basic algorithm.
 * For the combined algorithm, we can use multiple ops.
 */
typedef struct ucg_planc_ucx_allreduce {
    union {
        ucg_algo_rd_iter_t rd_iter;
        struct {
            ucg_algo_ring_iter_t iter;
            /* compute count to blocks, the number of blocks is group size.
             * if rank < split_rank, blkcount = large_blkcount, else blkcount = small_blkcount
             */
            int32_t spilt_rank;
            int32_t large_blkcount;
            int32_t small_blkcount;
        } ring;
        ucg_planc_ucx_allreduce_rabenseifner_args_t rabenseifner;
    };
} ucg_planc_ucx_allreduce_t;

void ucg_planc_ucx_allreduce_set_plan_attr(ucg_vgroup_t *vgroup,
                                            ucg_plan_attr_t *default_plan_attr);

/* xxx_op_new routines are provided for internal algorithm combination */
ucg_planc_ucx_op_t *ucg_planc_ucx_allreduce_rd_op_new(ucg_planc_ucx_group_t *ucx_group,
                                                      ucg_vgroup_t *vgroup,
                                                      const ucg_coll_args_t *args);
ucg_planc_ucx_op_t *ucg_planc_ucx_allreduce_reduce_scatter_op_new(ucg_planc_ucx_group_t *ucx_group,
                                                                  ucg_vgroup_t *vgroup,
                                                                  const ucg_coll_args_t *args);
ucg_planc_ucx_op_t *ucg_planc_ucx_allreduce_allgatherv_op_new(ucg_planc_ucx_group_t *ucx_group,
                                                              ucg_vgroup_t *vgroup,
                                                              const ucg_coll_args_t *args);

/* xxx_prepare routines are provided for core layer to creat collective request */
ucg_status_t ucg_planc_ucx_allreduce_rd_prepare(ucg_vgroup_t *vgroup,
                                                const ucg_coll_args_t *args,
                                                ucg_plan_op_t **op);
ucg_status_t ucg_planc_ucx_allreduce_na_rd_and_bntree_prepare(ucg_vgroup_t *vgroup,
                                                              const ucg_coll_args_t *args,
                                                              ucg_plan_op_t **op);
ucg_status_t ucg_planc_ucx_allreduce_sa_rd_and_bntree_prepare(ucg_vgroup_t *vgroup,
                                                              const ucg_coll_args_t *args,
                                                              ucg_plan_op_t **op);
ucg_status_t ucg_planc_ucx_allreduce_ring_prepare(ucg_vgroup_t *vgroup,
                                                  const ucg_coll_args_t *args,
                                                  ucg_plan_op_t **op);
ucg_status_t ucg_planc_ucx_allreduce_na_rd_and_kntree_prepare(ucg_vgroup_t *vgroup,
                                                              const ucg_coll_args_t *args,
                                                              ucg_plan_op_t **op);
ucg_status_t ucg_planc_ucx_allreduce_sa_rd_and_kntree_prepare(ucg_vgroup_t *vgroup,
                                                              const ucg_coll_args_t *args,
                                                              ucg_plan_op_t **op);
ucg_status_t ucg_planc_ucx_allreduce_na_kntree_prepare(ucg_vgroup_t *vgroup,
                                                       const ucg_coll_args_t *args,
                                                       ucg_plan_op_t **op);
ucg_status_t ucg_planc_ucx_allreduce_sa_kntree_prepare(ucg_vgroup_t *vgroup,
                                                       const ucg_coll_args_t *args,
                                                       ucg_plan_op_t **op);
ucg_status_t ucg_planc_ucx_allreduce_na_inc_prepare(ucg_vgroup_t *vgroup,
                                                    const ucg_coll_args_t *args,
                                                    ucg_plan_op_t **op);
ucg_status_t ucg_planc_ucx_allreduce_sa_inc_prepare(ucg_vgroup_t *vgroup,
                                                    const ucg_coll_args_t *args,
                                                    ucg_plan_op_t **op);
ucg_status_t ucg_planc_ucx_allreduce_rabenseifner_prepare(ucg_vgroup_t *vgroup,
                                                          const ucg_coll_args_t *args,
                                                          ucg_plan_op_t **op);
ucg_status_t ucg_planc_ucx_allreduce_na_rabenseifner_prepare(ucg_vgroup_t *vgroup,
                                                             const ucg_coll_args_t *args,
                                                             ucg_plan_op_t **op);
ucg_status_t ucg_planc_ucx_allreduce_sa_rabenseifner_prepare(ucg_vgroup_t *vgroup,
                                                             const ucg_coll_args_t *args,
                                                             ucg_plan_op_t **op);
ucg_status_t ucg_planc_ucx_allreduce_nta_kntree_prepare(ucg_vgroup_t *vgroup,
                                                        const ucg_coll_args_t *args,
                                                        ucg_plan_op_t **op);

#endif