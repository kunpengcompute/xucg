/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2024. All rights reserved.
 */

#ifndef UCG_PLANC_UCX_REDUCE_SCATTER_BLOCK_H_
#define UCG_PLANC_UCX_REDUCE_SCATTER_BLOCK_H_

#include "planc/ucx/planc_ucx_def.h"
#include "core/ucg_plan.h"
#include "util/algo/ucg_kntree.h"

typedef struct ucg_planc_ucx_reduce_scatter_block_config {
    int kntree_degree;
    int na_kntree_inter_degree;
    int na_kntree_intra_degree;
} ucg_planc_ucx_reduce_scatter_block_config_t;

typedef struct ucg_planc_ucx_reduce_scatter_block {
    union {
        struct {
            int32_t dtype_size;
            void *inbuf; //接收发来的数据，并进行op操作，需要存在2个，保证一个接收一个做op
            int32_t inbi; // 表示inbuf当前的缓存位置
            int64_t iter_idx; //迭代的索引
            int64_t total_count;
        } ring;
    };
} ucg_planc_ucx_reduce_scatter_block_t;

const ucg_plan_policy_t *ucg_planc_ucx_get_reduce_scatter_block_plan_policy(ucg_planc_ucx_node_level_t node_level,
                                                               ucg_planc_ucx_ppn_level_t ppn_level);

ucg_status_t ucg_planc_ucx_reduce_scatter_block_ring_op_progress(ucg_plan_op_t *ucg_op);

ucg_planc_ucx_op_t *ucg_planc_ucx_reduce_scatter_block_ring_op_new(ucg_planc_ucx_group_t *ucx_group,
                                                        ucg_vgroup_t *vgroup,
                                                        const ucg_coll_args_t *args);

ucg_status_t ucg_planc_ucx_reduce_scatter_block_ring_prepare(ucg_vgroup_t *vgroup,
                                                  const ucg_coll_args_t *args,
                                                  ucg_plan_op_t **op);

ucg_status_t ucg_planc_ucx_reduce_scatter_block_linear_prepare(ucg_vgroup_t *vgroup,
                                                               const ucg_coll_args_t *args,
                                                               ucg_plan_op_t **op);

#endif // UCG_PLANC_UCX_REDUCE_SCATTER_BLOCK_H_