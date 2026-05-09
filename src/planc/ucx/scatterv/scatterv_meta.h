/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

#ifndef UCG_PLANC_UCX_SCATTERV_META_H_
#define UCG_PLANC_UCX_SCATTERV_META_H_

typedef enum {
    UCG_SCATTERV_LINEAR,
    UCG_SCATTERV_KNTREE,
    UCG_SCATTERV_LINEAR_SM,
} ucg_base_algorithm_scatterv_t;

/**
 * @brief build scatterv op, which is executed in group of type group_type.
 */
ucg_planc_ucx_op_t *ucg_planc_ucx_scatterv_build_topo_group_op(ucg_planc_ucx_group_t *ucx_group,
                                                               ucg_vgroup_t *vgroup,
                                                               const ucg_coll_args_t *args,
                                                               ucg_base_algorithm_scatterv_t algorithm,
                                                               ucg_topo_group_type_t type);

#endif