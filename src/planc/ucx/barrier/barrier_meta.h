/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_PLANC_UCX_BARRIER_META_H_
#define UCG_PLANC_UCX_BARRIER_META_H_

/**
 * @brief Add fan_in_kntree op to meta op, the added op is executed in group of type group_type.
 */
ucg_status_t ucg_planc_ucx_barrier_add_fanin_kntree_op(ucg_plan_meta_op_t *meta_op,
                                                       ucg_planc_ucx_group_t *ucx_group,
                                                       ucg_vgroup_t *vgroup,
                                                       const ucg_coll_args_t *args,
                                                       const ucg_planc_ucx_barrier_config_t *config,
                                                       ucg_topo_group_type_t group_type);

/**
 * @brief Add barrier_rd op to meta op, the added op is executed in group of type group_type.
 */
ucg_status_t ucg_planc_ucx_barrier_add_barrier_rd_op(ucg_plan_meta_op_t *meta_op,
                                                     ucg_planc_ucx_group_t *ucx_group,
                                                     ucg_vgroup_t *vgroup,
                                                     const ucg_coll_args_t *args,
                                                     ucg_topo_group_type_t group_type);

/**
 * @brief Add fanout_kntree op to meta op, the added op is executed in group of type group_type.
 */
ucg_status_t ucg_planc_ucx_barrier_add_fanout_kntree_op(ucg_plan_meta_op_t *meta_op,
                                                        ucg_planc_ucx_group_t *ucx_group,
                                                        ucg_vgroup_t *vgroup,
                                                        const ucg_coll_args_t *args,
                                                        const ucg_planc_ucx_barrier_config_t *config,
                                                        ucg_topo_group_type_t group_type);
#endif