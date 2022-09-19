/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_PLANC_UCX_ALLREDUCE_META_H_
#define UCG_PLANC_UCX_ALLREDUCE_META_H_

/**
 * @brief Add reduce_kntree op to meta op, the added op is executed in group of type group_type.
 */
ucg_status_t ucg_planc_ucx_allreduce_add_reduce_kntree_op(ucg_plan_meta_op_t *meta_op,
                                                          ucg_planc_ucx_group_t *ucx_group,
                                                          ucg_vgroup_t *vgroup,
                                                          const ucg_coll_args_t *args,
                                                          const ucg_planc_ucx_allreduce_config_t *config,
                                                          ucg_topo_group_type_t group_type,
                                                          int32_t send_in_place);

/**
 * @brief Add allreduce_rd op to meta op, the added op is executed in group of type group_type.
 */
ucg_status_t ucg_planc_ucx_allreduce_add_allreduce_rd_op(ucg_plan_meta_op_t *meta_op,
                                                         ucg_planc_ucx_group_t *ucx_group,
                                                         ucg_vgroup_t *vgroup,
                                                         const ucg_coll_args_t *args,
                                                         ucg_topo_group_type_t group_type,
                                                         int32_t send_in_place);

/**
 * @brief Add bcast_kntree op to meta op, the added op is executed in group of type group_type.
 */
ucg_status_t ucg_planc_ucx_allreduce_add_bcast_kntree_op(ucg_plan_meta_op_t *meta_op,
                                                         ucg_planc_ucx_group_t *ucx_group,
                                                         ucg_vgroup_t *vgroup,
                                                         const ucg_coll_args_t *args,
                                                         const ucg_planc_ucx_allreduce_config_t *config,
                                                         ucg_topo_group_type_t group_type);

/**
 * @brief Only used by the rabenseifner, including special reduce_scatter and allgatherv.
 */
ucg_status_t ucg_planc_ucx_allreduce_add_reduce_scatter_op(ucg_plan_meta_op_t *meta_op,
                                                           ucg_planc_ucx_group_t *ucx_group,
                                                           ucg_vgroup_t *vgroup,
                                                           const ucg_coll_args_t *args,
                                                           ucg_topo_group_type_t group_type);
ucg_status_t ucg_planc_ucx_allreduce_add_allreduce_op(ucg_plan_meta_op_t *meta_op,
                                                      ucg_planc_ucx_group_t *ucx_group,
                                                      ucg_vgroup_t *vgroup,
                                                      const ucg_coll_args_t *args,
                                                      ucg_topo_group_type_t topo_type,
                                                      ucg_planc_ucx_algo_group_type_t group_type);
ucg_status_t ucg_planc_ucx_allreduce_add_allgatherv_op(ucg_plan_meta_op_t *meta_op,
                                                       ucg_planc_ucx_group_t *ucx_group,
                                                       ucg_vgroup_t *vgroup,
                                                       const ucg_coll_args_t *args,
                                                       ucg_topo_group_type_t group_type);

/**
 * @brief The send_in_place flag is set to 1 only when the previous op has output.
 */
void ucg_planc_ucx_allreduce_set_send_in_place_flag(ucg_vgroup_t *vgroup,
                                                    ucg_topo_group_type_t pre_group_type,
                                                    int32_t *send_in_place);
#endif