#ifndef UCG_PLANC_UCX_GATHERV_META_H_
#define UCG_PLANC_UCX_GATHERV_META_H_


typedef enum {
    UCG_GATHERV_LINEAR,
    UCG_GATHERV_LINEAR_SM,
    UCG_GATHERV_LINEAR_FC,
    UCG_GATHERV_KNTREE,
    UCG_GATHERV_BCAST_KNTREE,
} ucg_base_algorithm_gatherv_t;

ucg_planc_ucx_op_t *ucg_planc_ucx_gatherv_build_topo_group_op(ucg_planc_ucx_group_t *ucx_group,
                                                          ucg_vgroup_t *vgroup,
                                                          const ucg_coll_args_t *args,
                                                          ucg_base_algorithm_gatherv_t algorithm,
                                                          ucg_topo_group_type_t type);

ucg_planc_ucx_op_t *ucg_planc_ucx_gatherv_na_kntree_build_topo_group_op(ucg_planc_ucx_group_t *ucx_group,
                                                          ucg_vgroup_t *vgroup,
                                                          const ucg_coll_args_t *args,
                                                          ucg_base_algorithm_gatherv_t algorithm,
                                                          ucg_topo_group_type_t type,
                                                          int isintel);      

ucg_planc_ucx_op_t *ucg_planc_ucx_bcast_build_topo_group_op(ucg_planc_ucx_group_t *ucx_group,
                                                          ucg_vgroup_t *vgroup,
                                                          const ucg_coll_args_t *args,
                                                          ucg_base_algorithm_gatherv_t algorithm,
                                                          ucg_topo_group_type_t type,
                                                          int isintel);
#endif