#ifndef UCG_PLANC_UCX_GATHER_META_H_
#define UCG_PLANC_UCX_GATHER_META_H_


typedef enum {
    UCG_GATHER_LINEAR,
    UCG_GATHER_KNTREE,
    UCG_GATHER_NA_KNTREE,
} ucg_base_algorithm_gather_t;

ucg_planc_ucx_op_t *ucg_planc_ucx_gather_build_topo_group_op(ucg_planc_ucx_group_t *ucx_group,
                                                          ucg_vgroup_t *vgroup,
                                                          const ucg_coll_args_t *args,
                                                          ucg_base_algorithm_gather_t algorithm,
                                                          ucg_topo_group_type_t type);
#endif