#include "gatherv.h"
#include "core/ucg_group.h"
#include "core/ucg_topo.h"
#include "planc_ucx_meta.h"
#include "gatherv_meta.h"
#include "util/ucg_log.h"

ucg_planc_ucx_op_t *ucg_planc_ucx_gatherv_build_topo_group_op(ucg_planc_ucx_group_t *ucx_group,
                                                          ucg_vgroup_t *vgroup,
                                                          const ucg_coll_args_t *args,
                                                          ucg_base_algorithm_gatherv_t algorithm,
                                                          ucg_topo_group_type_t type)
{
    ucg_planc_ucx_op_t *ucx_op = NULL;
    ucg_topo_group_t *topo_group;
    topo_group = ucg_topo_get_group(vgroup->group->topo, type);
    if (topo_group == NULL) {
        return NULL;
    }

    if (topo_group->state == UCG_TOPO_GROUP_STATE_DISABLE) {
        /* I'm not in the topo group. */
        ucx_op = ucg_planc_ucx_empty_op_new(ucx_group, vgroup, args);
        return ucx_op;
    }

    if (topo_group->state != UCG_TOPO_GROUP_STATE_ENABLE) {
        /* The group state is incorrect. */
        return NULL;
    }

    switch(algorithm) {
    case UCG_GATHERV_LINEAR:
        ucx_op = ucg_planc_ucx_gatherv_linear_op_new(ucx_group, &topo_group->super, args);
        break;
    default:
        ucg_error("Do not support gatherv algorithm!");
        return NULL;
        break;
    }
    return ucx_op;
}

ucg_planc_ucx_op_t *ucg_planc_ucx_gatherv_na_kntree_build_topo_group_op(ucg_planc_ucx_group_t *ucx_group,
                                                          ucg_vgroup_t *vgroup,
                                                          const ucg_coll_args_t *args,
                                                          ucg_base_algorithm_gatherv_t algorithm,
                                                          ucg_topo_group_type_t type,
                                                          int isintel)
{
    ucg_planc_ucx_op_t *ucx_op = NULL;
    ucg_topo_group_t *topo_group;

    ucg_planc_ucx_gatherv_config_t *config;
    config = UCG_PLANC_UCX_CONTEXT_BUILTIN_CONFIG_BUNDLE(ucx_group->context, gatherv, UCG_COLL_TYPE_GATHERV);

    if (isintel) {
        config->kntree_degree = config->na_kntree_inter_degree;
    } else {
        config->kntree_degree = config->na_kntree_intra_degree;
    }

    topo_group = ucg_topo_get_group(vgroup->group->topo, type);
    if (topo_group == NULL) {
        return NULL;
    }

    if (topo_group->state == UCG_TOPO_GROUP_STATE_DISABLE) {
        /* I'm not in the topo group. */
        ucx_op = ucg_planc_ucx_empty_op_new(ucx_group, vgroup, args);
        return ucx_op;
    }

    if (topo_group->state != UCG_TOPO_GROUP_STATE_ENABLE) {
        /* The group state is incorrect. */
        return NULL;
    }

    switch(algorithm) {
    case UCG_GATHERV_KNTREE:
        ucx_op = ucg_planc_ucx_gatherv_kntree_op_new(ucx_group, &topo_group->super, args, config);
        break;
    default:
        ucg_error("Do not support gatherv algorithm!");
        return NULL;
        break;
    }
    return ucx_op;
}

ucg_planc_ucx_op_t *ucg_planc_ucx_bcast_build_topo_group_op(ucg_planc_ucx_group_t *ucx_group,
                                                          ucg_vgroup_t *vgroup,
                                                          const ucg_coll_args_t *args,
                                                          ucg_base_algorithm_gatherv_t algorithm,
                                                          ucg_topo_group_type_t type,
                                                          int isintel)
{
    ucg_planc_ucx_op_t *ucx_op = NULL;
    ucg_topo_group_t *topo_group;
    ucg_planc_ucx_bcast_config_t config_bcast;

    ucg_planc_ucx_gatherv_config_t *config_gatherv;
    config_gatherv = UCG_PLANC_UCX_CONTEXT_BUILTIN_CONFIG_BUNDLE(ucx_group->context, gatherv, UCG_COLL_TYPE_GATHERV);

    if (isintel) {
        config_bcast.kntree_degree = config_gatherv->na_kntree_inter_degree;
    } else {
        config_bcast.kntree_degree = config_gatherv->na_kntree_intra_degree;
    }

    topo_group = ucg_topo_get_group(vgroup->group->topo, type);
    if (topo_group == NULL) {
        return NULL;
    }

    if (topo_group->state == UCG_TOPO_GROUP_STATE_DISABLE) {
        /* I'm not in the topo group. */
        ucx_op = ucg_planc_ucx_empty_op_new(ucx_group, vgroup, args);
        return ucx_op;
    }

    if (topo_group->state != UCG_TOPO_GROUP_STATE_ENABLE) {
        /* The group state is incorrect. */
        return NULL;
    }

    switch(algorithm) {
    case UCG_GATHERV_BCAST_KNTREE:
        ucx_op = ucg_planc_ucx_bcast_kntree_op_new(ucx_group, &topo_group->super, args, &config_bcast);
        break;
    default:
        ucg_error("Do not support gatherv algorithm!");
        return NULL;
        break;
    }
    return ucx_op;
}