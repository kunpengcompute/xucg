/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2024. All rights reserved.
 */


#include "planc_ucx_plan.h"

UCG_PLAN_ATTR_TABLE_DEFINE(ucg_planc_ucx);

static const ucg_plan_policy_t* ucg_planc_ucx_get_plan_policy(ucg_coll_type_t coll_type,
                                                              ucg_planc_ucx_node_level_t node_level,
                                                              ucg_planc_ucx_ppn_level_t ppn_level,
                                                              ucg_planc_ucx_group_t *ucx_group)
{
    const ucg_plan_policy_t *policy = NULL;
    switch (coll_type) {
        case UCG_COLL_TYPE_BCAST:
        case UCG_COLL_TYPE_IBCAST:
            policy = ucg_planc_ucx_get_bcast_plan_policy(node_level, ppn_level, ucx_group);
            break;
        case UCG_COLL_TYPE_ALLREDUCE:
        case UCG_COLL_TYPE_IALLREDUCE:
            policy = ucg_planc_ucx_get_allreduce_plan_policy(node_level, ppn_level, ucx_group);
            break;
        case UCG_COLL_TYPE_BARRIER:
        case UCG_COLL_TYPE_IBARRIER:
            policy = ucg_planc_ucx_get_barrier_plan_policy(node_level, ppn_level);
            break;
        case UCG_COLL_TYPE_SCATTERV:
        case UCG_COLL_TYPE_ISCATTERV:
            policy = ucg_planc_ucx_get_scatterv_plan_policy(node_level, ppn_level);
            break;
        case UCG_COLL_TYPE_GATHER:
        case UCG_COLL_TYPE_IGATHER:
            policy = ucg_planc_ucx_get_gather_plan_policy(node_level, ppn_level);
            break;
        case UCG_COLL_TYPE_GATHERV:
        case UCG_COLL_TYPE_IGATHERV:
            policy = ucg_planc_ucx_get_gatherv_plan_policy(node_level, ppn_level);
            break;
        case UCG_COLL_TYPE_ALLGATHERV:
        case UCG_COLL_TYPE_IALLGATHERV:
            policy = ucg_planc_ucx_get_allgatherv_plan_policy(node_level, ppn_level, ucx_group);
            break;
        case UCG_COLL_TYPE_REDUCE:
        case UCG_COLL_TYPE_IREDUCE:
            policy = ucg_planc_ucx_get_reduce_plan_policy(node_level, ppn_level);
            break;
        case UCG_COLL_TYPE_REDUCE_SCATTER:
        case UCG_COLL_TYPE_IREDUCE_SCATTER:
            policy = ucg_planc_ucx_get_reduce_scatter_plan_policy(node_level, ppn_level);
            break;
        case UCG_COLL_TYPE_REDUCE_SCATTER_BLOCK:
        case UCG_COLL_TYPE_IREDUCE_SCATTER_BLOCK:
            policy = ucg_planc_ucx_get_reduce_scatter_block_plan_policy(node_level, ppn_level);
            break;
        default:
            break;
    }
    return policy;
}

static ucg_coll_type_t ucg_planc_ucx_coll_nonblock_2_block(ucg_coll_type_t coll)
{
    ucg_coll_type_t new_coll = coll;
    switch (coll) {
        case UCG_COLL_TYPE_IBCAST:
            new_coll = UCG_COLL_TYPE_BCAST;
            break;
        case UCG_COLL_TYPE_IALLREDUCE:
            new_coll = UCG_COLL_TYPE_ALLREDUCE;
            break;
        case UCG_COLL_TYPE_IBARRIER:
            new_coll = UCG_COLL_TYPE_BARRIER;
            break;
        case UCG_COLL_TYPE_ISCATTERV:
            new_coll = UCG_COLL_TYPE_SCATTERV;
            break;
        case UCG_COLL_TYPE_IGATHER:
            new_coll = UCG_COLL_TYPE_GATHER;
            break;
        case UCG_COLL_TYPE_IGATHERV:
            new_coll = UCG_COLL_TYPE_GATHERV;
            break;
        case UCG_COLL_TYPE_IALLGATHERV:
            new_coll = UCG_COLL_TYPE_ALLGATHERV;
            break;
        case UCG_COLL_TYPE_IREDUCE:
            new_coll = UCG_COLL_TYPE_REDUCE;
            break;
        case UCG_COLL_TYPE_IREDUCE_SCATTER:
            new_coll = UCG_COLL_TYPE_REDUCE_SCATTER;
            break;
        case UCG_COLL_TYPE_IREDUCE_SCATTER_BLOCK:
            new_coll = UCG_COLL_TYPE_REDUCE_SCATTER_BLOCK;
            break;
        default:
            break;
    }
    return new_coll;
}
static ucg_status_t ucg_planc_ucx_set_plan_attr(ucg_vgroup_t *vgroup,
                                                ucg_coll_type_t coll_type,
                                                const ucg_plan_policy_t *policy,
                                                ucg_plan_attr_t *attr)
{
    attr->id = policy->id;
    attr->range = policy->range;
    attr->score = policy->score;
    attr->vgroup = vgroup;

    /**
     * Non-blocking is treated as blocking because the @ucg_planc_ucx_xxx_plan_attr
     * array is registered as blocking mode only.
     */
    coll_type = ucg_planc_ucx_coll_nonblock_2_block(coll_type);

    ucg_plan_attr_t *plan_attr = UCG_PLAN_ATTR_ARRAY(ucg_planc_ucx, coll_type);
    if (plan_attr == NULL) {
        return UCG_ERR_NOT_FOUND;
    }
    for (; !UCG_PLAN_ATTR_IS_LAST(plan_attr); ++plan_attr) {
        if (plan_attr->id == attr->id) {
            attr->prepare = plan_attr->prepare;
            attr->name = plan_attr->name;
            attr->domain = plan_attr->domain;
            attr->deprecated = plan_attr->deprecated;
            return UCG_OK;
        }
    }
    return UCG_ERR_NOT_FOUND;
}

static ucg_status_t ucg_planc_ucx_add_default_plans(ucg_planc_ucx_group_t *ucx_group,
                                                    ucg_plans_t *plans)
{
    ucg_vgroup_t *vgroup = &ucx_group->super.super;
    ucg_group_t *group = vgroup->group;

    /* calc node_level and ppn_level */
    int32_t nnode = group->topo->detail.nnode;
    int32_t ave_ppn;
    ave_ppn = nnode == 0 ? UCG_TOPO_PPX_UNKNOWN : group->size / nnode;
    ucg_planc_ucx_node_level_t node_level = ucg_planc_ucx_get_node_level(nnode);
    ucg_planc_ucx_ppn_level_t ppn_level = ucg_planc_ucx_get_ppn_level(ave_ppn);

    ucg_plan_params_t params;
    params.mem_type = UCG_MEM_TYPE_HOST;

    const ucg_plan_policy_t *default_policy = NULL;
    ucg_coll_type_t coll_type = UCG_COLL_TYPE_BCAST;
    for (; coll_type < UCG_COLL_TYPE_LAST; ++coll_type) {
        /* get internal policy */
        default_policy = ucg_planc_ucx_get_plan_policy(coll_type, node_level, ppn_level, ucx_group);
        if (default_policy == NULL) {
            continue;
        }

        params.coll_type = coll_type;
        for (; !UCG_PLAN_POLICY_IS_LAST(default_policy); ++default_policy) {
            ucg_status_t status = ucg_planc_ucx_set_plan_attr(vgroup, coll_type,
                                                              default_policy, &params.attr);
            if (status != UCG_OK) {
                continue;
            }
            status = ucg_plans_add(plans, &params);
            if (status != UCG_OK) {
                ucg_error("Failed to add default plan, coll type %d", coll_type);
                return status;
            }
        }
    }
    return UCG_OK;
}

static ucg_status_t ucg_planc_ucx_add_user_plans(ucg_planc_ucx_group_t *ucx_group,
                                                 ucg_plans_t *plans)
{
    ucg_vgroup_t *vgroup = &ucx_group->super.super;
    ucg_planc_ucx_context_t *context = ucx_group->context;

    ucg_plan_params_t params;
    params.mem_type = UCG_MEM_TYPE_HOST;

    ucg_coll_type_t coll_type = UCG_COLL_TYPE_BCAST;
    for (; coll_type < UCG_COLL_TYPE_LAST; ++coll_type) {
        params.coll_type = coll_type;
        ucg_plan_policy_t *user_policy_ptr = context->user_policy[coll_type];
        for (; !UCG_PLAN_POLICY_IS_LAST(user_policy_ptr); ++user_policy_ptr) {
            ucg_status_t status = ucg_planc_ucx_set_plan_attr(vgroup, coll_type,
                                                              user_policy_ptr, &params.attr);
            if (status != UCG_OK) {
                continue;
            }
            status = ucg_plans_add(plans, &params);
            if (status != UCG_OK) {
                ucg_error("Failed to add user plan, coll type %d", coll_type);
                return status;
            }
        }
    }
    return UCG_OK;
}

static ucg_status_t ucg_planc_ucx_get_builtin_plans(ucg_planc_group_h planc_group,
                                                    ucg_plans_t *plans)
{
    UCG_CHECK_NULL_INVALID(planc_group, plans);

    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_group_t *ucx_group = ucg_derived_of(planc_group, ucg_planc_ucx_group_t);
    status = ucg_planc_ucx_add_default_plans(ucx_group, plans);
    if (status != UCG_OK) {
        return status;
    }
    status = ucg_planc_ucx_add_user_plans(ucx_group, plans);
    if (status != UCG_OK) {
        return status;
    }
    return status;
}

ucg_status_t ucg_planc_ucx_get_plans(ucg_planc_group_h planc_group, ucg_plans_t *plans)
{
    UCG_CHECK_NULL_INVALID(planc_group, plans);
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_group_t *ucx_group = ucg_derived_of(planc_group, ucg_planc_ucx_group_t);
    ucg_planc_ucx_context_t *context = ucx_group->context;
    ucg_planm_t *planm;

    status = ucg_planc_ucx_get_builtin_plans(planc_group, plans);
    if (status != UCG_OK) {
        return status;
    }

    for (int i = 0; i < context->num_planm_rscs; i++) {
        planm = context->planm_rscs[i].planm;
        status = planm->get_plans(planc_group, plans);
        if (status != UCG_OK) {
            ucg_error("Failed to get ucx plans in planm %s", planm->super.name);
            break;
        }
    }

    return status;
}