/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include <string.h>

#include "planc_ucx_plan.h"
#include "planc_ucx_group.h"
#include "planc_ucx_global.h"
#include "planc_ucx_p2p.h"
#include "planc/ucg_planm.h"
#include "util/ucg_log.h"
#include "util/ucg_malloc.h"

UCG_PLAN_ATTR_TABLE_DEFINE(ucg_planc_ucx);

static ucg_status_t ucg_planc_ucx_get_default_plan_attr(ucg_vgroup_t *vgroup,
                                                        ucg_coll_type_t coll_type,
                                                        ucg_plan_attr_t *default_plan_attr)
{
    ucg_plan_attr_t *plan_attr = UCG_PLAN_ATTR_ARRAY(ucg_planc_ucx, coll_type);
    if (plan_attr == NULL) {
        return UCG_ERR_UNSUPPORTED;
    }
    int algo_num = 1;  // last plan attr is {NULL}
    for (ucg_plan_attr_t *attr = plan_attr; !UCG_PLAN_ATTR_IS_LAST(attr); ++attr) {
        ++algo_num;
    }
    memcpy(default_plan_attr, plan_attr, algo_num * sizeof(ucg_plan_attr_t));

    switch (coll_type) {
        case UCG_COLL_TYPE_BCAST:
            ucg_planc_ucx_bcast_set_plan_attr(vgroup, default_plan_attr);
            break;
        case UCG_COLL_TYPE_ALLREDUCE:
            ucg_planc_ucx_allreduce_set_plan_attr(vgroup, default_plan_attr);
            break;
        case UCG_COLL_TYPE_BARRIER:
            ucg_planc_ucx_barrier_set_plan_attr(vgroup, default_plan_attr);
            break;
        case UCG_COLL_TYPE_SCATTERV:
            ucg_planc_ucx_scatterv_set_plan_attr(vgroup, default_plan_attr);
            break;
        case UCG_COLL_TYPE_GATHERV:
            ucg_planc_ucx_gatherv_set_plan_attr(vgroup, default_plan_attr);
            break;
        case UCG_COLL_TYPE_ALLGATHERV:
            ucg_planc_ucx_allgatherv_set_plan_attr(vgroup, default_plan_attr);
            break;
        case UCG_COLL_TYPE_REDUCE:
            ucg_planc_ucx_reduce_set_plan_attr(vgroup, default_plan_attr);
            break;
        default:
            ucg_error("Unknown coll type %d", coll_type);
            return UCG_ERR_UNSUPPORTED;
    }
    return UCG_OK;
}

static ucg_status_t ucg_planc_ucx_get_builtin_plans(ucg_planc_group_h planc_group, ucg_plans_t *plans)
{
    UCG_CHECK_NULL_INVALID(planc_group, plans);

    ucg_planc_ucx_group_t *ucx_group = ucg_derived_of(planc_group, ucg_planc_ucx_group_t);
    ucg_vgroup_t *vgroup = &ucx_group->super.super;
    ucg_planc_ucx_context_t *context = ucx_group->context;

    ucg_plan_params_t params;
    params.mem_type = UCG_MEM_TYPE_HOST;

    ucg_status_t status = UCG_OK;
    ucg_plan_attr_t *default_plan_attr = NULL;
    const int32_t max_plan_num = 128;  // max algo num of all coll op, usually enough
    default_plan_attr = ucg_malloc(max_plan_num * sizeof(ucg_plan_attr_t), "default plan attr");
    if (default_plan_attr == NULL) {
        return UCG_ERR_NO_MEMORY;
    }

    ucg_coll_type_t coll_type = UCG_COLL_TYPE_BCAST;
    for (; coll_type < UCG_COLL_TYPE_LAST; ++coll_type) {
        ucg_status_t stat = ucg_planc_ucx_get_default_plan_attr(vgroup, coll_type, default_plan_attr);
        if (stat != UCG_OK) {
            continue;
        }

        params.coll_type = coll_type;
        char *user_plan_attr = context->config.plan_attr[coll_type];
        for (ucg_plan_attr_t *attr = default_plan_attr; !UCG_PLAN_ATTR_IS_LAST(attr); ++attr) {
            params.attr = *attr;
            params.attr.vgroup = vgroup;
            /* apply user-configured attribute */
            status = ucg_plan_attr_update(&params.attr, user_plan_attr);
            if (status != UCG_OK) {
                ucg_warn("Failed to update plan attribute(%s), using default", user_plan_attr);
            }
            /* add plan */
            status = ucg_plans_add(plans, &params);
            if (status != UCG_OK) {
                ucg_error("Failed to add default plan, coll type %d", coll_type);
                goto err;
            }
        }
    }

err:
    ucg_free(default_plan_attr);
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