/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include <hccl/hccl.h>

#include "planc_hccl_plan.h"

#include "allreduce/allreduce.h"
#include "bcast/bcast.h"
#include "barrier/barrier.h"
#include "scatterv/scatterv.h"
#include "allgatherv/allgatherv.h"

#include "util/ucg_helper.h"
#include "util/ucg_log.h"


UCG_PLAN_ATTR_TABLE_DEFINE(ucg_planc_hccl);


ucg_status_t ucg_planc_hccl_op_set_sync(ucg_planc_hccl_op_t *op)
{
    ucg_assert(op->super.super.status == UCG_INPROGRESS);
    op->dev_status = UCG_INPROGRESS;
    ucg_planc_hccl_group_t *hccl_group = op->hccl_group;
    /**
     * All hccl collective routines are non-blocking, wo need a synchronization
     * point to sense the status. We know the operations on the same stream are
     * completed in order. Therefore, when aclrtMemcpyAsync() finishes, the
     * previous hccl collective routine is completed.
     */
    return UCG_PLANC_HCCL_CHECK_ACL(aclrtMemcpyAsync(&op->dev_status,
                                                     sizeof(ucg_status_t),
                                                     hccl_group->dev_status,
                                                     sizeof(ucg_status_t),
                                                     ACL_MEMCPY_DEVICE_TO_HOST,
                                                     hccl_group->stream));
}

ucg_status_t ucg_planc_hccl_op_progress(ucg_plan_op_t *ucg_op)
{
    ucg_planc_hccl_op_t *op = ucg_derived_of(ucg_op, ucg_planc_hccl_op_t);
    op->super.super.status = op->dev_status;
    return op->super.super.status;
}

ucg_status_t ucg_planc_hccl_op_discard(ucg_plan_op_t *ucg_op)
{
    ucg_planc_hccl_op_t *op = ucg_derived_of(ucg_op, ucg_planc_hccl_op_t);
    if (op->dev_stage_area != NULL) {
        aclrtFree(op->dev_stage_area);
        op->dev_stage_area = NULL;
    }
    UCG_CLASS_DESTRUCT(ucg_plan_op_t, &op->super);
    ucg_mpool_put(op);
    return UCG_OK;
}


ucg_status_t ucg_planc_hccl_get_plans(ucg_planc_group_h planc_group, ucg_plans_t *plans)
{
    UCG_CHECK_NULL_INVALID(planc_group, plans);

    ucg_planc_hccl_group_t *hccl_group = ucg_derived_of(planc_group, ucg_planc_hccl_group_t);
    ucg_planc_hccl_context_t *context = hccl_group->context;

    ucg_plan_params_t params;
    params.mem_type = UCG_MEM_TYPE_ACL;

    ucg_status_t status = UCG_OK;
    ucg_plan_attr_t *default_plan_attr = NULL;
    ucg_coll_type_t coll_type = UCG_COLL_TYPE_BCAST;
    for (; coll_type < UCG_COLL_TYPE_LAST; ++coll_type) {
        default_plan_attr = UCG_PLAN_ATTR_ARRAY(ucg_planc_hccl, coll_type);
        if (default_plan_attr == NULL) {
            continue;
        }

        params.coll_type = coll_type;
        char *user_plan_attr = context->plan_attr[coll_type];
        for (; !UCG_PLAN_ATTR_IS_LAST(default_plan_attr); ++default_plan_attr) {
            params.attr = *default_plan_attr;
            params.attr.vgroup = &hccl_group->super.super;
            /* apply user-configured attribute */
            status = ucg_plan_attr_update(&params.attr, user_plan_attr);
            if (status != UCG_OK) {
                ucg_warn("Failed to update plan attribute(%s), using default", user_plan_attr);
            }
            /* add plan */
            status = ucg_plans_add(plans, &params);
            if (status != UCG_OK) {
                ucg_error("Failed to add default plan, coll type %d", coll_type);
                return status;
            }
        }
    }
    return UCG_OK;
}