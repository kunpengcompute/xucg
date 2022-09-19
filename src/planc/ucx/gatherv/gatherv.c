/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "gatherv.h"
#include "planc_ucx_plan.h"
#include "planc_ucx_global.h"

#define PLAN_DOMAIN "planc ucx gatherv"

static ucg_plan_attr_t ucg_planc_ucx_gatherv_plan_attr[] = {
    {ucg_planc_ucx_gatherv_linear_prepare,
     1, "Linear", PLAN_DOMAIN},

    {NULL},
};

UCG_PLAN_ATTR_REGISTER_TABLE(ucg_planc_ucx, UCG_COLL_TYPE_GATHERV,
                             ucg_planc_ucx_gatherv_plan_attr);

UCG_PLANC_UCX_BUILTIN_ALGO_REGISTER(UCG_COLL_TYPE_GATHERV, NULL, 0)

void ucg_planc_ucx_gatherv_set_plan_attr(ucg_vgroup_t *vgroup,
                                         ucg_plan_attr_t *default_plan_attr)
{
    ucg_plan_attr_t *attr;
    for (attr = default_plan_attr; !UCG_PLAN_ATTR_IS_LAST(attr); ++attr) {
        ucg_plan_range_t range = {0, UCG_PLAN_RANGE_MAX};
        attr->range = range;
        attr->score = UCG_PLANC_UCX_DEFAULT_SCORE;
    }
    return;
}