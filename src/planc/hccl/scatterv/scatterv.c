/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "scatterv.h"


#define PLAN_DOMAIN "planc hccl scatterv"

static ucg_plan_attr_t ucg_planc_hccl_scatterv_plan_attr[] = {
    {ucg_planc_hccl_scatterv_linear_prepare,
     1, "linear", PLAN_DOMAIN,
     0, {0, UCG_PLAN_RANGE_MAX}, NULL, UCG_PLANC_HCCL_DEFAULT_SCORE},

    {ucg_planc_hccl_scatterv_kntree_prepare,
     2, "kntree", PLAN_DOMAIN,
     0, {0, UCG_PLAN_RANGE_MAX}, NULL, UCG_PLANC_HCCL_DEFAULT_SCORE},

    {NULL},
};

UCG_PLAN_ATTR_REGISTER_TABLE(ucg_planc_hccl, UCG_COLL_TYPE_SCATTERV,
                             ucg_planc_hccl_scatterv_plan_attr);