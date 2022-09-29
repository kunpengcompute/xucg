/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "allreduce.h"


#define PLAN_DOMAIN "planc hccl allreduce"

static ucg_plan_attr_t ucg_planc_hccl_allreduce_plan_attr[] = {
    {ucg_planc_hccl_allreduce_native_prepare,
     1, "native", PLAN_DOMAIN,
     0, {0, UCG_PLAN_RANGE_MAX}, NULL, UCG_PLANC_HCCL_DEFAULT_SCORE},

    {NULL},
};

UCG_PLAN_ATTR_REGISTER_TABLE(ucg_planc_hccl, UCG_COLL_TYPE_ALLREDUCE,
                             ucg_planc_hccl_allreduce_plan_attr);