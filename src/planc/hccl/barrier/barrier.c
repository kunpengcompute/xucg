/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "barrier.h"


#define PLAN_DOMAIN "planc hccl barrier"

static ucg_plan_attr_t ucg_planc_hccl_barrier_plan_attr[] = {
    {ucg_planc_hccl_barrier_native_prepare,
     1, "native", PLAN_DOMAIN,
     0, {0, UCG_PLAN_RANGE_MAX}, NULL, UCG_PLANC_HCCL_DEFAULT_SCORE},

    {NULL},
};

UCG_PLAN_ATTR_REGISTER_TABLE(ucg_planc_hccl, UCG_COLL_TYPE_BARRIER,
                             ucg_planc_hccl_barrier_plan_attr);