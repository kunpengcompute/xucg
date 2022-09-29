/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "allgatherv.h"


#define PLAN_DOMAIN "planc hccl allgatherv"

static ucg_plan_attr_t ucg_planc_hccl_allgatherv_plan_attr[] = {
    {ucg_planc_hccl_allgatherv_ring_prepare,
     1, "ring", PLAN_DOMAIN,
     0, {0, UCG_PLAN_RANGE_MAX}, NULL, UCG_PLANC_HCCL_DEFAULT_SCORE},

    {ucg_planc_hccl_allgatherv_ring_hpl_prepare,
     2, "ring_hpl", PLAN_DOMAIN,
     0, {0, UCG_PLAN_RANGE_MAX}, NULL, UCG_PLANC_HCCL_DEFAULT_SCORE},

    {NULL},
};

UCG_PLAN_ATTR_REGISTER_TABLE(ucg_planc_hccl, UCG_COLL_TYPE_ALLGATHERV,
                             ucg_planc_hccl_allgatherv_plan_attr);
