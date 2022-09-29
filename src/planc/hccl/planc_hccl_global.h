/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_PLANC_HCCL_GLOBAL_H_
#define UCG_PLANC_HCCL_GLOBAL_H_

#include "planc_hccl_plan.h"

typedef struct ucg_planc_hccl {
    ucg_planc_t super;
} ucg_planc_hccl_t;

ucg_planc_hccl_t *ucg_planc_hccl_instance();

#endif