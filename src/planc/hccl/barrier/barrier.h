/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_PLANC_HCCL_BARRIER_H_
#define UCG_PLANC_HCCL_BARRIER_H_

#include "planc_hccl_plan.h"

ucg_status_t ucg_planc_hccl_barrier_native_prepare(ucg_vgroup_t *vgroup,
                                                   const ucg_coll_args_t *args,
                                                   ucg_plan_op_t **op);

#endif