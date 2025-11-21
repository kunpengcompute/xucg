/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_PLANC_UCX_META_H_
#define UCG_PLANC_UCX_META_H_

#include "planc_ucx_plan.h"
#include "core/ucg_group.h"

ucg_status_t ucg_planc_ucx_add_empty_op(ucg_plan_meta_op_t *meta_op,
                                        ucg_planc_ucx_group_t *ucx_group,
                                        ucg_vgroup_t *vgroup);

ucg_planc_ucx_op_t* ucg_planc_ucx_empty_op_new(ucg_planc_ucx_group_t *ucx_group,
                                               ucg_vgroup_t *vgroup,
                                               const ucg_coll_args_t *args);

#endif