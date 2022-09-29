/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "planc_hccl_global.h"
#include "planc_hccl_base.h"
#include "planc_hccl_context.h"
#include "planc_hccl_group.h"
#include "planc_hccl_plan.h"

ucg_planc_hccl_t UCG_PLANC_OBJNAME(hccl) = {
    .super.super.name         = "hccl",
    .super.mem_query          = ucg_planc_hccl_mem_query,

    .super.config_read        = ucg_planc_hccl_config_read,
    .super.config_modify      = ucg_planc_hccl_config_modify,
    .super.config_release     = ucg_planc_hccl_config_release,

    .super.context_init       = ucg_planc_hccl_context_init,
    .super.context_cleanup    = ucg_planc_hccl_context_cleanup,
    .super.context_query      = ucg_planc_hccl_context_query,

    .super.group_create       = ucg_planc_hccl_group_create,
    .super.group_destroy      = ucg_planc_hccl_group_destroy,

    .super.get_plans          = ucg_planc_hccl_get_plans,
};

ucg_planc_hccl_t *ucg_planc_hccl_instance()
{
    return &UCG_PLANC_OBJNAME(hccl);
}