/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "planc_ucx_base.h"
#include "util/ucg_helper.h"
#include "util/ucg_log.h"

ucg_status_t ucg_planc_ucx_mem_query(const void *ptr, ucg_mem_attr_t *attr)
{
    UCG_CHECK_NULL_INVALID(ptr, attr);
    ucg_debug("Planc ucx don't support mem type detection");
    return UCG_ERR_UNSUPPORTED;
}