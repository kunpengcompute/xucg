/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "planc_hccl_base.h"
#include "planc_hccl_helper.h"

#include "external/acl_rt_exp.h"
#include "util/ucg_helper.h"

#include <stddef.h>

ucg_status_t ucg_planc_hccl_mem_query(const void *ptr, ucg_mem_attr_t *attr)
{
    UCG_CHECK_NULL_INVALID(ptr, attr);

    if (!(attr->field_mask & UCG_MEM_ATTR_FIELD_MEM_TYPE)) {
        return UCG_ERR_UNSUPPORTED;
    }

    aclrtPointerAttributes_t aclAttr;
    aclError rc = aclrtPointerGetAttributes(&aclAttr, ptr);
    if (rc != ACL_SUCCESS) {
        return UCG_ERR_NO_RESOURCE;
    }

    if (aclAttr.memoryType == ACL_MEMORY_TYPE_HOST) {
        /**
         * Memory that is not allocated by aclrtMallocHost() is still detected
         * as ACL_MEMORY_TYPE_HOST. To prevent other types of memory from being
         * detected as ACL_MEMORY_TYPE_HOST(e.g. cuda), return error.
         */
        return UCG_ERR_UNSUPPORTED;
    }
    attr->mem_type = UCG_MEM_TYPE_ACL;
    return UCG_OK;
}