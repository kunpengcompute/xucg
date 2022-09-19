/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "ucg/api/ucg.h"
#include "util/ucg_helper.h"

#include "planc/ucg_planc.h"
#include "util/ucg_helper.h"

#include <stddef.h>

const char* ucg_mem_type_string(ucg_mem_type_t mem_type)
{
    switch (mem_type) {
        case UCG_MEM_TYPE_HOST:
            return "host";
        case UCG_MEM_TYPE_ACL:
            return "acl";
        default:
            return "unknown";
    }
}

const char* ucg_status_string(ucg_status_t status)
{
    switch (status) {
        case UCG_OK:
            return "Success";
        case UCG_INPROGRESS:
            return "Operation in progress";
        case UCG_ERR_UNSUPPORTED:
            return "Operation is not supported";
        case UCG_ERR_INVALID_PARAM:
            return "Invalid parameter";
        case UCG_ERR_NO_RESOURCE:
            return "Resource are not available";
        case UCG_ERR_NOT_FOUND:
            return "Not found";
        case UCG_ERR_NO_MEMORY:
            return "Out of memory";
        case UCG_ERR_INCOMPATIBLE:
            return "Version incompatible";
        default:
            return "Unknown error";
    }
}

ucg_status_t ucg_mem_query(const void *ptr, ucg_mem_attr_t *attr)
{
    UCG_CHECK_NULL_INVALID(ptr, attr);
    /* There is no way to check whether the memory is on the host side. Therefore,
       if all planc cannot determine the memory type of the ptr, we regard it
       as host memory. */
    attr->mem_type = UCG_MEM_TYPE_HOST;
    int count = ucg_planc_count();
    for (int i = 0; i < count; ++i) {
        ucg_planc_t *planc = ucg_planc_get_by_idx(i);
        ucg_status_t status = planc->mem_query(ptr, attr);
        if (status == UCG_OK) {
            return UCG_OK;
        }
    }

    return UCG_OK;
}