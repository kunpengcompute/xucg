/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_PLANC_HCCL_HELPER_H_
#define UCG_PLANC_HCCL_HELPER_H_

#include <hccl/hccl.h>

#include "ucg/api/ucg.h"

#include "external/hccl_exp.h"
#include "util/ucg_log.h"
#include "util/ucg_helper.h"


/* Check the result of the acl function and return ucg_status_t. */
#define UCG_PLANC_HCCL_CHECK_ACL(_acl_func) \
    ({ \
        ucg_status_t status = UCG_OK; \
        aclError rc = _acl_func; \
        if (rc != ACL_SUCCESS) { \
            status = UCG_ERR_NO_RESOURCE; \
            ucg_error("ACL error %s", aclGetRecentErrMsg()); \
        } \
        status; \
    })

/* Check the result of the hccl function and return ucg_status_t. */
#define UCG_PLANC_HCCL_CHECK_HCCL(_hccl_func) \
    ({ \
        ucg_status_t status = UCG_OK; \
        HcclResult rc = _hccl_func; \
        if (rc != HCCL_SUCCESS) { \
            status = UCG_ERR_NO_RESOURCE; \
            ucg_error("HCCL error %s\n ACL error %s", \
                      HcclResultString(rc), aclGetRecentErrMsg()); \
        } \
        status; \
    })

/* Check the result of the acl function and goto label when error. */
#define UCG_PLANC_HCCL_CHECK_ACL_GOTO(_acl_func, _status, _label) \
    do { \
        _status = UCG_PLANC_HCCL_CHECK_ACL(_acl_func); \
        UCG_CHECK_GOTO(_status, _label); \
    } while (0)

/* Check the result of the hccl function and goto label when error. */
#define UCG_PLANC_HCCL_CHECK_HCCL_GOTO(_hccl_func, _status, _label) \
    do { \
        _status = UCG_PLANC_HCCL_CHECK_HCCL(_hccl_func); \
        UCG_CHECK_GOTO(_status, _label); \
    } while (0)

#endif