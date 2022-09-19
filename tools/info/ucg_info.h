/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_INFO_H_
#define UCG_INFO_H_

#include <ucg/api/ucg.h>


#define UCG_CHECK(_stmt) \
    do { \
        ucg_status_t status = _stmt;\
        if (status != UCG_OK) { \
            printf("[%s:%d]Failed to %s, %s\n", __FILE__, __LINE__, #_stmt, \
                   ucg_status_string(status)); \
        } \
    } while(0)


#ifdef UCG_BUILD_PLANC_HCCL
#include <acl/acl.h>
#define ACL_CHECK(_stmt) \
    do { \
        aclError rc = _stmt;\
        if (rc != ACL_SUCCESS) { \
            printf("[%s:%d]Failed to %s, %s\n", __FILE__, __LINE__, #_stmt, \
                   aclGetRecentErrMsg()); \
        } \
    } while(0)

#define ACL_INIT() \
    ACL_CHECK(aclInit(NULL)); \
    ACL_CHECK(aclrtSetDevice(0))

#define ACL_FINALIZE() \
    ACL_CHECK(aclrtResetDevice(0)); \
    ACL_CHECK(aclFinalize())
#else
    #define ACL_INIT()
    #define ACL_FINALIZE()
#endif

enum {
    PRINT_VERSION = UCG_BIT(0),
    PRINT_TYPES = UCG_BIT(1),
    PRINT_PLANS = UCG_BIT(2),
    PRINT_CONFIG = UCG_BIT(3),
};

void print_types();

void print_plans();

#endif