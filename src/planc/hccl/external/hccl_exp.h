
/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_PLANC_HCCL_HCCL_EXP_H_
#define UCG_PLANC_HCCL_HCCL_EXP_H_

#include <hccl/hccl_types.h>


static inline const char* HcclResultString(HcclResult err)
{
    switch (err) {
        case HCCL_SUCCESS:
            return "success";
        case HCCL_E_PARA:
            return "parameter error";
        case HCCL_E_PTR:
            return "empty pointer";
        case HCCL_E_MEMORY:
            return "memory error";
        case HCCL_E_INTERNAL:
            return "internal error";
        case HCCL_E_NOT_SUPPORT:
            return "not support feature";
        case HCCL_E_NOT_FOUND:
            return "not found specific resource";
        case HCCL_E_UNAVAIL:
            return "resource unavailable";
        case HCCL_E_SYSCALL:
            return "call system interface error";
        case HCCL_E_TIMEOUT:
            return "timeout";
        case HCCL_E_OPEN_FILE_FAILURE:
            return "open file fail";
        case HCCL_E_TCP_CONNECT:
            return "tcp connect fail";
        case HCCL_E_ROCE_CONNECT:
            return "roce connect fail";
        case HCCL_E_TCP_TRANSFER:
            return "tcp transfer fail";
        case HCCL_E_ROCE_TRANSFER:
            return "roce transfer fail";
        case HCCL_E_RUNTIME:
            return "call runtime api fail";
        case HCCL_E_DRV:
            return "call driver api fail";
        case HCCL_E_PROFILING:
            return "call profiling api fail";
        case HCCL_E_CCE:
            return "call cce api fail";
        case HCCL_E_NETWORK:
            return "call network api fail";
         case HCCL_E_RESERVED:
            return "reserved";
        default:
            return "unkown";
    }
}

#endif