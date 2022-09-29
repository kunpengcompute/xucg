/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

/**
 * @note We need a routine to identify the memory type, but the ACL does not
 * provide aclrtPointerGetAttributes() and not install the header file that
 * declare rtPointerGetAttributes(). We had to copy the types definitions associated
 * with rtPointerGetAttributes() into this file.
 *
 * Community edition 5.0.4.alpha005, pay attention to ABI compatibility.
 */

#ifndef UCG_PLANC_HCCL_ACL_RT_EXP_H_
#define UCG_PLANC_HCCL_ACL_RT_EXP_H_

#include <acl/acl.h>

/** @cond ACL_PRIVATE_TYPE */

/* The following types are copied from the files in the 'third_party/inc/runtime/'
   directory of ACL repository. */

typedef int32_t rtError_t;
static const int32_t RT_ERROR_NONE = 0; // success

/**
 * @ingroup dvrt_mem
 * @brief memory type
 */
typedef enum tagRtMemoryType {
    RT_MEMORY_TYPE_HOST = 1,
    RT_MEMORY_TYPE_DEVICE = 2,
    RT_MEMORY_TYPE_SVM = 3,
    RT_MEMORY_TYPE_DVPP = 4
} rtMemoryType_t;

/**
 * @ingroup dvrt_mem
 * @brief memory attribute
 */
typedef struct tagRtPointerAttributes {
    rtMemoryType_t memoryType;  // host memory or device memory
    rtMemoryType_t locationType;
    uint32_t deviceID;          // device ID
    uint32_t pageSize;
} rtPointerAttributes_t;

/**
 * @ingroup dvrt_mem
 * @brief get memory attribute:Host or Device
 * @param [in] ptr
 * @param [out] attributes
 * @return RT_ERROR_NONE for ok, errno for failed
 * @return RT_ERROR_INVALID_VALUE for error input
 */
rtError_t rtPointerGetAttributes(rtPointerAttributes_t *attributes, const void *ptr);
/** @endcond */


#define ACL_MEMORY_TYPE_HOST RT_MEMORY_TYPE_HOST
#define ACL_MEMORY_TYPE_DEVICE RT_MEMORY_TYPE_DEVICE
#define ACL_MEMORY_TYPE_SVM RT_MEMORY_TYPE_SVM
#define ACL_MEMORY_TYPE_DVPP RT_MEMORY_TYPE_DVPP

typedef rtPointerAttributes_t aclrtPointerAttributes_t;

static inline aclError aclrtPointerGetAttributes(aclrtPointerAttributes_t *attributes, const void *ptr)
{
    if (rtPointerGetAttributes(attributes, ptr) == RT_ERROR_NONE) {
        return ACL_SUCCESS;
    }
    return ACL_ERROR_INVALID_PARAM;
}

#endif