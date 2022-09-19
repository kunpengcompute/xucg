/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "ucg/api/ucg_version.h"

#include <stdint.h>


void ucg_get_version(uint32_t *major_version, uint32_t *minor_version,
                     uint32_t *patch_version)
{
    *major_version = UCG_API_MAJOR;
    *minor_version = UCG_API_MINOR;
    *patch_version = UCG_API_PATCH;
}

const char *ucg_get_version_string()
{
    return UCG_API_VERSION_STR;
}