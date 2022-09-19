/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_GLOBAL_H_
#define UCG_GLOBAL_H_

#include "ucg/api/ucg.h"

#include "util/ucg_list.h"
#include "util/ucg_log.h"

#define UCG_DEFAULT_ENV_PREFIX "UCG_"

typedef struct ucg_global_config {
    ucg_log_level_t log_level;
} ucg_global_config_t;

extern ucg_list_link_t ucg_config_global_list;

#endif