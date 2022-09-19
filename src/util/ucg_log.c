/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "ucg_log.h"
#include <stdio.h>

static ucs_log_component_config_t g_log_component = {
    UCS_LOG_LEVEL_INFO, "UCG"   /* default configure */
};

static ucs_log_level_t ucg_log_tables[] = {
    [UCG_LOG_LEVEL_FATAL] = UCS_LOG_LEVEL_FATAL,
    [UCG_LOG_LEVEL_ERROR] = UCS_LOG_LEVEL_ERROR,
    [UCG_LOG_LEVEL_WARN]  = UCS_LOG_LEVEL_WARN,
    [UCG_LOG_LEVEL_INFO]  = UCS_LOG_LEVEL_INFO,
    [UCG_LOG_LEVEL_DEBUG] = UCS_LOG_LEVEL_DEBUG,
    [UCG_LOG_LEVEL_TRACE] = UCS_LOG_LEVEL_TRACE
};

const char *ucg_log_level_names[] = {
    [UCG_LOG_LEVEL_FATAL] = "fatal",
    [UCG_LOG_LEVEL_ERROR] = "error",
    [UCG_LOG_LEVEL_WARN]  = "warn",
    [UCG_LOG_LEVEL_INFO]  = "info",
    [UCG_LOG_LEVEL_DEBUG] = "debug",
    [UCG_LOG_LEVEL_TRACE] = "trace",
    NULL,
};

ucs_log_component_config_t *ucg_log_component()
{
    return &g_log_component;
}

void ucg_log_configure(ucg_log_level_t max_level, const char *prefix)
{
    g_log_component.log_level = ucg_log_tables[max_level];
    snprintf(g_log_component.name, sizeof(g_log_component.name), "%s", prefix);
}