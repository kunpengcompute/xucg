/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_LOG_H_
#define UCG_LOG_H_

#ifdef UCG_ENABLE_DEBUG
    #define UCS_MAX_LOG_LEVEL UCS_LOG_LEVEL_LAST
#else
    #define UCS_MAX_LOG_LEVEL UCS_LOG_LEVEL_INFO
#endif

#include <ucs/debug/log_def.h>

typedef enum ucg_log_level {
    UCG_LOG_LEVEL_FATAL,    /**< fatal error, terminate the process */
    UCG_LOG_LEVEL_ERROR,
    UCG_LOG_LEVEL_WARN,
    UCG_LOG_LEVEL_INFO,
    UCG_LOG_LEVEL_DEBUG,    /**< low-volume debugging */
    UCG_LOG_LEVEL_TRACE     /**< high-volume debugging */
} ucg_log_level_t;

extern const char *ucg_log_level_names[];

/**
 * @brief Configure the log level. The default is INFO, and prefix is "UCG"
 *
 * @note It is not thread safe, and it should be configured in ucg_global_init()
 *       And the log level is still affected by UCS_MAX_LOG_LEVEL. The
 *       actual log output is less than or equal to UCS_MAX_LOG_LEVEL.
 *
 * @param [in] max_level    the maximum log output level
 *                          log level order : FATAL < ERROR < WARN < INFO < DEBUG < TRACE
 * @param [in] prefix       the prefix in log output
 */
void ucg_log_configure(ucg_log_level_t max_level, const char *prefix);

/**
 * @brief Get the global log component, just use for macro as follow
 */
ucs_log_component_config_t *ucg_log_component();

#define ucg_log_detail(_level, _fmt, ...) ucs_log_component(_level, ucg_log_component(), _fmt, ##__VA_ARGS__)

#define ucg_fatal(_fmt, ...)    ucg_log_detail(UCS_LOG_LEVEL_FATAL, _fmt, ##__VA_ARGS__)
#define ucg_error(_fmt, ...)    ucg_log_detail(UCS_LOG_LEVEL_ERROR, _fmt, ##__VA_ARGS__)
#define ucg_warn(_fmt, ...)     ucg_log_detail(UCS_LOG_LEVEL_WARN,  _fmt, ##__VA_ARGS__)
#define ucg_info(_fmt, ...)     ucg_log_detail(UCS_LOG_LEVEL_INFO,  _fmt, ##__VA_ARGS__)
#define ucg_debug(_fmt, ...)    ucg_log_detail(UCS_LOG_LEVEL_DEBUG, _fmt, ##__VA_ARGS__)
#define ucg_trace(_fmt, ...)    ucg_log_detail(UCS_LOG_LEVEL_TRACE, _fmt, ##__VA_ARGS__)

#endif