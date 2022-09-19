/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "ucg_global.h"
#include "ucg_dt.h"

#include "planc/ucg_planc.h"
#include "util/ucg_helper.h"
#include "util/ucg_parser.h"

#include <pthread.h>


/* Save all configuration items so that the ucg_info can display these. */
ucg_list_link_t ucg_config_global_list = {
    &ucg_config_global_list,
    &ucg_config_global_list,
};

static ucg_config_field_t ucg_global_config_table[] = {
    {"LOG_LEVEL", "warn",
     "UCG log level, possible values: fatal, error, warn, info, debug, trace",
     ucg_offsetof(ucg_global_config_t, log_level),
     UCG_CONFIG_TYPE_ENUM(ucg_log_level_names)},

    {NULL},
};
UCG_CONFIG_REGISTER_TABLE(ucg_global_config_table, "UCG global", NULL,
                          ucg_global_config_t, &ucg_config_global_list);

static int initialized = 0;
static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

static void ucg_global_cleanup_planc(int count)
{
    for (int i = 0; i < count; ++i) {
        ucg_planc_t *planc = ucg_planc_get_by_idx(i);
        if (planc->global_cleanup != NULL) {
            planc->global_cleanup();
        }
    }
    return;
}

static ucg_status_t ucg_global_init_planc(const ucg_global_params_t *params)
{
    int count = ucg_planc_count();
    ucg_status_t status = UCG_OK;
    for (int i = 0; i < count; ++i) {
        ucg_planc_t *planc = ucg_planc_get_by_idx(i);
        if (planc->global_init == NULL) {
            continue;
        }
        status = planc->global_init(params);
        if (status != UCG_OK) {
            ucg_global_cleanup_planc(i);
            return status;
        }
    }
    return status;
}

ucg_status_t ucg_global_init(const ucg_global_params_t *params)
{
    if (initialized) {
        return UCG_OK;
    }

    pthread_mutex_lock(&mutex);
    ucg_status_t status = UCG_OK;
    if (initialized) {
        goto out;
    }

    ucg_global_config_t config;
    status = ucg_config_parser_fill_opts(&config, ucg_global_config_table,
                                         UCG_DEFAULT_ENV_PREFIX, NULL, 0);
    if (status != UCG_OK) {
        ucg_error("Failed to read global configuration");
        goto out;
    }
    ucg_log_configure(config.log_level, "UCG");
    ucg_config_parser_release_opts(&config, ucg_global_config_table);

    status = ucg_planc_load();
    if (status != UCG_OK) {
        ucg_error("Failed to load plan component");
        goto out;
    }

    status = ucg_global_init_planc(params);
    if (status != UCG_OK) {
        ucg_error("Failed to initialize planc");
        goto unload_planc;
    }

    status = ucg_dt_global_init();
    if (status != UCG_OK) {
        ucg_error("Failed to initialize dt resource");
        goto cleanup_planc;
    }

    initialized = 1;
    goto out;

cleanup_planc:
    ucg_global_cleanup_planc(ucg_planc_count());
unload_planc:
    ucg_planc_unload();
out:
    pthread_mutex_unlock(&mutex);
    return status;
}

void ucg_global_cleanup()
{
    pthread_mutex_lock(&mutex);
    if (initialized) {
        ucg_planc_unload();
        ucg_global_cleanup_planc(ucg_planc_count());
        ucg_dt_global_cleanup();
        initialized = 0;
    }
    pthread_mutex_unlock(&mutex);

    return;
}