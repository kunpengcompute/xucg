/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */
#define _GNU_SOURCE // For dladdr()

#include "ucg_planm.h"
#include "util/ucg_parser.h"
#include "util/ucg_log.h"
#include "util/ucg_malloc.h"
#include "core/ucg_global.h"
#include "core/ucg_group.h"

#include <string.h>
#include <dlfcn.h>
#include <libgen.h>

/* Default directory for planm library, same to planc */
#ifndef UCG_PLANM_DIR
    #define UCG_PLANM_DIR "planc"
#endif

typedef struct {
    char *planm_path;
} ucg_planm_config_t;

static ucg_components_t ucg_planm_dummy = {0, NULL};

static ucg_config_field_t ucg_planm_config_table[] = {
    {"PLANM_PATH", "default", "Specifies dynamic plan components location, "
     "default path: 'planc' sub-directory inside the directory of libucg.so",
     ucg_offsetof(ucg_planm_config_t, planm_path), UCG_CONFIG_TYPE_STRING},
    {NULL},
};
UCG_CONFIG_REGISTER_TABLE(ucg_planm_config_table, "UCG PlanM", NULL,
                          ucg_planm_config_t, &ucg_config_global_list);

static char *ucg_planm_get_default_path()
{
    dlerror();
    Dl_info dl_info;
    int ret = dladdr((void*)&ucg_planm_dummy, &dl_info);
    if (ret == 0) {
        ucg_error("Failed to dladdr, %s", dlerror());
        return NULL;
    }

    char *lib_path = ucg_strdup(dl_info.dli_fname, "planm lib path");
    if (lib_path == NULL) {
        ucg_error("Failed to strdup");
        return NULL;
    }

    char *dir = dirname(lib_path);
    int max_length = strlen(dir)
                     + 1 /* '/' */
                     + strlen(UCG_PLANM_DIR)
                     + 1; /* '\0' */
    char *default_path = ucg_malloc(max_length, "planm default path");
    if (default_path == NULL) {
        ucg_free(lib_path);
        return NULL;
    }
    snprintf(default_path, max_length, "%s/%s", dir, UCG_PLANM_DIR);
    ucg_free(lib_path);

    ucg_trace("planm default path: %s", default_path);

    return default_path;
}

static void ucg_planm_put_default_path(char *default_path)
{
    ucg_free(default_path);
    return;
}

static char* ucg_planm_gen_pattern(const char* component_name)
{
    int pattern_length = strlen(UCG_PLANM_LIBNAME_PREFIX) +
                         strlen(component_name) +
                         strlen("*.so") +
                         1; /* '\0' */
    char *pattern = ucg_malloc(pattern_length, "planm library pattern");
    if (pattern == NULL) {
        ucg_error("Failed to alloc planm library pattern");
    }

    strcpy(pattern, UCG_PLANM_LIBNAME_PREFIX);
    strcat(pattern, component_name);
    strcat(pattern, "*.so");

    return pattern;
}

static inline void ucg_planm_release_pattern(char* pattern)
{
    if (pattern != NULL) {
        ucg_free(pattern);
    }
}

ucg_status_t ucg_planm_load(const char* component_name, ucg_components_t *ucg_planm)
{
    if (ucg_planm->num != 0) {
        /* Loaded, return error */
        return UCG_ERR_INVALID_PARAM;
    }

    ucg_planm_config_t config;
    ucg_status_t status;
    status = ucg_config_parser_fill_opts(&config, ucg_planm_config_table,
                                         UCG_DEFAULT_ENV_PREFIX, NULL, 0);
    if (status != UCG_OK) {
        ucg_error("Failed to read PlanM configuration");
        return status;
    }

    char *planm_path = config.planm_path;
    uint8_t use_default_path = !strcmp(planm_path, "default");
    if (use_default_path) {
        planm_path = ucg_planm_get_default_path();
        if (planm_path == NULL) {
            status = UCG_ERR_NO_RESOURCE;
            goto out;
        }
    }

    char *planm_pattern = ucg_planm_gen_pattern(component_name);
    if (planm_pattern == NULL) {
        status = UCG_ERR_NO_RESOURCE;
        goto out;
    }

    ucg_info("planm_pattern: %s", planm_pattern);
    status = ucg_components_load(planm_path, planm_pattern, ucg_planm);
    if (status != UCG_OK) {
        ucg_info("Failed to load planm");
    }

    for (int i = 0; i < ucg_planm->num; ++i) {
        ucg_info("Success to load planm %s", ucg_planm->components[i]->name);
    }

    if (use_default_path) {
        ucg_planm_put_default_path(planm_path);
    }

    ucg_planm_release_pattern(planm_pattern);

out:
    ucg_config_parser_release_opts(&config, ucg_planm_config_table);
    return status;
}

void ucg_planm_unload(ucg_components_t *ucg_planm)
{
    if (ucg_planm->num == 0) {
        return;
    }
    ucg_components_unload(ucg_planm);
    ucg_planm->num = 0;
    ucg_planm->components = NULL;
    return;
}

int32_t ucg_planm_count(ucg_components_t *ucg_planm)
{
    return ucg_planm->num;
}

ucg_planm_t* ucg_planm_get_by_idx(int32_t idx, ucg_components_t *ucg_planm)
{
    UCG_CHECK_OUT_RANGE(NULL, idx, 0, ucg_planm->num);

    return ucg_derived_of(ucg_planm->components[idx], ucg_planm_t);
}

ucg_planm_t* ucg_planm_get_by_name(const char *name, ucg_components_t *ucg_planm)
{
    UCG_CHECK_NULL(NULL, name);

    for (int i = 0; i < ucg_planm->num; ++i) {
        ucg_planm_t *planm = ucg_derived_of(ucg_planm->components[i], ucg_planm_t);
        if (strcmp(name, planm->super.name) == 0) {
            return planm;
        }
    }
    return NULL;
}