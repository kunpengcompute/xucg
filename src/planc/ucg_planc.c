/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */
#define _GNU_SOURCE // For dladdr()

#include "ucg_planc.h"
#include "util/ucg_parser.h"
#include "util/ucg_log.h"
#include "util/ucg_malloc.h"
#include "core/ucg_global.h"
#include "core/ucg_group.h"

#include <string.h>
#include <dlfcn.h>
#include <libgen.h>

/* Default directory for planc library. */
#ifndef UCG_PLANC_DIR
    #define UCG_PLANC_DIR "planc"
#endif

typedef struct {
    char *planc_path;
} ucg_planc_config_t;

static ucg_components_t ucg_planc = {0, NULL};

static ucg_config_field_t ucg_planc_config_table[] = {
    {"PLANC_PATH", "default", "Specifies dynamic plan components location, "
     "default path: 'planc' sub-directory inside the directory of libucg.so",
     ucg_offsetof(ucg_planc_config_t, planc_path), UCG_CONFIG_TYPE_STRING},
    {NULL},
};
UCG_CONFIG_REGISTER_TABLE(ucg_planc_config_table, "UCG PlanC", NULL,
                          ucg_planc_config_t, &ucg_config_global_list);


static char *ucg_planc_get_default_path()
{
    dlerror();
    Dl_info dl_info;
    int ret = dladdr((void*)&ucg_planc, &dl_info);
    if (ret == 0) {
        ucg_error("Failed to dladdr, %s", dlerror());
        return NULL;
    }

    char *lib_path = ucg_strdup(dl_info.dli_fname, "planc lib path");
    if (lib_path == NULL) {
        ucg_error("Failed to strdup");
        return NULL;
    }

    char *dir = dirname(lib_path);
    int max_length = strlen(dir)
                     + 1 /* '/' */
                     + strlen(UCG_PLANC_DIR)
                     + 1; /* '\0' */
    char *default_path = ucg_malloc(max_length, "planc default path");
    if (default_path == NULL) {
        ucg_free(lib_path);
        return NULL;
    }
    snprintf(default_path, max_length, "%s/%s", dir, UCG_PLANC_DIR);
    ucg_free(lib_path);

    ucg_trace("planc default path: %s", default_path);

    return default_path;
}

static void ucg_planc_put_default_path(char *default_path)
{
    ucg_free(default_path);
    return;
}

static ucg_status_t ucg_planc_group_ctor(ucg_planc_group_t *self, ucg_group_t *group)
{
    /* The members of a planc group are fully mapped to the group. */
    self->super.myrank = group->myrank;
    self->super.size = group->size;
    self->super.rank_map.type = UCG_RANK_MAP_TYPE_FULL;
    self->super.rank_map.size = group->size;
    self->super.group = group;
    return UCG_OK;
}

static void ucg_planc_group_dtor(ucg_planc_group_t *self)
{
    return;
}

ucg_status_t ucg_planc_load()
{
    if (ucg_planc.num != 0) {
        /* Loaded, return error */
        return UCG_ERR_INVALID_PARAM;
    }

    ucg_planc_config_t config;
    ucg_status_t status;
    status = ucg_config_parser_fill_opts(&config, ucg_planc_config_table,
                                         UCG_DEFAULT_ENV_PREFIX, NULL, 0);
    if (status != UCG_OK) {
        ucg_error("Failed to read PlanC configuration");
        return status;
    }

    char *planc_path = config.planc_path;
    uint8_t use_default_path = !strcmp(planc_path, "default");
    if (use_default_path) {
        planc_path = ucg_planc_get_default_path();
        if (planc_path == NULL) {
            status = UCG_ERR_NO_RESOURCE;
            goto out;
        }
    }

    status = ucg_components_load(planc_path, UCG_PLANC_LIBNAME_PATTERN,
                                 &ucg_planc);
    if (status != UCG_OK) {
        ucg_error("Failed to load planc");
    }

    for (int i = 0; i < ucg_planc.num; ++i) {
        ucg_info("Success to load planc %s", ucg_planc.components[i]->name);
    }

    if (use_default_path) {
        ucg_planc_put_default_path(planc_path);
    }

out:
    ucg_config_parser_release_opts(&config, ucg_planc_config_table);
    return status;
}

void ucg_planc_unload()
{
    if (ucg_planc.num == 0) {
        return;
    }
    ucg_components_unload(&ucg_planc);
    ucg_planc.num = 0;
    ucg_planc.components = NULL;
    return;
}

int32_t ucg_planc_count()
{
    return ucg_planc.num;
}

ucg_planc_t* ucg_planc_get_by_idx(int32_t idx)
{
    UCG_CHECK_OUT_RANGE(NULL, idx, 0, ucg_planc.num);

    return ucg_derived_of(ucg_planc.components[idx], ucg_planc_t);
}

ucg_planc_t* ucg_planc_get_by_name(const char *name)
{
    UCG_CHECK_NULL(NULL, name);

    for (int i = 0; i < ucg_planc.num; ++i) {
        ucg_planc_t *planc = ucg_derived_of(ucg_planc.components[i], ucg_planc_t);
        if (strcmp(name, planc->super.name) == 0) {
            return planc;
        }
    }
    return NULL;
}

UCG_CLASS_DEFINE(ucg_planc_group_t, ucg_planc_group_ctor, ucg_planc_group_dtor);