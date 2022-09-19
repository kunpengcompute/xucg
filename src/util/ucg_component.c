/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "ucg_component.h"

#include "util/ucg_log.h"
#include "util/ucg_helper.h"
#include "util/ucg_malloc.h"

#include <libgen.h>
#include <string.h>
#include <dlfcn.h>
#include <glob.h>
#include <stdio.h>

#define UCG_PATTERN_PREFIX "lib"
#define UCG_PATTERN_PREFIX_LEN 3
#define UCG_PATTERN_SUFFIX ".so"
#define UCG_PATTERN_SUFFIX_LEN 3
/* full pattern need extra space for saving '/' and '\0' */
#define UCG_PATTERN_EXTRA_LEN  2

static ucg_status_t ucg_components_load_one(const char *lib_path,
                                            ucg_component_t **component)
{
    char *lib_path_dup = ucg_strdup(lib_path, "component path");
    if (lib_path_dup == NULL) {
        return UCG_ERR_NO_MEMORY;
    }

    /* skip prefix */
    char *libname = basename(lib_path_dup) + UCG_PATTERN_PREFIX_LEN;
    /* ingore suffix*/
    int objname_len = strlen(libname) - UCG_PATTERN_SUFFIX_LEN;
    char objname[UCG_COMPONENT_OBJNAME_MAX_LEN + 1] = {0};
    if (objname_len > UCG_COMPONENT_OBJNAME_MAX_LEN) {
        ucg_error("Length of object name exceed %d", UCG_COMPONENT_OBJNAME_MAX_LEN);
        goto err;
    }
    strncpy(objname, libname, objname_len);

    dlerror(); /* Clear any existing error*/
    void *handle = dlopen(lib_path, RTLD_LAZY | RTLD_GLOBAL);
    if (handle == NULL) {
        ucg_error("Failed to load library, %s", dlerror());
        goto err;
    }

    *component = (ucg_component_t *)dlsym(handle, objname);
    if (*component == NULL) {
        ucg_error("Failed to find the object by %s, %s", objname, dlerror());
        goto err_dlclose;
    }

    (*component)->handle = handle;
    ucg_free(lib_path_dup);
    return UCG_OK;

err_dlclose:
    dlclose(handle);
err:
    ucg_free(lib_path_dup);
    return UCG_ERR_NO_RESOURCE;
}

static ucg_status_t ucg_components_check_pattern(const char *pattern)
{
    const char *prefix = pattern;
    const char *suffix = pattern + strlen(pattern) - UCG_PATTERN_SUFFIX_LEN;
    if (strncmp(prefix, UCG_PATTERN_PREFIX, UCG_PATTERN_PREFIX_LEN) != 0
        || strncmp(suffix, UCG_PATTERN_SUFFIX, UCG_PATTERN_SUFFIX_LEN) != 0) {
        return UCG_ERR_INVALID_PARAM;
    }
    return UCG_OK;
}

ucg_status_t ucg_components_load(const char *path, const char *pattern,
                                 ucg_components_t *components)
{
    if (path == NULL || pattern == NULL || components == NULL
        || ucg_components_check_pattern(pattern) != UCG_OK) {
        return UCG_ERR_INVALID_PARAM;
    }

    ucg_status_t status = UCG_ERR_NO_RESOURCE;
    int full_pattern_len = strlen(path) + strlen(pattern) + UCG_PATTERN_EXTRA_LEN;
    char *full_pattern = ucg_malloc(full_pattern_len, "full pattern");
    if (full_pattern == NULL) {
        ucg_error("Failed to allocate memory, %u", full_pattern_len);
        goto out;
    }

    if (snprintf(full_pattern, full_pattern_len, "%s/%s", path, pattern) < 0) {
        ucg_error("Failed to generate full pattern");
        goto out_free_pattern;
    }

    glob_t globbuf;
    if (glob(full_pattern, 0, NULL, &globbuf) != 0) {
        ucg_info("Failed to find libraries through %s", full_pattern);
        goto out_free_pattern;
    }

    ucg_component_t **comps;
    comps = (ucg_component_t **)ucg_malloc(globbuf.gl_pathc * sizeof(ucg_component_t *),
                                           "components");
    if (comps == NULL) {
        ucg_error("Failed to allocate %u bytes", full_pattern_len);
        goto out_free_globbuf;
    }

    components->num = 0;
    for (int i = 0; i < globbuf.gl_pathc; ++i) {
        status = ucg_components_load_one(globbuf.gl_pathv[i], &comps[components->num]);
        if (status != UCG_OK) {
            continue;
        }
        components->num++;
    }

    if (components->num > 0) {
        components->components = comps;
        status = UCG_OK;
    } else {
        ucg_free(comps);
        status = UCG_ERR_NO_RESOURCE;
    }

out_free_globbuf:
    globfree(&globbuf);
out_free_pattern:
    ucg_free(full_pattern);
out:
    return status;
}

void ucg_components_unload(ucg_components_t *components)
{
    if (components == NULL) {
        return;
    }

    ucg_component_t **comps = components->components;
    int num = components->num;
    for (int i = 0; i < num; ++i) {
        dlclose(comps[i]->handle);
    }
    ucg_free(comps);
    components->num = 0;
    components->components = NULL;
    return;
}