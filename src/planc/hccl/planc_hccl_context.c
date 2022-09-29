/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include <stdlib.h>

#include "planc_hccl_context.h"
#include "planc_hccl_plan.h"
#include "planc_hccl_helper.h"
#include "planc_hccl_global.h"

#include "core/ucg_global.h"
#include "core/ucg_context.h"

#include "util/ucg_malloc.h"
#include "util/ucg_parser.h"
#include "util/ucg_helper.h"
#include "util/ucg_cpu.h"

#define PLANC_HCCL_CONFIG_PREFIX "PLANC_HCCL_"

static ucg_config_field_t ucg_planc_hccl_config_table[] = {
    {"WHITELIST_DISABLE", "y",
     "Specifies whether to disable the communication trustlist",
     ucg_offsetof(ucg_planc_hccl_config_t, whitelist_disable),
     UCG_CONFIG_TYPE_BOOL},

    {"WHITELIST_FILE", "",
     "Specifies the whitelist file when enable whitelist",
     ucg_offsetof(ucg_planc_hccl_config_t, whitelist_file),
     UCG_CONFIG_TYPE_STRING},

    {"BCAST_ATTR", "", UCG_PLAN_ATTR_DESC,
     ucg_offsetof(ucg_planc_hccl_config_t, plan_attr[UCG_COLL_TYPE_BCAST]),
     UCG_CONFIG_TYPE_STRING},

    {"ALLREDUCE_ATTR", "", UCG_PLAN_ATTR_DESC,
     ucg_offsetof(ucg_planc_hccl_config_t, plan_attr[UCG_COLL_TYPE_ALLREDUCE]),
     UCG_CONFIG_TYPE_STRING},

    {"BARRIER_ATTR", "", UCG_PLAN_ATTR_DESC,
     ucg_offsetof(ucg_planc_hccl_config_t, plan_attr[UCG_COLL_TYPE_BARRIER]),
     UCG_CONFIG_TYPE_STRING},

    {"ALLTOALLV_ATTR", "", UCG_PLAN_ATTR_DESC,
     ucg_offsetof(ucg_planc_hccl_config_t, plan_attr[UCG_COLL_TYPE_ALLTOALLV]),
     UCG_CONFIG_TYPE_STRING},

    {"SCATTERV_ATTR", "", UCG_PLAN_ATTR_DESC,
     ucg_offsetof(ucg_planc_hccl_config_t, plan_attr[UCG_COLL_TYPE_SCATTERV]),
     UCG_CONFIG_TYPE_STRING},

    {"GATHERV_ATTR", "", UCG_PLAN_ATTR_DESC,
     ucg_offsetof(ucg_planc_hccl_config_t, plan_attr[UCG_COLL_TYPE_GATHERV]),
     UCG_CONFIG_TYPE_STRING},

    {"ALLGATHERV_ATTR", "", UCG_PLAN_ATTR_DESC,
     ucg_offsetof(ucg_planc_hccl_config_t, plan_attr[UCG_COLL_TYPE_ALLGATHERV]),
     UCG_CONFIG_TYPE_STRING},

    {NULL}
};
UCG_CONFIG_REGISTER_TABLE(ucg_planc_hccl_config_table, "UCG PlanC HCCL",
                          PLANC_HCCL_CONFIG_PREFIX, ucg_planc_hccl_config_t,
                          &ucg_config_global_list)

ucg_status_t ucg_planc_hccl_config_read(const char *env_prefix,
                                        const char *filename,
                                        ucg_planc_context_h *config)
{
    UCG_CHECK_NULL_INVALID(config);

    if (filename != NULL) {
        return UCG_ERR_UNSUPPORTED;
    }

    ucg_status_t status;
    char *full_env_prefix;
    ucg_planc_hccl_config_t *cfg;

    cfg = ucg_calloc(1, sizeof(ucg_planc_hccl_config_t), "ucg planc hccl config");
    if (cfg == NULL) {
        return UCG_ERR_NO_MEMORY;
    }

    if (env_prefix == NULL) {
        full_env_prefix = ucg_strdup(UCG_DEFAULT_ENV_PREFIX, "default planc hccl env prefix");
        if (full_env_prefix == NULL) {
            status = UCG_ERR_NO_MEMORY;
            goto err_free_cfg;
        }
    } else {
        int full_env_prefix_len = strlen(env_prefix)
                                  + 1 /* '_' */
                                  + sizeof(UCG_DEFAULT_ENV_PREFIX);
        full_env_prefix = ucg_malloc(full_env_prefix_len, "ucg planc hccl env prefix");
        if (full_env_prefix == NULL) {
            status = UCG_ERR_NO_MEMORY;
            goto err_free_cfg;
        }
        snprintf(full_env_prefix, full_env_prefix_len, "%s_%s", env_prefix, UCG_DEFAULT_ENV_PREFIX);
    }

    status = ucg_config_parser_fill_opts(cfg, ucg_planc_hccl_config_table,
                                         full_env_prefix, PLANC_HCCL_CONFIG_PREFIX, 0);
    ucg_free(full_env_prefix);
    if (status != UCG_OK) {
        ucg_error("Failed to read PlanC HCCL configuration");
        goto err_free_cfg;
    }

    *config = (ucg_planc_config_h)cfg;
    return UCG_OK;

err_free_cfg:
    ucg_free(cfg);
    return status;
}

ucg_status_t ucg_planc_hccl_config_modify(ucg_planc_config_h config,
                                          const char *name,
                                          const char *value)
{
    UCG_CHECK_NULL_INVALID(config, name, value);

    ucg_planc_hccl_config_t *cfg = (ucg_planc_hccl_config_t *)config;
    ucg_status_t status = ucg_config_parser_set_value(cfg, ucg_planc_hccl_config_table,
                                                      name, value);
    if (status != UCG_OK) {
        ucg_error("Failed to modify PlanC HCCL configuration");
    }
    return status;
}

void ucg_planc_hccl_config_release(ucg_planc_config_h config)
{
    UCG_CHECK_NULL_VOID(config);

    ucg_config_parser_release_opts(config, ucg_planc_hccl_config_table);
    ucg_free(config);
}

static void ucg_planc_hccl_free_plan_attr(ucg_planc_hccl_context_t *context)
{
    ucg_coll_type_t coll_type = UCG_COLL_TYPE_BCAST;
    for (; coll_type < UCG_COLL_TYPE_LAST; ++coll_type) {
        if (context->plan_attr[coll_type] != NULL) {
            ucg_free(context->plan_attr[coll_type]);
            context->plan_attr[coll_type] = NULL;
        }
    }
    return;
}

static ucg_status_t ucg_planc_hccl_fill_plan_attr(ucg_planc_hccl_context_t *context,
                                                  ucg_planc_hccl_config_t *config)
{
    ucg_coll_type_t coll_type = UCG_COLL_TYPE_BCAST;
    for (; coll_type < UCG_COLL_TYPE_LAST; ++coll_type) {
        if (config->plan_attr[coll_type] == NULL) {
            continue;
        }

        context->plan_attr[coll_type] = ucg_strdup(config->plan_attr[coll_type], "plan attr");
        if (context->plan_attr[coll_type] == NULL) {
            ucg_error("Failed to duplicate plan attribute");
            goto err_free_plan_attr;
        }
    }

    return UCG_OK;

err_free_plan_attr:
    ucg_planc_hccl_free_plan_attr(context);
    return UCG_ERR_NO_MEMORY;
}

static ucg_status_t ucg_planc_hccl_fill_config(ucg_planc_hccl_context_t *context,
                                               ucg_planc_hccl_config_t *config)
{
    ucg_status_t status;

    if (config->whitelist_disable) {
        setenv("HCCL_WHITELIST_DISABLE", "1", 1);
    } else {
        if (config->whitelist_file[0] == '\0') {
            ucg_error("Not specify HCCL_WHITELIST_FILE when enable whitelist");
            return UCG_ERR_INVALID_PARAM;
        }
        setenv("HCCL_WHITELIST_DISABLE", "0", 1);
        setenv("HCCL_WHITELIST_FILE", config->whitelist_file, 1);
    }

    status = ucg_planc_hccl_fill_plan_attr(context, config);
    if (status != UCG_OK) {
        return status;
    }

    return UCG_OK;
}

static void ucg_planc_hccl_free_config(ucg_planc_hccl_context_t *context)
{
    ucg_planc_hccl_free_plan_attr(context);
    return;
}

ucg_status_t ucg_planc_hccl_context_init(const ucg_planc_params_t *params,
                                         const ucg_planc_config_h config,
                                         ucg_planc_context_h *context)
{
    UCG_CHECK_NULL_INVALID(params, config, context);

    ucg_status_t status = UCG_OK;

    ucg_planc_hccl_context_t *ctx;
    ctx = ucg_calloc(1, sizeof(ucg_planc_hccl_context_t), "planc hccl context");
    if (ctx == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    ctx->ucg_context = params->context;

    UCG_PLANC_HCCL_CHECK_ACL_GOTO(aclrtGetCurrentContext(&ctx->acl_context),
                                  status, err_free_ctx);

    status = ucg_planc_hccl_fill_config(ctx, config);
    if (status != UCG_OK) {
        goto err_free_ctx;
    }

    status = ucg_mpool_init(&ctx->op_mp, 0, sizeof(ucg_planc_hccl_op_t),
                            0, UCG_CACHE_LINE_SIZE, UCG_ELEMS_PER_CHUNK,
                            -1, NULL, "planc hccl op");
    if (status != UCG_OK) {
        ucg_error("Failed to create mpool");
        goto err_free_ctx;
    }

    *context = (ucg_planc_context_h)ctx;
    return UCG_OK;

err_free_ctx:
    ucg_free(ctx);
    return status;
}

void ucg_planc_hccl_context_cleanup(ucg_planc_context_h context)
{
    UCG_CHECK_NULL_VOID(context);

    ucg_planc_hccl_context_t *ctx = (ucg_planc_hccl_context_t*)context;
    ucg_mpool_cleanup(&ctx->op_mp, 1);
    ucg_planc_hccl_free_config(ctx);
    ucg_free(ctx);

    return;
}

ucg_status_t ucg_planc_hccl_context_query(ucg_planc_context_h context,
                                          ucg_planc_context_attr_t *attr)
{
    UCG_CHECK_NULL_INVALID(context, attr);

    if (attr->field_mask & UCG_PLANC_CONTEXT_ATTR_FIELD_ADDR_LEN) {
        attr->addr_len = 0;
    }

    if (attr->field_mask & UCG_PLANC_CONTEXT_ATTR_FIELD_ADDR) {
        attr->addr = NULL;
    }

    return UCG_OK;
}
