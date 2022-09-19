/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include <ucp/api/ucp.h>

#include "planc_ucx_context.h"
#include "planc_ucx_p2p.h"
#include "planc_ucx_global.h"
#include "core/ucg_global.h"
#include "util/ucg_malloc.h"
#include "util/ucg_helper.h"
#include "util/ucg_cpu.h"

#define PLANC_UCX_CONFIG_PREFIX "PLANC_UCX_"

static ucg_config_field_t ucg_planc_ucx_config_table[] = {
    {"BCAST_ATTR", "", UCG_PLAN_ATTR_DESC,
     ucg_offsetof(ucg_planc_ucx_config_t, plan_attr[UCG_COLL_TYPE_BCAST]),
     UCG_CONFIG_TYPE_STRING},

    {"ALLREDUCE_ATTR", "", UCG_PLAN_ATTR_DESC,
     ucg_offsetof(ucg_planc_ucx_config_t, plan_attr[UCG_COLL_TYPE_ALLREDUCE]),
     UCG_CONFIG_TYPE_STRING},

    {"BARRIER_ATTR", "", UCG_PLAN_ATTR_DESC,
     ucg_offsetof(ucg_planc_ucx_config_t, plan_attr[UCG_COLL_TYPE_BARRIER]),
     UCG_CONFIG_TYPE_STRING},

    {"ALLTOALLV_ATTR", "", UCG_PLAN_ATTR_DESC,
     ucg_offsetof(ucg_planc_ucx_config_t, plan_attr[UCG_COLL_TYPE_ALLTOALLV]),
     UCG_CONFIG_TYPE_STRING},

    {"SCATTERV_ATTR", "", UCG_PLAN_ATTR_DESC,
     ucg_offsetof(ucg_planc_ucx_config_t, plan_attr[UCG_COLL_TYPE_SCATTERV]),
     UCG_CONFIG_TYPE_STRING},

    {"ALLGATHERV_ATTR", "", UCG_PLAN_ATTR_DESC,
     ucg_offsetof(ucg_planc_ucx_config_t, plan_attr[UCG_COLL_TYPE_ALLGATHERV]),
     UCG_CONFIG_TYPE_STRING},

    {"NPOLLS", "10",
     "Number of ucp progress polling cycles for p2p requests testing",
     ucg_offsetof(ucg_planc_ucx_config_t, n_polls),
     UCG_CONFIG_TYPE_UINT},

    {"ESTIMATED_NUM_EPS", "0",
     "An optimization hint of how many endpoints will be created on this context",
     ucg_offsetof(ucg_planc_ucx_config_t, estimated_num_eps),
     UCG_CONFIG_TYPE_UINT},

    {"ESTIMATED_NUM_PPN", "0",
     "An optimization hint of how many endpoints created on this context reside on the same node",
     ucg_offsetof(ucg_planc_ucx_config_t, estimated_num_ppn),
     UCG_CONFIG_TYPE_UINT},

    {"USE_OOB", "try",
     "The value can be \n"
     " - yes  : Forcibly use oob. If the oob does not exist, a failure will occur. \n"
     " - no   : Not use oob. \n"
     " - try  : Try to use oob, if the oob does not exist, it will create internal resource.",
     ucg_offsetof(ucg_planc_ucx_config_t, use_oob),
     UCG_CONFIG_TYPE_TERNARY},

    {"PLANM", "all",
     "Comma-separated list of plan module to use. The order is not meaningful\n"
     " - all    : use all available plan modules\n"
     " - none   : only use builtin plan based-on ucx\n"
     " - hicoll : use hicoll plan module based-on ucx\n",
     ucg_offsetof(ucg_planc_ucx_config_t, planm),
     UCG_CONFIG_TYPE_STRING_ARRAY},

    {NULL}
};
UCG_CONFIG_REGISTER_TABLE(ucg_planc_ucx_config_table, "UCG PlanC UCX", PLANC_UCX_CONFIG_PREFIX,
                          ucg_planc_ucx_config_t, &ucg_config_global_list)

static ucg_status_t ucg_planc_ucx_config_bundle_read(ucg_planc_ucx_config_bundle_t **bundle,
                                                     ucg_config_field_t *config_table,
                                                     size_t config_size,
                                                     const char *env_prefix,
                                                     const char *cfg_prefix)
{
    ucg_status_t status;

    ucg_planc_ucx_config_bundle_t *config_bundle;
    config_bundle = ucg_calloc(1, sizeof(*config_bundle) + config_size,
                               "ucg planc ucx coll config");
    if (config_bundle == NULL) {
        return UCG_ERR_NO_MEMORY;
    }

    config_bundle->table = config_table;
    status = ucg_config_parser_fill_opts(config_bundle->data, config_table,
                                         env_prefix, cfg_prefix, 0);
    if (status != UCG_OK) {
        goto err_free_bundle;
    }

    *bundle = config_bundle;
    return UCG_OK;

err_free_bundle:
    ucg_free(config_bundle);
    return status;
}

static ucg_status_t ucg_planc_ucx_fill_coll_config(ucg_planc_ucx_config_t *cfg,
                                                   const char *full_env_prefix)
{
    ucg_status_t status = UCG_OK;

    for (ucx_module_type_t module_type = 0; module_type < UCX_MODULE_LAST; ++module_type) {
        for (ucg_coll_type_t coll_type = 0; coll_type < UCG_COLL_TYPE_LAST; ++coll_type) {
            if (ucx_algo_global_table[coll_type][module_type].config_table == NULL) {
                cfg->config_bundle[coll_type][module_type] = NULL;
                continue;
            }
            status = ucg_planc_ucx_config_bundle_read(&cfg->config_bundle[coll_type][module_type],
                                                      ucx_algo_global_table[coll_type][module_type].config_table,
                                                      ucx_algo_global_table[coll_type][module_type].size,
                                                      full_env_prefix,
                                                      PLANC_UCX_CONFIG_PREFIX);
            if (status != UCG_OK) {
                break;
            }
        }
    }
    return status;
}

ucg_status_t ucg_planc_ucx_config_read(const char *env_prefix,
                                       const char *filename,
                                       ucg_planc_config_h *config)
{
    UCG_CHECK_NULL_INVALID(config);

    if (filename != NULL) {
        return UCG_ERR_UNSUPPORTED;
    }

    ucg_status_t status;
    char *full_env_prefix;
    ucg_planc_ucx_config_t *cfg;

    cfg = ucg_calloc(1, sizeof(ucg_planc_ucx_config_t), "ucg planc ucx config");
    if (cfg == NULL) {
        return UCG_ERR_NO_MEMORY;
    }

    if (env_prefix == NULL) {
        full_env_prefix = ucg_strdup(UCG_DEFAULT_ENV_PREFIX, "default planc ucx env prefix");
        if (full_env_prefix == NULL) {
            status = UCG_ERR_NO_MEMORY;
            goto err_free_cfg;
        }
    } else {
        int full_env_prefix_len = strlen(env_prefix)
                                  + 1 /* '_' */
                                  + sizeof(UCG_DEFAULT_ENV_PREFIX);
        full_env_prefix = ucg_malloc(full_env_prefix_len, "ucg planc ucx env prefix");
        if (full_env_prefix == NULL) {
            status = UCG_ERR_NO_MEMORY;
            goto err_free_cfg;
        }
        snprintf(full_env_prefix, full_env_prefix_len, "%s_%s", env_prefix, UCG_DEFAULT_ENV_PREFIX);
    }

    status = ucg_config_parser_fill_opts(cfg, ucg_planc_ucx_config_table,
                                         full_env_prefix, PLANC_UCX_CONFIG_PREFIX, 0);
    if (status != UCG_OK) {
        ucg_error("Failed to read PlanC UCX configuration");
        goto err_free_cfg;
    }

    status = ucg_planc_ucx_fill_coll_config(cfg, full_env_prefix);
    ucg_free(full_env_prefix);
    if (status != UCG_OK) {
        ucg_error("Failed to read PlanC UCX bundle configuration");
        goto err_release_cfg;
    }

    *config = (ucg_planc_config_h)cfg;
    return UCG_OK;

err_release_cfg:
    ucg_config_parser_release_opts(cfg, ucg_planc_ucx_config_table);
err_free_cfg:
    ucg_free(cfg);
    return status;
}

ucg_status_t ucg_planc_ucx_config_modify(ucg_planc_config_h config,
                                         const char *name,
                                         const char *value)
{
    UCG_CHECK_NULL_INVALID(config, name, value);

    ucg_planc_ucx_config_t *cfg = (ucg_planc_ucx_config_t*)config;
    ucg_status_t status = ucg_config_parser_set_value(cfg, ucg_planc_ucx_config_table,
                                                      name, value);
    if (status != UCG_OK) {
        ucx_module_type_t module_type;
        for (module_type = 0; module_type < UCX_MODULE_LAST; ++module_type) {
            ucg_coll_type_t coll_type;
            for (coll_type = 0; coll_type < UCG_COLL_TYPE_LAST; ++coll_type) {
                if (cfg->config_bundle[coll_type][module_type] != NULL) {
                    status = ucg_config_parser_set_value(cfg->config_bundle[coll_type][module_type]->data,
                                                         ucx_algo_global_table[coll_type][module_type].config_table,
                                                         name, value);
                    ucg_error("ucg_config_parser_set_value %s", ucg_status_string(status));
                    if (status == UCG_OK) {
                        goto out;
                    }
                }
            }
        }
        if (module_type == UCX_MODULE_LAST) {
            ucg_error("Failed to modify PlanC UCX configuration");
            return status;
        }
    }
out:
    return status;
}

static void ucg_planc_ucx_config_bundle_release(ucg_planc_ucx_config_t *cfg)
{
    for (ucx_module_type_t module_type = 0; module_type < UCX_MODULE_LAST; ++module_type) {
        for (ucg_coll_type_t coll_type = 0; coll_type < UCG_COLL_TYPE_LAST; ++coll_type) {
            if (cfg->config_bundle[coll_type][module_type] != NULL) {
                if (cfg->config_bundle[coll_type][module_type]->table != NULL) {
                    ucg_config_parser_release_opts(cfg->config_bundle[coll_type][module_type]->data,
                                                   cfg->config_bundle[coll_type][module_type]->table);
                }
                ucg_free(cfg->config_bundle[coll_type][module_type]);
            }
        }
    }
}

void ucg_planc_ucx_config_release(ucg_planc_config_h config)
{
    UCG_CHECK_NULL_VOID(config);

    ucg_planc_ucx_config_t *cfg = (ucg_planc_ucx_config_t*)config;
    ucg_planc_ucx_config_bundle_release(cfg);
    ucg_config_parser_release_opts(cfg, ucg_planc_ucx_config_table);
    ucg_free(cfg);
    return;
}

static void ucg_planc_ucx_context_free_config(ucg_planc_ucx_context_t *ctx)
{
    ucg_planc_ucx_config_t *cfg = &ctx->config;
    ucg_planc_ucx_config_bundle_release(cfg);
    ucg_config_parser_release_opts(cfg, ucg_planc_ucx_config_table);
    return;
}

static ucg_status_t ucg_planc_ucx_context_fill_config(ucg_planc_ucx_context_t *ctx,
                                                      ucg_planc_ucx_config_t *cfg)
{
    ucg_status_t status;
    status = ucg_config_parser_clone_opts(cfg, &ctx->config, ucg_planc_ucx_config_table);
    if (status != UCG_OK) {
        return status;
    }
    for (ucx_module_type_t module_type = 0; module_type < UCX_MODULE_LAST; ++module_type) {
        for (ucg_coll_type_t coll_type = 0; coll_type < UCG_COLL_TYPE_LAST; ++coll_type) {
            if (cfg->config_bundle[coll_type][module_type] == NULL) {
                continue;
            }
            ctx->config.config_bundle[coll_type][module_type] = ucg_malloc(
                sizeof(ucg_planc_ucx_config_bundle_t) + ucx_algo_global_table[coll_type][module_type].size,
                "ucg planc ucx config bundle");
            if (ctx->config.config_bundle[coll_type][module_type] == NULL) {
                status = UCG_ERR_NO_MEMORY;
                goto err_free_cfg;
            }
            status = ucg_config_parser_clone_opts(cfg->config_bundle[coll_type][module_type]->data,
                                                  ctx->config.config_bundle[coll_type][module_type]->data,
                                                  cfg->config_bundle[coll_type][module_type]->table);
            if (status != UCG_OK) {
                goto err_free_cfg;
            }
            ctx->config.config_bundle[coll_type][module_type]->table = cfg->config_bundle[coll_type][module_type]->table;
        }
    }

    return UCG_OK;

err_free_cfg:
    ucg_planc_ucx_context_free_config(ctx);
    return status;
}

static ucg_status_t ucg_planc_ucx_context_init_ucp_context(ucg_planc_ucx_context_t *ctx)
{
    ucs_status_t ucs_status;
    ucp_config_t *ucp_config;
    ucp_params_t ucp_params;
    ucp_context_h ucp_context;

    ucs_status = ucp_config_read(UCG_DEFAULT_ENV_PREFIX, NULL, &ucp_config);
    if (ucs_status != UCS_OK) {
        ucg_error("Failed to read ucp configuration, %s", ucs_status_string(ucs_status));
        goto err;
    }

    ucp_params.field_mask = UCP_PARAM_FIELD_FEATURES |
                            UCP_PARAM_FIELD_TAG_SENDER_MASK |
                            UCP_PARAM_FIELD_REQUEST_SIZE |
                            UCP_PARAM_FIELD_REQUEST_INIT |
                            UCP_PARAM_FIELD_ESTIMATED_NUM_EPS |
                            UCP_PARAM_FIELD_ESTIMATED_NUM_PPN;
    ucp_params.features = UCP_FEATURE_TAG;
    ucp_params.tag_sender_mask = UCG_PLANC_UCX_TAG_SENDER_MASK;
    ucp_params.request_size = sizeof(ucg_planc_ucx_p2p_req_t);
    ucp_params.request_init = ucg_planc_ucx_p2p_req_init;
    ucp_params.estimated_num_eps = ctx->ucg_context->oob_group.size;
    ucp_params.estimated_num_ppn = ctx->ucg_context->oob_group.num_local_procs;
    if (ctx->config.estimated_num_eps > 0) {
        ucp_params.estimated_num_eps = ctx->config.estimated_num_eps;
    }
    if (ctx->config.estimated_num_ppn > 0) {
        ucp_params.estimated_num_ppn = ctx->config.estimated_num_ppn;
    }

    ucs_status = ucp_init(&ucp_params, ucp_config, &ucp_context);
    ucp_config_release(ucp_config);
    if (ucs_status != UCS_OK) {
        ucg_error("Failed to init ucp context, %s", ucs_status_string(ucs_status));
        goto err;
    }

    ctx->ucp_context = ucp_context;

err:
    return ucg_status_s2g(ucs_status);
}

static ucg_status_t ucg_planc_ucx_context_init_ucp_worker(ucg_planc_ucx_context_t *ctx)
{
    ucs_status_t ucs_status;
    ucp_worker_params_t worker_params;
    ucp_worker_h ucp_worker;

    worker_params.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
    worker_params.thread_mode = UCS_THREAD_MODE_SINGLE;

    ucs_status = ucp_worker_create(ctx->ucp_context, &worker_params, &ucp_worker);
    if (ucs_status != UCS_OK) {
        ucg_error("Failed to create ucp worker, %s", ucs_status_string(ucs_status));
        goto err;
    }

    ctx->ucp_worker = ucp_worker;
    ctx->worker_address = NULL;

err:
    return ucg_status_s2g(ucs_status);
}

static int ucg_planc_ucx_ctx_is_required_planm(ucg_planm_t *planm,
                                               ucg_config_names_array_t required)
{
    int count = required.count;
    const char *planm_name = planm->super.name;
    for (int i = 0; i < count; ++i) {
        if (!strcmp(required.names[i], "none")) {
            return 0;
        }
        if (!strcmp(planm_name, required.names[i]) ||
            !strcmp(required.names[i], "all")) {
            return 1;
        }
    }
    return 0;
}

ucg_status_t ucg_planc_ucx_context_init(const ucg_planc_params_t *params,
                                        const ucg_planc_config_h config,
                                        ucg_planc_context_h *context)
{
    UCG_CHECK_NULL_INVALID(params, config, context);

    ucg_status_t status;
    ucg_planc_ucx_config_t *cfg = (ucg_planc_ucx_config_t *)config;
    ucg_planc_ucx_context_t *ctx;

    ctx = ucg_calloc(1, sizeof(ucg_planc_ucx_context_t), "planc ucx context");
    if (ctx == NULL) {
        return UCG_ERR_NO_MEMORY;
    }

    ctx->ucg_context = params->context;

    status = ucg_planc_ucx_context_fill_config(ctx, cfg);
    if (status != UCG_OK) {
        goto err_free_ctx;
    }

    /* Use oob resources may not be safe under multi-threads mode */
    if (params->thread_mode == UCG_THREAD_MODE_MULTI) {
        ctx->config.use_oob = UCG_NO;
    }

    int max_op_size = sizeof(ucg_planc_ucx_op_t);
    int count = ucg_planm_count(&ucg_planc_ucx_planm);
    ctx->num_planm_rscs = 0;
    ctx->planm_rscs = NULL;
    if (count > 0) {
        ctx->planm_rscs = ucg_calloc(count, sizeof(ucg_planc_ucx_resource_planm_t),
                                     "ucg planc ucx resource planm");
        if (ctx->planm_rscs == NULL) {
            status = UCG_ERR_NO_MEMORY;
            goto err_free_config;
        }
    }

    for (int i = 0; i < count; ++i) {
        ucg_planm_t *planm = ucg_planm_get_by_idx(i, &ucg_planc_ucx_planm);
        if (!ucg_planc_ucx_ctx_is_required_planm(planm, cfg->planm)) {
            ucg_debug("planm %s is not required", planm->super.name);
            continue;
        }
        ucg_planc_ucx_resource_planm_t *planm_rscs = &ctx->planm_rscs[ctx->num_planm_rscs];
        planm_rscs->planm = planm;
        ++ctx->num_planm_rscs;
        max_op_size = (planm->op_size > max_op_size) ? planm->op_size : max_op_size;
    }

    status = ucg_mpool_init(&ctx->op_mp, 0, max_op_size,
                            0, UCG_CACHE_LINE_SIZE, UCG_ELEMS_PER_CHUNK,
                            UINT_MAX, NULL, "planc ucx op mpool");
    if (status != UCG_OK) {
        ucg_error("Failed to create mpool");
        goto err_free_planm_rscs;
    }

    if (ctx->config.use_oob == UCG_YES || ctx->config.use_oob == UCG_TRY) {
        ctx->ucp_worker = ucg_planc_ucx_get_oob_ucp_worker();
        if (ctx->ucp_worker == NULL) {
            if (ctx->config.use_oob == UCG_YES) {
                ucg_error("Failed to reuse OOB ucp resources.");
                status = UCG_ERR_NO_RESOURCE;
                goto err_free_mpool;
            }
            ctx->config.use_oob = UCG_NO;
            ucg_info("OOB ucp resource is not available, creating internal ucp resource.");
        } else {
            ctx->config.use_oob = UCG_YES;
        }
    }

    if (ctx->config.use_oob == UCG_NO) {
        status = ucg_planc_ucx_context_init_ucp_context(ctx);
        if (status != UCG_OK) {
            goto err_free_mpool;
        }

        status = ucg_planc_ucx_context_init_ucp_worker(ctx);
        if (status != UCG_OK) {
            goto err_cleanup_context;
        }

        ctx->eps = ucg_calloc(ctx->ucg_context->oob_group.size, sizeof(ucp_ep_h), "ucp eps");
        if (ctx->eps == NULL) {
            ucg_error("Failed to allocate %zd bytes for ucp_eps",
                      ctx->ucg_context->oob_group.size * sizeof(ucp_ep_h));
            status = UCG_ERR_NO_MEMORY;
            goto err_destroy_worker;
        }
    }

    *context = (ucg_planc_context_h)ctx;
    return UCG_OK;

err_destroy_worker:
    ucp_worker_destroy(ctx->ucp_worker);
err_cleanup_context:
    ucp_cleanup(ctx->ucp_context);
err_free_mpool:
    ucg_mpool_cleanup(&ctx->op_mp, 1);
err_free_planm_rscs:
    ucg_free(ctx->planm_rscs);
err_free_config:
    ucg_planc_ucx_context_free_config(ctx);
err_free_ctx:
    ucg_free(ctx);
    return status;
}

void ucg_planc_ucx_context_cleanup(ucg_planc_context_h context)
{
    UCG_CHECK_NULL_VOID(context);

    ucg_planc_ucx_context_t *ctx = (ucg_planc_ucx_context_t *)context;

    if (ctx->worker_address != NULL) {
        ucp_worker_release_address(ctx->ucp_worker, ctx->worker_address);
    }
    ucg_free(ctx->planm_rscs);
    ucg_mpool_cleanup(&ctx->op_mp, 1);
    ucg_free(ctx->eps);

    if (ctx->config.use_oob == UCG_NO) {
        ucp_worker_destroy(ctx->ucp_worker);
        ucp_cleanup(ctx->ucp_context);
    }

    ucg_planc_ucx_context_free_config(ctx);

    ucg_free(ctx);
    return;
}

ucg_status_t ucg_planc_ucx_context_query(ucg_planc_context_h context,
                                         ucg_planc_context_attr_t *attr)
{
    UCG_CHECK_NULL_INVALID(context, attr);

    ucs_status_t ucs_status = UCS_OK;
    ucg_planc_ucx_context_t *ctx = (ucg_planc_ucx_context_t *)context;

    if (ctx->config.use_oob == UCG_YES) {
        if (attr->field_mask & UCG_PLANC_CONTEXT_ATTR_FIELD_ADDR_LEN) {
            attr->addr_len = 0;
        }
        if (attr->field_mask & UCG_PLANC_CONTEXT_ATTR_FIELD_ADDR) {
            attr->addr = NULL;
        }
        goto out;
    }

    if ((attr->field_mask & (UCG_PLANC_CONTEXT_ATTR_FIELD_ADDR | UCG_PLANC_CONTEXT_ATTR_FIELD_ADDR_LEN))
        && ctx->worker_address == NULL) {
        ucs_status = ucp_worker_get_address(ctx->ucp_worker, &ctx->worker_address, &ctx->ucp_addrlen);
        if (ucs_status != UCS_OK) {
            ucg_error("Failed to get ucp worker address, %s", ucs_status_string(ucs_status));
            goto out;
        }
    }

    if (attr->field_mask & UCG_PLANC_CONTEXT_ATTR_FIELD_ADDR_LEN) {
        attr->addr_len = ctx->ucp_addrlen;
    }
    if (attr->field_mask & UCG_PLANC_CONTEXT_ATTR_FIELD_ADDR) {
        attr->addr = ctx->worker_address;
    }

out:
    return ucg_status_s2g(ucs_status);
}