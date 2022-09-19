/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "ucg_context.h"
#include "ucg_global.h"
#include "ucg_request.h"
#include "ucg_plan.h"

#include "planc/ucg_planc.h"
#include "util/ucg_helper.h"
#include "util/ucg_malloc.h"
#include "util/ucg_parser.h"
#include "util/ucg_cpu.h"


#define UCG_CONTEXT_COPY_REQUIRED_FIELD(_field, _copy, _dst, _src, _err_label) \
    UCG_COPY_REQUIRED_FIELD(UCG_TOKENPASTE(UCG_PARAMS_FIELD_, _field), _copy, _dst, _src, _err_label)

#define UCG_CONTEXT_COPY_OPTIONAL_FIELD(_field, _copy, _dst, _src, _default, _err_label) \
    UCG_COPY_OPTIONAL_FIELD(UCG_TOKENPASTE(UCG_PARAMS_FIELD_, _field), _copy, _dst, _src, _default, _err_label)

static ucg_config_field_t ucg_context_config_table[] = {
    {"PLANC", "all",
     "Comma-separated list of plan component to use. The order is not meaningful\n"
     " - all    : use all available plan component\n"
     " - ucx    : use plan component based-on ucx\n"
     " - hccl   : use plan component based-on hccl",
     ucg_offsetof(ucg_config_t, planc), UCG_CONFIG_TYPE_STRING_ARRAY},

    {"USE_MT_MUTEX", "n",
     "Use mutex for multithreading support in UCG\n"
     " - y      : use mutex for multi-thread support\n"
     " - n      : use spinlock by default",
     ucg_offsetof(ucg_config_t, use_mt_mutex), UCG_CONFIG_TYPE_BOOL},

    {NULL},
};
UCG_CONFIG_REGISTER_TABLE(ucg_context_config_table, "UCG context", NULL,
                          ucg_config_t, &ucg_config_global_list);

static ucg_status_t ucg_config_apply_env_prefix(ucg_config_t *config,
                                                const char *env_prefix)
{
    if (env_prefix == NULL) {
        config->env_prefix = ucg_strdup(UCG_DEFAULT_ENV_PREFIX, "default env prefix");
        if (config->env_prefix == NULL) {
            return UCG_ERR_NO_MEMORY;
        }
        return UCG_OK;
    }

    int full_env_prefix_len = strlen(env_prefix)
                              + 1 /* '_' */
                              + sizeof(UCG_DEFAULT_ENV_PREFIX);
    char *full_env_prefix = ucg_malloc(full_env_prefix_len, "ucg env prefix");
    if (full_env_prefix == NULL) {
        return UCG_ERR_NO_MEMORY;
    }

    snprintf(full_env_prefix, full_env_prefix_len, "%s_%s", env_prefix,
             UCG_DEFAULT_ENV_PREFIX);

    config->env_prefix = full_env_prefix;
    return UCG_OK;
}

static void ucg_config_release_planc_cfg(ucg_config_t *config)
{
    ucg_planc_config_h *planc_cfg = config->planc_cfg;
    int count = config->num_planc_cfg;
    for (int i = 0; i < count; ++i) {
        ucg_planc_t *planc = ucg_planc_get_by_idx(i);
        planc->config_release(planc_cfg[i]);
    }
    ucg_free(planc_cfg);
    return;
}

static ucg_status_t ucg_config_read_planc_cfg(ucg_config_t *config,
                                              const char *env_prefix,
                                              const char *filename)
{
    int count = ucg_planc_count();
    ucg_assert(count > 0);
    config->num_planc_cfg = 0;
    config->planc_cfg = ucg_calloc(count, sizeof(ucg_planc_config_h), "ucg planc cfg");
    if (config->planc_cfg == NULL) {
        return UCG_ERR_NO_MEMORY;
    }

    ucg_status_t status;
    for (int i = 0; i< count; ++i) {
        ucg_planc_t *planc = ucg_planc_get_by_idx(i);
        status = planc->config_read(env_prefix, filename, &config->planc_cfg[i]);
        if (status != UCG_OK) {
            ucg_error("Failed to read configuration of planc %s", planc->super.name);
            goto err_release;
        }
        ++config->num_planc_cfg;
    }

    return UCG_OK;

err_release:
    ucg_config_release_planc_cfg(config);
    return status;
}

static ucg_status_t ucg_context_check_version(uint32_t major_version,
                                              uint32_t minor_version)
{
    uint32_t api_major_version;
    uint32_t api_minor_version;
    uint32_t api_patch_version;
    ucg_get_version(&api_major_version, &api_minor_version, &api_patch_version);

    if (api_major_version != major_version ||
        (api_major_version == major_version && api_minor_version < minor_version)) {
        ucg_error("UCG version is incompatible, required: %u.%u, actual: %u.%u.%u",
                  major_version, minor_version,
                  api_major_version, api_minor_version, api_patch_version);
        return UCG_ERR_INCOMPATIBLE;
    }
    return UCG_OK;
}

static ucg_status_t ucg_context_apply_params(ucg_context_t *context,
                                             const ucg_params_t *params)
{
    uint64_t field_mask = params->field_mask;

    UCG_CONTEXT_COPY_REQUIRED_FIELD(OOB_GROUP, UCG_COPY_VALUE,
                                    context->oob_group, params->oob_group,
                                    err);

    UCG_CONTEXT_COPY_REQUIRED_FIELD(LOCATION_CB, UCG_COPY_VALUE,
                                    context->get_location, params->get_location,
                                    err);

    UCG_CONTEXT_COPY_OPTIONAL_FIELD(THREAD_MODE, UCG_COPY_VALUE,
                                    context->thread_mode, params->thread_mode,
                                    UCG_THREAD_MODE_SINGLE, err);

    if (context->thread_mode == UCG_THREAD_MODE_MULTI) {
#ifndef UCG_ENABLE_MT
        ucg_error("UCG is built without multi-thread support.");
        goto err;
#endif
    }

    return UCG_OK;

err:
    return UCG_ERR_INVALID_PARAM;
}

static int ucg_context_is_required_planc(ucg_planc_t *planc,
                                         ucg_config_names_array_t required)
{
    int count = required.count;
    const char *planc_name = planc->super.name;
    for (int i = 0; i < count; ++i) {
        if (!strcmp(planc_name, required.names[i]) ||
            !strcmp(required.names[i], "all")) {
            return 1;
        }
    }
    return 0;
}

static void ucg_context_free_resource_planc(ucg_context_t *context)
{
    int count = context->num_planc_rscs;
    for (int i = 0; i < count; ++i) {
        ucg_resource_planc_t *rsc = &context->planc_rscs[i];
        rsc->planc->context_cleanup(rsc->ctx);
    }
    ucg_free(context->planc_rscs);
    return;
}

static ucg_status_t ucg_context_fill_resource_planc(ucg_context_t *context,
                                                    const ucg_config_t *config)
{
    ucg_status_t status = UCG_OK;

    int count = ucg_planc_count();
    ucg_assert(count > 0);
    /* allocate enough space to avoid realloc. */
    context->num_planc_rscs = 0;
    context->planc_rscs = ucg_calloc(count, sizeof(ucg_resource_planc_t), "ucg resource planc");
    if (context->planc_rscs == NULL) {
        return UCG_ERR_NO_MEMORY;
    }

    ucg_planc_params_t params = {
        .context = context,
        .thread_mode = context->thread_mode,
    };
    for (int i = 0; i < count; ++i) {
        ucg_planc_t *planc = ucg_planc_get_by_idx(i);
        if (!ucg_context_is_required_planc(planc, config->planc)) {
            ucg_debug("planc %s is not required", planc->super.name);
            continue;
        }
        ucg_resource_planc_t *planc_rscs = &context->planc_rscs[context->num_planc_rscs];
        status = planc->context_init(&params, config->planc_cfg[i],
                                     &planc_rscs->ctx);
        if (status != UCG_OK) {
            ucg_error("Failed to init context of planc %s", planc->super.name);
            goto err_free_resource;
        }
        planc_rscs->planc = planc;
        ++context->num_planc_rscs;
    }

    if (context->num_planc_rscs == 0) {
        goto err_free_resource;
    }
    return UCG_OK;

err_free_resource:
    ucg_context_free_resource_planc(context);
    return UCG_ERR_NO_RESOURCE;
}

static void ucg_context_free_resource_mt(ucg_context_t *context)
{
    ucg_lock_destroy(&context->mt_lock);
    return;
}

static ucg_status_t ucg_context_fill_resource_mt(ucg_context_t *context,
                                                 const ucg_config_t *config)
{
    ucg_lock_type_t lock_type = UCG_LOCK_TYPE_NONE;
    if (context->thread_mode != UCG_THREAD_MODE_MULTI) {
        goto lock_init;
    }

    ucg_status_t status;
    ucg_planc_context_attr_t attr = {
        .field_mask = UCG_PLANC_CONTEXT_ATTR_FIELD_THREAD_MODE,
    };
    int num_planc_rscs = context->num_planc_rscs;
    for (int i = 0; i < num_planc_rscs; ++i) {
        ucg_resource_planc_t*rsc = &context->planc_rscs[i];
        /* Some planc may not support the query of thread mode which indicates
           that is a non-thread-safe planc, so set the initial thread mode. */
        attr.thread_mode = UCG_THREAD_MODE_SINGLE;
        status = rsc->planc->context_query(rsc->ctx, &attr);
        if (status != UCG_OK || attr.thread_mode == UCG_THREAD_MODE_SINGLE) {
            ucg_debug("There's a non-thread-safe planc, using context lock.");
            lock_type = config->use_mt_mutex ? UCG_LOCK_TYPE_MUTEX : UCG_LOCK_TYPE_SPINLOCK;
            break;
        }
    }

lock_init:
    return ucg_lock_init(&context->mt_lock, lock_type);
}

static void ucg_context_free_resource(ucg_context_t *context)
{
    ucg_context_free_resource_mt(context);
    ucg_context_free_resource_planc(context);
    return;
}

static ucg_status_t ucg_context_fill_resource(ucg_context_t *context,
                                              const ucg_config_t *config)
{
    ucg_status_t status;
    status = ucg_context_fill_resource_planc(context, config);
    if (status != UCG_OK) {
        goto err;
    }

    status = ucg_context_fill_resource_mt(context, config);
    if (status != UCG_OK) {
        goto err_free_resource_planc;
    }

    return UCG_OK;

err_free_resource_planc:
    ucg_context_free_resource_planc(context);
err:
    return status;
}

static ucg_proc_info_t* ucg_context_get_local_proc_info(ucg_context_t *context)
{
    int num_planc_rscs = context->num_planc_rscs;
    uint32_t proc_info_size = sizeof(ucg_proc_info_t) +
                              sizeof(ucg_addr_desc_t) * num_planc_rscs;
    uint32_t addr_offset = proc_info_size;

    ucg_status_t status;
    ucg_planc_context_attr_t attr;
    attr.field_mask = UCG_PLANC_CONTEXT_ATTR_FIELD_ADDR_LEN;
    for (int i = 0; i < num_planc_rscs; ++i) {
        ucg_resource_planc_t *planc_rsc = &context->planc_rscs[i];
        status = planc_rsc->planc->context_query(planc_rsc->ctx, &attr);
        if (status != UCG_OK) {
            ucg_error("Failed to query planc address length");
            goto err;
        }
        proc_info_size += attr.addr_len;
    }

    ucg_proc_info_t *proc = ucg_malloc(proc_info_size, "local proc");
    if (proc == NULL) {
        ucg_error("Failed to allocate %u bytes", proc_info_size);
        goto err;
    }

    proc->size = proc_info_size;
    status = context->get_location(context->oob_group.myrank, &proc->location);
    if (status != UCG_OK) {
        ucg_error("Failed to get location of rank %d", context->oob_group.myrank);
        goto err_free_proc;
    }
    ucg_debug("Location of rank %d: subnet %d, node %d, socket %d", context->oob_group.myrank,
        (proc->location.field_mask & UCG_LOCATION_FIELD_SUBNET_ID) ? proc->location.subnet_id : -1,
        (proc->location.field_mask & UCG_LOCATION_FIELD_NODE_ID) ? proc->location.node_id : -1,
        (proc->location.field_mask & UCG_LOCATION_FIELD_SOCKET_ID) ? proc->location.socket_id : -1);

    proc->num_addr_desc = num_planc_rscs;
    attr.field_mask |= UCG_PLANC_CONTEXT_ATTR_FIELD_ADDR;
    for (int i = 0; i < num_planc_rscs; ++i) {
        ucg_resource_planc_t *planc_rsc = &context->planc_rscs[i];
        status = planc_rsc->planc->context_query(planc_rsc->ctx, &attr);
        if (status != UCG_OK) {
            ucg_error("Failed to query planc address");
            goto err_free_proc;
        }
        if (attr.addr_len > 0) {
            memcpy((uint8_t*)proc + addr_offset, attr.addr, attr.addr_len);
        }
        proc->addr_desc[i].len = attr.addr_len;
        proc->addr_desc[i].offset = addr_offset;
        addr_offset += attr.addr_len;
    }

    return proc;

err_free_proc:
    ucg_free(proc);
err:
    return NULL;
}

static ucg_status_t ucg_context_get_max_size(ucg_context_t *context,
                                             uint32_t size,
                                             uint32_t *max_size)
{
    ucg_oob_group_t *oob_group = &context->oob_group;
    uint32_t oob_group_size = oob_group->size;
    uint32_t *size_array = ucg_malloc(sizeof(uint32_t) * oob_group_size,
                                      "size array");
    if (size_array == NULL) {
        return UCG_ERR_NO_MEMORY;
    }

    ucg_status_t status = oob_group->allgather(&size, size_array,
                                               sizeof(uint32_t),
                                               oob_group->group);
    if (status != UCG_OK) {
        ucg_error("Failed to allgather size");
        goto out;
    }

    *max_size = 0;
    for (int i = 0; i < oob_group_size; ++i) {
        if (*max_size < size_array[i]) {
            *max_size = size_array[i];
        }
    }

out:
    ucg_free(size_array);
    return status;
}

static ucg_status_t ucg_context_fill_procs(ucg_context_t *context)
{
    ucg_status_t status;

    ucg_proc_info_t *local_proc = ucg_context_get_local_proc_info(context);
    if (local_proc == NULL) {
        return UCG_ERR_NO_RESOURCE;
    }

    uint32_t max_proc_info_size;
    status = ucg_context_get_max_size(context, local_proc->size, &max_proc_info_size);
    if (status != UCG_OK) {
        goto err_free_local_proc;
    }

    ucg_assert(max_proc_info_size > 0);
    ucg_oob_group_t *oob_group = &context->oob_group;
    ucg_proc_info_t *procs = ucg_malloc(max_proc_info_size * (oob_group->size + 1), "procs");
    if (procs == NULL) {
        status = UCG_ERR_NO_MEMORY;
        goto err_free_local_proc;
    }
    /* local_proc->size may less than max_proc_info_size, using the last space
       to save local process information to avoid memory read out-of-bound. */
    ucg_proc_info_t *tmp_local_proc = (ucg_proc_info_t*)((uint8_t*)procs + max_proc_info_size * oob_group->size);
    memcpy(tmp_local_proc, local_proc, local_proc->size);
    status = oob_group->allgather(tmp_local_proc, procs, max_proc_info_size,
                                  oob_group->group);
    if (status != UCG_OK) {
        ucg_error("Failed to allgather proc info");
        goto err_free_procs;
    }
    ucg_free(local_proc);

    context->procs.count = oob_group->size;
    context->procs.stride = max_proc_info_size;
    context->procs.info = (uint8_t*)procs;
    return UCG_OK;

err_free_procs:
    ucg_free(procs);
err_free_local_proc:
    ucg_free(local_proc);
    return status;
}

static void ucg_context_free_procs(ucg_context_t *context)
{
    context->procs.count = 0;
    context->procs.stride = 0;
    ucg_free(context->procs.info);
    return;
}

static ucg_status_t ucg_context_init_version(uint32_t major_version,
                                             uint32_t minor_version,
                                             const ucg_params_t *params,
                                             const ucg_config_h config,
                                             ucg_context_h *context)
{
    UCG_CHECK_NULL_INVALID(params, config, context);

    ucg_status_t status;
    status = ucg_context_check_version(major_version, minor_version);
    if (status != UCG_OK) {
        return status;
    }

    ucg_context_t *ctx = ucg_calloc(1, sizeof(ucg_context_t), "ucg context");
    if (ctx == NULL) {
        return UCG_ERR_NO_MEMORY;
    }

    status = ucg_context_apply_params(ctx, params);
    if (status != UCG_OK) {
        goto err_free_ctx;
    }

    status = ucg_context_fill_resource(ctx, config);
    if (status != UCG_OK) {
        goto err_free_ctx;
    }

    status = ucg_context_fill_procs(ctx);
    if (status != UCG_OK) {
        goto err_free_resource;
    }
    ucg_list_head_init(&ctx->plist);

    status = ucg_mpool_init(&ctx->meta_op_mp, 0, sizeof(ucg_plan_meta_op_t),
                            0, UCG_CACHE_LINE_SIZE, UCG_ELEMS_PER_CHUNK,
                            UINT_MAX, NULL, "meta op mpool");
    if (status != UCG_OK) {
        ucg_error("Failed to create mpool");
        goto err_free_procs;
    }

    ucg_debug("Initialized ucg context %p, oob group size %u, myrank %d, "
              "thread mode %d", ctx, ctx->oob_group.size,
              ctx->oob_group.myrank, ctx->thread_mode);

    *context = ctx;
    return UCG_OK;

err_free_procs:
    ucg_context_free_procs(ctx);
err_free_resource:
    ucg_context_free_resource(ctx);
err_free_ctx:
    ucg_free(ctx);
    return status;
}

static int ucg_context_progress(ucg_context_h context)
{
    ucg_context_lock(context);
    int count = 0;
    ucg_request_t *req = NULL;
    ucg_request_t *tmp_req = NULL;
    ucg_list_for_each_safe(req, tmp_req, &context->plist, list) {
        ucg_status_t status = ucg_request_test(req);
        if (status != UCG_INPROGRESS) {
            ++count;
        }
    }
    ucg_context_unlock(context);

    return count;
}

static void ucg_context_cleanup(ucg_context_h context)
{
    UCG_CHECK_NULL_VOID(context);

    ucg_mpool_cleanup(&context->meta_op_mp, 1);
    ucg_context_free_procs(context);
    ucg_context_free_resource(context);
    ucg_free(context);
    return;
}

void* ucg_context_get_proc_addr(ucg_context_t *context, ucg_rank_t rank, ucg_planc_t *planc)
{
    ucg_assert(context != NULL && planc != NULL);
    ucg_assert(rank != UCG_INVALID_RANK && rank < context->oob_group.size);
    int num_planc_rscs = context->num_planc_rscs;
    for (int i = 0; i < num_planc_rscs; ++i) {
        if (planc == context->planc_rscs[i].planc) {
            ucg_proc_info_t *proc_info = UCG_PROC_INFO(context, rank);
            if (UCG_PROC_ADDR_LEN(proc_info, i) == 0) {
                return NULL;
            }
            return UCG_PROC_ADDR(proc_info, i);
        }
    }
    return NULL;
}

ucg_status_t ucg_context_get_location(ucg_context_t *context, ucg_rank_t rank,
                                      ucg_location_t *location)
{
    ucg_assert(context != NULL && location != NULL);
    ucg_assert(rank != UCG_INVALID_RANK && rank < context->oob_group.size);
    ucg_proc_info_t *proc = UCG_PROC_INFO(context, rank);
    *location = proc->location;
    return UCG_OK;
}

ucg_status_t ucg_config_read(const char *env_prefix, const char *filename,
                             ucg_config_h *config)
{
    UCG_CHECK_NULL_INVALID(config);

    ucg_status_t status;

    if (filename != NULL) {
        return UCG_ERR_UNSUPPORTED;
    }

    ucg_config_h cfg = ucg_malloc(sizeof(ucg_config_t), "ucg config");
    if (cfg == NULL) {
        status = UCG_ERR_NO_MEMORY;
        goto err;
    }

    status = ucg_config_apply_env_prefix(cfg, env_prefix);
    if (status != UCG_OK) {
        goto err_free_cfg;
    }

    status = ucg_config_parser_fill_opts(cfg, ucg_context_config_table,
                                         cfg->env_prefix, NULL, 0);
    if (status != UCG_OK) {
        goto err_free_env_prefix;
    }

    status = ucg_config_read_planc_cfg(cfg, env_prefix, filename);
    if (status != UCG_OK) {
        goto err_free_opts;
    }

    *config = cfg;
    return UCG_OK;

err_free_opts:
    ucg_config_parser_release_opts(cfg, ucg_context_config_table);
err_free_env_prefix:
    ucg_free(cfg->env_prefix);
err_free_cfg:
    ucg_free(cfg);
err:
    return status;
}

ucg_status_t ucg_config_modify(ucg_config_h config, const char *name,
                               const char *value)
{
    UCG_CHECK_NULL_INVALID(config, name, value);

    /* Try all components. */
    ucg_status_t status;
    status = ucg_config_parser_set_value(config, ucg_context_config_table, name, value);
    if (status == UCG_OK) {
        return UCG_OK;
    }

    int count = ucg_planc_count();
    for (int i = 0; i < count; ++i) {
        ucg_planc_t *planc = ucg_planc_get_by_idx(i);
        status = planc->config_modify(config->planc_cfg[i], name, value);
        if (status == UCG_OK) {
            return UCG_OK;
        }
    }

    return UCG_ERR_NOT_FOUND;
}

void ucg_config_release(ucg_config_h config)
{
    UCG_CHECK_NULL_VOID(config);

    ucg_config_release_planc_cfg(config);
    ucg_config_parser_release_opts(config, ucg_context_config_table);
    ucg_free(config->env_prefix);
    ucg_free(config);
    return;
}

ucg_status_t ucg_init_version(uint32_t major_version, uint32_t minor_version,
                              const ucg_params_t *params, const ucg_config_h config,
                              ucg_context_h *context)
{
    return ucg_context_init_version(major_version, minor_version, params, config, context);
}

int ucg_progress(ucg_context_h context)
{
    return ucg_context_progress(context);
}

void ucg_cleanup(ucg_context_h context)
{
    ucg_context_cleanup(context);
    return;
}