/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#include "ucg_context.h"
#include "ucg_group.h"

#include <ucg/api/ucg_version.h>
#include <ucp/core/ucp_types.h>
#include <ucp/core/ucp_context.h>
#include <ucs/debug/debug.h>
#include <ucs/config/parser.h>
#include <ucs/debug/memtrack.h>
#include <ucs/sys/string.h>

#define UCG_CONFIG_ALL "all"

ucs_config_field_t ucg_config_table[] = {
    {"PLANNERS", UCG_CONFIG_ALL,
    "Specifies which collective planner(s) to use. The order is not meaningful.\n"
    "\"all\" would use all available planners.",
    ucs_offsetof(ucg_config_t, planners), UCS_CONFIG_TYPE_STRING_ARRAY},
};
UCS_CONFIG_REGISTER_TABLE(ucg_config_table, "UCG context", NULL, ucg_config_t);

static ucs_status_t ucg_apply_params(ucg_context_h context, const ucg_params_t *params)
{
    ucs_status_t status = UCS_ERR_INVALID_PARAM;
    uint64_t field_mask = params->field_mask;
    context->params.field_mask = field_mask;

    UCG_CHECK_REQUIRED_FIELD(field_mask, UCG_PARAM_FIELD_ADDRESS_CB, err);
    context->params.address.lookup_f = params->address.lookup_f;
    context->params.address.release_f = params->address.release_f;
  
    UCG_CHECK_REQUIRED_FIELD(field_mask, UCG_PARAM_FIELD_REDUCE_CB, err);
    context->params.mpi_reduce_f = params->mpi_reduce_f;

    UCG_CHECK_REQUIRED_FIELD(field_mask, UCG_PARAM_FIELD_COMMUTE_CB, err);
    context->params.op_is_commute_f = params->op_is_commute_f;

    UCG_CHECK_REQUIRED_FIELD(field_mask, UCG_PARAM_FIELD_MPI_IN_PLACE, err);
    context->params.mpi_in_place = params->mpi_in_place;

    return UCS_OK;
err:
    return status;
}

static int ucg_str_array_find(char **array, unsigned count, char *str)
{
    int i;
    for(i = 0; i < count; ++i) {
        if (0 == strcmp(array[i], str)) {
            return 1;
        }
    }
    return 0;
}

static void ucg_insert_planner(ucg_context_h context, 
                               ucg_planner_h planner)
{
    ucs_list_link_t *head = &context->planners_head;
    ucg_planner_h elem;
    ucs_list_for_each(elem, head, list) {
        /* planner with high priority is in the front */
        if (planner->priority >= elem->priority) {
            ucs_list_insert_before(&elem->list, &planner->list);
            return;
        }
    }
    ucs_list_add_tail(head, &planner->list);
    return;
}

static void ucg_release_planners(ucg_context_h context)
{
    ucg_planner_h elem;
    ucg_planner_h telem;
    ucs_list_for_each_safe(elem, telem, &context->planners_head, list) {
        ucs_list_del(&elem->list);
        ucs_free(elem);
    }
    return;
}

static ucs_status_t ucg_fill_planners(ucg_context_h context, 
                                      const ucg_config_t *config)
{
    ucs_status_t status;
    ucs_list_head_init(&context->planners_head);
    
    ucg_plan_component_h *plan_components;
    unsigned num_plan_components;
    status = ucg_query_components(&plan_components, &num_plan_components);
    if (status != UCS_OK) {
        ucs_error("Failed to query components");
        goto err;
    }

    int use_all = ucg_str_array_find(config->planners.names, 
                                     config->planners.count, 
                                     UCG_CONFIG_ALL);

    int i;
    ucg_planner_h planner;
    for (i = 0; i < num_plan_components; ++i) {
        status = plan_components[i]->query(&planner);
        if (status != UCS_OK) {
            ucs_error("Failed to query planner of \"%s\".", plan_components[i]->name);
            goto err_release_planner;
        }
        if (!use_all && !ucg_str_array_find(config->planners.names,
                                            config->planners.count,
                                            planner->name)) {
            ucs_free(planner);
            continue;
        }
        ucg_insert_planner(context, planner);
        ucs_debug("Insert planner \"%s\"", planner->name);
    }

    ucg_release_components(plan_components);

    return UCS_OK;

err_release_planner:
    ucg_release_planners(context);
err_free_component:
    ucg_release_components(plan_components);
err:
    return status;
}

static void ucg_cleanup_planc_context(ucg_context_h context)
{
    ucg_planner_h planner;
    int i = 0;
    ucs_list_for_each(planner, &context->planners_head, list) {
        ucg_planc_ctx_h planc_ctx = context->planc_ctx[i++];
        if (planc_ctx == NULL) {
            break;
        }

        planner->plan_component->cleanup(planc_ctx);
    }
    ucs_free(context->planc_ctx);

    return;
}

static ucs_status_t ucg_init_planc_context(ucg_context_h context)
{
    int num_planners = ucs_list_length(&context->planners_head);
    int malloc_size = num_planners * sizeof(ucg_planc_ctx_h);
    context->planc_ctx = (ucg_planc_ctx_h*)ucs_calloc(1, malloc_size, "planc context");
    if (context->planc_ctx == NULL) {
        ucs_error("Failed to allocate component context");
        return UCS_ERR_NO_MEMORY;
    }

    ucs_status_t status;
    ucg_planner_h planner;
    int i =0;
    ucs_list_for_each(planner, &context->planners_head, list) {
        status = planner->plan_component->init(context, &(context->planc_ctx[i++]));
        if (status != UCS_OK) {
            ucs_error("Failed to init context of \"%s\"", planner->plan_component->name);
            goto err_cleanup_planc_ctx;
        }
    }

    return UCS_OK;

err_cleanup_planc_ctx:
    ucg_cleanup_planc_context(context);
    return status;
}

ucs_status_t ucg_init_version(unsigned api_major_version,
                              unsigned api_minor_version,
                              const ucg_params_t *params,
                              const ucg_config_t *config,
                              ucg_context_h *ctx_p)
{
    ucs_status_t status;
    unsigned major_version;
    unsigned minor_version;
    unsigned release_version;
    
    ucg_get_version(&major_version, &minor_version, &release_version);

    if ((api_major_version != major_version) ||
        (api_major_version == major_version && 
         api_minor_version > minor_version)) {
        ucs_debug_address_info_t addr_info;
        status = ucs_debug_lookup_address(ucg_init_version, &addr_info);
        ucs_warn("UCG version is incompatible, required: %d.%d, actual: %d.%d (release %d %s)",
                  api_major_version, api_minor_version,
                  major_version, minor_version, release_version,
                  status == UCS_OK ? addr_info.file.path : "");
    }

    ucg_context_t *context = ucs_calloc(1, sizeof(ucg_context_t), "ucg context");
    if (context == NULL) {
        ucs_error("Failed to allocate ucg context");
        status = UCS_ERR_NO_MEMORY;
        goto err;
    }

    status = ucg_apply_params(context, params);
    if (status != UCS_OK) {
        ucs_error("Failed to apply params");
        goto err_free_ctx;
    }

    ucg_config_t *dfl_config = NULL;
    if (config == NULL) {
        status = ucg_config_read(NULL, NULL, &dfl_config);
        if (status != UCS_OK) {
            ucs_error("Failed to read ucg configuration");
            goto err_free_ctx;
        }
        config = dfl_config;
    }

    status = ucg_fill_planners(context, config);
    if (status != UCS_OK) {
        ucs_error("Failed to fill planners");
        goto err_release_config;
    }

    status = ucg_init_planc_context(context);
    if (status != UCS_OK) {
        ucs_error("Failed to init component context");
        goto err_release_planner;
    }

    if (dfl_config != NULL) {
        ucg_config_release(dfl_config);
    }

    ucs_list_head_init(&context->groups_head);
    *ctx_p = context;
    
    return UCS_OK;

err_release_planner:
    ucg_release_planners(context);
err_release_config:
    if (dfl_config != NULL) {
        ucg_config_release(dfl_config);
    }
err_free_ctx:
    ucs_free(context);
err:
    return status;
}

ucs_status_t ucg_init(const ucg_params_t *params,
                      const ucg_config_t *config,
                      ucg_context_h *ctx_p)
{
    return ucg_init_version(UCG_API_MAJOR, UCG_API_MINOR, params, config, ctx_p);
}

void ucg_cleanup(ucg_context_h ctx_p)
{
    if (!ucs_list_is_empty(&ctx_p->groups_head)) {
        ucs_warn("Some ucg group are not destroyed.");
        ucg_group_h group;
        ucg_group_h tgroup;
        ucs_list_for_each_safe(group, tgroup, &ctx_p->groups_head, list) {
            ucg_group_destroy(group);
        }
    }
    
    ucg_cleanup_planc_context(ctx_p);
    ucg_release_planners(ctx_p);
    ucs_free(ctx_p);
    
    return;
}

ucs_status_t ucg_config_read(const char *env_prefix, const char *filename,
                             ucg_config_t **config_p)
{
    unsigned full_prefix_len = sizeof(UCS_DEFAULT_ENV_PREFIX) + 1;
    unsigned env_prefix_len  = 0;
    ucg_config_t *config;
    ucs_status_t status;

    config = ucs_malloc(sizeof(*config), "ucg config");
    if (config == NULL) {
        status = UCS_ERR_NO_MEMORY;
        goto err;
    }

    if (env_prefix != NULL) {
        env_prefix_len   = strlen(env_prefix);
        full_prefix_len += env_prefix_len;
    }

    config->env_prefix = ucs_malloc(full_prefix_len, "ucg config");
    if (config->env_prefix == NULL) {
        status = UCS_ERR_NO_MEMORY;
        goto err_free_config;
    }

    if (env_prefix_len != 0) {
        ucs_snprintf_zero(config->env_prefix, full_prefix_len, "%s_%s",
                          env_prefix, UCS_DEFAULT_ENV_PREFIX);
    } else {
        ucs_snprintf_zero(config->env_prefix, full_prefix_len, "%s",
                          UCS_DEFAULT_ENV_PREFIX);
    }

    status = ucs_config_parser_fill_opts(config, ucg_config_table,
                                         config->env_prefix, NULL, 0);
    if (status != UCS_OK) {
        goto err_free_prefix;
    }

    *config_p = config;
    return UCS_OK;

err_free_prefix:
    ucs_free(config->env_prefix);
err_free_config:
    ucs_free(config);
err:
    return status;
}

void ucg_config_release(ucg_config_t *config)
{
    ucs_config_parser_release_opts(config, ucg_config_table);
    ucs_free(config->env_prefix);
    ucs_free(config);
}

ucs_status_t ucg_config_modify(ucg_config_t *config, const char *name,
                               const char *value)
{
    return ucs_config_parser_set_value(config, ucg_config_table, name, value);
}

void ucg_config_print(const ucg_config_t *config, FILE *stream,
                      const char *title, ucs_config_print_flags_t print_flags)
{
    ucs_config_parser_print_opts(stream, title, config, ucg_config_table,
                                 NULL, UCS_DEFAULT_ENV_PREFIX, print_flags);
}

ucs_status_t ucg_context_set_am_handler(uct_am_callback_t cb,
                                        void* arg,
                                        uct_am_tracer_t tracer,
                                        uint8_t *am_id)
{
    static uint8_t last_am_id = UCP_AM_ID_LAST;
    if (last_am_id >= UCP_AM_ID_MAX) {
        ucs_error("active message id out-of-range (last_am_id: %d max: %d)", last_am_id, UCP_AM_ID_LAST);
        return UCS_ERR_NO_RESOURCE;
    }
    
    *am_id = last_am_id;
    ucp_am_handler_t *am_handler = ucp_am_handlers + last_am_id++;
    am_handler->features = UCP_FEATURE_AM;
    am_handler->cb = cb;
    am_handler->tracer = (ucp_am_tracer_t)tracer;
    am_handler->alt_arg = arg;
    
    return UCS_OK;
}