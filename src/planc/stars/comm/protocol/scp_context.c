/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#include "scp_context.h"

#define SCP_RSC_CONFIG_ALL "all"

const char *SCT_COMPONENT_LIST[MAX_SCT_COMPONENT_NUM] = {SCT_STARS_SDMA_ACC, SCT_STARS_RC_ACC};

static ucs_config_field_t scp_config_table[] = {
    {"NET_DEVICES",
        SCP_RSC_CONFIG_ALL,
        "Specifies which network device(s) to use. The order is not meaningful.\n"
        "\"all\" would use all available devices.",
        ucs_offsetof(scp_config_t, devices[UCT_DEVICE_TYPE_NET]),
        UCS_CONFIG_TYPE_STRING_ARRAY},

    {"SHM_DEVICES",
        SCP_RSC_CONFIG_ALL,
        "Specifies which intra-node device(s) to use. The order is not meaningful.\n"
        "\"all\" would use all available devices.",
        ucs_offsetof(scp_config_t, devices[UCT_DEVICE_TYPE_SHM]),
        UCS_CONFIG_TYPE_STRING_ARRAY},

    {"ACC_DEVICES",
        SCP_RSC_CONFIG_ALL,
        "Specifies which accelerator device(s) to use. The order is not meaningful.\n"
        "\"all\" would use all available devices.",
        ucs_offsetof(scp_config_t, devices[UCT_DEVICE_TYPE_ACC]),
        UCS_CONFIG_TYPE_STRING_ARRAY},

    {"SELF_DEVICES",
        SCP_RSC_CONFIG_ALL,
        "Specifies which loop-back device(s) to use. The order is not meaningful.\n"
        "\"all\" would use all available devices.",
        ucs_offsetof(scp_config_t, devices[UCT_DEVICE_TYPE_SELF]),
        UCS_CONFIG_TYPE_STRING_ARRAY},

    {"TLS",
        SCP_RSC_CONFIG_ALL,
        "Comma-separated list of transports to use. The order is not meaningful.\n"
        " - all           : use all the available transports.\n"
        " - sdma_acc    : shared memory transports - only memory mappers.\n"
        " - rc_acc      : rc with stars accelerated verbs (uses ud for bootstrap).\n"
        " Using a \\ prefix before a transport name treats it as an explicit transport name\n"
        " and disables aliasing.\n",
        ucs_offsetof(scp_config_t, tls),
        UCS_CONFIG_TYPE_STRING_ARRAY},

    {"MAX_WORKER_NAME",
        UCS_PP_MAKE_STRING(SCP_WORKER_NAME_MAX),
        "Maximal length of worker name. Sent to remote peer as part of worker address\n"
        "if UCX_ADDRESS_DEBUG_INFO is set to 'yes'",
        ucs_offsetof(scp_config_t, ctx.max_worker_name),
        UCS_CONFIG_TYPE_UINT},

    {"USE_MT_MUTEX",
        "n",
        "Use mutex for multithreading support in UCP.\n"
        "n      - Not use mutex for multithreading support in UCP (use spinlock by default).\n"
        "y      - Use mutex for multithreading support in UCP.\n",
        ucs_offsetof(scp_config_t, ctx.use_mt_mutex),
        UCS_CONFIG_TYPE_BOOL},

    {"MAX_RAILS",
        UCS_PP_MAKE_STRING(2),
        "Set the stars multi rails",
        ucs_offsetof(scp_config_t, ctx.multirails),
        UCS_CONFIG_TYPE_UINT},

    {"MULTI_THRES",
        UCS_PP_MAKE_STRING(1024),
        "Set the multirails enabling threshold(bytes)",
        ucs_offsetof(scp_config_t, ctx.multirails_threshold),
        UCS_CONFIG_TYPE_ULONG},
    
    {"STREAM_DEPTH",
        UCS_PP_MAKE_STRING(4096),
        "Set the stream depth of stars handle",
        ucs_offsetof(scp_config_t, ctx.stream_depth),
        UCS_CONFIG_TYPE_ULONG},

    {NULL}};

UCS_CONFIG_REGISTER_TABLE(scp_config_table, "SCP context", NULL, scp_config_t,
                          &ucs_config_global_list)

ucg_status_t scp_config_read(const char *env_prefix, scp_config_t **config_p)
{
    unsigned full_prefix_len = sizeof(STARS_DEFAULT_ENV_PREFIX) + 1;
    unsigned env_prefix_len  = 0;
    scp_config_t *config;
    ucs_status_t ucs_status;
    ucg_status_t status = UCG_OK;

    config = ucg_malloc(sizeof(*config), "ucp config");
    if (config == NULL) {
        return UCG_ERR_NO_MEMORY;
    }

    if (env_prefix != NULL) {
        env_prefix_len   = strlen(env_prefix);
        full_prefix_len += env_prefix_len;
    }

    config->env_prefix = ucg_malloc(full_prefix_len, "scp config");
    if (config->env_prefix == NULL) {
        status = UCG_ERR_NO_MEMORY;
        goto err_free_config;
    }

    if (env_prefix_len != 0) {
        ucs_snprintf_zero(config->env_prefix, full_prefix_len, "%s_%s",
                          env_prefix, STARS_DEFAULT_ENV_PREFIX);
    } else {
        ucs_snprintf_zero(config->env_prefix, full_prefix_len, "%s",
                          STARS_DEFAULT_ENV_PREFIX);
    }

    ucs_status = ucg_config_parser_fill_opts(config, UCG_CONFIG_GET_TABLE(scp_config_table),
                                             config->env_prefix, 0);
    if (ucs_status != UCS_OK) {
        status = ucg_status_s2g(ucs_status);
        goto err_free_prefix;
    }

    *config_p = config;
    return UCG_OK;

err_free_prefix:
    ucg_free(config->env_prefix);
err_free_config:
    ucg_free(config);
    return status;
}

static uct_md_h scp_context_find_tl_md(ucp_context_h context, const char *md_name)
{
    char *tl_mds_base;
    char *tl_mds_elem;
    uint8_t num_mds = GET_VAL_BY_ADDRESS(uint8_t, context, 24);
    uct_md_resource_desc_t *rsc;
    uct_md_h uct_md;

    SET_PTR_BY_VOID_ADDRESS(tl_mds_base, char *, context, 16);
    /* loop all md of ucp try to find match ib md which already open ib device */
    for (scp_rsc_index_t rsc_index = 0; rsc_index < num_mds; ++rsc_index) {
        tl_mds_elem = tl_mds_base + 256 * rsc_index;
        SET_PTR_BY_VOID_ADDRESS(uct_md, uct_md_h, tl_mds_elem, 0);
        rsc = (uct_md_resource_desc_t *)((char *)tl_mds_elem + 9);

        if (!strcmp(rsc->md_name, md_name)) {
            /* md_name, alias ib device name */
            return uct_md;
        }
    }

    return NULL;
}

static uint64_t scp_str_array_search(const char **array, unsigned array_len, const char *str)
{
    uint64_t result = 0;
    for (unsigned i = 0; i < array_len; ++i) {
        if (!strcmp(array[i], str)) {
            result |= UCS_BIT(i);
        }
    }
    return result;
}

static int scp_config_is_tl_enabled(const char **names, unsigned count, const char *tl_name, int is_alias)
{
    char strict_name[UCT_TL_NAME_MAX + 1];
    snprintf(strict_name, sizeof(strict_name), "\\%s", tl_name);

    return /* strict name, with leading \\ */
            (!is_alias && scp_str_array_search(names, count, strict_name)) ||
            /* plain transport name */
            scp_str_array_search(names, count, tl_name) ||
            /* all available transports */
            scp_str_array_search(names, count, SCP_RSC_CONFIG_ALL);
}

static uint8_t scp_is_resc_in_tl_list(const char *tl_name, const char **names, unsigned count)
{
    return scp_config_is_tl_enabled(names, count, tl_name, 0) ? 1 : 0;
}

static ucg_status_t scp_fill_tl_md(scp_context_h context,
                                   scp_tl_cmpt_t *tl_cmpt,
                                   const ucp_context_h ucp_context,
                                   const uct_md_resource_desc_t *md_rsc,
                                   scp_tl_md_t *tl_md)
{
    scp_config_t *config = context->scp_config;
    uct_md_h match_md = NULL;
    if (!strcmp(tl_cmpt->attr.name, SCT_STARS_RC_ACC)) {
        if (!scp_is_resc_in_tl_list(STARS_RC_TL_NAME, (const char**)config->tls.names,
                                    config->tls.count)) {
            return UCG_ERR_NOT_FOUND;
        }

        /* will try get information for the ib which maybe already opened by ucx ib,
           which don't support open again */
        match_md = scp_context_find_tl_md(ucp_context, md_rsc->md_name);
        if (match_md == NULL) {
            ucg_info("Failed to find match md name:%s", md_rsc->md_name);
        }
    } else if (!strcmp(tl_cmpt->attr.name, SCT_STARS_SDMA_ACC)) {
        if (!scp_is_resc_in_tl_list(STARS_SDMA_TL_NAME, (const char **)config->tls.names,
            config->tls.count)) {
            return UCG_ERR_NOT_FOUND;
        }
    } else {
        ucg_fatal("Unexpected component %s", tl_cmpt->attr.name);
    }

    ucs_status_t ucs_status;
    sct_md_config_t *md_config = NULL;
    ucs_status = sct_md_config_read(tl_cmpt->cmpt, NULL, NULL, &md_config);
    if (ucs_status != UCS_OK) {
        ucg_error("Failed to read memory domain config for %s", md_rsc->md_name);
        return ucg_status_s2g(ucs_status);
    }

    ucs_status = sct_md_open(tl_cmpt->cmpt, md_rsc->md_name, md_config, match_md, &tl_md->md);
    sct_config_release(md_config);
    if (ucs_status != UCS_OK) {
        ucg_debug("Failed to open memory domain for %s", md_rsc->md_name);
        return ucg_status_s2g(ucs_status);
    }

    ucs_status = sct_md_query(tl_md->md, &tl_md->attr);
    if (ucs_status != UCS_OK) {
        sct_md_close(tl_md->md);
        ucg_error("Failed to query memory domain for %s", md_rsc->md_name);
        return ucg_status_s2g(ucs_status);
    }

    tl_md->rsc = *md_rsc;
    ucg_info("Open memory domain for cmpt %s md_name %s", tl_cmpt->attr.name, md_rsc->md_name);
    return UCG_OK;
}

static int scp_is_resource_in_device_list(const sct_tl_resource_desc_t *resource,
                                          const ucs_config_names_array_t *devices,
                                          uint64_t *dev_cfg_mask,
                                          uct_device_type_t dev_type)
{
    uint64_t mask, exclusive_mask;

    /* go over the device list from the user and check (against the available resources)
     * which can be satisfied */
    ucg_assert(devices[dev_type].count <= 64); /* Using uint64_t bitmap */
    mask = scp_str_array_search((const char**)devices[dev_type].names,
                                devices[dev_type].count, resource->dev_name);
    if (!mask) {
        /* if the user's list is 'all', use all the available resources */
        mask = scp_str_array_search((const char**)devices[dev_type].names,
                                    devices[dev_type].count, SCP_RSC_CONFIG_ALL);
    }

    /* warn if we got new device which appears more than once */
    exclusive_mask = mask & ~(*dev_cfg_mask);
    if (exclusive_mask && !ucg_is_pow2(exclusive_mask)) {
        ucg_warn("device '%s' is specified multiple times",
                 devices[dev_type].names[ucg_ilog2(exclusive_mask)]);
    }

    *dev_cfg_mask |= mask;
    return !!mask;
}

static int scp_is_resource_enabled(const sct_tl_resource_desc_t *resource,
                                   const scp_config_t *config)
{
    uint64_t dev_cfg_masks[UCT_DEVICE_TYPE_LAST] = {};
    return scp_is_resource_in_device_list(resource, config->devices,
                                          &dev_cfg_masks[resource->dev_type],
                                          resource->dev_type);
}

static void scp_add_tl_resource_if_enabled(scp_context_h context, scp_tl_md_t *md,
                                           scp_md_index_t md_idx,
                                           const scp_config_t *config,
                                           const sct_tl_resource_desc_t *resource,
                                           uint8_t *num_resources_p)
{
    if (!scp_is_resource_enabled(resource, config)) {
        return;
    }

    context->tl_rscs[context->num_tls].tl_rsc       = *resource;
    context->tl_rscs[context->num_tls].md_index     = md_idx;
    context->tl_rscs[context->num_tls].flags        = 0;

    /* loop already tl resources try to find match dev */
    uint8_t dev_idx = 0;
    for (uint8_t idx = 0; idx < context->num_tls; ++idx) {
        if (!strcmp(context->tl_rscs[idx].tl_rsc.dev_name, resource->dev_name)) {
            dev_idx = context->tl_rscs[idx].dev_index;
            break;
        } else {
            dev_idx = ucs_max(context->tl_rscs[idx].dev_index + 1, idx);
        }
    }
    context->tl_rscs[context->num_tls].dev_index = dev_idx;

    ucg_info("add tls num_tls %d tl_name %s device_name %s dev_idx %d md_idx %d", context->num_tls,
             resource->tl_name, resource->dev_name, (int)dev_idx, (int)md_idx);

    ++context->num_tls;
    ++(*num_resources_p);
}

static ucg_status_t scp_add_tl_resources(scp_context_h context, scp_md_index_t md_idx,
                                         uint8_t *num_resc_p)
{
    ucg_status_t status = UCG_OK;
    uint8_t num_tl_resc = 0;
    sct_tl_resource_desc_t *tl_resc = NULL;
    scp_tl_md_t *tl_md = &context->tl_mds[md_idx];

    *num_resc_p = 0;

    ucs_status_t ucs_status =
        sct_md_query_tl_resources(tl_md->md, &tl_resc, &num_tl_resc);
    UCG_ASSERT_RET(ucs_status == UCS_OK, ucg_status_s2g(ucs_status));

    if (num_tl_resc == 0) {
        ucg_info("No tl resources found for md %s", tl_md->rsc.md_name);
        goto free_resc;
    }

    context->tl_rscs = ucg_realloc(context->tl_rscs,
                      sizeof(*context->tl_rscs) *
                      (context->num_tls + num_tl_resc),
                      "scp resources");
    UCG_ASSERT_GOTO(context->tl_rscs != NULL, free_resc, UCG_ERR_NO_MEMORY);

    /* copy only the resources enabled by user configuration */
    for (uint8_t idx = 0; idx < num_tl_resc; ++idx) {
        scp_add_tl_resource_if_enabled(context, tl_md, md_idx,
                                       context->scp_config,
                                       &tl_resc[idx], num_resc_p);
    }

free_resc:
    sct_release_tl_resource_list(tl_resc);
    return status;
}

static ucg_status_t scp_add_component_resources(scp_context_h context, scp_rsc_index_t cmpt_idx,
                                                const ucp_context_h ucp_context)
{
    ucg_status_t status;

    const scp_tl_cmpt_t *tl_cmpt = &context->tl_cmpts[cmpt_idx];
    uct_component_attr_t component_attr;
    component_attr.field_mask = UCT_COMPONENT_ATTR_FIELD_MD_RESOURCES;
    component_attr.md_resources = ucg_alloca(sizeof(*component_attr.md_resources) *
                                             tl_cmpt->attr.md_resource_count); // 48Byte = 3 * 12Byte

    ucs_status_t ucs_status
        = sct_component_query(tl_cmpt->cmpt, &component_attr);
    UCG_ASSERT_RET(ucs_status == UCS_OK, ucg_status_s2g(ucs_status));

    uint8_t md_idx = context->num_mds, num_tl_resc = 0;
    /* Iterate through memory domain resources */
    for (uint8_t idx = 0; idx < tl_cmpt->attr.md_resource_count; ++idx) {
        status = scp_fill_tl_md(context, &context->tl_cmpts[cmpt_idx],
                                ucp_context,
                                &component_attr.md_resources[idx],
                                &context->tl_mds[md_idx]);
        if (status != UCG_OK) {
            continue;
        }
        context->tl_mds[md_idx].cmpt_index = cmpt_idx;

        status = scp_add_tl_resources(context, md_idx, &num_tl_resc);
        UCG_CHECK_GOTO(status, err_close_mds);

        if (ucg_unlikely(num_tl_resc == 0)) {
            sct_md_close(context->tl_mds[md_idx].md);
            continue;
        }

        ++md_idx;
    }

    context->num_mds = md_idx;

    /* Only IB device is initialized. UCG_OK is returned to prevent a failure response
       when no IB device exists in the environment. */
    return UCG_OK;
err_close_mds:
    for (uint8_t idx = 0; idx < md_idx; ++idx) {
        sct_md_close(context->tl_mds[idx].md);
    }
    return status;
}

static uint8_t scp_is_match_sct_component(scp_config_h config, char *name)
{
    for (uint8_t idx = 0; idx < MAX_SCT_COMPONENT_NUM; ++idx) {
        if (!strcmp(name, SCT_COMPONENT_LIST[idx])) {
            return 1; /* 1 means find */
        }
    }
    return 0;
}

static ucg_status_t scp_fill_resources(scp_context_h context, const ucp_context_h ucp_context)
{
    ucg_status_t status = UCG_OK;

    sct_component_h *components;
    unsigned num_components = 0;
    ucs_status_t ucs_status;
    ucs_status = sct_query_components(&components, &num_components);
    CHKERR_JUMP(UCS_OK != ucs_status, "query for components", error_ret);

    context->num_cmpts = 0;
    context->tl_cmpts = ucg_malloc(MAX_SCT_COMPONENT_NUM * sizeof(*context->tl_cmpts),
                                   "scp tl components");
    UCG_ASSERT_GOTO(context->tl_cmpts != NULL,
                    out_release_components, UCG_ERR_NO_MEMORY);

    uint32_t max_mds = 0;
    uct_component_attr_t *component_attr;
    for (uint8_t idx = 0; idx < num_components; ++idx) {
        component_attr = &context->tl_cmpts[context->num_cmpts].attr;
        memset(component_attr, 0, sizeof(*component_attr));
        component_attr->field_mask =
            UCT_COMPONENT_ATTR_FIELD_NAME              |
            UCT_COMPONENT_ATTR_FIELD_MD_RESOURCE_COUNT |
            UCT_COMPONENT_ATTR_FIELD_FLAGS;

        ucs_status = sct_component_query(components[idx], component_attr);
        UCG_ASSERT_GOTO(ucs_status == UCS_OK, err_free_tl_cmpts,
                        ucg_status_s2g(ucs_status));

        if (!scp_is_match_sct_component(context->scp_config, component_attr->name)) {
            continue;
        }

        context->tl_cmpts[context->num_cmpts].cmpt = components[idx];
        max_mds += component_attr->md_resource_count; /* 1 sdma md + all rc iface md */
        ++context->num_cmpts;

        ucg_assert(context->num_cmpts <= MAX_SCT_COMPONENT_NUM);
    }

    /* Allocate actual array of MDs */
    context->tl_mds = ucg_malloc(max_mds * sizeof(*context->tl_mds),
                                 "scp_tl_mds"); // 768Byte max_mds 3
    UCG_ASSERT_GOTO(context->tl_mds != NULL,
                    err_free_tl_cmpts, UCG_ERR_NO_MEMORY);

    /* Collect resources of each component */
    for (uint8_t idx = 0; idx < context->num_cmpts; ++idx) {
        status = scp_add_component_resources(context, idx, ucp_context);
        UCG_CHECK_GOTO(status, err_free_tl_mds);
    }

    context->tl_bitmap = UCS_MASK(context->num_tls);
    SCP_THREAD_LOCK_INIT(&context->mt_lock);

    return UCG_OK;

err_free_tl_mds:
    ucg_free(context->tl_mds);
err_free_tl_cmpts:
    ucg_free(context->tl_cmpts);
out_release_components:
    sct_release_component_list(components);
error_ret:
    return ucg_status_s2g(status);
}

ucg_status_t scp_context_init(const ucp_context_h ucp_context, scp_context_h *context_p)
{
    scp_context_h context = ucg_malloc(sizeof(scp_context_t), "scp context");
    UCG_ASSERT_RET(context != NULL, UCG_ERR_NO_MEMORY);

    ucg_status_t status;
    status = scp_config_read(NULL, &context->scp_config);
    UCG_ASSERT_CODE_RET(status);

    context->tl_cmpts   = NULL;
    context->num_cmpts  = 0;
    context->tl_mds     = NULL;
    context->num_mds    = 0;
    context->tl_rscs    = NULL;
    context->num_tls    = 0;

    status = scp_fill_resources(context, ucp_context);
    UCG_ASSERT_CODE_GOTO(status, free_context);

    *context_p = context;
    return UCG_OK;

free_context:
    ucg_free(context);
    return status;
}

void scp_free_resources(scp_context_h context)
{
    scp_rsc_index_t i;

    ucg_free(context->tl_rscs);
    for (i = 0; i < context->num_mds; ++i) {
        sct_md_close(context->tl_mds[i].md);
    }
    ucg_free(context->tl_mds);
    ucg_free(context->tl_cmpts);
}

void scp_free_config(scp_context_h context)
{
    ucg_free(context->scp_config->env_prefix);
}

void scp_cleanup(scp_context_h context)
{
    scp_free_resources(context);
    scp_free_config(context);
    SCP_THREAD_LOCK_FINALIZE(&context->mt_lock);
    ucg_free(context);
}
