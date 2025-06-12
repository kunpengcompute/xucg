/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

#include "sct_md.h"
#include "sct_iface.h"


ucs_config_field_t sct_md_config_table[] = {
    {NULL}
};

ucs_config_field_t sct_md_config_rcache_table[] = {
    {"RCACHE_MEM_PRIO", "1000", "Registration cache memory event priority",
     ucs_offsetof(sct_md_rcache_config_t, event_prio), UCS_CONFIG_TYPE_UINT},

    {"RCACHE_OVERHEAD", "180ns", "Registration cache lookup overhead",
     ucs_offsetof(sct_md_rcache_config_t, overhead), UCS_CONFIG_TYPE_TIME},

    {"RCACHE_ADDR_ALIGN", UCS_PP_MAKE_STRING(ARCH_CACHE_LINE_SIZE),
     "Registration cache address alignment, must be power of 2\n"
     "between "UCS_PP_MAKE_STRING(UCS_PGT_ADDR_ALIGN)"and system page size",
     ucs_offsetof(sct_md_rcache_config_t, alignment), UCS_CONFIG_TYPE_UINT},

    {NULL}
};

ucs_status_t sct_md_open(sct_component_h component, const char *md_name,
                         const sct_md_config_t *config, uct_md_h context, sct_md_h *md_p)
{
    ucs_status_t status;
    sct_md_h md;

    status = component->md_open(component, md_name, config, context, &md);
    if (status != UCS_OK) {
        return status;
    }

    *md_p = md;
    return UCS_OK;
}

void sct_md_close(sct_md_h md)
{
    md->ops->close(md);
}

ucs_status_t sct_md_query_tl_resources(sct_md_h md,
                                       sct_tl_resource_desc_t **resources_p,
                                       uint8_t *num_resources_p)
{
    sct_component_t *component = md->component;
    sct_tl_resource_desc_t *resources, *tmp;
    sct_tl_device_resource_t *tl_devices;
    unsigned i, num_resources, num_tl_devices;
    ucs_status_t status;
    sct_tl_t *tl;

    resources     = NULL;
    num_resources = 0;

    ucs_list_for_each(tl, &component->tl_list, list) {
        status = tl->query_devices(md, &tl_devices, &num_tl_devices);
        if (status != UCS_OK) {
            ucg_debug("failed to query %s resources: %s", tl->name,
                      ucs_status_string(status));
            continue;
        }

        if (num_tl_devices == 0) {
            ucg_free(tl_devices);
            continue;
        }

        tmp = ucg_realloc(resources,
                          sizeof(*resources) * (num_resources + num_tl_devices),
                          "md_resources");
        if (tmp == NULL) {
            ucg_free(tl_devices);
            status = UCS_ERR_NO_MEMORY;
            goto err;
        }

        /* add tl devices to overall list of resources */
        for (i = 0; i < num_tl_devices; ++i) {
            ucs_strncpy_zero(tmp[num_resources + i].tl_name, tl->name,
                             sizeof(tmp[num_resources + i].tl_name));
            ucs_strncpy_zero(tmp[num_resources + i].dev_name, tl_devices[i].name,
                             sizeof(tmp[num_resources + i].dev_name));
            tmp[num_resources + i].dev_type   = tl_devices[i].type;
            tmp[num_resources + i].sys_device = tl_devices[i].sys_device;
        }

        resources      = tmp;
        num_resources += num_tl_devices;
        ucg_free(tl_devices);
    }

    *resources_p     = resources;
    *num_resources_p = num_resources;
    return UCS_OK;

err:
    ucg_free(resources);
    return status;
}

void sct_release_tl_resource_list(sct_tl_resource_desc_t *resources)
{
    ucg_free(resources);
}

ucs_status_t sct_md_query_single_md_resource(sct_component_t *component,
                                             uct_md_resource_desc_t **resources_p,
                                             unsigned *num_resources_p)
{
    uct_md_resource_desc_t *resource;

    resource = ucg_malloc(sizeof(*resource), "md resource");
    if (resource == NULL) {
        return UCS_ERR_NO_MEMORY;
    }

    ucs_snprintf_zero(resource->md_name, UCT_MD_NAME_MAX, "%s",
                      component->name);

    *resources_p     = resource;
    *num_resources_p = 1;
    return UCS_OK;
}

ucs_status_t sct_md_query_empty_md_resource(uct_md_resource_desc_t **resources_p,
                                            unsigned *num_resources_p)
{
    *resources_p     = NULL;
    *num_resources_p = 0;
    return UCS_OK;
}

ucs_status_t sct_md_stub_rkey_unpack(sct_component_t *component,
                                     const void *rkey_buffer, uct_rkey_t *rkey_p,
                                     void **handle_p)
{
    *rkey_p   = 0xdeadbeef;
    *handle_p = NULL;
    return UCS_OK;
}

static sct_tl_t *sct_find_tl(sct_component_h component, uint64_t md_flags,
                             const char *tl_name)
{
    sct_tl_t *tl;
    ucs_list_for_each(tl, &component->tl_list, list) {
        if ((tl_name != NULL) && !strcmp(tl_name, tl->name)) {
            return tl;
        }
    }
    return NULL;
}

ucs_status_t sct_md_iface_config_read(sct_md_h md, const char *tl_name,
                                      const char *env_prefix, const char *filename,
                                      sct_iface_config_t **config_p)
{
    sct_config_bundle_t *bundle = NULL;
    uct_md_attr_t md_attr;
    ucs_status_t status;
    sct_tl_t *tl;

    if (tl_name == NULL) {
        return UCS_ERR_INVALID_PARAM;
    }

    status = sct_md_query(md, &md_attr);
    if (status != UCS_OK) {
        ucg_error("Failed to query MD");
        return status;
    }

    tl = sct_find_tl(md->component, md_attr.cap.flags, tl_name);
    if (tl == NULL) {
        ucg_debug("component '%s' does not exist tl '%s'",
                  md->component->name, tl_name);
        return UCS_ERR_NO_DEVICE; /* Non-existing transport */
    }

    status = sct_config_read(&bundle, tl->config.table, tl->config.size,
                             env_prefix, tl->config.prefix);
    if (status != UCS_OK) {
        ucg_error("Failed to read iface config");
        return status;
    }

    *config_p = (sct_iface_config_t*) bundle->data;
    return UCS_OK;
}

ucs_status_t sct_iface_open(sct_md_h md, sct_worker_h worker,
                            const sct_iface_params_t *params,
                            const sct_iface_config_t *config,
                            sct_iface_h *iface_p)
{
    uct_md_attr_t md_attr;
    ucs_status_t status = sct_md_query(md, &md_attr);
    UCG_ASSERT_RET(status == UCS_OK, ucg_status_s2g(status));

    sct_tl_t *tl = sct_find_tl(md->component, md_attr.cap.flags,
                               params->mode.device.tl_name);
    if (ucg_unlikely(tl == NULL)) {
        /* Non-existing transport */
        return UCS_ERR_NO_DEVICE;
    }

    return tl->iface_open(md, worker, params, config, iface_p);
}

ucs_status_t sct_md_config_read(sct_component_h component,
                                const char *env_prefix, const char *filename,
                                sct_md_config_t **config_p)
{
    sct_config_bundle_t *bundle = NULL;
    ucs_status_t status;

    status = sct_config_read(&bundle, component->md_config.table,
                             component->md_config.size, env_prefix,
                             component->md_config.prefix);
    if (status != UCS_OK) {
        ucg_error("Failed to read MD config");
        return status;
    }

    *config_p = (sct_md_config_t*) bundle->data;
    return UCS_OK;
}

void sct_config_release(void *config)
{
    sct_config_bundle_t *bundle = (sct_config_bundle_t *)config - 1;

    ucs_config_parser_release_opts(config, bundle->table);
    ucg_free((void*)(bundle->table_prefix));
    ucg_free(bundle);
}

ucs_status_t sct_md_mkey_pack(sct_md_h md, sct_mem_h memh, void *rkey_buffer)
{
    void *rbuf = sct_md_fill_md_name(md, rkey_buffer);
    return md->ops->mkey_pack(md, memh, rbuf);
}

ucs_status_t sct_rkey_unpack(sct_component_h component, const void *rkey_buffer,
                             uct_rkey_bundle_t *rkey_ob)
{
    return component->rkey_unpack(component, rkey_buffer, &rkey_ob->rkey,
                                  &rkey_ob->handle);
}

ucs_status_t sct_md_query(sct_md_h md, uct_md_attr_t *md_attr)
{
    ucs_status_t status;

    status = md->ops->query(md, md_attr);
    if (status != UCS_OK) {
        ucg_debug("query failed!");
        return status;
    }

    /* Component name + data */
    memcpy(md_attr->component_name, md->component->name, UCT_COMPONENT_NAME_MAX);

    return UCS_OK;
}

static ucs_status_t sct_mem_check_flags(unsigned flags)
{
    if (!(flags & UCT_MD_MEM_ACCESS_ALL)) {
        return UCS_ERR_INVALID_PARAM;
    }
    return UCS_OK;
}

ucs_status_t sct_mem_alloc_check_params(size_t length,
                                        const uct_alloc_method_t *methods,
                                        unsigned num_methods,
                                        const sct_mem_alloc_params_t *params)
{
    const uct_alloc_method_t *method;
    ucs_status_t status;

    if (params->field_mask & UCT_MEM_ALLOC_PARAM_FIELD_FLAGS) {
        status = sct_mem_check_flags(params->flags);
        if (status != UCS_OK) {
            return status;
        }

        /* assuming flags are valid */
        if (params->flags & UCT_MD_MEM_FLAG_FIXED) {
            if (!(params->field_mask & UCT_MEM_ALLOC_PARAM_FIELD_ADDRESS)) {
                ucg_debug("UCT_MD_MEM_FLAG_FIXED requires setting of"
                          " UCT_MEM_ALLOC_PARAM_FIELD_ADDRESS field");
                return UCS_ERR_INVALID_PARAM;
            }

            if ((params->address == NULL) ||
                ((uintptr_t)params->address % ucs_get_page_size())) {
                ucg_debug("UCT_MD_MEM_FLAG_FIXED requires valid page size aligned address");
                return UCS_ERR_INVALID_PARAM;
            }
        }
    }

    if (length == 0) {
        ucg_debug("the length value for allocating memory is set to zero: %s",
                  ucs_status_string(UCS_ERR_INVALID_PARAM));
        return UCS_ERR_INVALID_PARAM;
    }

    for (method = methods;
         method < methods + num_methods; ++method) {
        if (*method == UCT_ALLOC_METHOD_MD) {
            if (!(params->field_mask & UCT_MEM_ALLOC_PARAM_FIELD_MDS) ||
                (params->mds.count < 1)) {
                ucg_debug("methods include UCT_ALLOC_METHOD but params->mds"
                          " not populated correctly: %s",
                          ucs_status_string(UCS_ERR_INVALID_PARAM));
                return UCS_ERR_INVALID_PARAM;
            }
        }
    }

    return UCS_OK;
}

ucs_status_t sct_md_mem_alloc(sct_md_h md, size_t *length_p, void **address_p,
                              ucs_memory_type_t mem_type, unsigned flags,
                              const char *alloc_name, sct_mem_h *memh_p)
{
    return md->ops->mem_alloc(md, length_p, address_p, mem_type, flags,
                              alloc_name, memh_p);
}

ucs_status_t sct_md_mem_free(sct_md_h md, sct_mem_h memh)
{
    return md->ops->mem_free(md, memh);
}

ucs_status_t sct_md_mem_reg(sct_md_h md, void *address, size_t length,
                            unsigned flags, sct_mem_h *memh_p)
{
    ucs_status_t status;

    if ((length == 0) || (address == NULL)) {
        sct_md_log_mem_reg_error(flags,
                                 "sct_md_mem_reg(address=%p length=%zu): "
                                 "invalid parameters", address, length);
        return UCS_ERR_INVALID_PARAM;
    }

    status = sct_mem_check_flags(flags);
    if (status != UCS_OK) {
        sct_md_log_mem_reg_error(flags,
                                 "sct_md_mem_reg(flags=0x%x): invalid flags",
                                 flags);
        return status;
    }

    return md->ops->mem_reg(md, address, length, flags, memh_p);
}

ucs_status_t sct_md_mem_dereg(sct_md_h md, sct_mem_h memh)
{
    return md->ops->mem_dereg(md, memh);
}

ucs_status_t sct_md_mem_query(sct_md_h md, const void *addr, const size_t length,
                              uct_md_mem_attr_t *mem_attr_p)
{
    return md->ops->mem_query(md, addr, length, mem_attr_p);
}
