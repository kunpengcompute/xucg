/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
 * Description: sdma_md.c
 * Author:
 * Create: 2021
 * Notes:
 */
#include "sdma_offload_md.h"


static ucs_status_t sct_sdma_ofd_md_query(sct_md_h uct_md, uct_md_attr_t *attr)
{
    /* Dummy memory registration provided. No real memory handling exists */
    attr->cap.flags             = UCT_MD_FLAG_REG
                                | UCT_MD_FLAG_NEED_RKEY
                                | UCT_MD_FLAG_ALLOC
                                | UCT_MD_FLAG_FIXED
                                | UCT_MD_FLAG_RKEY_PTR;

    attr->cap.reg_mem_types     = UCS_BIT(UCS_MEMORY_TYPE_HOST);
    attr->cap.alloc_mem_types   = UCS_BIT(UCS_MEMORY_TYPE_HOST);
    attr->cap.detect_mem_types  = 0;
    attr->cap.access_mem_types  = UCS_BIT(UCS_MEMORY_TYPE_HOST);
    attr->cap.max_alloc         = ULONG_MAX;
    attr->cap.max_reg           = ULONG_MAX;
    attr->rkey_packed_size      = 0; /* sct_md_query adds UCT_COMPONENT_NAME_MAX to this */
    attr->reg_cost              = ucs_linear_func_make(0, 0);
    memset(&attr->local_cpus, 0xff, sizeof(attr->local_cpus));

    return UCS_OK;
}

void sct_sdma_ofd_md_close(sct_md_h md)
{
    sct_sdma_ofd_md_t *sdma_md =
        ucs_derived_of(md, sct_sdma_ofd_md_t);
    ucg_mpool_cleanup(&sdma_md->sdma_params_pool, 1);
    ucg_mpool_cleanup(&sdma_md->event_params_pool, 1);
    ucg_mpool_cleanup(&sdma_md->notify_params_pool, 1);
    sct_stars_unload();
}

static ucs_status_t sct_sdma_ofd_mem_reg(sct_md_h md, void *address, size_t length,
                                         unsigned flags, sct_mem_h *memh_p)
{
    sct_sdma_key_t *sdma_memh
        = ucg_calloc(1, sizeof(sct_sdma_key_t), "uct_sdma_ofd_key_t");
    if (sdma_memh == NULL) {
        ucg_error("Failed to allocate memory for sdma_ofd_memh");
        return UCS_ERR_NO_MEMORY;
    }

    sct_sdma_ofd_md_t *sdma_md = ucs_derived_of(md, sct_sdma_ofd_md_t);
    ucs_status_t status
        = api_stars_pin_umem(sdma_md->super.stars_dev_id, address,
                             (uint32_t)length, &sdma_memh->cookie);
    if (status != UCS_OK) {
        ucg_error("sdma_ofd_pin_umem failed , status: %d", status);
        ucg_free(sdma_memh);
        return UCS_ERR_IO_ERROR;
    }

    sdma_memh->address = address;
    *memh_p = sdma_memh;
    return UCS_OK;
}

static ucs_status_t sct_sdma_ofd_mem_dereg(sct_md_h md, sct_mem_h memh)
{
    sct_sdma_ofd_md_t *sdma_md = ucs_derived_of(md, sct_sdma_ofd_md_t);
    ucs_status_t status;
    sct_sdma_key_t *sdma_memh = memh;

    status = (ucs_status_t)api_stars_unpin_umem(sdma_md->super.stars_dev_id, sdma_memh->cookie);
    if (status != UCS_OK) {
        ucg_error("sdma_ofd_unpin_umem failed , status: %d.\n", status);
        return UCS_ERR_IO_ERROR;
    }

    return UCS_OK;
}

static ucs_status_t sct_sdma_ofd_mem_alloc(sct_md_h tl_md, size_t *length_p, void **address_p,
                                           ucs_memory_type_t mem_type, unsigned flags,
                                           const char *alloc_name, sct_mem_h *memh_p)
{
    ucs_status_t status;

    void *sdma_ofd_mem_addr = ucg_malloc(*length_p, "sdma_ofd_mem_addr");
    if (sdma_ofd_mem_addr == NULL) {
        ucg_error("Failed to allocate memory for sdma_ofd_mem_addr");
        return UCS_ERR_NO_MEMORY;
    }

    status = sct_sdma_ofd_mem_reg(tl_md, sdma_ofd_mem_addr, *length_p, UCT_MD_MEM_ACCESS_ALL, memh_p);
    if (status != UCS_OK) {
        ucg_free(sdma_ofd_mem_addr);
        ucg_error("Failed to pinned memory for sdma_ofd_mem_addr");
        return status;
    }
    *address_p = sdma_ofd_mem_addr;
    ucg_debug("uct_sdma_mem_alloc sdma_ofd_mem_addr: %p ", sdma_ofd_mem_addr);

    return UCS_OK;
}

ucs_status_t sct_sdma_ofd_mem_free(sct_md_h md, sct_mem_h memh)
{
    sct_sdma_key_t *sdma_memh = memh;
    ucs_status_t status;

    if (sdma_memh == NULL || sdma_memh->address == NULL) {
        return UCS_ERR_NO_MEMORY;
    }

    status = sct_sdma_ofd_mem_dereg(md, memh);
    if (status != UCS_OK) {
        ucg_error("Failed to unpinned memory for sdma_ofd_mem_addr");
    }
    ucg_free(sdma_memh->address);
    ucg_free(sdma_memh);

    return status;
}

static sct_md_ops_t sct_sdma_ofd_md_ops = {
    .close                  = sct_sdma_ofd_md_close,
    .query                  = sct_sdma_ofd_md_query,
    .mem_alloc              = sct_sdma_ofd_mem_alloc,
    .mem_free               = sct_sdma_ofd_mem_free,
    .mem_reg                = sct_sdma_ofd_mem_reg,
    .mem_dereg              = sct_sdma_ofd_mem_dereg,
    .mkey_pack              = ucs_empty_function_return_success,
};

static ucs_status_t sct_sdma_ofd_md_open(sct_component_t *component, const char *md_name,
                                         const sct_md_config_t *config, uct_md_h context,
                                         sct_md_h *md_p)
{
    ucs_status_t status = UCS_ERR_INVALID_PARAM;

    if (sct_stars_load() != UCG_OK) {
        ucg_error("Failed to load stars module.");
        return status;
    }

    const scs_stars_info_t *stars_info = sct_stars_get_info();
    if (ucg_unlikely(!stars_info)) {
        return status;
    }

    sct_sdma_ofd_md_t *md = ucg_malloc(sizeof(*md), "sct_sdma_ofd_md_t");
    if (md == NULL) {
        return UCS_ERR_NO_MEMORY;
    }

    ucg_status_t ucg_status;
    ucg_status = ucg_mpool_init(&md->sdma_params_pool, 0, sizeof(sdma_trans_parm_t),
                                0, UCG_CACHE_LINE_SIZE, UCG_ELEMS_PER_CHUNK,
                                UINT_MAX, NULL, "sdma_trans_parm_t_pool");
    if (ucg_unlikely(ucg_status != UCG_OK)) {
        status = UCS_ERR_NO_RESOURCE;
        goto free_md;
    }

    ucg_status = ucg_mpool_init(&md->event_params_pool, 0, sizeof(event_trans_parm_t),
                                0, UCG_CACHE_LINE_SIZE, UCG_ELEMS_PER_CHUNK,
                                UINT_MAX, NULL, "event_trans_parm_t_pool");
    if (ucg_unlikely(ucg_status != UCG_OK)) {
        status = UCS_ERR_NO_RESOURCE;
        goto err_free_sdma_params;
    }

    ucg_status = ucg_mpool_init(&md->notify_params_pool, 0, sizeof(write_notify_trans_param_t),
                                0, UCG_CACHE_LINE_SIZE, UCG_ELEMS_PER_CHUNK,
                                UINT_MAX, NULL, "notify_trans_parm_t_pool");
    if (ucg_unlikely(ucg_status != UCG_OK)) {
        status = UCS_ERR_NO_RESOURCE;
        goto err_free_event_params;
    }

    md->super.stars_dev_id  = scs_stars_get_machine_info()->affinity.die_id;
    md->num_devices         = stars_info->driver.dev_num;
    md->super.ops           = &sct_sdma_ofd_md_ops;
    md->super.component     = &sct_sdma_ofd_cmpt;
    *md_p = &md->super;
    return UCS_OK;

err_free_event_params:
    ucg_mpool_cleanup(&md->event_params_pool, 1);
err_free_sdma_params:
    ucg_mpool_cleanup(&md->sdma_params_pool, 1);
free_md:
    ucg_free(md);
    return status;
}

sct_component_t sct_sdma_ofd_cmpt = {
    .name               = SCT_STARS_SDMA_ACC,
    .query_md_resources = sct_md_query_single_md_resource,
    .md_open            = sct_sdma_ofd_md_open,
    .rkey_unpack        = sct_md_stub_rkey_unpack,
    .md_config          = SCT_MD_DEFAULT_CONFIG_INITIALIZER,
    .tl_list            = SCT_COMPONENT_TL_LIST_INITIALIZER(&sct_sdma_ofd_cmpt),
    .flags = 0,
};

SCT_COMPONENT_REGISTER(&sct_sdma_ofd_cmpt);
