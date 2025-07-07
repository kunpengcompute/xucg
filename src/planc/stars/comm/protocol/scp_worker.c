/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#include "scp_worker.h"

#include "scp_address.h"
#include "sct_md.h"

static ucg_status_t scp_worker_iface_open(scp_worker_h worker, scp_rsc_index_t tl_id,
                                          sct_iface_params_t *iface_params,
                                          scp_worker_iface_h *wiface_p)
{
    scp_worker_iface_h wiface = ucg_malloc(sizeof(*wiface), "scp_iface");
    UCG_ASSERT_RET(wiface != NULL, UCG_ERR_NO_MEMORY);
    wiface->worker     = worker;
    wiface->rsc_index  = tl_id;
    wiface->flags      = 0;
    wiface->iface      = NULL;

    ucs_status_t ucs_status;
    scp_context_h context = worker->context;
    scp_tl_resource_desc_t *resource = &context->tl_rscs[tl_id];
    sct_md_h md = context->tl_mds[resource->md_index].md;
    sct_iface_config_t *iface_config; /* Defines interface configuration options */
    ucs_status = sct_md_iface_config_read(md, resource->tl_rsc.tl_name, NULL, NULL, &iface_config);
    CHKERR_JUMP(UCS_OK != ucs_status, "setup iface_config", err_free_iface);

    UCS_CPU_ZERO(&iface_params->cpu_mask);

    /* Open communication interface */
    ucs_status = sct_iface_open(md, wiface->worker->uct, iface_params, iface_config,
                                &wiface->iface);
    sct_config_release(iface_config);
    CHKERR_JUMP(UCS_OK != ucs_status, "open interface", err_free_iface);

    /* Get interface attributes */
    ucs_status = sct_iface_query(wiface->iface, &wiface->attr);
    CHKERR_JUMP(UCS_OK != ucs_status, "query iface", error_close_iface);

    /* Filter out ports with speeds below 100Gbps */
    if (wiface->attr.bandwidth.shared < (100e9 / 8.0f)) {
        ucg_info("Stars don't support ports (dev name: %s bandwidth: %.4f) with speeds below 100Gbps",
               worker->context->tl_rscs[wiface->rsc_index].tl_rsc.dev_name,
               wiface->attr.bandwidth.shared);
        ucs_status = UCS_ERR_UNSUPPORTED;
        goto error_close_iface;
    }

    *wiface_p = wiface;
    return UCG_OK;

error_close_iface:
    sct_iface_close(wiface->iface);
err_free_iface:
    ucg_free(wiface);
    return ucg_status_s2g(ucs_status);
}

static void scp_worker_iface_close(scp_worker_iface_h wiface)
{
    if (wiface->iface != NULL) {
        sct_iface_close(wiface->iface);
        wiface->iface = NULL;
    }
}

void scp_worker_iface_cleanup(scp_worker_iface_h wiface)
{
    scp_worker_iface_close(wiface);
    ucg_free(wiface);
}

static inline uint8_t scp_worker_cmp_iface(sct_iface_attr_t *left,
                                           sct_iface_attr_t *right)
{
    return (ucs_linear_func_apply(left->latency, 1) <=
        ucs_linear_func_apply(right->latency, 1)) ? 0 : 1;
}

static inline uint8_t scp_worker_cmp_md_subnet(scp_context_h context,
                                               scp_worker_iface_h left,
                                               scp_worker_iface_h right)
{
    uint8_t md_idx_a = context->tl_rscs[left->rsc_index].md_index;
    uint8_t md_idx_b = context->tl_rscs[right->rsc_index].md_index;

    return (context->tl_mds[md_idx_a].md->subnet_id >
           context->tl_mds[md_idx_b].md->subnet_id ? 1 : 0);
}

static ucg_status_t scp_worker_refill_mds(scp_worker_h worker)
{
    scp_worker_iface_h *ifaces_p = worker->ifaces;
    scp_context_h context = worker->context;
    scp_tl_resource_desc_t* tl_rscs = context->tl_rscs;
    scp_md_index_t num_mds = context->num_mds;
    scp_tl_md_t *mds = context->tl_mds;
    uint8_t iface_num = worker->num_ifaces;
    scp_rsc_index_t rsc_idx, md_idx, new_md_idx = 0;
    uint32_t mds_map = 0;

    scp_tl_md_t *temp_mds = (scp_tl_md_t *)ucg_calloc(num_mds, sizeof(scp_tl_md_t), "temp mds");
    if (!temp_mds) {
        ucg_error("Failed to alloc memory in mds resill!\n");
        return UCG_ERR_NO_MEMORY;
    }
    // Fill new mds.
    for (; new_md_idx < iface_num; new_md_idx++) {
        rsc_idx = ifaces_p[new_md_idx]->rsc_index;
        md_idx = tl_rscs[rsc_idx].md_index;
        temp_mds[new_md_idx] = mds[md_idx];
        tl_rscs[rsc_idx].md_index = new_md_idx;
        mds_map |= UCS_BIT(md_idx);
    }
    // Some iface is closed, fill their mds.
    for (scp_md_index_t idx = 0; idx < num_mds; idx++) {
        if (mds_map & UCS_BIT(idx)) {
            continue;
        }
        temp_mds[new_md_idx++] = mds[idx];
    }
    memcpy(mds, temp_mds, num_mds * sizeof(scp_tl_md_t));
    ucg_free(temp_mds);
    return UCG_OK;
}

static ucg_status_t scp_worker_select_ifaces(scp_worker_h worker)
{
    ucg_status_t status;
    scp_worker_iface_h iface_p = NULL;
    scp_worker_iface_h *ifaces_p = worker->ifaces;
    scp_context_h context = worker->context;
    uint8_t iface_num = worker->num_ifaces;

    /* sort iface array and mds by latency*/
    for (uint8_t i = 0; i < iface_num - 1; i++) {
        for (uint8_t j = 0; j < iface_num - i - 1; j++) {
            if (scp_worker_cmp_iface(&ifaces_p[j]->attr, &ifaces_p[j + 1]->attr)) {
                // exchange iface
                iface_p = ifaces_p[j];
                ifaces_p[j] = ifaces_p[j + 1];
                ifaces_p[j + 1] = iface_p;
            }
        }
    }

    uint8_t multirails = context->scp_config->ctx.multirails;
    ucg_debug("Set the max num of multirails : %u", multirails);
    if (multirails <= iface_num) {
        iface_num = multirails;
    }
    /* sort selected ifaces and mds by subnet, to avoid crossed link */
    for (uint8_t i = 0; i < iface_num - 1; i++) {
        for (uint8_t j = 0; j < iface_num - i - 1; j++) {
            if (scp_worker_cmp_md_subnet(context, ifaces_p[j], ifaces_p[j + 1])) {
                // exchange iface
                iface_p = ifaces_p[j];
                ifaces_p[j] = ifaces_p[j + 1];
                ifaces_p[j + 1] = iface_p;
            }
        }
    }

    /* Refill mds by order of iface. */
    status = scp_worker_refill_mds(worker);
    if (status != UCG_OK) {
        ucg_error("Failed to refill mds!\n");
        return status;
    }

    /* close some iface that unused later */
    for (uint8_t idx = iface_num; idx < worker->num_ifaces; ++idx) {
        iface_p = worker->ifaces[idx];
        context->tl_bitmap &= ~UCS_BIT(iface_p->rsc_index);
        ucg_debug("will clean iface %d, rsc idx %d name %s dev %s",
                  (uint32_t)idx, (uint32_t)iface_p->rsc_index,
                  context->tl_rscs[iface_p->rsc_index].tl_rsc.tl_name,
                  context->tl_rscs[iface_p->rsc_index].tl_rsc.dev_name);
        scp_worker_iface_cleanup(iface_p);
        worker->ifaces[idx] = NULL;
    }

    worker->num_ifaces = iface_num;
    return UCG_OK;
}

static ucg_status_t scp_worker_add_resource_ifaces(scp_worker_h worker)
{
    ucg_status_t status;
    scp_context_h context = worker->context;
    worker->ifaces = ucg_malloc(context->num_tls * sizeof(scp_worker_iface_h),
                                "scp ifaces array");
    UCG_ASSERT_RET(worker->ifaces != NULL, UCG_ERR_NO_MEMORY);

    scp_tl_resource_desc_t *resource;
    sct_iface_params_t iface_params;
    scp_rsc_index_t iface_idx = 0;
    scp_rsc_index_t tl_id = 0;

    uint64_t tl_bitmap = context->tl_bitmap;
    ucs_for_each_bit(tl_id, tl_bitmap) {
        resource = &context->tl_rscs[tl_id];
        iface_params.mode.device.tl_name  = resource->tl_rsc.tl_name;
        iface_params.mode.device.dev_name = resource->tl_rsc.dev_name;

        status = scp_worker_iface_open(worker, tl_id, &iface_params,
                                       &worker->ifaces[iface_idx++]);
        if (status != UCG_OK) {
            context->tl_bitmap &= ~UCS_BIT(tl_id);
            iface_idx--;
            continue;
        }
    }

    worker->num_ifaces = iface_idx;

    /* select the best tl resources */
    return scp_worker_select_ifaces(worker);
}

ucg_status_t scp_create_worker(scp_context_h context, scp_worker_h *worker_p)
{
    ucs_status_t ucs_status;
    ucg_status_t status = UCG_ERR_UNSUPPORTED;

    scp_worker_h worker = ucg_malloc(sizeof(*worker), "scp worker");
    UCG_ASSERT_RET(worker != NULL, UCG_ERR_NO_MEMORY);

    pthread_mutex_init(&worker->async, NULL);
    pthread_mutex_init(&worker->submit_mutex, NULL);
    worker->context              = context;
    worker->uuid                 = 0;
    worker->num_ifaces           = 0;
    worker->ifaces               = NULL;
    worker->address              = NULL;
    worker->address_len          = 0;

    status = ucg_mpool_init(&worker->ep_mp, 0, sizeof(scp_ep_t),
                            0, UCG_CACHE_LINE_SIZE, UCG_ELEMS_PER_CHUNK,
                            UINT_MAX, NULL, "worker ep mpool");
    UCG_ASSERT_CODE_RET(status);

    status = ucg_mpool_init(&worker->ep_addr_mp, 0, SCP_EP_PACK_SIZE,
                            0, UCG_CACHE_LINE_SIZE, UCG_ELEMS_PER_CHUNK,
                            UINT_MAX, NULL, "worker ep addr mpool");
    UCG_ASSERT_CODE_GOTO(status, err_destory_ep_pool);

    ucs_status = sct_worker_create(&worker->async, UCS_THREAD_MODE_SINGLE, &worker->uct);
    if (ucg_unlikely(ucs_status != UCS_OK)) {
        status = ucg_status_s2g(ucs_status);
        ucg_error("Failed to create uct worker, %s", ucs_status_string(ucs_status));
        goto err;
    }

    /* Open all resources as interfaces on this worker */
    status = scp_worker_add_resource_ifaces(worker);
    UCG_ASSERT_CODE_GOTO(status, err);

    *worker_p = worker;
    return UCG_OK;

err:
    ucg_mpool_cleanup(&worker->ep_addr_mp, 1);
    for (int i = 0; i < worker->num_ifaces; ++i) {
        scp_worker_iface_cleanup(worker->ifaces[i]);
    }
    ucg_free(worker->ifaces);
    worker->ifaces = NULL;
    ucg_free(worker);
err_destory_ep_pool:
    ucg_mpool_cleanup(&worker->ep_mp, 1);
    return status;
}

ucg_status_t scp_worker_get_address(scp_worker_h worker, scp_address_t **address_p,
                                    size_t *address_length_p)
{
    *address_p = NULL;
    *address_length_p = 0;

    SCP_WORKER_THREAD_CS_ENTER_CONDITIONAL(worker);
    ucg_status_t status = scp_address_pack(worker, (void**)address_p, address_length_p);
    SCP_WORKER_THREAD_CS_EXIT_CONDITIONAL(worker);

    return status;
}

ucg_status_t scp_worker_create_stars_stream(scp_worker_h worker, scp_ofd_stars_stream_h stream, uint16_t stream_depth)
{
    ucg_status_t status;

    stream->num = 0;
    stream->stars_handle = ucg_malloc(worker->num_ifaces * sizeof(void *), "stream stars handle");
    UCG_ASSERT_RET(stream->stars_handle != NULL, UCG_ERR_NO_MEMORY);

    ucs_status_t ucs_status;
    for (uint8_t idx = 0; idx < worker->num_ifaces; ++idx) {
        ucs_status = sct_iface_create_stars_stream(worker->ifaces[idx]->iface,
                                                   &stream->stars_handle[idx], stream_depth);
        UCG_ASSERT_GOTO(ucs_status == UCS_OK, free_stream, ucg_status_s2g(ucs_status));
        ucg_debug("create stars handle %p", stream->stars_handle[idx]);
        stream->num++;
    }

    return UCG_OK;

free_stream:
    scp_worker_delete_stars_stream(worker, stream);
    return status;
}

ucg_status_t scp_worker_delete_stars_stream(scp_worker_h worker, scp_ofd_stars_stream_h stream)
{
    UCG_ASSERT_RET(stream->num <= worker->num_ifaces, UCG_ERR_INVALID_PARAM);

    for (uint8_t idx = 0; idx < stream->num; ++idx) {
        sct_iface_delete_stars_stream(worker->ifaces[idx]->iface,
                                      stream->stars_handle[idx]);
        ucg_debug("delete stars handle %p", stream->stars_handle[idx]);
    }

    return UCG_OK;
}

ucg_status_t scp_worker_progress(scp_worker_h worker)
{
    ucs_status_t ucs_status;
    for (uint8_t idx = 0; idx < worker->num_ifaces; ++idx) {
        ucs_status = sct_iface_notify_progress(worker->ifaces[idx]->iface);
        UCG_ASSERT_RET(ucs_status == UCS_OK, ucg_status_s2g(ucs_status));
    }
    return UCG_OK;
}

void scp_worker_destroy(scp_worker_h worker)
{
    UCG_CHECK_NULL_VOID(worker);

    pthread_mutex_destroy(&worker->async);
    sct_worker_destroy(worker->uct);

    ucg_mpool_cleanup(&worker->ep_addr_mp, 1);
    ucg_mpool_cleanup(&worker->ep_mp, 1);

    for (int i = 0; i < worker->num_ifaces; ++i) {
        scp_worker_iface_cleanup(worker->ifaces[i]);
    }
    ucg_free(worker->ifaces);
    worker->ifaces = NULL;
    ucg_free(worker);
    return;
}