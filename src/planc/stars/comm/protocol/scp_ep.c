/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

#include "scp_ep.h"

#include "scp_address.h"

static ucg_status_t scp_ep_create_base(scp_worker_h worker, scp_ep_h *ep_p)
{
    ucg_status_t status;
    scp_ep_h ep = scp_worker_alloc_ep(worker);
    UCG_ASSERT_RET(ep != NULL, UCG_ERR_NO_MEMORY);

    ep->self_addr = scp_worker_alloc_ep_addr(worker);
    UCG_ASSERT_GOTO(ep->self_addr != NULL, free_ep,
                    UCG_ERR_NO_MEMORY);

    ep->peer_addr = scp_worker_alloc_ep_addr(worker);
    UCG_ASSERT_GOTO(ep->peer_addr != NULL, free_self_addr,
                    UCG_ERR_NO_MEMORY);

    ep->worker              = worker;
    ep->flag                = 0;
    ep->sdma_ep_idx         = UINT8_MAX;
    ep->sct_ep_num          = 0;
    ep->conn_num            = 0;
    ep->ep_state            = EP_INITED;

    for (uint8_t idx = 0; idx < SCP_MAX_LANE; ++idx) {
        ep->sct_eps[idx] = NULL;
        ep->remote_lanes[idx] = SCP_MAX_LANE;
        ep->weights[idx] = 0;
    }

    *ep_p = ep;
    return UCG_OK;

free_self_addr:
    ucg_mpool_put(ep->self_addr);
    ep->self_addr = NULL;
free_ep:
    ucg_mpool_put(ep);
    *ep_p = NULL;
    return UCG_ERR_NO_MEMORY;
}

static void scp_ep_destory_base(scp_ep_h ep)
{
    if (ep->self_addr) {
        ucg_mpool_put(ep->self_addr);
        ep->self_addr = NULL;
    }

    if (ep->self_addr) {
        ucg_mpool_put(ep->peer_addr);
        ep->self_addr = NULL;
    }

    if (ep) {
        ucg_mpool_put(ep);
        ep = NULL;
    }
}

static ucg_status_t scp_select_lanes(scp_worker_h worker, scp_rsc_index_t *ifaces_idxs,
                                     uint8_t *iface_cnt, scp_rsc_index_t *sdma_rsc_idx,
                                     int inter_node_flag)
{
    scp_context_h context = worker->context;
    uint8_t multirails = context->scp_config->ctx.multirails;
    scp_rsc_index_t rsc_idx;

    uint8_t cnt = 0;
    for (uint8_t idx = 0; idx < worker->num_ifaces; ++idx) {
        rsc_idx = worker->ifaces[idx]->rsc_index;
        int match_sdma = scp_context_is_rsc_match(context, rsc_idx, STARS_SDMA_TL_NAME);
        if (match_sdma) {
            if (inter_node_flag) {
                ucg_debug("All processes uniformly disable sdma in"
                         " inter-node communication");
                match_sdma = 0;
            } else {
                *sdma_rsc_idx = worker->ifaces[idx]->rsc_index;
            }
        }

        int match_rc = scp_context_is_rsc_match(context, rsc_idx, STARS_RC_TL_NAME);
        if (!(match_sdma || match_rc)) {
            continue;
        }

        ifaces_idxs[cnt++] = idx;

        if (cnt >= multirails) {
            *iface_cnt = multirails;
            return UCG_OK;
        }
    }

    *iface_cnt = cnt;
    return (cnt > 0) ? UCG_OK : UCG_ERR_NOT_FOUND;
}

static ucg_status_t scp_create_sct_ep(scp_worker_iface_h wiface, sct_ep_h *ep_p)
{
    sct_ep_h sct_ep = NULL;
    uint64_t iface_flag = wiface->attr.cap.flags;

    /* Only support UCT_IFACE_FLAG_CONNECT_TO_EP and UCT_EP_PARAM_FIELD_IFACE,
       UCT_EP_PARAM_FIELD_IFACE means create sct ep in connection procession
    */
    if (!(iface_flag & UCT_IFACE_FLAG_CONNECT_TO_EP)) {
        ucg_assert((iface_flag & UCT_IFACE_FLAG_CONNECT_TO_IFACE) != 0);
        goto out;
    }

    sct_ep_params_t ep_params;
    ep_params.field_mask = UCT_EP_PARAM_FIELD_IFACE;
    ep_params.iface = wiface->iface;
    ucs_status_t status = sct_ep_create(&ep_params, &sct_ep);
    if (status != UCS_OK) {
        ucg_error("Failed to create uct ep: %s", ucs_status_string(status));
        return UCG_ERR_UNSUPPORTED;
    }

out:
    *ep_p = sct_ep;
    return UCG_OK;
}

static ucg_status_t scp_ep_create_inner(scp_ep_h ep, int inter_node_flag)
{
    scp_rsc_index_t ifaces_idxs[SCP_MAX_LANE];
    uint8_t iface_cnt = 0;
    scp_worker_iface_h wiface;
    ucg_status_t status;

    scp_rsc_index_t sdma_rsc_idx = UINT8_MAX;
    status = scp_select_lanes(ep->worker, ifaces_idxs, &iface_cnt,
                              &sdma_rsc_idx, inter_node_flag);
    if (ucg_unlikely(status != UCG_OK)) {
        ucg_error("Failed to found iface, ret %d find count %d",
                  status, iface_cnt);
        return status;
    }

    uint8_t ep_idx = 0;
    for (uint8_t idx = 0; idx < iface_cnt; ++idx) {
        wiface = ep->worker->ifaces[ifaces_idxs[idx]];
        status = scp_create_sct_ep(wiface, &ep->sct_eps[ep_idx]);
        if (ucg_unlikely(status != UCG_OK)) {
            ucg_warn("Failed to init lane of index: %d.", ifaces_idxs[idx]);
            continue;
        }

        if (sdma_rsc_idx == wiface->rsc_index) {
            ep->sdma_ep_idx = ep_idx;
        }

        ep->sct_eps_rsc_idx[ep_idx] = ifaces_idxs[idx];
        ep->worker->iface2idx[ep_idx] = ifaces_idxs[idx];
        ep_idx++;
    }

    ep->sct_ep_num = ep_idx;
    return (ep_idx > 0) ? UCG_OK : UCG_ERR_NO_RESOURCE;
}

ucg_status_t scp_ep_create(scp_worker_h worker, scp_ep_h *ep_p, int inter_node_flag)
{
    ucg_status_t status;
    scp_ep_h ep = NULL;

    SCP_ASYNC_BLOCK(&worker->async);
    {
        status = scp_ep_create_base(worker, &ep);
        UCG_CHECK_GOTO(status, out);

        status = scp_ep_create_inner(ep, inter_node_flag);
        UCG_CHECK_GOTO(status, destory_base);
    }
    SCP_ASYNC_UNBLOCK(&worker->async);

    *ep_p = ep;
    return UCG_OK;

destory_base:
    scp_ep_destory_base(ep);
out:
    return status;
}

void scp_ep_destroy(scp_ep_h ep)
{
    if (ucg_unlikely(ep == NULL)) {
        return;
    }

    for (int i = 0; i < ep->sct_ep_num; ++i) {
        if (ep->sct_eps[i] != NULL) {
            sct_ep_destroy(ep->sct_eps[i]);
            ucg_debug("destroyed rc ep %p", ep->sct_eps[i]);
        }
    }

    scp_ep_destory_base(ep);
}

static ucg_status_t scp_do_connect_sct_ep(sct_ep_h *sct_ep, scp_worker_iface_h wiface,
                                          const uct_device_addr_t *dev_addr,
                                          const uct_iface_addr_t *iface_addr,
                                          const uct_ep_addr_t *ep_addr)
{
    ucs_status_t ucs_status;

    if (!sct_iface_is_reachable(wiface->iface, dev_addr, iface_addr)) {
        ucg_warn("Destination is unreachable.");
        return UCG_ERR_UNREACHABLE;
    }

    if (wiface->attr.cap.flags & UCT_IFACE_FLAG_CONNECT_TO_EP) {
        ucs_status = sct_ep_connect_to_ep(*sct_ep, dev_addr, ep_addr);
    } else if (wiface->attr.cap.flags & UCT_IFACE_FLAG_CONNECT_TO_IFACE) {
        sct_ep_params_t ep_params;
        ep_params.field_mask |= UCT_EP_PARAM_FIELD_IFACE |
                                UCT_EP_PARAM_FIELD_DEV_ADDR |
                                UCT_EP_PARAM_FIELD_IFACE_ADDR;
        ep_params.iface      = wiface->iface;
        ep_params.dev_addr   = dev_addr;
        ep_params.iface_addr = iface_addr;
        ucs_status = sct_ep_create(&ep_params, sct_ep);
        ucg_debug("uct ep connect to iface %s", ucs_status_string(ucs_status));
    } else {
        ucg_error("unsupported connect mode.");
        ucs_status = UCS_ERR_UNSUPPORTED;
    }

    if (ucs_status != UCS_OK) {
        ucg_error("Failed to connect endpoint: %s", ucs_status_string(ucs_status));
    }

    return ucg_status_s2g(ucs_status);
}

static ucg_status_t scp_connect_sct_ep(scp_ep_h ep, scp_unpacked_address_h scp_addr,
                                       scp_ep_unpacked_h scp_peer, uint8_t self_idx,
                                       uint8_t peer_idx)
{
    if (self_idx >= ep->sct_ep_num || peer_idx >= scp_peer->num_ep_addrs) {
        return UCG_ERR_INVALID_PARAM;
    }

    scp_worker_iface_t *wiface = scp_ep_get_wiface(ep, self_idx);
    UCG_ASSERT_RET(wiface != NULL, UCG_ERR_INVALID_ADDR);

    sct_ep_h *sct_ep = &ep->sct_eps[self_idx];
    scp_address_entry_ep_addr_h sct_peer = &scp_peer->ep_addrs[peer_idx];
    scp_address_entry_h peer = &scp_addr->address_list[sct_peer->rsc_index];

    return scp_do_connect_sct_ep(sct_ep, wiface, peer->dev_addr,
                                 peer->iface_addr, sct_peer->addr);
}

static ucg_status_t scp_connect_sdma(scp_ep_h ep, scp_unpacked_address_h scp_addr,
                                     scp_ep_unpacked_h peer)
{
    uint8_t self_sdma_idx = ep->sdma_ep_idx;
    uint8_t peer_sdma_idx = peer->sdma_ep_idx;

    if (self_sdma_idx >= SCP_MAX_LANE ||
        peer_sdma_idx >= SCP_MAX_LANE) {
        return UCG_ERR_UNSUPPORTED;
    }

    ucg_status_t status = UCG_OK;
    status = scp_connect_sct_ep(ep, scp_addr, peer,
                                self_sdma_idx, peer_sdma_idx);
    UCG_CHECK_GOTO(status, out);

    ep->local_lanes[ep->conn_num++] = self_sdma_idx;
    ep->remote_lanes[self_sdma_idx] = peer_sdma_idx;
    ep->weights[self_sdma_idx] = 1;

out:
    return status;
}

static ucg_status_t scp_connect_rc(scp_ep_h ep, scp_unpacked_address_h scp_addr,
                                   scp_ep_unpacked_h peer_addr, uint32_t *ep_map)
{
    ucg_status_t status = UCG_OK;
    uint8_t self_sdma_idx = ep->sdma_ep_idx;
    uint32_t self_map = UCS_BIT(self_sdma_idx);
    uint8_t peer_sdma_idx = peer_addr->sdma_ep_idx;
    uint32_t peer_map = UCS_BIT(peer_sdma_idx);
    uint8_t multirails = ep->worker->context->scp_config->ctx.multirails;

    for (uint8_t self_idx = 0; self_idx < ep->sct_ep_num; ++self_idx) {
        for (uint8_t peer_idx = 0; peer_idx < peer_addr->num_ep_addrs; ++peer_idx) {
            if ((self_map & UCS_BIT(self_idx)) || (peer_map & UCS_BIT(peer_idx))) {
                continue;
            }

            status = scp_connect_sct_ep(ep, scp_addr, peer_addr, self_idx, peer_idx);
            if (ucg_unlikely(status != UCG_OK)) {
                ucg_debug("Failed to connect rc ep, ret %d", status);
                continue;
            }

            self_map |= UCS_BIT(self_idx);
            peer_map |= UCS_BIT(peer_idx);
            ep->remote_lanes[self_idx] = peer_idx;
            ep->local_lanes[ep->conn_num++] = self_idx;
            UCG_MATCH_GOTO(--multirails == 0, out);
        }
    }

out:
    *ep_map = self_map;
    return UCG_OK;
}

static void scp_calc_bandwidths(scp_ep_h ep)
{
    double total_bandwidth = 0;
    double bandwidths[SCP_MAX_LANE];

    for (uint8_t conn_idx = 0, ep_idx = 0; conn_idx < ep->conn_num; ++conn_idx) {
        ep_idx = ep->local_lanes[conn_idx];
        double bandwidth = ep->bandwidths[ep_idx];
        bandwidths[ep_idx] = bandwidth;
        total_bandwidth += bandwidth;
    }
    for (uint8_t conn_idx = 0, ep_idx = 0; conn_idx < ep->conn_num; ++conn_idx) {
        ep_idx = ep->local_lanes[conn_idx];
        ep->weights[ep_idx] = bandwidths[ep_idx] / total_bandwidth;
    }
}

static ucg_status_t scp_connect_ep_inner(scp_ep_h ep, scp_unpacked_address_h scp_addr,
                                         scp_ep_unpacked_h peer_addr)
{
    ucg_status_t status = UCG_OK;

    scp_ep_unpacked_h self_addr = (scp_ep_unpacked_h)ep->self_addr;
    if (ep->sdma_ep_idx != UINT8_MAX && peer_addr->guid == self_addr->guid) {
        status = scp_connect_sdma(ep, scp_addr, peer_addr);
        UCG_ASSERT_CODE_GOTO(status, out);
        if (ep->sct_ep_num == 1) {
            return UCG_OK;
        }
    }

    uint32_t select_map = 0;
    status = scp_connect_rc(ep, scp_addr, peer_addr, &select_map);
    if (ucg_unlikely(status != UCG_OK || select_map == 0)) {
        ucg_error("protocol ep connect failed");
        return UCG_ERR_UNSUPPORTED;
    }

    scp_calc_bandwidths(ep);

out:
	return status;
}

ucg_status_t scp_connect_ep(scp_address_t *address, scp_ep_h *ep_p)
{
    UCG_ASSERT_RET(ep_p != NULL, UCG_ERR_INVALID_PARAM);

    scp_unpacked_address_h scp_addr;
    ucg_status_t status = scp_address_unpack((void *)address, &scp_addr);
    UCG_ASSERT_CODE_RET(status);

    scp_ep_h ep = *ep_p;
    UCG_ASSERT_RET(ep->peer_addr != NULL, UCG_ERR_INVALID_PARAM);

    scp_ep_unpacked_h ep_addr;
    status = scp_ep_address_unpack((void *)ep->peer_addr, &ep_addr);
    UCG_ASSERT_CODE_RET(status);

    status = scp_connect_ep_inner(ep, scp_addr, ep_addr);
    UCG_ASSERT_CODE_RET(status);

    ep->ep_state = EP_CONNECTED;
    /* release exchage buffer */
    ucg_mpool_put(ep->peer_addr);
    ep->peer_addr = NULL;
    ucg_mpool_put(ep->self_addr);
    ep->self_addr = NULL;

    return status;
}

ucg_status_t scp_ep_alloc_event(scp_ep_h ep, scp_event_h event, events_pool_h *events_pool)
{
    ucs_status_t status = UCS_ERR_INVALID_PARAM;

    sct_ep_h sct_ep = NULL;
    sct_event_h sct_event = NULL;
    uint8_t err_ep_num = 0;

    for (uint8_t dst_idx, src_idx = 0; src_idx < ep->sct_ep_num; ++src_idx) {
        dst_idx = ep->remote_lanes[src_idx];
        if (dst_idx >= SCP_MAX_LANE) {
            /* skip unconnected ep */
            continue;
        }

        sct_ep = ep->sct_eps[src_idx];
        sct_event = &event->sct_event[dst_idx];
        scs_eid_pool_status_t eid_pool_status = sct_get_event_from_pool(sct_ep, events_pool, sct_event);
        if (eid_pool_status == SCS_EVENT_POOL_OK) {
            status = UCS_OK;
        } else {
            status = sct_ep_alloc_event(sct_ep, sct_event, 0);
            if (status != UCS_OK) {
                err_ep_num = src_idx;
                goto err;
            }
        }
        UCG_ASSERT_RET(status == UCS_OK, ucg_status_s2g(status));
    err:
    for (uint8_t dst_idx = 0, src_idx = 0; src_idx < err_ep_num; ++src_idx) {
        dst_idx = ep->remote_lanes[src_idx];
        if (dst_idx >= SCP_MAX_LANE) {
            continue;
        }
        sct_ep = ep->sct_eps[src_idx];
        sct_event = &event->sct_event[dst_idx];
        scs_eid_pool_status_t eid_pool_status = sct_put_event_to_pool(sct_ep, eid_pool, sct_event);
        if (eid_pool_status == SCS_EVENT_POOL_OK) {
            status = UCS_OK;
        } else {
            status = sct_ep_free_event(sct_event, 0);
        }
        UCG_ASSERT_RET(status == UCS_OK, ucg_status_s2g(status));
    }
 
    return UCG_ERR_NO_RESOURCE;
    }

    return ucg_status_s2g(status);
}

ucg_status_t scp_ep_free_event(scp_ep_h ep, scp_event_h event, events_pool_h *eid_pool)
{
    ucs_status_t status = UCS_ERR_INVALID_PARAM;

    sct_ep_h sct_ep = NULL;
    sct_event_h sct_event;
    for (uint8_t dst_idx = 0, src_idx = 0; src_idx < ep->sct_ep_num; ++src_idx) {
        dst_idx = ep->remote_lanes[src_idx];
        if (dst_idx >= SCP_MAX_LANE) {
            /* skip unconnected ep */
            continue;
        }
        sct_ep = ep->sct_eps[src_idx];
        sct_event = &event->sct_event[dst_idx];
        scs_eid_pool_status_t eid_pool_status = sct_put_event_to_pool(sct_ep, eid_pool, sct_event);
        if (eid_pool_status == SCS_EVENT_POOL_OK) {
            status = UCS_OK;
        } else {
            status = sct_ep_free_event(sct_event, 0);
        }
        UCG_ASSERT_RET(status == UCS_OK, ucg_status_s2g(status));
    }

    return ucg_status_s2g(status);
}