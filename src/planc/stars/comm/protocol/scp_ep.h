/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#ifndef XUCG_SCP_EP_H
#define XUCG_SCP_EP_H

#include "scp_worker.h"

/*
 * Each handle may get two WAIT tasks, so we prepare two scp_events for a EP_BARRIER request.
 */
#define EP_BARRIER_EVENT_NUM 2

typedef enum scp_ep_state {
    EP_INITED,
    EP_CONNECTED,
    EP_BROKEN,
} scp_ep_state_t;

typedef struct scp_ep {
    scp_worker_h        worker;                                     /* Worker this endpoint belongs to */
    scp_ep_state_t      ep_state;
    uint32_t            flag;                                       /* SCP endpoint flag */
    uint8_t             conn_num;
    uint8_t             sct_ep_num;
    sct_ep_h            sct_eps[SCP_MAX_LANE];                      /* Transports for every lane */
    scp_rsc_index_t     sct_eps_rsc_idx[SCP_MAX_LANE];
    scp_rsc_index_t     sdma_ep_idx;                                /* means sct_eps[sdma_ep_idx] is sdma sct ep */
    scp_address_t       *self_addr;
    scp_address_t       *peer_addr;
    uint8_t             remote_lanes[SCP_MAX_LANE];
    uint8_t             local_lanes[SCP_MAX_LANE];
    float               weights[SCP_MAX_LANE];
    double              bandwidths[SCP_MAX_LANE];
} scp_ep_t;

ucg_status_t scp_ep_create(scp_worker_h worker, scp_ep_h *ep_p, int inter_node_flag);
void scp_ep_destroy(scp_ep_h ep);

ucg_status_t scp_connect_ep(scp_address_t *address, scp_ep_h *ep_p);

ucg_status_t scp_ep_alloc_event(scp_ep_h ep, scp_event_h event, events_pool_h *events_pool);
ucg_status_t scp_ep_free_event(scp_ep_h ep, scp_event_h event, events_pool_h *events_pool);

UCS_F_ALWAYS_INLINE scp_worker_iface_t* scp_ep_get_wiface(scp_ep_h ep, uint8_t sct_ep_idx)
{
    scp_rsc_index_t rsc_idx = ep->sct_eps_rsc_idx[sct_ep_idx];
    return ep->worker->ifaces[rsc_idx];
}

/* 1 means connected, 0 means unconnected */
static inline uint8_t scp_ep_is_connected(scp_ep_h ep)
{
    return (uint8_t)(ep->ep_state == EP_CONNECTED);
}

#endif // XUCG_SCP_EP_H
