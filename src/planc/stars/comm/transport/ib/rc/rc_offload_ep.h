/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

#ifndef SCT_RC_OFFLOAD_EP_H_
#define SCT_RC_OFFLOAD_EP_H_

#include "rc_ep.h"
#include "rc_offload_md.h"


typedef struct sct_rc_ofd_ep_address {
    sct_ib_uint24_t  qp_num;
} UCS_S_PACKED sct_rc_ofd_ep_address_t;

typedef struct sct_rc_ofd_txcnt {
    uint16_t       pi;
    uint16_t       ci;
} sct_rc_ofd_txcnt_t;

typedef struct sct_rc_ofd_ep {
    sct_rc_ep_t            super;
    sct_rc_ofd_txcnt_t     txcnt;
    struct ibv_qp          *qp;
    uint8_t			       sl;
} sct_rc_ofd_ep_t;

UCS_CLASS_DECLARE_NEW_FUNC(sct_rc_ofd_ep_t, sct_ep_t, const sct_ep_params_t *);
UCS_CLASS_DECLARE_DELETE_FUNC(sct_rc_ofd_ep_t, sct_ep_t);

typedef struct {
    sct_rc_ep_cleanup_ctx_t super;
    struct ibv_qp *qp;
} sct_rc_ofd_ep_cleanup_ctx_t;


ucs_status_t sct_rc_ofd_ep_alloc_event(sct_ep_h tl_ep, sct_event_h event, uint8_t flag);

ucs_status_t sct_rc_ofd_ep_put_with_notify(sct_ep_h tl_ep, sct_ofd_req_h req, const sct_iov_t *iov);
ucs_status_t sct_rc_ofd_ep_wait_notify(sct_ep_h tl_ep, sct_ofd_req_h req, sct_wait_elem_h elem);
ucs_status_t sct_rc_ofd_ep_barrier(sct_ep_h ep, sct_ofd_req_h req,
                                   sct_event_h *notify_event, sct_event_h *wait_event, int event_num);

ucs_status_t sct_rc_ofd_ep_get_address(sct_ep_h tl_ep, uct_ep_addr_t *addr);

ucs_status_t sct_rc_ofd_ep_connect_to_ep(sct_ep_h tl_ep,
                                         const uct_device_addr_t *dev_addr,
                                         const uct_ep_addr_t *ep_addr);

void sct_rc_ofd_ep_cleanup_qp(sct_ib_async_event_wait_t *wait_ctx);

#endif
