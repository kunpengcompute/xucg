/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */
#ifndef SCT_SDMA_OFFLOAD_EP_H
#define SCT_SDMA_OFFLOAD_EP_H

#include "sdma_offload_iface.h"

typedef struct sct_sdma_ofd_ep {
    sct_base_ep_t       super;
    int                 dst_pasid;
    int                 dst_devid;
    unsigned            src_pasid;
} sct_sdma_ofd_ep_t;

UCS_CLASS_DECLARE_NEW_FUNC(sct_sdma_ofd_ep_t, sct_ep_t, const sct_ep_params_t *);
UCS_CLASS_DECLARE_DELETE_FUNC(sct_sdma_ofd_ep_t, sct_ep_t);

ucs_status_t sct_sdma_ofd_ep_alloc_event(sct_ep_h tl_ep, sct_event_h event, uint8_t flag);

ucs_status_t sct_sdma_ofd_ep_put_with_notify(sct_ep_h tl_ep, sct_ofd_req_h sct_req, const sct_iov_t *iov);
ucs_status_t sct_sdma_ofd_ep_wait_notify(sct_ep_h tl_ep, sct_ofd_req_h sct_req, sct_wait_elem_h elem);
ucs_status_t sct_sdma_ofd_ep_barrier(sct_ep_h ep, sct_ofd_req_h req,
                                     sct_event_h *notify_event, sct_event_h *wait_event,
                                     int event_num);

#endif
