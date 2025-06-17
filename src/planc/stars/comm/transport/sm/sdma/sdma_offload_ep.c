/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#include "sdma_offload_ep.h"

#include "sdma_offload_md.h"


ucs_status_t sct_sdma_ofd_ep_alloc_event(sct_ep_h tl_ep, sct_event_h event, uint8_t flag)
{
    sct_sdma_ofd_iface_t *iface = ucs_derived_of(tl_ep->iface, sct_sdma_ofd_iface_t);
    event->dev_id = iface->super.md->stars_dev_id;
    return scs_stars_alloc_events(event->dev_id, 1, &event->event_id, flag);
}

static void sct_sdma_ofd_ep_set_sdma_parm(uint32_t src_pasid, uint32_t dst_pasid,
                                          uint64_t src_addr, uint64_t dst_addr,
                                          unsigned length, sdma_trans_parm_t *sdma_parm)
{
    sdma_parm->s_addr = src_addr;
    sdma_parm->d_addr = dst_addr;
    sdma_parm->s_substreamid = src_pasid;
    sdma_parm->d_substreamid = dst_pasid;
    sdma_parm->length = length;
    sdma_parm->s_stride_len = 0;
    sdma_parm->d_stride_len = 0;
    sdma_parm->stride_num = 0;
    sdma_parm->opcode = 0;
}

static void sct_sdma_ofd_ep_set_event_parm(sct_event_h event,
                                           event_trans_parm_t *event_parm)
{
    event_parm->type = STARS_EVENT_TYPE_INNODE;
    event_parm->event_id = event->event_id.event_id;
    event_parm->event_addr = 0;
    ucg_debug("wait notify for dev %d event_id %u",
              event->dev_id, event->event_id.event_id);
}

static void sct_sdma_ofd_ep_set_notify_parm(sct_event_h event,
                                            write_notify_trans_param_t *event_parm)
{
    event_parm->eventid = event->event_id.event_id;
    event_parm->devid = event->dev_id;
    ucg_debug("will notify eventid %d devid %d",
              event_parm->eventid, event_parm->devid);
}

static ucs_status_t sct_sdma_ofd_ep_set_trans_parm(sct_ofd_req_h req,
                                                   uint32_t opcode,
                                                   uint32_t flag,
                                                   void *parms)
{
    stars_trans_parm_t *trans_parm = scs_stars_get_trans_parm();
    if (ucg_unlikely(!trans_parm)) {
        ucg_fatal("invalid stars trans parm");
    }

    trans_parm->opcode      = opcode;
    trans_parm->wr_cqe_flag = flag;
    trans_parm->trans_parms = parms;
    trans_parm->next        = NULL;

    if (opcode == STARS_SCH_SDMA) {
        trans_parm->parms_len = sizeof(sdma_trans_parm_t);
    } else if (opcode == STARS_EVENT_WAIT ||
               opcode == STARS_EVENT_RECORD) {
        trans_parm->parms_len = sizeof(event_trans_parm_t);
    } else if (opcode == STARS_WRITE_NOTIFY) {
        trans_parm->parms_len = sizeof(write_notify_trans_param_t);
    } else {
        ucg_fatal("unsupported opcode %d", opcode);
    }

    sct_ofd_req_push_trans_tail(req, trans_parm);

    if (flag == 1) {
        req->stars.cqe_cnt++;
    }

    return UCS_OK;
}

ucs_status_t sct_sdma_ofd_ep_put_with_notify(sct_ep_h tl_ep, sct_ofd_req_h sct_req,
                                             const sct_iov_t *iov)
{
    ucs_status_t status;
    sct_sdma_ofd_iface_t *iface = ucs_derived_of(tl_ep->iface, sct_sdma_ofd_iface_t);

    /* when sdma get zero pack, just skip the SCH_SDMA step */
    if (iov->length > 0) {
        sdma_trans_parm_t *sdma_parm =
            sct_sdma_ofd_md_get_sdma_elem(iface->super.md);
        if (ucg_unlikely(sdma_parm == NULL)) {
            ucg_fatal("Invalid resource.");
        }
        sct_sdma_ofd_ep_t *ep = ucs_derived_of(tl_ep, sct_sdma_ofd_ep_t);
        sct_sdma_ofd_ep_set_sdma_parm(ep->src_pasid, ep->dst_pasid, (uint64_t)iov->buffer,
                                      iov->remote_addr, iov->length, sdma_parm);
        ucs_status_t status = sct_sdma_ofd_ep_set_trans_parm(sct_req, STARS_SCH_SDMA, 0, sdma_parm);
        if (ucg_unlikely(status != UCS_OK)) {
            ucg_error("failed to set sdma offload schedule params");
            return status;
        }
    }

    if (iov->unnotify) {
        return UCS_OK;
    }

    write_notify_trans_param_t *event_parm =
        sct_sdma_ofd_md_get_notify_param(iface->super.md);
    if (ucg_unlikely(event_parm == NULL)) {
        ucg_fatal("Invalid stars event param resource.");
    }

    sct_sdma_ofd_ep_set_notify_parm(iov->sct_event, event_parm);

    status = sct_sdma_ofd_ep_set_trans_parm(sct_req, STARS_WRITE_NOTIFY,
                                            iov->flag, event_parm);
    if (ucg_unlikely(status != UCS_OK)) {
        ucg_error("failed to set sdma offload event params");
        return status;
    }

    return UCS_OK;
}

ucs_status_t sct_sdma_ofd_ep_wait_notify(sct_ep_h tl_ep, sct_ofd_req_h sct_req,
                                         sct_wait_elem_h elem)
{
    sct_sdma_ofd_iface_t *iface = ucs_derived_of(tl_ep->iface, sct_sdma_ofd_iface_t);
    ucs_status_t status = UCS_OK;

    event_trans_parm_t *event_parm =
        sct_sdma_ofd_md_get_event_param(iface->super.md);
    if (ucg_unlikely(event_parm == NULL)) {
        ucg_fatal("Invalid stars event param resource.");
    }

    sct_sdma_ofd_ep_set_event_parm(elem->sct_event, event_parm);
    status = sct_sdma_ofd_ep_set_trans_parm(sct_req, STARS_EVENT_WAIT,
                                            elem->flag, event_parm);
    if (ucg_unlikely(status != UCS_OK)) {
        ucg_error("failed to set event wait params");
    }
    return status;
}

ucs_status_t sct_sdma_ofd_ep_barrier(sct_ep_h ep, sct_ofd_req_h req, sct_event_h *notify_event, sct_event_h *wait_event, int event_num)
{
    ucs_status_t status = UCS_OK;
    sct_sdma_ofd_iface_t *iface = ucs_derived_of(ep->iface, sct_sdma_ofd_iface_t);

    for (int event_idx = 0; event_idx < event_num; event_idx++) {
        if (wait_event[event_idx] != NULL) {
            sct_wait_elem_t wait_elem;
            wait_elem.sct_event = wait_event[event_idx];
            wait_elem.flag = 1;
            status = sct_sdma_ofd_ep_wait_notify(ep, req, &wait_elem);
            if (ucg_unlikely(status != UCS_OK)) {
                ucg_error("failed to set wait notify req in barrier");
                return status;
            }
        }

        if (notify_event[event_idx] != NULL) {
            write_notify_trans_param_t *event_parm =
                    sct_sdma_ofd_md_get_notify_param(iface->super.md);
            if (ucg_unlikely(event_parm == NULL)) {
                ucg_fatal("Invalid stars event param resource.");
            }

            sct_sdma_ofd_ep_set_notify_parm(notify_event[event_idx], event_parm);
            status = sct_sdma_ofd_ep_set_trans_parm(req, STARS_WRITE_NOTIFY,
                                                    1, event_parm);
            if (ucg_unlikely(status != UCS_OK)) {
                ucg_error("failed to set event wait params");
            }
        }
    }

    return status;
}

static UCS_CLASS_INIT_FUNC(sct_sdma_ofd_ep_t, const sct_ep_params_t *params)
{
    SCT_EP_PARAMS_CHECK_DEV_IFACE_ADDRS(params);

    sct_sdma_ofd_iface_t *iface = ucs_derived_of(params->iface, sct_sdma_ofd_iface_t);
    UCS_CLASS_CALL_SUPER_INIT(sct_base_ep_t, &iface->super);

    sct_sdma_ofd_iface_addr_t *iface_addr =
        (sct_sdma_ofd_iface_addr_t *)params->iface_addr;
    self->dst_devid = iface_addr->dev_id;
    self->dst_pasid = iface_addr->pasid;

    int ret = api_stars_sdma_authorize(self->dst_devid, &self->dst_pasid, 1);
    if (ucg_unlikely(ret != 0)) {
        ucg_error("Stars_authorize failed status: %d", ret);
        return UCS_ERR_NO_RESOURCE;
    }

    if (self->dst_devid == iface->sdma_md->super.stars_dev_id) {
        self->src_pasid = iface->sdma_pasid;
        ucg_debug("success created local ep %p from iface %p, src { dev %d, pasid %u } dst { dev %d, pasid %u }",
                  self, iface, iface->sdma_md->super.stars_dev_id, self->src_pasid, self->dst_devid, self->dst_pasid);
        return UCS_OK;
    }

    ucg_assert(self->dst_devid >= 0);
    ucg_assert(self->dst_devid < iface->sdma_md->num_devices);
    ret = api_stars_get_pasid(self->dst_devid, &self->src_pasid);
    if (ucg_unlikely(ret != 0)) {
        ucg_error("Failed to get stars_pasid, ret %d", ret);
        return UCS_ERR_NO_RESOURCE;
    }

    ucg_debug("success created remote ep %p from iface %p, src { dev %d, pasid %u } dst { dev %d, pasid %u }",
              self, iface, iface->sdma_md->super.stars_dev_id, self->src_pasid, self->dst_devid, self->dst_pasid);
    return UCS_OK;
}

static UCS_CLASS_CLEANUP_FUNC(sct_sdma_ofd_ep_t)
{
}

UCS_CLASS_DEFINE(sct_sdma_ofd_ep_t, sct_base_ep_t)
UCS_CLASS_DEFINE_NEW_FUNC(sct_sdma_ofd_ep_t, sct_ep_t, const sct_ep_params_t *);
UCS_CLASS_DEFINE_DELETE_FUNC(sct_sdma_ofd_ep_t, sct_ep_t);
