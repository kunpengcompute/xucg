/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

#include "rc_offload_ep.h"

#include "rc_offload_iface.h"
#include "rc_offload_md.h"


#define HNS_ROCE_WR_WRITE_WITH_NOTIFY 22

ucs_status_t sct_rc_ofd_ep_alloc_event(sct_ep_h tl_ep, sct_event_h event, uint8_t flag)
{
    sct_rc_ofd_iface_t *iface = ucs_derived_of(tl_ep->iface, sct_rc_ofd_iface_t);
    event->dev_id = iface->super.super.super.md->stars_dev_id;
    return scs_stars_alloc_events(event->dev_id, 1, &event->event_id, flag);
}

static void sct_rc_ofd_wr_write(struct ibv_qp_ex *qpx,
                                uint64_t event_addr,
                                struct ibv_send_wr *wr)
{
    if (wr->send_flags & IBV_SEND_INLINE) {
        ucg_fatal("unexpected IBV_SEND_INLINE");
    }

    qpx->wr_id      = wr->wr_id;
    qpx->wr_flags   = wr->send_flags;

    if (wr->opcode == HNS_ROCE_WR_WRITE_WITH_NOTIFY) {
        hnsdv_wr_write_notify(qpx, wr->wr.rdma.rkey, wr->wr.rdma.remote_addr, event_addr, 1);
    } else if (wr->opcode == IBV_WR_RDMA_WRITE) {
        ibv_wr_rdma_write(qpx, wr->wr.rdma.rkey, wr->wr.rdma.remote_addr);
    } else {
        ucg_fatal("unspport opcode %d", wr->opcode);
    }

    ibv_wr_set_sge_list(qpx, (size_t)wr->num_sge, wr->sg_list);
    return;
}

static void sct_rc_ofd_ep_post_send(sct_rc_ofd_ep_t *ep, struct ibv_send_wr *wr,
                                    int send_flags, sct_event_h event)
{
    sct_rc_ofd_iface_t *iface = ucs_derived_of(ep->super.super.super.iface,
                                               sct_rc_ofd_iface_t);

    uint64_t event_addr = 0;
    if (wr->opcode == HNS_ROCE_WR_WRITE_WITH_NOTIFY) {
        event_addr = event->event_id.offset;
        ucg_debug("ep post send, event_id %u, offset %lu",
                  event->event_id.event_id, event->event_id.offset);
    }

    wr->send_flags = send_flags;
    wr->wr_id      = ++ep->txcnt.pi;

    struct ibv_qp_ex *qpx;
    qpx = ibv_qp_to_qp_ex(ep->qp);
    if (ucg_unlikely(qpx == NULL)) {
        ucg_fatal("invalid ibv qp ex");
    }
    ibv_wr_start(qpx);

    sct_rc_ofd_wr_write(qpx, event_addr, wr);

    int ret = ibv_wr_complete(qpx);
    if (ucg_unlikely(ret != 0)) {
        ucg_fatal("ibv_wr_complete fail, ret %d", ret);
    }
    return;
}

static void sct_rc_ofd_db_post_send(sct_rc_ofd_ep_t *ep,
                                    sct_ofd_req_h req,
                                    int send_flags,
                                    uint32_t wr_id)
{
    int cqe_en;
    if (send_flags & IBV_SEND_SIGNALED) {
        cqe_en = 1;
        req->stars.cqe_cnt++;
    } else {
        cqe_en = 0;
    }

    sct_rc_ofd_iface_t *iface = ucs_derived_of(ep->super.super.super.iface,
                                               sct_rc_ofd_iface_t);
    rdma_trans_parm_t *rdma_parm = sct_rc_ofd_md_get_rdma_elem(iface->super.super.super.md);
    if (ucg_unlikely(rdma_parm == NULL)) {
        ucg_fatal("Invalid resource.");
    }

    rdma_parm->task_count      = cqe_en;
    rdma_parm->qp_num          = ep->qp->qp_num;
    rdma_parm->db_value        = wr_id;
    rdma_parm->cmd             = STARS_QP_CMD_TYPE_SQ_DB;
    rdma_parm->streamid        = 0;
    rdma_parm->substreamid     = 0;
    rdma_parm->hac_functionId  = 0;
    rdma_parm->sl              = ep->sl;
    rdma_parm->real_sqe_type   = STARS_RDMA_PARM_REAL_RDMA;

    stars_trans_parm_t *trans_parm = scs_stars_get_trans_parm();
    if (ucg_unlikely(!trans_parm)) {
        ucg_fatal("invalid stars trans parm");
    }
    trans_parm->opcode         = STARS_SCH_RDMA;
    trans_parm->wr_cqe_flag    = cqe_en;
    trans_parm->trans_parms    = (void *)rdma_parm;
    trans_parm->parms_len      = sizeof(rdma_trans_parm_t);
    trans_parm->next           = NULL;

    ucg_status_t status = sct_inc_ofd_sq_ci(req, (uintptr_t)ep);
    if (ucg_unlikely(status != UCG_OK)) {
        ucg_fatal("failed to inc sq ci");
    }
    sct_ofd_req_push_trans_tail(req, trans_parm);
    return;
}

ucs_status_t sct_rc_ofd_ep_put_with_notify(sct_ep_h tl_ep, sct_ofd_req_h req, const sct_iov_t *iov)
{
    sct_rc_ofd_iface_t *iface   = ucs_derived_of(tl_ep->iface, sct_rc_ofd_iface_t);
    sct_rc_ofd_ep_t *ep         = ucs_derived_of(tl_ep, sct_rc_ofd_ep_t);

    SCT_RC_CHECK_RES(&iface->super, &ep->super);

    struct ibv_sge sge;
    sge.addr   = (uintptr_t)iov->buffer;
    sge.length = iov->length;
    sge.lkey   = sct_ib_memh_get_lkey(iov->memh);

    struct ibv_send_wr wr;
    wr.num_sge = 1;
    wr.opcode  = iov->unnotify ? IBV_WR_RDMA_WRITE : HNS_ROCE_WR_WRITE_WITH_NOTIFY;
    wr.sg_list = &sge;
    wr.wr.rdma.remote_addr = iov->remote_addr;
    wr.wr.rdma.rkey        = iov->rkey;

    sct_rc_ofd_ep_post_send(ep, &wr, iov->flag ? IBV_SEND_SIGNALED : 0, iov->sct_event);
    sct_rc_ofd_db_post_send(ep, req, wr.send_flags, wr.wr_id);
    return UCS_OK;
}

ucs_status_t sct_rc_ofd_ep_wait_notify(sct_ep_h tl_ep, sct_ofd_req_h req, sct_wait_elem_h elem)
{
    sct_rc_ofd_iface_t *iface = ucs_derived_of(tl_ep->iface, sct_rc_ofd_iface_t);
    event_trans_parm_t *event_parm = sct_rc_ofd_md_get_event_param(iface->super.super.super.md);
    if (ucg_unlikely(event_parm == NULL)) {
        ucg_fatal("Invalid stars event param resource.");
    }

    stars_trans_parm_t *trans_parm = scs_stars_get_trans_parm();
    if (ucg_unlikely(!trans_parm)) {
        ucg_fatal("invalid stars trans parm");
    }

    event_parm->type          = STARS_EVENT_TYPE_INNODE;
    event_parm->event_id      = elem->sct_event->event_id.event_id;
    event_parm->event_addr    = 0;

    trans_parm->opcode        = STARS_EVENT_WAIT;
    trans_parm->wr_cqe_flag   = elem->flag;
    trans_parm->trans_parms   = (void *)event_parm;
    trans_parm->parms_len     = sizeof(event_trans_parm_t);
    trans_parm->next          = NULL;
    sct_ofd_req_push_trans_tail(req, trans_parm);

    if (elem->flag == 1) {
        req->stars.cqe_cnt++;
    }

    ucg_debug("rc ep wait notify, event_id: %u flag %d", event_parm->event_id, elem->flag);
    return UCS_OK;
}

static void sct_rc_ofd_ep_set_notify_parm(sct_event_h event,
                                          write_notify_trans_param_t *event_parm)
{
    event_parm->eventid = event->event_id.event_id;
    event_parm->devid = event->dev_id;
    ucg_debug("will notify eventid %d devid %d",
              event_parm->eventid, event_parm->devid);
}

ucs_status_t sct_rc_ofd_ep_barrier(sct_ep_h ep, sct_ofd_req_h req,
                                   sct_event_h *notify_event, sct_event_h *wait_event,
                                   int event_num)
{
    ucs_status_t status = UCS_OK;
    sct_rc_ofd_iface_t *iface = ucs_derived_of(ep->iface, sct_rc_ofd_iface_t);

    for (int event_idx = 0; event_idx < event_num; event_idx++) {
        if (wait_event[event_idx] != NULL) {
            sct_wait_elem_t wait_elem;
            wait_elem.sct_event = wait_event[event_idx];
            wait_elem.flag = 1;
            status = sct_rc_ofd_ep_wait_notify(ep, req, &wait_elem);
            if (ucg_unlikely(status != UCS_OK)) {
                ucg_error("failed to set wait notify req in barrier");
                return status;
            }
        }

        if (notify_event[event_idx] != NULL) {
            write_notify_trans_param_t *event_parm =
                    sct_rc_ofd_md_get_notify_param(iface->super.super.super.md);
            if (ucg_unlikely(event_parm == NULL)) {
                ucg_fatal("Invalid stars event param resource.");
            }

            sct_rc_ofd_ep_set_notify_parm(notify_event[event_idx], event_parm);
            stars_trans_parm_t *trans_parm = scs_stars_get_trans_parm();
            if (ucg_unlikely(!trans_parm)) {
                ucg_fatal("invalid stars trans parm");
            }

            trans_parm->opcode      = STARS_WRITE_NOTIFY;
            trans_parm->wr_cqe_flag = 1;
            trans_parm->trans_parms = event_parm;
            trans_parm->next        = NULL;
            trans_parm->parms_len   = sizeof(write_notify_trans_param_t);
            sct_ofd_req_push_trans_tail(req, trans_parm);
            req->stars.cqe_cnt++;
        }
    }

    return UCS_OK;
}

ucs_status_t sct_rc_ofd_ep_get_address(sct_ep_h tl_ep, uct_ep_addr_t *addr)
{
    sct_rc_ofd_ep_t *ep = ucs_derived_of(tl_ep, sct_rc_ofd_ep_t);
    sct_rc_ofd_ep_address_t *rc_addr = (sct_rc_ofd_ep_address_t *)addr;
    sct_ib_pack_uint24(rc_addr->qp_num, ep->qp->qp_num);

    return UCS_OK;
}

ucs_status_t sct_rc_ofd_ep_connect_to_ep(sct_ep_h tl_ep,
                                         const uct_device_addr_t *dev_addr,
                                         const uct_ep_addr_t *ep_addr)
{
    sct_rc_ofd_ep_t *ep                    = ucs_derived_of(tl_ep, sct_rc_ofd_ep_t);
    sct_rc_iface_t *iface                  = ucs_derived_of(tl_ep->iface, sct_rc_iface_t);
    const sct_ib_address_t *ib_addr        = (const sct_ib_address_t *)dev_addr;
    const sct_rc_ofd_ep_address_t *rc_addr = (const sct_rc_ofd_ep_address_t *)ep_addr;
    ucs_status_t status;
    uint32_t qp_num;
    struct ibv_ah_attr ah_attr;
    enum ibv_mtu path_mtu;

    sct_ib_iface_fill_ah_attr_from_addr(&iface->super, ib_addr,
                                        ep->super.path_index, &ah_attr,
                                        &path_mtu);
    ucg_assert(path_mtu != SCT_IB_ADDRESS_INVALID_PATH_MTU);

    qp_num = sct_ib_unpack_uint24(rc_addr->qp_num);
    status = sct_rc_iface_qp_connect(iface, ep->qp, qp_num, &ah_attr, path_mtu);
    ep->sl = ah_attr.sl;
    if (status != UCS_OK) {
        return status;
    }

    ep->super.flags |= UCT_RC_EP_FLAG_CONNECTED;
    return UCS_OK;
}

UCS_CLASS_INIT_FUNC(sct_rc_ofd_ep_t, const sct_ep_params_t *params)
{
    sct_rc_ofd_iface_t *iface = ucs_derived_of(params->iface, sct_rc_ofd_iface_t);
    sct_ib_qp_attr_t attr = {};
    ucs_status_t status;

    status = sct_rc_ofd_iface_qp_create(iface, &self->qp, &attr,
                                        iface->super.config.tx_qp_len);
    if (status != UCS_OK) {
        goto err;
    }

    UCS_CLASS_CALL_SUPER_INIT(sct_rc_ep_t, &iface->super, self->qp->qp_num,
                              params);

    status = sct_rc_iface_qp_init(&iface->super, self->qp);
    if (status != UCS_OK) {
        goto err_qp_cleanup;
    }

    sct_ib_md_t *md = sct_ib_iface_md(&iface->super.super);
    status = sct_ib_device_async_event_register(&md->dev,
                                                IBV_EVENT_QP_LAST_WQE_REACHED,
                                                self->qp->qp_num);
    if (status != UCS_OK) {
        goto err_qp_cleanup;
    }

    sct_rc_iface_add_qp(&iface->super, &self->super, self->qp->qp_num);
    sct_rc_txqp_available_set(&self->super.txqp, iface->config.tx_max_wr);
    self->txcnt.pi = self->txcnt.ci = 0;
    return UCS_OK;

err_qp_cleanup:
    sct_ib_destroy_qp(self->qp);
err:
    return status;
}

void sct_rc_ofd_ep_cleanup_qp(sct_ib_async_event_wait_t *wait_ctx)
{
    sct_rc_ofd_ep_cleanup_ctx_t *ep_cleanup_ctx
                = ucs_derived_of(wait_ctx, sct_rc_ofd_ep_cleanup_ctx_t);
    uint32_t qp_num = ep_cleanup_ctx->qp->qp_num;

    sct_ib_destroy_qp(ep_cleanup_ctx->qp);
    sct_rc_ep_cleanup_qp_done(&ep_cleanup_ctx->super, qp_num);
    return;
}

UCS_CLASS_CLEANUP_FUNC(sct_rc_ofd_ep_t)
{
    sct_rc_ofd_iface_t *iface = ucs_derived_of(self->super.super.super.iface,
                                               sct_rc_ofd_iface_t);
    sct_rc_ofd_ep_cleanup_ctx_t *ep_cleanup_ctx;

    ep_cleanup_ctx = ucg_malloc(sizeof(*ep_cleanup_ctx), "ep_cleanup_ctx");
    ep_cleanup_ctx->qp = self->qp;

    ucg_assert(self->txcnt.pi >= self->txcnt.ci);
    iface->super.tx.cq_available += self->txcnt.pi - self->txcnt.ci;
    ucg_assert(iface->super.tx.cq_available < iface->super.config.tx_ops_count);
    sct_ib_modify_qp(self->qp, IBV_QPS_ERR);
    sct_rc_ep_cleanup_qp(&iface->super, &self->super, &ep_cleanup_ctx->super,
                         self->qp->qp_num);
}

UCS_CLASS_DEFINE(sct_rc_ofd_ep_t, sct_rc_ep_t);
UCS_CLASS_DEFINE_NEW_FUNC(sct_rc_ofd_ep_t, sct_ep_t, const sct_ep_params_t *);
UCS_CLASS_DEFINE_DELETE_FUNC(sct_rc_ofd_ep_t, sct_ep_t);
