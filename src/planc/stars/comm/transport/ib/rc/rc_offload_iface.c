/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

#include "rc_offload_iface.h"

static ucs_config_field_t sct_rc_ofd_iface_config_table[] = {
    {"RC_", "", NULL,
     ucs_offsetof(sct_rc_ofd_iface_config_t, super),
     UCS_CONFIG_TYPE_TABLE(sct_rc_iface_config_table)},

    {"TX_MAX_WR", "-1",
     "Limits the number of outstanding posted work requests. The actual limit is\n"
     "a minimum between this value and the TX queue length. -1 means no limit.",
     ucs_offsetof(sct_rc_ofd_iface_config_t, tx_max_wr), UCS_CONFIG_TYPE_UINT},

    {NULL}
};

void sct_ib_ofd_iface_fill_attr(sct_ib_iface_t *iface, sct_ib_qp_attr_t *attr)
{
    attr->ibv.send_cq             = iface->cq[UCT_IB_DIR_TX];
    attr->ibv.recv_cq             = iface->cq[UCT_IB_DIR_RX];

    attr->ibv.srq           = NULL;
    attr->ibv.cap           = attr->cap;
    attr->ibv.qp_type       = IBV_QPT_RC;
    attr->ibv.sq_sig_all    = 0;

    if (!(attr->ibv.comp_mask & IBV_QP_INIT_ATTR_PD)) {
        attr->ibv.comp_mask = IBV_QP_INIT_ATTR_PD;
    }

    if (!(attr->ibv.comp_mask & IBV_QP_INIT_ATTR_SEND_OPS_FLAGS)) {
        attr->ibv.comp_mask |= IBV_QP_INIT_ATTR_SEND_OPS_FLAGS;
    }

    attr->ibv.pd    = sct_ib_iface_md(iface)->pd;
    attr->port      = iface->config.port_num;
}

static ucs_status_t sct_ib_ofd_iface_create_qp(sct_ib_iface_t *iface,
                                               sct_ib_qp_attr_t *attr,
                                               struct ibv_qp **qp_p)
{
    sct_ib_device_t *dev = sct_ib_iface_device(iface);
    sct_ib_ofd_iface_fill_attr(iface, attr);

    struct hnsdv_qp_init_attr hns_qp_attr = {0};
    hns_qp_attr.comp_mask = HNSDV_QP_INIT_ATTR_MASK_QP_CREATE_FLAGS;
    hns_qp_attr.create_flags = HNSDV_QP_CREATE_ENABLE_STARS_MODE;

    struct ibv_qp *qp = hnsdv_create_qp(dev->ibv_context, &attr->ibv, &hns_qp_attr);
    if (ucg_unlikely(qp == NULL)) {
        ucg_error("iface=%p: failed to create RC QP "
                  "TX wr:%d inl:%d resp:%d: %m",
                  iface, attr->cap.max_send_wr,
                  attr->cap.max_inline_data, attr->max_inl_cqe[UCT_IB_DIR_TX]);
        return UCS_ERR_IO_ERROR;
    }

    attr->cap  = attr->ibv.cap;
    *qp_p      = qp;

    ucg_debug("iface=%p: created RC QP 0x%x on %s:%d "
              "TX wr:%d inl:%d resp:%d",
              iface, qp->qp_num,
              sct_ib_device_name(dev), iface->config.port_num,
              attr->cap.max_send_wr,
              attr->cap.max_inline_data, attr->max_inl_cqe[UCT_IB_DIR_TX]);

    return UCS_OK;
}

ucs_status_t sct_rc_ofd_iface_qp_create(sct_rc_ofd_iface_t *iface, struct ibv_qp **qp_p,
                                        sct_ib_qp_attr_t *attr, unsigned max_send_wr)
{
    attr->cap.max_send_wr            = max_send_wr;
    attr->cap.max_inline_data        = iface->super.config.tx_min_inline;
    attr->sq_sig_all                 = !iface->super.config.tx_moderation;
    attr->max_inl_cqe[UCT_IB_DIR_TX] = iface->super.super.config.max_inl_cqe[UCT_IB_DIR_TX];

    return sct_ib_ofd_iface_create_qp(&iface->super.super, attr, qp_p);
}

static void sct_rc_ofd_wait_flag_reset(sct_rc_ofd_iface_t *iface)
{
    ucs_spin_lock(&(iface->spinlock));
    iface->wait_flag = 0;
    ucs_spin_unlock(&(iface->spinlock));
}

static void sct_rc_ofd_handle_ci(void *ptr, uint32_t ci)
{
    sct_rc_ofd_ep_t *ep = ucs_derived_of(ptr, sct_rc_ofd_ep_t);
    ucg_debug("will update qp num %d ci %d", ep->qp->qp_num, ci);
    hnsdv_update_sq_ci(ep->qp, ci);
    ep->txcnt.ci += ci;
}

static void sct_rc_ofd_update_ci(sct_rc_ofd_iface_t *iface, sct_ofd_req_h req)
{
    uintptr_t key;
    uint32_t value;
    kh_foreach(&req->sq_ci_hash, key, value,
               sct_rc_ofd_handle_ci((void *)key, value));
}

static ucs_status_t sct_rc_ofd_iface_notify_progress(sct_iface_h tl_iface)
{
    sct_rc_ofd_iface_t *iface = ucs_derived_of(tl_iface, sct_rc_ofd_iface_t);

    ucs_spin_lock(&(iface->spinlock));
    if (iface->wait_flag == 1) {
        ucs_spin_unlock(&(iface->spinlock));
        return UCS_OK;
    }

    iface->wait_flag = 1;
    ucs_spin_unlock(&(iface->spinlock));

    int ret;
    sct_ofd_req_h req = NULL;
    ucs_queue_iter_t iter;

    uint32_t waited_cnt = 0;
    ucs_queue_for_each_safe(req, iter, &iface->req_queue, progress) {
        ucg_debug("will progreess stars { handle %p, task_id %d, cqe_cnt %d }",
                  req->stars.handle, req->stars.task_id, req->stars.cqe_cnt);
        req->stars.output.out_pos = 0;
        ret = api_stars_wait_cqe_with_id(req->stars.handle, req->stars.task_id,
                                         req->stars.cqe_cnt, &waited_cnt, NULL,
                                         0);
        if (ucg_unlikely(ret != 0 || waited_cnt > req->stars.cqe_cnt)) {
            ucg_error("failed to wait stars cqe, ret %d, waited %d, excepted %d",
                      ret, waited_cnt, req->stars.cqe_cnt);
            return UCS_ERR_IO_ERROR;
        }

        ucg_debug("progreess stars done { task_id %d,  waited_cnt %d, cqe_cnt  %d",
                  req->stars.task_id, waited_cnt, req->stars.cqe_cnt);

        sct_stars_print_wait_output(&req->stars.output);

        req->stars.cqe_cnt -= waited_cnt;
        if (!req->stars.cqe_cnt) {
            sct_rc_ofd_update_ci(iface, req);
            if (req->cb != NULL) {
                req->cb(req->context, UCG_OK);
            }

            sct_ofd_req_clean(req);
            ucs_queue_del_iter(&iface->req_queue, iter);
        }
    }

    sct_rc_ofd_wait_flag_reset(iface);
    return UCS_OK;
}

static ucs_status_t sct_rc_ofd_iface_query(sct_iface_h tl_iface, sct_iface_attr_t *iface_attr)
{
    sct_rc_ofd_iface_t *iface = ucs_derived_of(tl_iface, sct_rc_ofd_iface_t);
    ucs_status_t status = sct_rc_iface_query(&iface->super, iface_attr);
    UCG_ASSERT_RET(status == UCS_OK, ucg_status_s2g(status));

    iface_attr->cap.flags   = UCT_IFACE_FLAG_CONNECT_TO_EP;
    iface_attr->latency.m   += 1e-9;
    iface_attr->overhead    = 1e-9;
    iface_attr->ep_addr_len = sizeof(sct_rc_ofd_ep_address_t);
    return UCS_OK;
}

uint8_t sct_rc_ofd_iface_get_stars_dev_id(const sct_iface_h tl_iface)
{
    sct_rc_ofd_iface_t *iface = ucs_derived_of(tl_iface, sct_rc_ofd_iface_t);
    return iface->super.super.super.md->stars_dev_id;
}

ucs_status_t sct_ib_ofd_create_cq(sct_ib_iface_t *iface, sct_ib_dir_t dir,
                                  const sct_ib_iface_init_attr_t *init_attr,
                                  int preferred_cpu, size_t inl)
{
    struct ibv_cq_init_attr_ex cq_attr = {0};
    cq_attr.cqe         = init_attr->cq_len[dir];
    cq_attr.channel     = iface->comp_channel;
    cq_attr.comp_vector = preferred_cpu;
    if (init_attr->flags & UCT_IB_CQ_IGNORE_OVERRUN) {
        cq_attr.comp_mask = IBV_CQ_INIT_ATTR_MASK_FLAGS;
        cq_attr.flags     = IBV_CREATE_CQ_ATTR_IGNORE_OVERRUN;
    }

    struct hnsdv_cq_init_attr hns_cq_attr = {0};
    hns_cq_attr.comp_mask = HNSDV_CQ_INIT_ATTR_MASK_CREATE_FLAGS;
    if (dir == UCT_IB_DIR_TX) {
        hns_cq_attr.create_flags = HNSDV_CQ_CREATE_ENABLE_POE_MODE;
        hns_cq_attr.poe_channel = 0x0;
    } else {
        hns_cq_attr.create_flags = HNSDV_CQ_CREATE_ENABLE_NOTIFY;
        hns_cq_attr.notify_mode = 1;
        hns_cq_attr.notify_idx = 0;
    }

    struct ibv_cq_ex *cq_ex;
    sct_ib_device_t *dev = sct_ib_iface_device(iface);
    cq_ex = hnsdv_create_cq_ex(dev->ibv_context, &cq_attr, &hns_cq_attr);
    if (ucg_unlikely(!cq_ex)) {
        ucg_error("hnsdv_create_cq_ex %s failed: %m %d", dev->ibv_context->device->name, errno);
        return UCS_ERR_IO_ERROR;
    }

    struct ibv_cq *cq = ibv_cq_ex_to_cq(cq_ex);
    if (ucg_unlikely(!cq)) {
        ucg_error("ibv_cq_ex_to_cq(cqe=%d) %s failed: %m",
                  init_attr->cq_len[dir], dev->ibv_context->device->name);
        return UCS_ERR_IO_ERROR;
    }

    iface->cq[dir]                 = cq;
    iface->config.max_inl_cqe[dir] = inl;
    return UCS_OK;
}

ucs_status_t sct_rc_ofd_iface_submit_request(sct_iface_h tl_iface, sct_ofd_req_h req)
{
    sct_rc_ofd_iface_t *iface = ucs_derived_of(tl_iface, sct_rc_ofd_iface_t);

    sct_stars_print_trans_parm_info(req->stars.trans_task.head.next, req->stars.trans_task.count);

    int ret = api_stars_send_task_with_id(req->stars.handle, req->stars.trans_task.head.next,
                                          req->stars.trans_task.count, &req->stars.task_id);
    if (ucg_unlikely(ret != 0)) {
        ucg_fatal("failded to send stars task, ret %d (%m)", ret);
        return UCS_ERR_IO_ERROR;
    }

    ucg_debug("submit stars task, handle %p, taskid %d, task_cnt %d", req->stars.handle,
              req->stars.task_id, req->stars.trans_task.count);
    ucs_queue_push(&iface->req_queue, &req->progress);
    return UCS_OK;
}

static ucs_status_t sct_rc_ofd_iface_create_stars_stream(sct_iface_h tl_iface, void **handle_p, uint16_t stream_depth)
{
    sct_rc_ofd_iface_t *iface = ucs_derived_of(tl_iface, sct_rc_ofd_iface_t);
    sct_md_h tl_md = iface->super.super.super.md;
    sct_rc_ofd_md_t *md = ucs_derived_of(tl_md, sct_rc_ofd_md_t);
    stars_handle_attrs_t handle_config;
    handle_config.streamDepth = stream_depth;

    void *handle = api_stars_get_handle_ex(tl_md->stars_dev_id, md->dev_attr.pool_id, &handle_config);

    if (ucg_unlikely(handle == NULL)) {
        ucg_error("Failed to create stars handle %m");
        return UCS_ERR_NO_RESOURCE;
    }

    *handle_p = handle;
    return UCS_OK;
}

static UCS_CLASS_DECLARE_DELETE_FUNC(sct_rc_ofd_iface_t, sct_iface_t);

static sct_rc_iface_ops_t sct_rc_ofd_iface_ops = {
    {
        {
        .ep_put_with_notify       = sct_rc_ofd_ep_put_with_notify,
        .ep_wait_notify           = sct_rc_ofd_ep_wait_notify,
        .ep_barrier               = sct_rc_ofd_ep_barrier,
        .ep_alloc_event           = sct_rc_ofd_ep_alloc_event,
        .ep_create                = UCS_CLASS_NEW_FUNC_NAME(sct_rc_ofd_ep_t),
        .ep_destroy               = UCS_CLASS_DELETE_FUNC_NAME(sct_rc_ofd_ep_t),
        .ep_get_address           = sct_rc_ofd_ep_get_address,
        .ep_connect_to_ep         = sct_rc_ofd_ep_connect_to_ep,
        .iface_create_stars_stream = sct_rc_ofd_iface_create_stars_stream,
        .iface_delete_stars_stream = sct_iface_free_stars_stream,
        .iface_submit_req         = sct_rc_ofd_iface_submit_request,
        .iface_notify_progress    = sct_rc_ofd_iface_notify_progress,
        .iface_close              = UCS_CLASS_DELETE_FUNC_NAME(sct_rc_ofd_iface_t),
        .iface_query              = sct_rc_ofd_iface_query,
        .iface_get_device_address = sct_ib_iface_get_device_address,
        .iface_get_address        = ucs_empty_function_return_success,
        .iface_is_reachable       = sct_ib_iface_is_reachable,
        .iface_get_stars_dev_id   = sct_rc_ofd_iface_get_stars_dev_id
        },
        .create_cq                = sct_ib_ofd_create_cq,
    },
    .cleanup_qp               = sct_rc_ofd_ep_cleanup_qp,
};

static UCS_CLASS_INIT_FUNC(sct_rc_ofd_iface_t, sct_md_h tl_md,
                           sct_worker_h worker, const sct_iface_params_t *params,
                           const sct_iface_config_t *tl_config)
{
    sct_rc_ofd_iface_config_t *config   = ucs_derived_of(tl_config, sct_rc_ofd_iface_config_t);
    sct_ib_iface_config_t *ib_config    = &config->super.super.super;
    sct_ib_iface_init_attr_t init_attr  = {};

    init_attr.cq_len[UCT_IB_DIR_RX]  = config->super.tx_cq_len;
    init_attr.cq_len[UCT_IB_DIR_TX]  = config->super.tx_cq_len;
    init_attr.seg_size               = ib_config->seg_size;

    UCS_CLASS_CALL_SUPER_INIT(sct_rc_iface_t, &sct_rc_ofd_iface_ops, tl_md,
                              worker, params, &config->super.super, &init_attr);

    self->id                         = ucs_generate_uuid((uintptr_t)self);
    self->wait_flag                  = 0;
    self->config.tx_max_wr           = ucs_min(config->tx_max_wr,
                                               self->super.config.tx_qp_len);
    self->config.max_send_sge        = SCT_IB_MAX_IOV;
    self->super.config.tx_moderation = ucs_min(config->super.tx_cq_moderation,
                                               self->config.tx_max_wr / SCT_TX_CQ_MOD);
    self->super.super.config.sl      = 0;

    ucs_spinlock_init(&(self->spinlock), 0);
    ucs_queue_head_init(&self->req_queue);

    ucg_debug("The tx_cq_moderation of rc is %u, tx_cq_len is %u, tx_max_wr is %u",
             config->super.tx_cq_moderation, config->super.tx_cq_len, config->tx_max_wr);
    return UCS_OK;
}

static UCS_CLASS_CLEANUP_FUNC(sct_rc_ofd_iface_t)
{
    ucs_spinlock_destroy(&(self->spinlock));
    sct_rc_iface_cleanup_eps(&self->super);
}

UCS_CLASS_DEFINE(sct_rc_ofd_iface_t, sct_rc_iface_t);
static UCS_CLASS_DEFINE_NEW_FUNC(sct_rc_ofd_iface_t, sct_iface_t, sct_md_h,
                                 sct_worker_h, const sct_iface_params_t*,
                                 const sct_iface_config_t*);
static UCS_CLASS_DEFINE_DELETE_FUNC(sct_rc_ofd_iface_t, sct_iface_t);

static ucs_status_t sct_rc_ofd_query_tl_devices(sct_md_h md,
                                                sct_tl_device_resource_t **tl_devices_p,
                                                unsigned *num_tl_devices_p)
{
    sct_ib_md_t *ib_md = ucs_derived_of(md, sct_ib_md_t);
    int flags = ib_md->config.eth_pause ? 0 : UCT_IB_DEVICE_FLAG_LINK_IB;
    ucg_debug("The eth_pause mode is %s", ib_md->config.eth_pause ? "enable" : "disable");
    return sct_ib_device_query_ports(&ib_md->dev, flags, tl_devices_p,
                                     num_tl_devices_p);
}

SCT_TL_DEFINE(&sct_ib_component, rc_acc, sct_rc_ofd_query_tl_devices,
              sct_rc_ofd_iface_t, "RC_", sct_rc_ofd_iface_config_table,
              sct_rc_ofd_iface_config_t);
