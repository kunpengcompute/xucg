/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

#include "rc_ep.h"
#include "rc_iface.h"


ucs_config_field_t sct_rc_iface_common_config_table[] = {
    {SCT_IB_CONFIG_PREFIX, "TX_INLINE_RESP=64;SEG_SIZE=8256", NULL,
     ucs_offsetof(sct_rc_iface_common_config_t, super),
     UCS_CONFIG_TYPE_TABLE(sct_ib_iface_config_table)},

    {"TIMEOUT", "1.0s",
     "Transport timeout",
     ucs_offsetof(sct_rc_iface_common_config_t, tx.timeout), UCS_CONFIG_TYPE_TIME},

    {"RETRY_COUNT", "7",
     "Transport retries",
     ucs_offsetof(sct_rc_iface_common_config_t, tx.retry_count), UCS_CONFIG_TYPE_UINT},

    {"RNR_TIMEOUT", "1ms",
     "RNR timeout",
     ucs_offsetof(sct_rc_iface_common_config_t, tx.rnr_timeout), UCS_CONFIG_TYPE_TIME},

    {"RNR_RETRY_COUNT", "7",
     "RNR retries",
     ucs_offsetof(sct_rc_iface_common_config_t, tx.rnr_retry_count), UCS_CONFIG_TYPE_UINT},

    {NULL}
};


/* Config relevant for rc_mlx5 and rc_verbs only (not for dc) */
ucs_config_field_t sct_rc_iface_config_table[] = {
    {"RC_", "MAX_NUM_EPS=256", NULL,
     ucs_offsetof(sct_rc_iface_config_t, super),
     UCS_CONFIG_TYPE_TABLE(sct_rc_iface_common_config_table)},

    {"TX_CQ_MODERATION", "64",
     "Maximum number of send WQEs which can be posted without requesting a completion.",
     ucs_offsetof(sct_rc_iface_config_t, tx_cq_moderation), UCS_CONFIG_TYPE_UINT},

    {"TX_CQ_LEN", "4096",
     "Length of send completion queue. This limits the total number of outstanding signaled sends.",
     ucs_offsetof(sct_rc_iface_config_t, tx_cq_len), UCS_CONFIG_TYPE_UINT},

    {NULL}
};

ucs_status_t sct_rc_iface_query(sct_rc_iface_t *iface, sct_iface_attr_t *iface_attr)
{
    ucs_status_t status =
        sct_ib_iface_query(&iface->super, SCT_IB_RETH_LEN, iface_attr);
    UCG_ASSERT_RET(status == UCS_OK, ucg_status_s2g(status));

    iface_attr->iface_addr_len  = 0;
    iface_attr->cap.flags       = UCT_IFACE_FLAG_CONNECT_TO_EP;
    iface_attr->cap.event_flags = UCT_IFACE_FLAG_EVENT_SEND_COMP |
                                  UCT_IFACE_FLAG_EVENT_RECV      |
                                  UCT_IFACE_FLAG_EVENT_FD;

    iface_attr->cap.put.align_mtu = sct_ib_mtu_value(iface->super.config.path_mtu);

    /* Error Handling */
    iface_attr->cap.flags        |= UCT_IFACE_FLAG_ERRHANDLE_PEER_FAILURE;
    return UCS_OK;
}

void sct_rc_iface_add_qp(sct_rc_iface_t *iface, sct_rc_ep_t *ep,
                         unsigned qp_num)
{
    sct_rc_ep_t ***ptr, **memb;

    ucs_spin_lock(&iface->eps_lock);
    ptr = &iface->eps[qp_num >> SCT_RC_QP_TABLE_ORDER];
    if (*ptr == NULL) {
        *ptr = ucg_calloc(UCS_BIT(SCT_RC_QP_TABLE_MEMB_ORDER), sizeof(**ptr),
                          "rc qp table");
        ucg_assert(*ptr != NULL);
    }

    memb = &(*ptr)[qp_num & UCS_MASK(SCT_RC_QP_TABLE_MEMB_ORDER)];
    ucg_assert(*memb == NULL);
    *memb = ep;
    ucs_spin_unlock(&iface->eps_lock);
}

void sct_rc_iface_remove_qp(sct_rc_iface_t *iface, unsigned qp_num)
{
    sct_rc_ep_t **memb;

    ucs_spin_lock(&iface->eps_lock);
    memb = &iface->eps[qp_num >> SCT_RC_QP_TABLE_ORDER]
                      [qp_num &  UCS_MASK(SCT_RC_QP_TABLE_MEMB_ORDER)];
    ucg_assert(*memb != NULL);
    *memb = NULL;
    ucs_spin_unlock(&iface->eps_lock);
}

static int sct_rc_iface_config_limit_value(const char *name,
                                           int provided, int limit)
{
    if (provided > limit) {
        ucg_warn("using maximal value for %s (%d) instead of %d",
                 name, limit, provided);
        return limit;
    } else {
        return provided;
    }
}

UCS_CLASS_INIT_FUNC(sct_rc_iface_t, sct_rc_iface_ops_t *ops, sct_md_h md,
                    sct_worker_h worker, const sct_iface_params_t *params,
                    const sct_rc_iface_common_config_t *config,
                    sct_ib_iface_init_attr_t *init_attr)
{
    ucs_status_t status;

    UCS_CLASS_CALL_SUPER_INIT(sct_ib_iface_t, &ops->super, md, worker, params,
                              &config->super, init_attr);

    self->tx.cq_available       = init_attr->cq_len[UCT_IB_DIR_TX] - 1;
    self->config.tx_qp_len      = config->super.tx.queue_len;
    self->config.tx_min_inline  = config->super.tx.min_inline;
    self->config.tx_ops_count   = init_attr->cq_len[UCT_IB_DIR_TX];
    self->config.min_rnr_timer  = sct_ib_to_rnr_fabric_time(config->tx.rnr_timeout);
    self->config.timeout        = sct_ib_to_qp_fabric_time(config->tx.timeout);
    self->config.rnr_retry      = sct_rc_iface_config_limit_value("RNR_RETRY_COUNT",
                                                                  config->tx.rnr_retry_count,
                                                                  SCT_RC_QP_MAX_RETRY_COUNT);
    self->config.retry_cnt      = sct_rc_iface_config_limit_value("RETRY_COUNT",
                                                                  config->tx.retry_count,
                                                                  SCT_RC_QP_MAX_RETRY_COUNT);

    status = ucs_spinlock_init(&self->eps_lock, 0);
    if (status != UCS_OK) {
        goto err;
    }

    memset(self->eps, 0, sizeof(self->eps));
    ucs_list_head_init(&self->ep_list);
    ucs_list_head_init(&self->ep_gc_list);

    return UCS_OK;
err:
    return status;
}

void sct_rc_iface_cleanup_eps(sct_rc_iface_t *iface)
{
    sct_rc_iface_ops_t *ops = ucs_derived_of(iface->super.ops, sct_rc_iface_ops_t);
    sct_rc_ep_cleanup_ctx_t *cleanup_ctx, *tmp;

    ucs_list_for_each_safe(cleanup_ctx, tmp, &iface->ep_gc_list, list) {
        ops->cleanup_qp(&cleanup_ctx->super);
    }

    ucg_assert(ucs_list_is_empty(&iface->ep_gc_list));
}

static UCS_CLASS_CLEANUP_FUNC(sct_rc_iface_t)
{
    for (unsigned i = 0; i < SCT_RC_QP_TABLE_SIZE; ++i) {
        ucg_free(self->eps[i]);
    }

    if (!ucs_list_is_empty(&self->ep_list)) {
        ucg_warn("some eps were not destroyed");
    }

    ucs_spinlock_destroy(&self->eps_lock);
}

UCS_CLASS_DEFINE(sct_rc_iface_t, sct_ib_iface_t);

ucs_status_t sct_rc_iface_qp_init(sct_rc_iface_t *iface, struct ibv_qp *qp)
{
    struct ibv_qp_attr qp_attr;
    int ret;

    memset(&qp_attr, 0, sizeof(qp_attr));

    qp_attr.qp_state              = IBV_QPS_INIT;
    qp_attr.pkey_index            = iface->super.pkey_index;
    qp_attr.port_num              = iface->super.config.port_num;
    qp_attr.qp_access_flags       = IBV_ACCESS_LOCAL_WRITE  |
                                    IBV_ACCESS_REMOTE_WRITE;
    ret = ibv_modify_qp(qp, &qp_attr,
                        IBV_QP_STATE      |
                        IBV_QP_PKEY_INDEX |
                        IBV_QP_PORT       |
                        IBV_QP_ACCESS_FLAGS);
    if (ucg_unlikely(ret)) {
        ucg_error("error modifying QP to INIT: %m");
        return UCS_ERR_IO_ERROR;
    }

    return UCS_OK;
}

ucs_status_t sct_rc_iface_qp_connect(sct_rc_iface_t *iface, struct ibv_qp *qp,
                                     const uint32_t dest_qp_num,
                                     struct ibv_ah_attr *ah_attr,
                                     enum ibv_mtu path_mtu)
{
    struct ibv_qp_attr qp_attr;
    long qp_attr_mask;
    int ret;

    ucg_assert(path_mtu != 0);

    memset(&qp_attr, 0, sizeof(qp_attr));

    qp_attr.qp_state              = IBV_QPS_RTR;
    qp_attr.dest_qp_num           = dest_qp_num;
    qp_attr.rq_psn                = 0;
    qp_attr.path_mtu              = path_mtu;
    qp_attr.min_rnr_timer         = iface->config.min_rnr_timer;
    qp_attr.ah_attr               = *ah_attr;
    qp_attr_mask                  = IBV_QP_STATE              |
                                    IBV_QP_AV                 |
                                    IBV_QP_PATH_MTU           |
                                    IBV_QP_DEST_QPN           |
                                    IBV_QP_RQ_PSN             |
                                    IBV_QP_MAX_DEST_RD_ATOMIC |
                                    IBV_QP_MIN_RNR_TIMER;

    ret = ibv_modify_qp(qp, &qp_attr, qp_attr_mask);
    if (ret) {
        ucg_error("error modifying QP to RTR: %m");
        return UCS_ERR_IO_ERROR;
    }

    qp_attr.qp_state              = IBV_QPS_RTS;
    qp_attr.sq_psn                = 0;
    qp_attr.timeout               = iface->config.timeout;
    qp_attr.rnr_retry             = iface->config.rnr_retry;
    qp_attr.retry_cnt             = iface->config.retry_cnt;
    qp_attr_mask                  = IBV_QP_STATE              |
                                    IBV_QP_TIMEOUT            |
                                    IBV_QP_RETRY_CNT          |
                                    IBV_QP_RNR_RETRY          |
                                    IBV_QP_SQ_PSN             |
                                    IBV_QP_MAX_QP_RD_ATOMIC;

    ret = ibv_modify_qp(qp, &qp_attr, qp_attr_mask);
    if (ret) {
        ucg_error("error modifying QP to RTS: %m");
        return UCS_ERR_IO_ERROR;
    }
    ah_attr->sl = qp_attr.ah_attr.sl;
    ucg_debug("connected rc qp 0x%x on "SCT_IB_IFACE_FMT" to lid %d(+%d) sl %d "
              "remote_qp 0x%x mtu %zu timer %dx%d rnr %dx%d",
              qp->qp_num, SCT_IB_IFACE_ARG(&iface->super), ah_attr->dlid,
              ah_attr->src_path_bits, ah_attr->sl, qp_attr.dest_qp_num,
              sct_ib_mtu_value(qp_attr.path_mtu), qp_attr.timeout,
              qp_attr.retry_cnt, qp_attr.min_rnr_timer, qp_attr.rnr_retry);

    return UCS_OK;
}
