/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#ifndef SCT_IB_OFFLOAD_MD_H_
#define SCT_IB_OFFLOAD_MD_H_

#include <infiniband/hnsdv.h>

#include "ib_md.h"


#define SCT_TX_CQ_MOD               4

typedef struct sct_rc_ofd_md {
    sct_ib_md_t             super;
    ucg_mpool_t             rdma_params_pool;
    ucg_mpool_t             event_params_pool;
    ucg_mpool_t             notify_params_pool;
    struct {
        uint8_t pool_id;
    } dev_attr;
} sct_rc_ofd_md_t;

UCS_F_ALWAYS_INLINE rdma_trans_parm_t* sct_rc_ofd_md_get_rdma_elem(sct_md_h sct_md)
{
    sct_rc_ofd_md_t *md = ucs_derived_of(sct_md, sct_rc_ofd_md_t);
    return ucg_mpool_get(&md->rdma_params_pool);
}

UCS_F_ALWAYS_INLINE void sct_rc_ofd_md_put_param(void *parm)
{
    ucg_mpool_put(parm);
}

UCS_F_ALWAYS_INLINE event_trans_parm_t* sct_rc_ofd_md_get_event_param(sct_md_h sct_md)
{
    sct_rc_ofd_md_t *md = ucs_derived_of(sct_md, sct_rc_ofd_md_t);
    return ucg_mpool_get(&md->event_params_pool);
}

UCS_F_ALWAYS_INLINE write_notify_trans_param_t* sct_rc_ofd_md_get_notify_param(sct_md_h sct_md)
{
    sct_rc_ofd_md_t *md = ucs_derived_of(sct_md, sct_rc_ofd_md_t);
    return ucg_mpool_get(&md->notify_params_pool);
}

#endif
