/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "scatterv.h"
#include "planc_hccl_dt.h"

#include "util/ucg_helper.h"

/* Function encapsulated to ensure that trigger modes are consistent. */
static HcclResult Hccl_scatterv_linear(const void *sendbuf, const int32_t *sendcounts,
                                       const int32_t *displs, HcclDataType sendtype,
                                       void *recvbuf, int32_t recvcount,
                                       HcclDataType recvtype, int32_t root,
                                       ucg_planc_hccl_group_t *hccl_group)
{
    if (UCG_PLANC_HCCL_GROUP_MYRANK(hccl_group) != root) {
        if (recvcount == 0) {
            return HCCL_SUCCESS;
        }
        return HcclRecv(recvbuf, recvcount, recvtype, root,
                        hccl_group->comm, hccl_group->stream);
    }

    HcclResult rc;
    uint32_t group_size = UCG_PLANC_HCCL_GROUP_SIZE(hccl_group);
    int sendtype_size = ucg_planc_hccl_dt_size(sendtype);
    int recvtype_size = ucg_planc_hccl_dt_size(recvtype);
    for (int i = 0; i < group_size; ++i) {
        void *sbuf = (char*)sendbuf + displs[i] * sendtype_size;
        int32_t sendcount = sendcounts[i];
        if (sendcount == 0) {
            continue;
        }
        if (i == root) {
            if (recvbuf != UCG_IN_PLACE) {
                int dst_size = recvcount * recvtype_size;
                int src_size = sendcount * sendtype_size;
                rc = aclrtMemcpyAsync(recvbuf, dst_size, sbuf, src_size,
                                      ACL_MEMCPY_DEVICE_TO_DEVICE,
                                      hccl_group->stream);
                if (rc != HCCL_SUCCESS) {
                    return rc;
                }
            }
        } else {
            rc = HcclSend(sbuf, sendcount, sendtype, i,
                          hccl_group->comm, hccl_group->stream);
            if (rc != HCCL_SUCCESS) {
                return rc;
            }
        }
    }
    return HCCL_SUCCESS;
}

static int32_t ucg_planc_hccl_scatterv_check_comm(const ucg_coll_scatterv_args_t *coll_args,
                                                  ucg_planc_hccl_group_t *hccl_group)
{
    int32_t root = coll_args->root;
    const int32_t *sendcounts = coll_args->sendcounts;
    int32_t myrank = UCG_PLANC_HCCL_GROUP_MYRANK(hccl_group);
    int32_t group_size = UCG_PLANC_HCCL_GROUP_SIZE(hccl_group);

    if (myrank == root) {
        for (int32_t i = 0; i < group_size; i++) {
            if ((i != root) && (sendcounts[i] > 0)) {
                return 1;
            }
        }
    } else {
        if (coll_args->recvcount > 0) {
            return 1;
        }
    }

    return 0;
}

static ucg_status_t ucg_planc_hccl_scatterv_linear_op_trigger(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status = UCG_OK;
    const ucg_coll_scatterv_args_t *coll_args = &ucg_op->super.args.scatterv;
    ucg_planc_hccl_op_t *op = ucg_derived_of(ucg_op, ucg_planc_hccl_op_t);
    ucg_planc_hccl_group_t *hccl_group = op->hccl_group;

    /* When count is equal to 0, the synchronization overhead of scatterv is optimized. */
    if (ucg_planc_hccl_scatterv_check_comm(coll_args, hccl_group)) {
        UCG_PLANC_HCCL_OP_TRIGGER(
            Hccl_scatterv_linear(coll_args->sendbuf, coll_args->sendcounts,
                                 coll_args->displs,
                                 ucg_planc_hccl_dt_u2h(coll_args->sendtype),
                                 coll_args->recvbuf, coll_args->recvcount,
                                 ucg_planc_hccl_dt_u2h(coll_args->recvtype),
                                 coll_args->root,
                                 hccl_group),
            op, status);
    } else {
        op->super.super.status = UCG_OK;
        status = UCG_OK;
    }

    return status;
}

ucg_status_t ucg_planc_hccl_scatterv_linear_prepare(ucg_vgroup_t *vgroup,
                                                    const ucg_coll_args_t *args,
                                                    ucg_plan_op_t **op)
{
    UCG_CHECK_NULL_INVALID(vgroup, args, op);

    const ucg_coll_scatterv_args_t *coll_args = &args->scatterv;
    if (!ucg_planc_hccl_dt_is_supported(coll_args->sendtype) ||
        !ucg_planc_hccl_dt_is_supported(coll_args->recvtype)) {
        return UCG_ERR_UNSUPPORTED;
    }

    ucg_planc_hccl_group_t *hccl_group = ucg_derived_of(vgroup, ucg_planc_hccl_group_t);
    ucg_planc_hccl_op_t *hccl_op = ucg_mpool_get(&hccl_group->context->op_mp);
    if (hccl_op == NULL) {
        return UCG_ERR_NO_MEMORY;
    }

    ucg_status_t status;
    status = UCG_CLASS_CONSTRUCT(ucg_plan_op_t, &hccl_op->super, vgroup,
                                 ucg_planc_hccl_scatterv_linear_op_trigger,
                                 ucg_planc_hccl_op_progress,
                                 ucg_planc_hccl_op_discard,
                                 args);
    if (status != UCG_OK) {
        ucg_error("Failed to initialize super of hccl op");
        ucg_mpool_put(hccl_op);
        return status;
    }

    hccl_op->hccl_group = hccl_group;

    *op = &hccl_op->super;
    return UCG_OK;
}