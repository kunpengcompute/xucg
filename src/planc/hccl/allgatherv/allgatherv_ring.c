/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "allgatherv.h"
#include "planc_hccl_dt.h"

#include "util/ucg_helper.h"


/* Function encapsulated to ensure that trigger modes are consistent. */
static HcclResult HcclAllgatherv(const void *sendbuf, int32_t sendcount,
                                 HcclDataType sendtype, void *recvbuf,
                                 const int *recvcounts, const int32_t *displs,
                                 HcclDataType recvtype,
                                 ucg_planc_hccl_group_t *hccl_group)
{
    uint32_t group_size = UCG_PLANC_HCCL_GROUP_SIZE(hccl_group);
    ucg_rank_t myrank = UCG_PLANC_HCCL_GROUP_MYRANK(hccl_group);

    HcclResult rc;
    int sendtype_size = ucg_planc_hccl_dt_size(sendtype);
    int recvtype_size = ucg_planc_hccl_dt_size(recvtype);
    void *tmprecv = (char*)recvbuf + displs[myrank] * recvtype_size;
    if (sendbuf != UCG_IN_PLACE) {
        int dst_size = recvcounts[myrank] * recvtype_size;
        int src_size = sendcount * sendtype_size;
        rc = aclrtMemcpyAsync(tmprecv, dst_size, sendbuf, src_size,
                              ACL_MEMCPY_DEVICE_TO_DEVICE, hccl_group->stream);
        if (rc != HCCL_SUCCESS) {
            return rc;
        }
    }

    /**
     * When HCCL P2P is used for communication between two processes, one process
     * must do HcclSend() and the corresponding process must do HcclRecv(). Otherwise,
     * the process will be suspended. To avoid this situation, let rank 0 sends and
     * then receives, and other processes receive and then send. In this case,
     * rank 0 sends to rank 1, so that rank 1 can successfully execute sending,
     * and then rank 2 can continue to execute sending. By analogy, all processes
     * can complete sending and receiving.
     */
    void *tmpsend = NULL;
    int sendto = (myrank + 1) % group_size;
    int recvfrom = (myrank - 1 + group_size) % group_size;
    for (int i = 0; i < group_size - 1; ++i) {
        int recvdatafrom = (myrank - i - 1 + group_size) % group_size;
        tmprecv = (char*)recvbuf + displs[recvdatafrom] * recvtype_size;
        int senddatafrom = (myrank - i + group_size) % group_size;
        tmpsend = (char*)recvbuf + displs[senddatafrom] * recvtype_size;
        if (myrank == 0) {
            rc = HcclSend(tmpsend, recvcounts[senddatafrom], recvtype, sendto,
                          hccl_group->comm, hccl_group->stream);
            if (rc != HCCL_SUCCESS) {
                return rc;
            }

            rc = HcclRecv(tmprecv, recvcounts[recvdatafrom], recvtype, recvfrom,
                          hccl_group->comm, hccl_group->stream);
            if (rc != HCCL_SUCCESS) {
                return rc;
            }
        } else {
            rc = HcclRecv(tmprecv, recvcounts[recvdatafrom], recvtype, recvfrom,
                          hccl_group->comm, hccl_group->stream);
            if (rc != HCCL_SUCCESS) {
                return rc;
            }

            rc = HcclSend(tmpsend, recvcounts[senddatafrom], recvtype, sendto,
                          hccl_group->comm, hccl_group->stream);
            if (rc != HCCL_SUCCESS) {
                return rc;
            }
        }
    }

    return HCCL_SUCCESS;
}

static ucg_status_t ucg_planc_hccl_allgatherv_ring_op_trigger(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status = UCG_OK;
    const ucg_coll_allgatherv_args_t *coll_args = &ucg_op->super.args.allgatherv;
    ucg_planc_hccl_op_t *op = ucg_derived_of(ucg_op, ucg_planc_hccl_op_t);
    ucg_planc_hccl_group_t *hccl_group = op->hccl_group;

    UCG_PLANC_HCCL_OP_TRIGGER(
        HcclAllgatherv(coll_args->sendbuf, coll_args->sendcount,
                       ucg_planc_hccl_dt_u2h(coll_args->sendtype),
                       coll_args->recvbuf, coll_args->recvcounts,
                       coll_args->displs,
                       ucg_planc_hccl_dt_u2h(coll_args->recvtype),
                       hccl_group),
        op, status);
    return status;
}

ucg_status_t ucg_planc_hccl_allgatherv_ring_prepare(ucg_vgroup_t *vgroup,
                                                    const ucg_coll_args_t *args,
                                                    ucg_plan_op_t **op)
{
    UCG_CHECK_NULL_INVALID(vgroup, args, op);

    const ucg_coll_allgatherv_args_t *coll_args = &args->allgatherv;
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
                                 ucg_planc_hccl_allgatherv_ring_op_trigger,
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