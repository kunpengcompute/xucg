/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "allgatherv.h"
#include "planc_hccl_dt.h"

#include "util/ucg_helper.h"

/**
 * Ring-HPL algorithm for allgatherv with p - 1 steps:
 *
 * Example on 4 processes:
 *  Step 0: Initial state
 *    #     0      1      2      3
 *         [0] -> [ ]    [ ]    [ ]
 *         [ ] <- [1]    [ ]    [ ]
 *         [ ]    [ ]    [2] -> [ ]
 *         [ ]    [ ]    [ ] <- [3]
 *   Step 1:
 *    #     0      1      2      3
 *      <- [0]    [0]    [ ]    [ ]
 *         [1]    [1] -> [ ]    [ ]
 *         [ ]    [ ] <- [2]    [2]
 *         [ ]    [ ]    [3]    [3] ->
 *   Step 2:
 *    #     0      1      2      3
 *         [0]    [0]    [ ] <- [0]
 *         [1]    [1]    [1] -> [ ]
 *         [ ] <- [2]    [2]    [2]
 *         [3] -> [ ]    [3]    [3]
 *   Final state:
 *    #     0      1      2      3
 *         [0]    [0]    [0]    [0]
 *         [1]    [1]    [1]    [1]
 *         [2]    [2]    [2]    [2]
 *         [3]    [3]    [3]    [3]
 *
 */

/* Function encapsulated to ensure that trigger modes are consistent. */
static HcclResult hccl_allgatherv_ring_hpl_kernel(const void *sendbuf, int32_t sendcount,
                                                  HcclDataType sendtype, void *recvbuf,
                                                  const int *recvcounts, const int32_t *displs,
                                                  HcclDataType recvtype,
                                                  ucg_planc_hccl_group_t *hccl_group)
{
    uint32_t group_size = UCG_PLANC_HCCL_GROUP_SIZE(hccl_group);
    ucg_rank_t my_rank  = UCG_PLANC_HCCL_GROUP_MYRANK(hccl_group);
    uint32_t step_idx, block_idx;
    int64_t sdispls, rdispls;
    int scount, rcount;
    ucg_rank_t peer, left_peer, right_peer;
    unsigned is_left;
    int stype_size = ucg_planc_hccl_dt_size(sendtype);
    int rtype_size = ucg_planc_hccl_dt_size(recvtype);
    HcclResult rc;

    if (sendbuf != UCG_IN_PLACE) {
        int dst_size = recvcounts[my_rank] * rtype_size;
        int src_size = sendcount * stype_size;
        rc = aclrtMemcpyAsync(recvbuf + displs[my_rank] * rtype_size, dst_size, sendbuf, src_size,
                              ACL_MEMCPY_DEVICE_TO_DEVICE, hccl_group->stream);
        if (rc != HCCL_SUCCESS) {
            return rc;
        }
    }

    left_peer  = (my_rank - 1 + group_size) % group_size;
    right_peer = (my_rank + 1) % group_size;

    // TODO : treat different send datatype / recv datatype case 
    for (step_idx = 0; step_idx < group_size - 1; step_idx++) {
        peer = ((my_rank + step_idx) & 1) != 0 ? left_peer : right_peer;
        if (group_size > 2) {
            is_left = peer == right_peer ? 1 : 0;
        } else {
            is_left = my_rank == 0 ? 1 : 0;
        }

        /* set blocks for sender */
        if (peer == right_peer) {
            block_idx   = (my_rank - step_idx / 2 + group_size) % group_size;
        } else {
            block_idx   = (my_rank + step_idx / 2 + group_size) % group_size;
        }
        sdispls = displs[block_idx] * rtype_size;
        scount  = recvcounts[block_idx];

        /* set blocks for receiver */
        if (peer == right_peer) {
            block_idx   = (peer + step_idx / 2 + group_size) % group_size;
        } else {
            block_idx   = (peer - step_idx / 2 + group_size) % group_size;
        }
        rdispls = displs[block_idx] * rtype_size;
        rcount  = recvcounts[block_idx];

        if (is_left) {
            rc = HcclSend(recvbuf + sdispls, scount, sendtype, peer, hccl_group->comm, hccl_group->stream);
            if (rc != HCCL_SUCCESS) {
                return rc;
            }
            rc = HcclRecv(recvbuf + rdispls, rcount, recvtype, peer, hccl_group->comm, hccl_group->stream);
            if (rc != HCCL_SUCCESS) {
                return rc;
            }
        } else {
            rc = HcclRecv(recvbuf + rdispls, rcount, recvtype, peer, hccl_group->comm, hccl_group->stream);
            if (rc != HCCL_SUCCESS) {
                return rc;
            }
            rc = HcclSend(recvbuf + sdispls, scount, sendtype, peer, hccl_group->comm, hccl_group->stream);
            if (rc != HCCL_SUCCESS) {
                return rc;
            }
        }
    }

        return HCCL_SUCCESS;
}

static ucg_status_t ucg_planc_hccl_allgatherv_ring_hpl_op_trigger(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status = UCG_OK;
    const ucg_coll_allgatherv_args_t *coll_args = &ucg_op->super.args.allgatherv;
    ucg_planc_hccl_op_t *op = ucg_derived_of(ucg_op, ucg_planc_hccl_op_t);
    ucg_planc_hccl_group_t *hccl_group = op->hccl_group;

    UCG_PLANC_HCCL_OP_TRIGGER(
        hccl_allgatherv_ring_hpl_kernel(coll_args->sendbuf, coll_args->sendcount,
                                        ucg_planc_hccl_dt_u2h(coll_args->sendtype),
                                        coll_args->recvbuf, coll_args->recvcounts,
                                        coll_args->displs,
                                        ucg_planc_hccl_dt_u2h(coll_args->recvtype),
                                        hccl_group),
        op, status);
    return status;
}

ucg_status_t ucg_planc_hccl_allgatherv_ring_hpl_prepare(ucg_vgroup_t *vgroup,
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
                                 ucg_planc_hccl_allgatherv_ring_hpl_op_trigger,
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