/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "bcast.h"
#include "planc_hccl_dt.h"

#include "util/ucg_helper.h"

/**
 * Increasing-ring (modified) algorithm for broadcast with p - 1 steps:
 *
 * Example on 4 processes:
 *  Step 0: Initial state
 *    #     0      1      2      3
 *         [x] -> [ ]    [ ]    [ ]
 *   Step 1:
 *    #     0      1      2      3
 *         [x]    [x]    [ ]    [ ]
 *          -------------->
 *   Step 2:
 *    #     0      1      2      3
 *         [x]    [x]    [x] -> [ ]
 *   Final state:
 *    #     0      1      2      3
 *         [x]    [x]    [x]    [x]
 *
 */

/* Function encapsulated to ensure that trigger modes are consistent. */
static HcclResult hccl_bcast_ring_modified_kernel(void *sendbuf, int32_t count,
                                                  HcclDataType datatype, const int32_t root,
                                                  ucg_planc_hccl_group_t *hccl_group)
{
    uint32_t group_size = UCG_PLANC_HCCL_GROUP_SIZE(hccl_group);
    ucg_rank_t my_rank  = UCG_PLANC_HCCL_GROUP_MYRANK(hccl_group);
    ucg_rank_t peer, left_peer, right_peer, left_left_peer, right_right_peer;
    HcclResult rc;

    left_left_peer   = (my_rank - 2 + group_size) % group_size;
    left_peer        = (my_rank - 1 + group_size) % group_size;
    right_peer       = (my_rank + 1) % group_size;
    right_right_peer = (my_rank + 2) % group_size;

    if (my_rank == root) {
        rc = HcclSend(sendbuf, count, datatype, right_peer, hccl_group->comm, hccl_group->stream);
        if (rc != HCCL_SUCCESS) {
            return rc;
        }
        if (group_size > 2) {
            rc = HcclSend(sendbuf, count, datatype, right_right_peer, hccl_group->comm, hccl_group->stream);
            if (rc != HCCL_SUCCESS) {
                return rc;
            }
        }
    } else {
        peer =  (group_size > 2 && left_left_peer == root) ? root : left_peer;
        rc = HcclRecv(sendbuf, count, datatype, peer, hccl_group->comm, hccl_group->stream);
        if (rc != HCCL_SUCCESS) {
            return rc;
        }
        if (left_peer != root && right_peer != root) {
            rc = HcclSend(sendbuf, count, datatype, right_peer, hccl_group->comm, hccl_group->stream);
            if (rc != HCCL_SUCCESS) {
                return rc;
            }            
        }
    }

    return HCCL_SUCCESS;
}

static ucg_status_t ucg_planc_hccl_bcast_ring_modified_op_trigger(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status = UCG_OK;
    ucg_coll_bcast_args_t *coll_args = &ucg_op->super.args.bcast;
    ucg_planc_hccl_op_t *op = ucg_derived_of(ucg_op, ucg_planc_hccl_op_t);
    ucg_planc_hccl_group_t *hccl_group = op->hccl_group;

    UCG_PLANC_HCCL_OP_TRIGGER(hccl_bcast_ring_modified_kernel(coll_args->buffer, coll_args->count,
                                                              ucg_planc_hccl_dt_u2h(coll_args->dt),
                                                              coll_args->root,
                                                              hccl_group),
                              op, status);
    return status;
}

ucg_status_t ucg_planc_hccl_bcast_ring_modified_prepare(ucg_vgroup_t *vgroup,
                                                        const ucg_coll_args_t *args,
                                                        ucg_plan_op_t **op)
{
    UCG_CHECK_NULL_INVALID(vgroup, args, op);

    const ucg_coll_bcast_args_t *coll_args = &args->bcast;
    if (!ucg_planc_hccl_dt_is_supported(coll_args->dt)) {
        return UCG_ERR_UNSUPPORTED;
    }

    ucg_planc_hccl_group_t *hccl_group = ucg_derived_of(vgroup, ucg_planc_hccl_group_t);
    ucg_planc_hccl_op_t *hccl_op = ucg_mpool_get(&hccl_group->context->op_mp);
    if (hccl_op == NULL) {
        return UCG_ERR_NO_MEMORY;
    }

    ucg_status_t status;
    status = UCG_CLASS_CONSTRUCT(ucg_plan_op_t, &hccl_op->super, vgroup,
                                 ucg_planc_hccl_bcast_ring_modified_op_trigger,
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