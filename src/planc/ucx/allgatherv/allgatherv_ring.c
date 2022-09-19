/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "allgatherv.h"
#include "planc_ucx_plan.h"
#include "planc_ucx_p2p.h"
#include "core/ucg_dt.h"
#include "core/ucg_group.h"
#include "util/ucg_log.h"
#include "util/ucg_malloc.h"

enum {
    UCG_ALLGATHERV_RING_SEND = UCG_BIT(0),
    UCG_ALLGATHERV_RING_RECV = UCG_BIT(1),
};

#define UCG_ALLGATHERV_RING_FLAGS UCG_ALLGATHERV_RING_SEND | UCG_ALLGATHERV_RING_RECV

/**
 * Ring algorithm for allgatherv with p - 1 steps:
 *  0 -> 1 -> 2 -> 3
 *
 * Example on 4 processes:
 *  Initial state
 *      #    0       1       2       3
 *          [0]     [ ]     [ ]     [ ]
 *          [ ]     [1]     [ ]     [ ]
 *          [ ]     [ ]     [2]     [ ]
 *          [ ]     [ ]     [ ]     [3]
 *  Step 0:
 *      #    0       1       2       3
 *          [0]     [0]     [ ]     [ ]
 *          [ ]     [1]     [1]     [ ]
 *          [ ]     [ ]     [2]     [2]
 *          [3]     [ ]     [ ]     [3]
 *  Step 1:
 *      #    0       1       2       3
 *          [0]     [0]     [0]     [ ]
 *          [ ]     [1]     [1]     [1]
 *          [2]     [ ]     [2]     [2]
 *          [3]     [3]     [ ]     [3]
 *  Step 2:
 *      #    0       1       2       3
 *          [0]     [0]     [0]     [0]
 *          [1]     [1]     [1]     [1]
 *          [2]     [2]     [2]     [2]
 *          [3]     [3]     [3]     [3]
 *
 */
static ucg_status_t ucg_planc_ucx_allgatherv_ring_op_progress(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_rank_t myrank = op->super.vgroup->myrank;
    uint32_t group_size = vgroup->size;
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(op, &params);
    ucg_algo_ring_iter_t *iter = &op->allgatherv.ring_iter;
    ucg_rank_t left_peer = ucg_algo_ring_iter_left_value(iter);
    ucg_rank_t right_peer = ucg_algo_ring_iter_right_value(iter);
    ucg_coll_allgatherv_args_t *args = &op->super.super.args.allgatherv;
    uint32_t recvtype_extent = ucg_dt_extent(args->recvtype);

    while (!ucg_algo_ring_iter_end(iter)) {
        int step_idx = ucg_algo_ring_iter_idx(iter);
        if (ucg_test_and_clear_flags(&op->flags, UCG_ALLGATHERV_RING_RECV)) {
            int block_idx = (myrank - step_idx - 1 + group_size) % group_size;
            void *recvbuf = args->recvbuf + (int64_t)args->displs[block_idx] * recvtype_extent;
            status = ucg_planc_ucx_p2p_irecv(recvbuf, args->recvcounts[block_idx],
                                             args->recvtype, left_peer, op->tag,
                                             vgroup, &params);
            UCG_CHECK_GOTO(status, out);
        }
        if (ucg_test_and_clear_flags(&op->flags, UCG_ALLGATHERV_RING_SEND)) {
            int block_idx = (myrank - step_idx + group_size) % group_size;
            void *sendbuf = args->recvbuf + (int64_t)args->displs[block_idx] * recvtype_extent;
            status = ucg_planc_ucx_p2p_isend(sendbuf, args->recvcounts[block_idx],
                                             args->recvtype, right_peer, op->tag,
                                             vgroup, &params);
            UCG_CHECK_GOTO(status, out);
        }

        status = ucg_planc_ucx_p2p_testall(op->ucx_group, params.state);
        UCG_CHECK_GOTO(status, out);
        ucg_algo_ring_iter_inc(iter);
        op->flags |= UCG_ALLGATHERV_RING_FLAGS;
    }

out:
    op->super.super.status = status;
    return status;
}

static ucg_status_t ucg_planc_ucx_allgatherv_ring_op_trigger(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status;
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_planc_ucx_op_reset(op);
    op->flags = UCG_ALLGATHERV_RING_FLAGS;
    ucg_algo_ring_iter_reset(&op->allgatherv.ring_iter);

    ucg_coll_allgatherv_args_t *args = &op->super.super.args.allgatherv;
    const void *sendbuf = args->sendbuf;
    if (sendbuf != UCG_IN_PLACE) {
        ucg_rank_t myrank = op->super.vgroup->myrank;
        void *recvbuf = args->recvbuf;
        ucg_dt_t *recvtype = args->recvtype;
        ucg_dt_t *sendtype = args->sendtype;
        uint32_t recvtype_extent = ucg_dt_extent(recvtype);
        status = ucg_dt_memcpy((char *)recvbuf + args->displs[myrank] * recvtype_extent,
                               args->recvcounts[myrank], recvtype,
                               sendbuf, args->sendcount, sendtype);
        UCG_CHECK_GOTO(status, out);
    }

    status = ucg_planc_ucx_allgatherv_ring_op_progress(ucg_op);
out:
    return status == UCG_INPROGRESS ? UCG_OK : status;
}

static inline
ucg_planc_ucx_op_t *ucg_planc_ucx_allgatherv_ring_op_new(ucg_planc_ucx_group_t *ucx_group,
                                                         ucg_vgroup_t *vgroup,
                                                         const ucg_coll_args_t *args)
{
    ucg_status_t status;
    ucg_planc_ucx_op_t *ucx_op = ucg_mpool_get(&ucx_group->context->op_mp);
    if (ucx_op == NULL) {
        goto err;
    }

    status = UCG_CLASS_CONSTRUCT(ucg_plan_op_t, &ucx_op->super, vgroup,
                                 ucg_planc_ucx_allgatherv_ring_op_trigger,
                                 ucg_planc_ucx_allgatherv_ring_op_progress,
                                 ucg_planc_ucx_op_discard,
                                 args);
    if (status != UCG_OK) {
        ucg_error("Failed to initialize super of ucx op");
        goto err_free_op;
    }
    ucg_planc_ucx_op_init(ucx_op, ucx_group);
    ucg_algo_ring_iter_init(&ucx_op->allgatherv.ring_iter, vgroup->size, vgroup->myrank);
    return ucx_op;

err_free_op:
    ucg_mpool_put(ucx_op);
err:
    return NULL;
}

ucg_status_t ucg_planc_ucx_allgatherv_ring_prepare(ucg_vgroup_t *vgroup,
                                                   const ucg_coll_args_t *args,
                                                   ucg_plan_op_t **op)
{
    UCG_CHECK_NULL_INVALID(vgroup, args, op);

    ucg_planc_ucx_group_t *ucx_group = ucg_derived_of(vgroup, ucg_planc_ucx_group_t);
    ucg_planc_ucx_op_t *ucx_op = ucg_planc_ucx_allgatherv_ring_op_new(ucx_group, vgroup, args);
    if (ucx_op == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    *op = &ucx_op->super;

    return UCG_OK;
}