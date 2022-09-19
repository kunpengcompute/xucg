/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "bcast.h"
#include "planc_ucx_plan.h"
#include "planc_ucx_p2p.h"
#include "core/ucg_dt.h"
#include "core/ucg_group.h"
#include "util/algo/ucg_ring.h"
#include "util/ucg_log.h"
#include "util/ucg_malloc.h"

enum {
    UCG_BCAST_RING_SEND = UCG_BIT(0), /* send to my right */
    UCG_BCAST_RING_RECV = UCG_BIT(1), /* receive from my left */
};

/**
 * Ring algorithm for broadcast: 8
 * Receive from my left, and send to my right. But there are two special
 * - root only sends to its left
 * - the left of root only receives from its left
 * Looks like the following:
 *        z    root -> x -> y
 *        ^                 |
 *        |<----------------|
 */
static ucg_status_t ucg_planc_ucx_bcast_ring_op_progress(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_planc_ucx_group_t *ucx_group = op->ucx_group;
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(op, &params);
    ucg_coll_bcast_args_t *args = &ucg_op->super.args.bcast;
    ucg_algo_ring_iter_t *iter = &op->bcast.ring_iter;
    ucg_rank_t peer;

    if (ucg_test_and_clear_flags(&op->flags, UCG_BCAST_RING_RECV)) {
        peer = ucg_algo_ring_iter_left_value(iter);
        status = ucg_planc_ucx_p2p_irecv(args->buffer, args->count, args->dt,
                                         peer, op->tag, vgroup, &params);
        UCG_CHECK_GOTO(status, out);
    }
    status = ucg_planc_ucx_p2p_testall(ucx_group, params.state);
    UCG_CHECK_GOTO(status, out);

    if (ucg_test_and_clear_flags(&op->flags, UCG_BCAST_RING_SEND)) {
        peer = ucg_algo_ring_iter_right_value(iter);
        status = ucg_planc_ucx_p2p_isend(args->buffer, args->count, args->dt,
                                         peer, op->tag, vgroup, &params);
        UCG_CHECK_GOTO(status, out);
    }
    status = ucg_planc_ucx_p2p_testall(ucx_group, params.state);
out:
    op->super.super.status = status;
    return status;
}

static ucg_status_t ucg_planc_ucx_bcast_ring_op_trigger(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status;
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_planc_ucx_op_reset(op);
    ucg_algo_ring_iter_t *iter = &op->bcast.ring_iter;
    ucg_algo_ring_iter_reset(iter);

    ucg_coll_bcast_args_t *args = &ucg_op->super.args.bcast;
    ucg_rank_t myrank = op->super.vgroup->myrank;
    ucg_rank_t right_peer = ucg_algo_ring_iter_right_value(iter);
    if (myrank == args->root) {
        op->flags = UCG_BCAST_RING_SEND;
    } else if (right_peer == args->root) {
        op->flags = UCG_BCAST_RING_RECV;
    } else {
        op->flags = UCG_BCAST_RING_RECV | UCG_BCAST_RING_SEND;
    }
    status = ucg_planc_ucx_bcast_ring_op_progress(ucg_op);
    return status == UCG_INPROGRESS ? UCG_OK : status;
}

static ucg_status_t ucg_planc_ucx_bcast_ring_op_discard(ucg_plan_op_t *ucg_op)
{
    UCG_CLASS_DESTRUCT(ucg_plan_op_t, ucg_op);
    ucg_mpool_put(ucg_op);
    return UCG_OK;
}

ucg_status_t ucg_planc_ucx_bcast_ring_prepare(ucg_vgroup_t *vgroup,
                                              const ucg_coll_args_t *args,
                                              ucg_plan_op_t **op)
{
    UCG_CHECK_NULL_INVALID(vgroup, args, op);

    ucg_status_t status;
    ucg_planc_ucx_group_t *ucx_group = ucg_derived_of(vgroup, ucg_planc_ucx_group_t);
    ucg_planc_ucx_op_t *ucx_op = ucg_mpool_get(&ucx_group->context->op_mp);
    if (ucx_op == NULL) {
        return UCG_ERR_NO_MEMORY;
    }

    status = UCG_CLASS_CONSTRUCT(ucg_plan_op_t, &ucx_op->super, vgroup,
                                 ucg_planc_ucx_bcast_ring_op_trigger,
                                 ucg_planc_ucx_bcast_ring_op_progress,
                                 ucg_planc_ucx_bcast_ring_op_discard,
                                 args);
    if (status != UCG_OK) {
        ucg_error("Failed to initialize super of ucx op");
        goto err_free_op;
    }
    ucg_planc_ucx_op_init(ucx_op, ucx_group);
    ucg_algo_ring_iter_init(&ucx_op->bcast.ring_iter, vgroup->size, vgroup->myrank);
    *op = &ucx_op->super;
    return UCG_OK;

err_free_op:
    ucg_mpool_put(ucx_op);
    return status;
}