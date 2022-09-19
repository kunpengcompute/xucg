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
    UCG_UCX_ALLGATHERV_RING_HPL_SEND = UCG_BIT(0),
    UCG_UCX_ALLGATHERV_RING_HPL_RECV = UCG_BIT(1),
};

#define UCG_ALLGATHERV_RING_HPL_FLAGS UCG_UCX_ALLGATHERV_RING_HPL_SEND | UCG_UCX_ALLGATHERV_RING_HPL_RECV

/**
 * Ring-HPL algorithm for allgatherv with p - 1 steps:
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
 *          [1]     [1]     [ ]     [ ]
 *          [ ]     [ ]     [2]     [2]
 *          [ ]     [ ]     [3]     [3]
 *  Step 1:
 *      #    0       1       2       3
 *          [0]     [0]     [ ]     [0]
 *          [1]     [1]     [1]     [ ]
 *          [ ]     [2]     [2]     [2]
 *          [3]     [ ]     [3]     [3]
 *  Step 2:
 *      #    0       1       2       3
 *          [0]     [0]     [0]     [0]
 *          [1]     [1]     [1]     [1]
 *          [2]     [2]     [2]     [2]
 *          [3]     [3]     [3]     [3]
 *
 */
static ucg_status_t ucg_planc_ucx_allgatherv_ring_hpl_op_progress(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_coll_allgatherv_args_t *args = &ucg_op->super.args.allgatherv;
    void *recvbuf = args->recvbuf;
    int32_t scount, rcount;
    ucg_dt_t *recvtype = args->recvtype;
    uint32_t rtype_size = ucg_dt_size(recvtype);
    ucg_rank_t peer;
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_rank_t my_rank = vgroup->myrank;
    uint32_t group_size = vgroup->size;
    uint32_t block_idx;
    int64_t senddispls, recvdispls;
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(op, &params);
    ucg_algo_ring_iter_t *iter = &op->allgatherv.ring_iter;

    while (!ucg_algo_ring_iter_end(iter)) {
        int step_idx = ucg_algo_ring_iter_idx(iter);
        peer = ((my_rank + step_idx) & 1) != 0 ? iter->left : iter->right;

        if (ucg_test_and_clear_flags(&op->flags, UCG_UCX_ALLGATHERV_RING_HPL_SEND)) {
            /* set blocks for sender */
            if (peer == iter->right) {
                block_idx = (my_rank - step_idx / 2 + group_size) % group_size;
            } else {
                block_idx = (my_rank + step_idx / 2 + group_size) % group_size;
            }
            senddispls = (int64_t)args->displs[block_idx] * rtype_size;
            scount = args->recvcounts[block_idx];
            status = ucg_planc_ucx_p2p_isend(recvbuf + senddispls, scount, args->sendtype,
                                             peer, op->tag, vgroup, &params);
            UCG_CHECK_GOTO(status, out);
        }

        if (ucg_test_and_clear_flags(&op->flags, UCG_UCX_ALLGATHERV_RING_HPL_RECV)) {
            /* set blocks for receiver */
            if (peer == iter->right) {
                block_idx = (peer + step_idx / 2 + group_size) % group_size;
            } else {
                block_idx = (peer - step_idx / 2 + group_size) % group_size;
            }
            recvdispls = (int64_t)args->displs[block_idx] * rtype_size;
            rcount = args->recvcounts[block_idx];
            status = ucg_planc_ucx_p2p_irecv(recvbuf + recvdispls, rcount, args->recvtype,
                                             peer, op->tag, vgroup, &params);
            UCG_CHECK_GOTO(status, out);
        }

        status = ucg_planc_ucx_p2p_testall(op->ucx_group, params.state);
        UCG_CHECK_GOTO(status, out);
        ucg_algo_ring_iter_inc(iter);
        op->flags |= UCG_ALLGATHERV_RING_HPL_FLAGS;
    }

out:
    op->super.super.status = status;
    return op->super.super.status;
}

static inline ucg_status_t ucg_planc_ucx_allgatherv_ring_hpl_init(ucg_plan_op_t *ucg_op)
{
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_coll_allgatherv_args_t *args = &ucg_op->super.args.allgatherv;
    void *recvbuf = args->recvbuf;
    const void *sendbuf = args->sendbuf;
    ucg_dt_t *sendtype = args->sendtype;
    ucg_dt_t *recvtype = args->recvtype;
    uint32_t rtype_size = ucg_dt_size(recvtype);
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_rank_t my_rank = vgroup->myrank;
    op->flags |=  UCG_ALLGATHERV_RING_HPL_FLAGS;

    if (UCG_IN_PLACE != sendbuf) {
        UCG_PLANC_UCX_CHECK_GOTO(ucg_dt_memcpy((char *)recvbuf + args->displs[my_rank] * rtype_size,
                                                args->recvcounts[my_rank], recvtype,
                                                sendbuf, args->sendcount, sendtype),
                                 op,
                                 out);
    }

out:
    return UCG_OK;
}

static ucg_status_t ucg_planc_ucx_allgatherv_ring_hpl_op_trigger(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status;

    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_planc_ucx_op_reset(op);
    ucg_algo_ring_iter_reset(&op->allgatherv.ring_iter);

    status = ucg_planc_ucx_allgatherv_ring_hpl_init(ucg_op);

    status = ucg_planc_ucx_allgatherv_ring_hpl_op_progress(ucg_op);
    if (status == UCG_INPROGRESS) {
        /* op is progressing and request start successfully */
        status = UCG_OK;
    }

    return status;
}

ucg_status_t ucg_planc_ucx_allgatherv_ring_hpl_prepare(ucg_vgroup_t *vgroup,
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
                                 ucg_planc_ucx_allgatherv_ring_hpl_op_trigger,
                                 ucg_planc_ucx_allgatherv_ring_hpl_op_progress,
                                 ucg_planc_ucx_op_discard,
                                 args);
    if (status != UCG_OK) {
        ucg_error("Failed to initialize super of ucx op");
        goto err_free_op;
    }

    ucg_planc_ucx_op_init(ucx_op, ucx_group);
    ucg_algo_ring_iter_init(&ucx_op->allgatherv.ring_iter, vgroup->size, vgroup->myrank);

    *op = &ucx_op->super;

    return UCG_OK;

err_free_op:
    ucg_mpool_put(ucx_op);
    return status;
}