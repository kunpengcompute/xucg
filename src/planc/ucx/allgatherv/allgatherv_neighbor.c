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
    UCG_NEIGHBOR_INIT = UCG_BIT(0),
    UCG_NEIGHBOR_INIT_SEND = UCG_BIT(1),
    UCG_NEIGHBOR_INIT_RECV = UCG_BIT(2),
    UCG_NEIGHBOR_LOOP = UCG_BIT(3),
    UCG_NEIGHBOR_LOOP_SEND = UCG_BIT(4),
    UCG_NEIGHBOR_LOOP_RECV = UCG_BIT(5),
};

#define UCG_NEIGHBOR_FLAGS UCG_NEIGHBOR_INIT | \
                           UCG_NEIGHBOR_INIT_SEND | \
                           UCG_NEIGHBOR_INIT_RECV | \
                           UCG_NEIGHBOR_LOOP | \
                           UCG_NEIGHBOR_LOOP_SEND | \
                           UCG_NEIGHBOR_LOOP_RECV

static inline ucg_status_t ucg_planc_ucx_allgatherv_neighbor_check(ucg_vgroup_t *vgroup)
{
    uint32_t group_size = vgroup->size;
    if (group_size % 2) {
        ucg_info("Allgatherv neighbor don't support odd processes");
        return UCG_ERR_UNSUPPORTED;
    }
    return UCG_OK;
}

static ucg_status_t ucg_planc_ucx_allgatherv_neighbor_op_init(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status;
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_coll_allgatherv_args_t *args = &ucg_op->super.args.allgatherv;
    ucg_rank_t my_rank = vgroup->myrank;
    uint32_t rtype_ext = ucg_dt_extent(args->recvtype);
    int32_t neighbor = op->allgatherv.neighbor.neighbor[0];
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(op, &params);

    if (ucg_test_and_clear_flags(&op->flags, UCG_NEIGHBOR_INIT_RECV)) {
        void *tmprecv = (char *)args->recvbuf + (int64_t)args->displs[neighbor] * rtype_ext;
        int32_t rcount = args->recvcounts[neighbor];
        status = ucg_planc_ucx_p2p_irecv(tmprecv, rcount, args->recvtype,
                                         neighbor, op->tag, vgroup, &params);
        UCG_CHECK_GOTO(status, out);
    }

    if (ucg_test_and_clear_flags(&op->flags, UCG_NEIGHBOR_INIT_SEND)) {
        void *tmpsend = (char *)args->recvbuf + (int64_t)args->displs[my_rank] * rtype_ext;
        int32_t scount = args->recvcounts[my_rank];
        status = ucg_planc_ucx_p2p_isend(tmpsend, scount, args->recvtype,
                                         neighbor, op->tag, vgroup, &params);
        UCG_CHECK_GOTO(status, out);
    }

    status = ucg_planc_ucx_p2p_testall(op->ucx_group, params.state);
    UCG_CHECK_GOTO(status, out);
out:
    return status;
}

static ucg_status_t ucg_planc_ucx_allgatherv_neighbor_op_loop(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status;
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_coll_allgatherv_args_t *args = &ucg_op->super.args.allgatherv;
    uint32_t group_size = vgroup->size;
    uint32_t rtype_ext = ucg_dt_extent(args->recvtype);
    int32_t neighbor[2], offset_at_step[2];
    int32_t *recv_data_from[2], *send_data_from;
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(op, &params);

    neighbor[0] = op->allgatherv.neighbor.neighbor[0];
    neighbor[1] = op->allgatherv.neighbor.neighbor[1];
    offset_at_step[0] = op->allgatherv.neighbor.offset_at_step[0];
    offset_at_step[1] = op->allgatherv.neighbor.offset_at_step[1];
    recv_data_from[0] = &op->allgatherv.neighbor.recv_data_from[0];
    recv_data_from[1] = &op->allgatherv.neighbor.recv_data_from[1];
    send_data_from = &op->allgatherv.neighbor.send_data_from;

    while (op->allgatherv.neighbor.loop_count < op->allgatherv.neighbor.loop_max) {
        const int i_parity = op->allgatherv.neighbor.loop_count % 2;
        if (ucg_test_and_clear_flags(&op->flags, UCG_NEIGHBOR_LOOP_RECV)) {
            *recv_data_from[i_parity] = (*recv_data_from[i_parity] + offset_at_step[i_parity] + group_size) % group_size;
            void *tmprecv = (char *)args->recvbuf + (int64_t)args->displs[*recv_data_from[i_parity]] * rtype_ext;
            int32_t rcount = args->recvcounts[*recv_data_from[i_parity]];
            status = ucg_planc_ucx_p2p_irecv(tmprecv, rcount, args->recvtype,
                                             neighbor[i_parity], op->tag, vgroup, &params);
            UCG_CHECK_GOTO(status, out);
            tmprecv = (char *)args->recvbuf + (int64_t)args->displs[*recv_data_from[i_parity] + 1] * rtype_ext;
            rcount = args->recvcounts[*recv_data_from[i_parity] + 1];
            status = ucg_planc_ucx_p2p_irecv(tmprecv, rcount, args->recvtype,
                                             neighbor[i_parity], op->tag, vgroup, &params);
            UCG_CHECK_GOTO(status, out);
        }

        if (ucg_test_and_clear_flags(&op->flags, UCG_NEIGHBOR_LOOP_SEND)) {
            void *tmpsend = (char *)args->recvbuf + (int64_t)args->displs[*send_data_from] * rtype_ext;
            int32_t scount = args->recvcounts[*send_data_from];
            status = ucg_planc_ucx_p2p_isend(tmpsend, scount, args->recvtype,
                                             neighbor[i_parity], op->tag, vgroup, &params);
            UCG_CHECK_GOTO(status, out);
            tmpsend = (char *)args->recvbuf + (int64_t)args->displs[*send_data_from + 1] * rtype_ext;
            scount = args->recvcounts[*send_data_from + 1];
            status = ucg_planc_ucx_p2p_isend(tmpsend, scount, args->recvtype,
                                             neighbor[i_parity], op->tag, vgroup, &params);
            UCG_CHECK_GOTO(status, out);
        }

        status = ucg_planc_ucx_p2p_testall(op->ucx_group, params.state);
        UCG_CHECK_GOTO(status, out);
        op->flags |= (UCG_NEIGHBOR_LOOP_RECV | UCG_NEIGHBOR_LOOP_SEND);
        *send_data_from = *recv_data_from[i_parity];
        op->allgatherv.neighbor.loop_count++;
    }

out:
    return status;
}

/**
 * @brief Neighbor Exchange algorithm for allgatherv.
 *
 * Example on 6 nodes:
 *  Initial state
 *      #    0       1       2       3       4       5
 *          [0]     [ ]     [ ]     [ ]     [ ]     [ ]
 *          [ ]     [1]     [ ]     [ ]     [ ]     [ ]
 *          [ ]     [ ]     [2]     [ ]     [ ]     [ ]
 *          [ ]     [ ]     [ ]     [3]     [ ]     [ ]
 *          [ ]     [ ]     [ ]     [ ]     [4]     [ ]
 *          [ ]     [ ]     [ ]     [ ]     [ ]     [5]
 *  Step 0 (label phase_init):
 *      #    0       1       2       3       4       5
 *          [0]     [0]     [ ]     [ ]     [ ]     [ ]
 *          [1]     [1]     [ ]     [ ]     [ ]     [ ]
 *          [ ]     [ ]     [2]     [2]     [ ]     [ ]
 *          [ ]     [ ]     [3]     [3]     [ ]     [ ]
 *          [ ]     [ ]     [ ]     [ ]     [4]     [4]
 *          [ ]     [ ]     [ ]     [ ]     [5]     [5]
 *  Step 1 (label phase_loop):
 *      #    0       1       2       3       4       5
 *          [0]     [0]     [0]     [ ]     [ ]     [0]
 *          [1]     [1]     [1]     [ ]     [ ]     [1]
 *          [ ]     [2]     [2]     [2]     [2]     [ ]
 *          [ ]     [3]     [3]     [3]     [3]     [ ]
 *          [4]     [ ]     [ ]     [4]     [4]     [4]
 *          [5]     [ ]     [ ]     [5]     [5]     [5]
 *  Step 2 (label phase_loop):
 *      #    0       1       2       3       4       5
 *          [0]     [0]     [0]     [0]     [0]     [0]
 *          [1]     [1]     [1]     [1]     [1]     [1]
 *          [2]     [2]     [2]     [2]     [2]     [2]
 *          [3]     [3]     [3]     [3]     [3]     [3]
 *          [4]     [4]     [4]     [4]     [4]     [4]
 *          [5]     [5]     [5]     [5]     [5]     [5]
 *
 * @note Limitations:
 *      - Algorithm works only on even number of processes.
 */
static ucg_status_t ucg_planc_ucx_allgatherv_neighbor_op_progress(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status;
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);

    if (ucg_test_flags(op->flags, UCG_NEIGHBOR_INIT)) {
        status = ucg_planc_ucx_allgatherv_neighbor_op_init(ucg_op);
        UCG_CHECK_GOTO(status, out);
        ucg_clear_flags(&op->flags, UCG_NEIGHBOR_INIT);
    }

    if (ucg_test_flags(op->flags, UCG_NEIGHBOR_LOOP)) {
        status = ucg_planc_ucx_allgatherv_neighbor_op_loop(ucg_op);
        UCG_CHECK_GOTO(status, out);
        ucg_clear_flags(&op->flags, UCG_NEIGHBOR_LOOP);
    }

out:
    op->super.super.status = status;
    return status;
}

static inline void ucg_planc_ucx_allgatherv_neighbor_op_reset(ucg_planc_ucx_op_t *op)
{
    ucg_planc_ucx_op_reset(op);

    ucg_rank_t my_rank = op->super.vgroup->myrank;
    uint32_t group_size = op->super.vgroup->size;
    op->flags = UCG_NEIGHBOR_FLAGS;
    op->allgatherv.neighbor.loop_count = 1;
    op->allgatherv.neighbor.loop_max = group_size / 2;
    int32_t even_rank = !(my_rank % 2);
    if (even_rank) {
        op->allgatherv.neighbor.neighbor[0] = (my_rank + 1) % group_size;
        op->allgatherv.neighbor.neighbor[1] = (my_rank - 1 + group_size) % group_size;
        op->allgatherv.neighbor.recv_data_from[0] = my_rank;
        op->allgatherv.neighbor.recv_data_from[1] = my_rank;
        op->allgatherv.neighbor.offset_at_step[0] = (+2);
        op->allgatherv.neighbor.offset_at_step[1] = (-2);
        op->allgatherv.neighbor.send_data_from = my_rank;
    } else {
        op->allgatherv.neighbor.neighbor[0] = (my_rank - 1 + group_size) % group_size;
        op->allgatherv.neighbor.neighbor[1] = (my_rank + 1) % group_size;
        op->allgatherv.neighbor.recv_data_from[0] = op->allgatherv.neighbor.neighbor[0];
        op->allgatherv.neighbor.recv_data_from[1] = op->allgatherv.neighbor.neighbor[0];
        op->allgatherv.neighbor.offset_at_step[0] = (-2);
        op->allgatherv.neighbor.offset_at_step[1] = (+2);
        op->allgatherv.neighbor.send_data_from = op->allgatherv.neighbor.recv_data_from[0];
    }
}

static ucg_status_t ucg_planc_ucx_allgatherv_neighbor_op_trigger(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status;
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_coll_allgatherv_args_t *args =  &ucg_op->super.args.allgatherv;
    ucg_rank_t my_rank = op->super.vgroup->myrank;

    ucg_planc_ucx_allgatherv_neighbor_op_reset(op);

    if (UCG_IN_PLACE != args->sendbuf) {
        uint32_t rtype_ext = ucg_dt_extent(args->recvtype);
        void *tmprecv = (char *)args->recvbuf + args->displs[my_rank] * rtype_ext;
        status = ucg_dt_memcpy(tmprecv, args->recvcounts[my_rank], args->recvtype,
                               args->sendbuf, args->sendcount, args->sendtype);
        if (status != UCG_OK) {
            return status;
        }
    }

    status = ucg_planc_ucx_allgatherv_neighbor_op_progress(ucg_op);
    if (status == UCG_INPROGRESS) {
        /* op is progressing and request start successfully */
        status = UCG_OK;
    }
    return status;
}

ucg_planc_ucx_op_t *ucg_planc_ucx_allgatherv_neighbor_op_new(ucg_planc_ucx_group_t *ucx_group,
                                                             ucg_vgroup_t *vgroup,
                                                             const ucg_coll_args_t *args)
{
    ucg_planc_ucx_op_t *ucx_op = ucg_mpool_get(&ucx_group->context->op_mp);
    if (ucx_op == NULL) {
        goto err;
    }

    ucg_status_t status;
    status = UCG_CLASS_CONSTRUCT(ucg_plan_op_t, &ucx_op->super, vgroup,
                                 ucg_planc_ucx_allgatherv_neighbor_op_trigger,
                                 ucg_planc_ucx_allgatherv_neighbor_op_progress,
                                 ucg_planc_ucx_op_discard,
                                 args);
    if (status != UCG_OK) {
        ucg_error("Failed to initialize super of ucx op");
        goto err_free_op;
    }

    ucg_planc_ucx_op_init(ucx_op, ucx_group);
    return ucx_op;

err_free_op:
    ucg_mpool_put(ucx_op);
err:
    return NULL;
}

ucg_status_t ucg_planc_ucx_allgatherv_neighbor_prepare(ucg_vgroup_t *vgroup,
                                                       const ucg_coll_args_t *args,
                                                       ucg_plan_op_t **op)
{
    UCG_CHECK_NULL_INVALID(vgroup, args, op);

    ucg_status_t status;
    status = ucg_planc_ucx_allgatherv_neighbor_check(vgroup);
    if (status != UCG_OK) {
        return UCG_ERR_UNSUPPORTED;
    }

    ucg_planc_ucx_group_t *ucx_group = ucg_derived_of(vgroup, ucg_planc_ucx_group_t);
    ucg_planc_ucx_op_t *ucx_op = ucg_planc_ucx_allgatherv_neighbor_op_new(ucx_group, vgroup, args);
    if (ucx_op == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    *op = &ucx_op->super;
    return UCG_OK;
}