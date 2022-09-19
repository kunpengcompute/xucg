/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "allreduce.h"
#include "planc_ucx_plan.h"
#include "planc_ucx_p2p.h"
#include "core/ucg_dt.h"
#include "core/ucg_group.h"
#include "util/ucg_log.h"
#include "util/ucg_malloc.h"
#include "util/algo/ucg_ring.h"

/* op flags needed by allreduce ring. */
enum {
    UCG_RING_REDUCE_SCATTER = UCG_BIT(0),
    UCG_RING_REDUCE_SCATTER_SEND = UCG_BIT(1), /* reduce scatter phase send to peer */
    UCG_RING_REDUCE_SCATTER_RECV = UCG_BIT(2), /* reduce scatter phase receive from peer */
    UCG_RING_ALLGATHERV = UCG_BIT(3),
    UCG_RING_ALLGATHERV_SEND = UCG_BIT(4), /* allgatherv phase send to peer */
    UCG_RING_ALLGATHERV_RECV = UCG_BIT(5), /* allgatherv phase recv from peer */
};

#define UCG_RING_REDUCE_SCATTER_FLAGS UCG_RING_REDUCE_SCATTER | \
                                      UCG_RING_REDUCE_SCATTER_SEND | \
                                      UCG_RING_REDUCE_SCATTER_RECV
#define UCG_RING_ALLGATHERV_FLAGS UCG_RING_ALLGATHERV | \
                                  UCG_RING_ALLGATHERV_SEND | \
                                  UCG_RING_ALLGATHERV_RECV
#define UCG_RING_FLAGS UCG_RING_REDUCE_SCATTER_FLAGS | UCG_RING_ALLGATHERV_FLAGS

static ucg_status_t ucg_planc_ucx_allreduce_ring_check(ucg_vgroup_t *vgroup,
                                                       const ucg_coll_args_t *args)
{
    uint32_t group_size = vgroup->size;
    int32_t count = args->allreduce.count;
    if (count < group_size) {
        ucg_info("Allreduce ring don't support count < group_size");
        return UCG_ERR_UNSUPPORTED;
    }
    ucg_op_flag_t flags = args->allreduce.op->flags;
    if (!(flags & UCG_OP_FLAG_IS_COMMUTATIVE)) {
        ucg_info("Allreduce ring don't support non-commutative op");
        return UCG_ERR_UNSUPPORTED;
    }
    return UCG_OK;
}

static ucg_status_t ucg_planc_ucx_allreduce_ring_op_reduce_scatter(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status;
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_coll_allreduce_args_t *args = &ucg_op->super.args.allreduce;
    ucg_rank_t my_rank = vgroup->myrank;
    uint32_t group_size = vgroup->size;
    uint32_t dt_ext = ucg_dt_extent(args->dt);
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(op, &params);
    ucg_algo_ring_iter_t *iter = &op->allreduce.ring.iter;
    ucg_rank_t left_peer = ucg_algo_ring_iter_left_value(iter);
    ucg_rank_t right_peer = ucg_algo_ring_iter_right_value(iter);
    int32_t large_blkcount = op->allreduce.ring.large_blkcount;
    int32_t small_blkcount = op->allreduce.ring.small_blkcount;
    int32_t max_blkcount = large_blkcount;
    int32_t spilt_rank = op->allreduce.ring.spilt_rank;
    void *temp_staging_area = op->staging_area - args->dt->true_lb;
    while (!ucg_algo_ring_iter_end(iter)) {
        int32_t step_idx = ucg_algo_ring_iter_idx(iter);
        if (ucg_test_and_clear_flags(&op->flags, UCG_RING_REDUCE_SCATTER_RECV)) {
            status = ucg_planc_ucx_p2p_irecv(temp_staging_area, max_blkcount,
                                             args->dt, left_peer, op->tag,
                                             vgroup, &params);
            UCG_CHECK_GOTO(status, out);
        }
        if (ucg_test_and_clear_flags(&op->flags, UCG_RING_REDUCE_SCATTER_SEND)) {
            int32_t sendblock = (my_rank + group_size - step_idx) % group_size;
            int32_t blockoffset = ((sendblock < spilt_rank) ? (sendblock * large_blkcount) :
                                    (sendblock * small_blkcount + spilt_rank));
            int32_t blockcount = ((sendblock < spilt_rank) ?
                                    large_blkcount : small_blkcount);
            void *tmpsend = args->recvbuf + (int64_t)blockoffset * dt_ext;
            status = ucg_planc_ucx_p2p_isend(tmpsend, blockcount, args->dt,
                                             right_peer, op->tag, vgroup, &params);
            UCG_CHECK_GOTO(status, out);
        }
        status = ucg_planc_ucx_p2p_testall(op->ucx_group, params.state);
        UCG_CHECK_GOTO(status, out);
        int32_t recvblock = (my_rank + group_size - step_idx - 1) % group_size;
        int32_t blockoffset = ((recvblock < spilt_rank) ? (recvblock *large_blkcount) :
                                (recvblock *small_blkcount + spilt_rank));
        int32_t blockcount = ((recvblock < spilt_rank) ? large_blkcount : small_blkcount);
        void *tmprecv = args->recvbuf + blockoffset * dt_ext;
        status = ucg_op_reduce(args->op, temp_staging_area, tmprecv, blockcount, args->dt);
        UCG_CHECK_GOTO(status, out);
        ucg_algo_ring_iter_inc(iter);
        op->flags |= (UCG_RING_REDUCE_SCATTER_RECV | UCG_RING_REDUCE_SCATTER_SEND);
    }
    ucg_algo_ring_iter_reset(iter);

out:
    return status;
}

static ucg_status_t ucg_planc_ucx_allreduce_ring_op_allgatherv(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status;
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_coll_allreduce_args_t *args = &ucg_op->super.args.allreduce;
    ucg_rank_t my_rank = vgroup->myrank;
    uint32_t group_size = vgroup->size;
    uint32_t dt_ext = ucg_dt_extent(args->dt);
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(op, &params);
    ucg_algo_ring_iter_t *iter = &op->allreduce.ring.iter;
    ucg_rank_t left_peer = ucg_algo_ring_iter_left_value(iter);
    ucg_rank_t right_peer = ucg_algo_ring_iter_right_value(iter);
    int32_t large_blkcount = op->allreduce.ring.large_blkcount;
    int32_t small_blkcount = op->allreduce.ring.small_blkcount;
    int32_t max_blkcount = large_blkcount;
    int32_t spilt_rank = op->allreduce.ring.spilt_rank;

    while (!ucg_algo_ring_iter_end(iter)) {
        int32_t step_idx = ucg_algo_ring_iter_idx(iter);
        if (ucg_test_and_clear_flags(&op->flags, UCG_RING_ALLGATHERV_RECV)) {
            int32_t recvblock = (my_rank + group_size - step_idx) % group_size;
            int32_t recv_block_offset = ((recvblock < spilt_rank) ?
                                            (recvblock * large_blkcount) :
                                            (recvblock * small_blkcount + spilt_rank));
            void *tmprecv = args->recvbuf + (int64_t)recv_block_offset * dt_ext;
            status = ucg_planc_ucx_p2p_irecv(tmprecv, max_blkcount, args->dt,
                                             left_peer, op->tag, vgroup, &params);
            UCG_CHECK_GOTO(status, out);
        }
        if (ucg_test_and_clear_flags(&op->flags, UCG_RING_ALLGATHERV_SEND)) {
            int32_t sendblock = (my_rank + group_size - step_idx + 1) % group_size;
            int32_t send_block_offset = ((sendblock < spilt_rank) ?
                                            (sendblock * large_blkcount) :
                                            (sendblock * small_blkcount + spilt_rank));
            int32_t blockcount = ((sendblock < spilt_rank) ? large_blkcount : small_blkcount);
            void *tmpsend = args->recvbuf + (int64_t)send_block_offset * dt_ext;
            status = ucg_planc_ucx_p2p_isend(tmpsend, blockcount, args->dt,
                                             right_peer, op->tag, vgroup, &params);
            UCG_CHECK_GOTO(status, out);
        }
        status = ucg_planc_ucx_p2p_testall(op->ucx_group, params.state);
        UCG_CHECK_GOTO(status, out);
        ucg_algo_ring_iter_inc(iter);
        op->flags |= (UCG_RING_ALLGATHERV_RECV | UCG_RING_ALLGATHERV_SEND);
    }
    ucg_algo_ring_iter_reset(iter);

out:
    return status;
}

/**
 * @brief Ring algorithm for allreduce operation.
 *
 *          Example on 5 nodes:
 *          Initial state
 *      #       0               1               2               3               4
 *              [00]            [10]            [20]            [30]            [40]
 *              [01]            [11]            [21]            [31]            [41]
 *              [02]            [12]            [22]            [32]            [42]
 *              [03]            [13]            [23]            [33]            [43]
 *              [04]            [14]            [24]            [34]            [44]
 *
 *          COMPUTATION PHASE
 *          Step 0: rank r sends block r to rank (r+1) and receives bloc (r-1)
 *                  from rank (r-1) [with wraparound].
 *      #       0               1               2               3               4
 *              [00]            [00+10]         [20]            [30]            [40]
 *              [01]            [11]            [11+21]         [31]            [41]
 *              [02]            [12]            [22]            [22+32]         [42]
 *              [03]            [13]            [23]            [33]            [33+43]
 *              [44+04]         [14]            [24]            [34]            [44]
 *
 *          Step 1: rank r sends block (r-1) to rank (r+1) and receives bloc
 *                  (r-2) from rank (r-1) [with wraparound].
 *      #       0               1               2               3               4
 *              [00]            [00+10]         [01+10+20]      [30]            [40]
 *              [01]            [11]            [11+21]         [11+21+31]      [41]
 *              [02]            [12]            [22]            [22+32]         [22+32+42]
 *              [33+43+03]      [13]            [23]            [33]            [33+43]
 *              [44+04]         [44+04+14]      [24]            [34]            [44]
 *
 *          Step 2: rank r sends block (r-2) to rank (r+1) and receives bloc
 *                  (r-2) from rank (r-1) [with wraparound].
 *      #       0               1               2               3               4
 *              [00]            [00+10]         [01+10+20]      [01+10+20+30]   [40]
 *              [01]            [11]            [11+21]         [11+21+31]      [11+21+31+41]
 *              [22+32+42+02]   [12]            [22]            [22+32]         [22+32+42]
 *              [33+43+03]      [33+43+03+13]   [23]            [33]            [33+43]
 *              [44+04]         [44+04+14]      [44+04+14+24]   [34]            [44]
 *
 *          Step 3: rank r sends block (r-3) to rank (r+1) and receives bloc
 *                  (r-3) from rank (r-1) [with wraparound].
 *      #       0               1               2               3               4
 *              [00]            [00+10]         [01+10+20]      [01+10+20+30]   [FULL]
 *              [FULL]          [11]            [11+21]         [11+21+31]      [11+21+31+41]
 *              [22+32+42+02]   [FULL]          [22]            [22+32]         [22+32+42]
 *              [33+43+03]      [33+43+03+13]   [FULL]          [33]            [33+43]
 *              [44+04]         [44+04+14]      [44+04+14+24]   [FULL]          [44]
 *
 *          DISTRIBUTION PHASE: ring ALLGATHER with ranks shifted by 1.
 *
 * @note Limitations:
 *      - The algorithm DOES NOT preserve order of operations so it
 *        can be used only for commutative operations.
 *      - In addition, algorithm cannot work if the total count is
 *        less than size.
 */
static ucg_status_t ucg_planc_ucx_allreduce_ring_op_progress(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status;
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);

    if (ucg_test_flags(op->flags, UCG_RING_REDUCE_SCATTER)) {
        status = ucg_planc_ucx_allreduce_ring_op_reduce_scatter(ucg_op);
        UCG_CHECK_GOTO(status, out);
        ucg_clear_flags(&op->flags, UCG_RING_REDUCE_SCATTER);
    }

    if (ucg_test_flags(op->flags, UCG_RING_ALLGATHERV)) {
        status = ucg_planc_ucx_allreduce_ring_op_allgatherv(ucg_op);
        UCG_CHECK_GOTO(status, out);
        ucg_clear_flags(&op->flags, UCG_RING_ALLGATHERV);
    }

out:
    op->super.super.status = status;
    return status;
}

static ucg_status_t ucg_planc_ucx_allreduce_ring_op_trigger(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status;
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_coll_allreduce_args_t *args = &ucg_op->super.args.allreduce;

    ucg_planc_ucx_op_reset(op);
    op->flags = UCG_RING_FLAGS;
    ucg_algo_ring_iter_reset(&op->allreduce.ring.iter);

    if (args->sendbuf != UCG_IN_PLACE) {
        status = ucg_dt_memcpy(args->recvbuf, args->count, args->dt,
                               args->sendbuf, args->count, args->dt);
        /* Special case for group size == 1 */
        if (op->super.vgroup->size == 1) {
            op->super.super.status = UCG_OK;
            return UCG_OK;
        }
    }

    status = ucg_planc_ucx_allreduce_ring_op_progress(ucg_op);
    if (status == UCG_INPROGRESS) {
        /* op is progressing and request start successfully */
        status = UCG_OK;
    }
    return status;
}

ucg_status_t ucg_planc_ucx_allreduce_ring_op_init(ucg_planc_ucx_op_t *ucg_op)
{
    ucg_coll_allreduce_args_t *args = &ucg_op->super.super.args.allreduce;
    int32_t count = args->count;
    uint32_t group_size = ucg_op->super.vgroup->size;
    ucg_rank_t my_rank = ucg_op->super.vgroup->myrank;
    int32_t large_blkcount = count / group_size;
    int32_t small_blkcount = large_blkcount;
    ucg_rank_t spilt_rank = count % group_size;
    if (spilt_rank != 0) {
        large_blkcount += 1;
    }
    ucg_op->allreduce.ring.spilt_rank = spilt_rank;
    ucg_op->allreduce.ring.large_blkcount = large_blkcount;
    ucg_op->allreduce.ring.small_blkcount = small_blkcount;
    ucg_dt_t *dt = args->dt;
    int64_t data_size = dt->true_extent + (int64_t)dt->extent * (large_blkcount - 1);
    ucg_op->staging_area = ucg_malloc(data_size, "alloc staging area");
    if (!ucg_op->staging_area) {
        return UCG_ERR_NO_MEMORY;
    }
    ucg_algo_ring_iter_init(&ucg_op->allreduce.ring.iter, group_size, my_rank);
    return UCG_OK;
}

ucg_planc_ucx_op_t *ucg_planc_ucx_allreduce_ring_op_new(ucg_planc_ucx_group_t *ucx_group,
                                                        ucg_vgroup_t *vgroup,
                                                        const ucg_coll_args_t *args)
{
    ucg_planc_ucx_op_t *ucx_op = ucg_mpool_get(&ucx_group->context->op_mp);
    if (ucx_op == NULL) {
        goto err;
    }

    ucg_status_t status;
    status = UCG_CLASS_CONSTRUCT(ucg_plan_op_t, &ucx_op->super, vgroup,
                                 ucg_planc_ucx_allreduce_ring_op_trigger,
                                 ucg_planc_ucx_allreduce_ring_op_progress,
                                 ucg_planc_ucx_op_discard,
                                 args);

    if (status != UCG_OK) {
        ucg_error("Failed to initialize super of ucx op");
        goto err_free_op;
    }

    ucg_planc_ucx_op_init(ucx_op, ucx_group);

    status = ucg_planc_ucx_allreduce_ring_op_init(ucx_op);
    if (status != UCG_OK) {
        ucg_error("Failed to initialize allreduce ring ucx op");
        goto err_destruct;
    }

    return ucx_op;

err_destruct:
    UCG_CLASS_DESTRUCT(ucg_plan_op_t, &ucx_op->super);
err_free_op:
    ucg_mpool_put(ucx_op);
err:
    return NULL;
}

ucg_status_t ucg_planc_ucx_allreduce_ring_prepare(ucg_vgroup_t *vgroup,
                                                  const ucg_coll_args_t *args,
                                                  ucg_plan_op_t **op)
{
    UCG_CHECK_NULL_INVALID(vgroup, args, op);

    ucg_status_t status;
    status = ucg_planc_ucx_allreduce_ring_check(vgroup, args);
    if (status != UCG_OK) {
        return UCG_ERR_UNSUPPORTED;
    }

    ucg_planc_ucx_group_t *ucx_group = ucg_derived_of(vgroup, ucg_planc_ucx_group_t);
    ucg_planc_ucx_op_t *ucx_op = ucg_planc_ucx_allreduce_ring_op_new(ucx_group, vgroup, args);
    if (ucx_op == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    *op = &ucx_op->super;
    return UCG_OK;
}