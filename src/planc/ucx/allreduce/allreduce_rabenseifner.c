/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "allreduce.h"
#include "allreduce_meta.h"
#include "planc_ucx_plan.h"
#include "planc_ucx_meta.h"
#include "core/ucg_topo.h"
#include "core/ucg_group.h"
#include "core/ucg_plan.h"
#include "util/ucg_log.h"

enum {
    UCG_RABENSEIFNER_RANK_BASE,
    UCG_RABENSEIFNER_RANK_PROXY,
    UCG_RABENSEIFNER_RANK_EXTRA,
};

enum {
    UCG_PROXY_SEND = UCG_BIT(0),
    UCG_PROXY_RECV = UCG_BIT(1),
    UCG_PROXY_REDUCE = UCG_BIT(2),
    UCG_PROXY_RECV_RESULT = UCG_BIT(3),
    UCG_PROXY_SEND_RESULT = UCG_BIT(4),
    UCG_EXTRA_SEND = UCG_BIT(5),
    UCG_EXTRA_RECV = UCG_BIT(6),
    UCG_EXTRA_REDUCE = UCG_BIT(7),
    UCG_EXTRA_SEND_RESULT = UCG_BIT(8),
    UCG_EXTRA_RECV_RESULT = UCG_BIT(9),
    UCG_BASE_REDUCE_SCATTER = UCG_BIT(10),
    UCG_BASE_REDUCE_SCATTER_SEND = UCG_BIT(11),
    UCG_BASE_REDUCE_SCATTER_RECV = UCG_BIT(12),
    UCG_BASE_ALLGATHERV = UCG_BIT(13),
    UCG_BASE_ALLGATHERV_SEND = UCG_BIT(14),
    UCG_BASE_ALLGATHERV_RECV = UCG_BIT(15),
};

#define UCG_RABENSEIFNER_PROXY_FLAGS UCG_PROXY_SEND | UCG_PROXY_RECV | \
                                     UCG_PROXY_REDUCE | UCG_PROXY_RECV_RESULT | \
                                     UCG_PROXY_SEND_RESULT
#define UCG_RABENSEIFNER_EXTRA_FLAGS UCG_EXTRA_SEND | UCG_EXTRA_RECV | \
                                     UCG_EXTRA_REDUCE | UCG_EXTRA_SEND_RESULT | \
                                     UCG_EXTRA_RECV_RESULT
#define UCG_RABENSEIFNER_BASE_FLAGS UCG_BASE_REDUCE_SCATTER | \
                                    UCG_BASE_REDUCE_SCATTER_SEND | \
                                    UCG_BASE_REDUCE_SCATTER_RECV | \
                                    UCG_BASE_ALLGATHERV | \
                                    UCG_BASE_ALLGATHERV_SEND | \
                                    UCG_BASE_ALLGATHERV_RECV

static ucg_status_t ucg_planc_ucx_allreduce_rabenseifner_check(ucg_vgroup_t *vgroup,
                                                               const ucg_coll_args_t *args)
{
    uint32_t group_size = vgroup->size;
    int32_t count = args->allreduce.count;
    if (count < group_size) {
        ucg_info("Allreduce rabenseifner don't support count < group_size");
        return UCG_ERR_UNSUPPORTED;
    }
    ucg_op_flag_t flags = args->allreduce.op->flags;
    if (!(flags & UCG_OP_FLAG_IS_COMMUTATIVE)) {
        ucg_info("Allreduce rabenseifner don't support non-commutative op");
        return UCG_ERR_UNSUPPORTED;
    }
    return UCG_OK;
}

static
ucg_status_t ucg_planc_ucx_allreduce_reduce_scatter_op_base_progress(ucg_planc_ucx_op_t *op)
{
    ucg_status_t status = UCG_OK;
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_rank_t myrank = vgroup->myrank;
    uint32_t size = vgroup->size;
    ucg_coll_allreduce_args_t *args = &op->super.super.args.allreduce;
    void *staging_area = op->staging_area - args->dt->true_lb;
    void *recvbuf = args->recvbuf;
    uint32_t extent = ucg_dt_extent(args->dt);
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(op, &params);

    int32_t nstep = ucg_ilog2(size);
    int32_t nprocs_pof2 = UCG_BIT(nstep);
    int32_t nprocs_rem = size - nprocs_pof2;
    int32_t *mask = &op->allreduce.rabenseifner.mask;
    int32_t *step_idx = &op->allreduce.rabenseifner.step_index;
    ucg_rank_t new_rank = op->allreduce.rabenseifner.new_rank;
    int32_t *window_size = &op->allreduce.rabenseifner.window_size;
    int32_t *sindex = op->allreduce.rabenseifner.send_index;
    int32_t *rindex = op->allreduce.rabenseifner.recv_index;
    int32_t *scount = op->allreduce.rabenseifner.send_count;
    int32_t *rcount = op->allreduce.rabenseifner.recv_count;

    while (*mask < nprocs_pof2) {
        ucg_rank_t new_peer = new_rank ^ *mask;
        ucg_rank_t peer = (new_peer < nprocs_rem) ? new_peer * 2 : new_peer + nprocs_rem;
        int32_t step = *step_idx;
        int32_t wsize = *window_size;
        if (myrank < peer) {
            rcount[step] = wsize / 2;
            scount[step] = wsize - rcount[step];
            sindex[step] = rindex[step] + rcount[step];
        } else {
            scount[step] = wsize / 2;
            rcount[step] = wsize - scount[step];
            rindex[step] = sindex[step] + scount[step];
        }

        if (ucg_test_and_clear_flags(&op->flags, UCG_BASE_REDUCE_SCATTER_SEND)) {
            status = ucg_planc_ucx_p2p_isend(recvbuf + (int64_t)sindex[step] * extent,
                                             scount[step], args->dt, peer,
                                             op->tag, vgroup, &params);
            UCG_CHECK_GOTO(status, out);
        }

        if (ucg_test_and_clear_flags(&op->flags, UCG_BASE_REDUCE_SCATTER_RECV)) {
            status = ucg_planc_ucx_p2p_irecv(staging_area + (int64_t)rindex[step] * extent,
                                             rcount[step], args->dt, peer,
                                             op->tag, vgroup, &params);
            UCG_CHECK_GOTO(status, out);
        }

        status = ucg_planc_ucx_p2p_testall(op->ucx_group, params.state);
        UCG_CHECK_GOTO(status, out);
        status = ucg_op_reduce(args->op, staging_area + rindex[step] * extent,
                               recvbuf + rindex[step] * extent, rcount[step],
                               args->dt);
        UCG_CHECK_GOTO(status, out);

        if (step + 1 < nstep) {
            rindex[step + 1] = rindex[step];
            sindex[step + 1] = rindex[step];
            *window_size = rcount[step];
            ++(*step_idx);
        }
        *mask <<= 1;
        op->flags |= UCG_BASE_REDUCE_SCATTER_SEND | UCG_BASE_REDUCE_SCATTER_RECV;
    }
    *mask = nprocs_pof2 >> 1;
    *step_idx = nstep - 1;
out:
    return status;
}

static ucg_status_t ucg_planc_ucx_allreduce_reduce_scatter_op_base(ucg_planc_ucx_op_t *op)
{
    ucg_status_t status = UCG_OK;

    if (ucg_test_flags(op->flags, UCG_BASE_REDUCE_SCATTER)) {
        status = ucg_planc_ucx_allreduce_reduce_scatter_op_base_progress(op);
        UCG_CHECK_GOTO(status, out);
        ucg_clear_flags(&op->flags, UCG_BASE_REDUCE_SCATTER);
    }

out:
    op->super.super.status = status;
    return status;
}

static ucg_status_t ucg_planc_ucx_allreduce_reduce_scatter_op_proxy(ucg_planc_ucx_op_t *op)
{
    ucg_status_t status = UCG_OK;
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_rank_t myrank = vgroup->myrank;
    ucg_coll_allreduce_args_t *args = &op->super.super.args.allreduce;
    void *staging_area = op->staging_area - args->dt->true_lb;
    void *recvbuf = args->recvbuf;
    int32_t count = args->count;
    uint32_t extent = ucg_dt_extent(args->dt);
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(op, &params);

    ucg_rank_t peer = myrank + 1;
    int count_lhalf = count / 2;
    int count_rhalf = count - count_lhalf;
    if (ucg_test_and_clear_flags(&op->flags, UCG_PROXY_SEND)) {
        status = ucg_planc_ucx_p2p_isend(recvbuf + (int64_t)count_lhalf * extent,
                                         count_rhalf, args->dt, peer,
                                         op->tag, vgroup, &params);
        UCG_CHECK_GOTO(status, out);
    }
    if (ucg_test_and_clear_flags(&op->flags, UCG_PROXY_RECV)) {
        status = ucg_planc_ucx_p2p_irecv(staging_area, count_lhalf, args->dt, peer,
                                         op->tag, vgroup, &params);
        UCG_CHECK_GOTO(status, out);
    }
    status = ucg_planc_ucx_p2p_testall(op->ucx_group, params.state);
    UCG_CHECK_GOTO(status, out);
    if (ucg_test_and_clear_flags(&op->flags, UCG_PROXY_REDUCE)) {
        status = ucg_op_reduce(args->op, staging_area, recvbuf, count_lhalf, args->dt);
        UCG_CHECK_GOTO(status, out);
    }
    if (ucg_test_and_clear_flags(&op->flags, UCG_PROXY_RECV_RESULT)) {
        status = ucg_planc_ucx_p2p_irecv(recvbuf + (int64_t)count_lhalf * extent,
                                         count_rhalf, args->dt, peer,
                                         op->tag, vgroup, &params);
        UCG_CHECK_GOTO(status, out);
    }
    status = ucg_planc_ucx_p2p_testall(op->ucx_group, params.state);
    UCG_CHECK_GOTO(status, out);

    if (ucg_test_flags(op->flags, UCG_RABENSEIFNER_RANK_BASE)) {
        status = ucg_planc_ucx_allreduce_reduce_scatter_op_base(op);
        UCG_CHECK_GOTO(status, out);
        ucg_clear_flags(&op->flags, UCG_RABENSEIFNER_RANK_BASE);
    }
    status = ucg_planc_ucx_p2p_testall(op->ucx_group, params.state);
out:
    return status;
}

static ucg_status_t ucg_planc_ucx_allreduce_reduce_scatter_op_extra(ucg_planc_ucx_op_t *op)
{
    ucg_status_t status = UCG_OK;
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_rank_t myrank = vgroup->myrank;
    ucg_coll_allreduce_args_t *args = &op->super.super.args.allreduce;
    void *staging_area = op->staging_area - args->dt->true_lb;
    void *recvbuf = args->recvbuf;
    int32_t count = args->count;
    uint32_t extent = ucg_dt_extent(args->dt);
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(op, &params);

    ucg_rank_t peer = myrank - 1;
    int count_lhalf = count / 2;
    int count_rhalf = count - count_lhalf;
    if (ucg_test_and_clear_flags(&op->flags, UCG_EXTRA_SEND)) {
        status = ucg_planc_ucx_p2p_isend(recvbuf, count_lhalf, args->dt, peer,
                                         op->tag, vgroup, &params);
        UCG_CHECK_GOTO(status, out);
    }
    if (ucg_test_and_clear_flags(&op->flags, UCG_EXTRA_RECV)) {
        status = ucg_planc_ucx_p2p_irecv(staging_area + (int64_t)count_lhalf * extent,
                                         count_rhalf, args->dt, peer,
                                         op->tag, vgroup, &params);
        UCG_CHECK_GOTO(status, out);
    }
    status = ucg_planc_ucx_p2p_testall(op->ucx_group, params.state);
    UCG_CHECK_GOTO(status, out);
    if (ucg_test_and_clear_flags(&op->flags, UCG_EXTRA_REDUCE)) {
        status = ucg_op_reduce(args->op, staging_area + count_lhalf * extent,
                               recvbuf + count_lhalf * extent, count_rhalf, args->dt);
        UCG_CHECK_GOTO(status, out);
    }
    if (ucg_test_and_clear_flags(&op->flags, UCG_EXTRA_SEND_RESULT)) {
        status = ucg_planc_ucx_p2p_isend(recvbuf + (int64_t)count_lhalf * extent,
                                         count_rhalf, args->dt, peer,
                                         op->tag, vgroup, &params);
        UCG_CHECK_GOTO(status, out);
    }
    status = ucg_planc_ucx_p2p_testall(op->ucx_group, params.state);
out:
    return status;
}

static ucg_status_t ucg_planc_ucx_allreduce_reduce_scatter_op_progress(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status;
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    uint64_t type = op->allreduce.rabenseifner.rank_type;

    if (type == UCG_RABENSEIFNER_RANK_BASE) {
        status = ucg_planc_ucx_allreduce_reduce_scatter_op_base(op);
    } else if (type == UCG_RABENSEIFNER_RANK_PROXY) {
        status = ucg_planc_ucx_allreduce_reduce_scatter_op_proxy(op);
    } else {
        status = ucg_planc_ucx_allreduce_reduce_scatter_op_extra(op);
    }
    op->super.super.status = status;
    return status;
}

static ucg_status_t ucg_planc_ucx_allreduce_rabenseifner_common_op_reset(ucg_planc_ucx_op_t *ucg_op)
{
    ucg_op->allreduce.rabenseifner.mask = 1;
    ucg_op->allreduce.rabenseifner.step_index = 0;

    const ucg_coll_allreduce_args_t *coll_args = &ucg_op->super.super.args.allreduce;
    ucg_op->allreduce.rabenseifner.window_size = coll_args->count;

    int32_t *send_index = ucg_op->allreduce.rabenseifner.send_index;
    int32_t *recv_index = ucg_op->allreduce.rabenseifner.recv_index;
    send_index[0] = recv_index[0] = 0;
    return UCG_OK;
}

static ucg_status_t ucg_planc_ucx_allreduce_reduce_scatter_op_trigger(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status;
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_planc_ucx_op_reset(op);
    ucg_planc_ucx_allreduce_rabenseifner_common_op_reset(op);

    uint64_t type = op->allreduce.rabenseifner.rank_type;
    switch (type) {
        case UCG_RABENSEIFNER_RANK_BASE:
            op->flags = UCG_RABENSEIFNER_BASE_FLAGS;
            break;
        case UCG_RABENSEIFNER_RANK_PROXY:
            op->flags = UCG_RABENSEIFNER_BASE_FLAGS | UCG_RABENSEIFNER_PROXY_FLAGS;
            break;
        case UCG_RABENSEIFNER_RANK_EXTRA:
            op->flags = UCG_RABENSEIFNER_EXTRA_FLAGS;
            break;
        default:
            ucg_error("Unknown rank type %lu", type);
            break;
    }

    ucg_coll_allreduce_args_t *args = &ucg_op->super.args.allreduce;
    if (args->sendbuf != UCG_IN_PLACE) {
        status = ucg_dt_memcpy(args->recvbuf, args->count, args->dt,
                               args->sendbuf, args->count, args->dt);
        if (status != UCG_OK) {
            return status;
        }
    }

    status = ucg_planc_ucx_allreduce_reduce_scatter_op_progress(ucg_op);
    return status == UCG_INPROGRESS ? UCG_OK : status;
}

static ucg_status_t ucg_planc_ucx_allreduce_rabenseifner_common_op_discard(ucg_plan_op_t *ucg_op)
{
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);

    if (op->staging_area != NULL) {
        ucg_free(op->staging_area);
    }
    if (op->allreduce.rabenseifner.recv_count != NULL) {
        ucg_free(op->allreduce.rabenseifner.recv_count);
    }
    if (op->allreduce.rabenseifner.send_count != NULL) {
        ucg_free(op->allreduce.rabenseifner.send_count);
    }
    if (op->allreduce.rabenseifner.recv_index != NULL) {
        ucg_free(op->allreduce.rabenseifner.recv_index);
    }
    if (op->allreduce.rabenseifner.send_index != NULL) {
        ucg_free(op->allreduce.rabenseifner.send_index);
    }
    UCG_CLASS_DESTRUCT(ucg_plan_op_t, &op->super);
    ucg_mpool_put(op);
    return UCG_OK;
}

static ucg_status_t ucg_planc_ucx_allreduce_rabenseifner_common_op_init(ucg_planc_ucx_op_t *ucg_op)
{
    ucg_vgroup_t *vgroup = ucg_op->super.vgroup;
    ucg_rank_t myrank = vgroup->myrank;
    uint32_t group_size = vgroup->size;

    ucg_op->allreduce.rabenseifner.mask = 1;
    ucg_op->allreduce.rabenseifner.step_index = 0;

    uint64_t rank_type;
    ucg_rank_t new_rank;
    int32_t nstep = ucg_ilog2(group_size);
    int32_t nprocs_pof2 = UCG_BIT(nstep);
    int32_t nprocs_rem = group_size - nprocs_pof2;
    if (myrank < 2 * nprocs_rem) {
        if (myrank % 2 == 0) {
            rank_type = UCG_RABENSEIFNER_RANK_PROXY;
            new_rank = myrank / 2;
        } else {
            rank_type = UCG_RABENSEIFNER_RANK_EXTRA;
            new_rank = -1;
        }
    } else {
        rank_type = UCG_RABENSEIFNER_RANK_BASE;
        new_rank = myrank - nprocs_rem;
    }
    ucg_op->allreduce.rabenseifner.rank_type = rank_type;
    ucg_op->allreduce.rabenseifner.new_rank = new_rank;

    int32_t *send_index = ucg_malloc(nstep *sizeof(int32_t), "allreduce send index");
    if (send_index == NULL) {
        goto err;
    }
    int32_t *recv_index = ucg_malloc(nstep *sizeof(int32_t), "allreduce recv index");
    if (recv_index == NULL) {
        goto err_free_send_index;
    }
    int32_t *send_count = ucg_malloc(nstep *sizeof(int32_t), "allreduce send count");
    if (send_count == NULL) {
        goto err_free_recv_index;
    }
    int32_t *recv_count = ucg_malloc(nstep *sizeof(int32_t), "allreduce recv count");
    if (recv_count == NULL) {
        goto err_free_send_count;
    }
    send_index[0] = recv_index[0] = 0;
    ucg_op->allreduce.rabenseifner.send_index = send_index;
    ucg_op->allreduce.rabenseifner.recv_index = recv_index;
    ucg_op->allreduce.rabenseifner.send_count = send_count;
    ucg_op->allreduce.rabenseifner.recv_count = recv_count;

    const ucg_coll_allreduce_args_t *coll_args = &ucg_op->super.super.args.allreduce;
    ucg_op->allreduce.rabenseifner.window_size = coll_args->count;

    int64_t data_size = coll_args->dt->true_extent + (int64_t)coll_args->dt->extent * (coll_args->count - 1);
    ucg_op->staging_area = ucg_malloc(data_size, "allreduce staging area");
    if (ucg_op->staging_area == NULL) {
        goto err_free_recv_count;
    }

    return UCG_OK;

err_free_recv_count:
    ucg_free(recv_count);
err_free_send_count:
    ucg_free(send_count);
err_free_recv_index:
    ucg_free(recv_index);
err_free_send_index:
    ucg_free(send_index);
err:
    return UCG_ERR_NO_MEMORY;
}

ucg_planc_ucx_op_t *ucg_planc_ucx_allreduce_reduce_scatter_op_new(ucg_planc_ucx_group_t *ucx_group,
                                                                  ucg_vgroup_t *vgroup,
                                                                  const ucg_coll_args_t *args)
{
    UCG_CHECK_NULL(NULL, ucx_group, vgroup, args);

    ucg_planc_ucx_op_t *op = ucg_mpool_get(&ucx_group->context->op_mp);
    if (op == NULL) {
        goto err;
    }

    ucg_status_t status;
    status = UCG_CLASS_CONSTRUCT(ucg_plan_op_t, &op->super, vgroup,
                                 ucg_planc_ucx_allreduce_reduce_scatter_op_trigger,
                                 ucg_planc_ucx_allreduce_reduce_scatter_op_progress,
                                 ucg_planc_ucx_allreduce_rabenseifner_common_op_discard,
                                 args);
    if (status != UCG_OK) {
        ucg_error("Failed to initialize super of ucx op");
        goto err_free_op;
    }
    ucg_planc_ucx_op_init(op, ucx_group);

    status = ucg_planc_ucx_allreduce_rabenseifner_common_op_init(op);
    if (status != UCG_OK) {
        ucg_error("Failed to initialize allreduce reduce_scatter ucx op");
        goto err_destruct;
    }
    return op;

err_destruct:
    UCG_CLASS_DESTRUCT(ucg_plan_op_t, &op->super);
err_free_op:
    ucg_mpool_put(op);
err:
    return NULL;
}

static
ucg_status_t ucg_planc_ucx_allreduce_allgatherv_op_base_init(ucg_planc_ucx_op_t *op)
{
    ucg_status_t status = UCG_OK;
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_rank_t myrank = vgroup->myrank;
    uint32_t size = vgroup->size;

    int32_t nstep = ucg_ilog2(size);
    int32_t nprocs_pof2 = UCG_BIT(nstep);
    int32_t nprocs_rem = size - nprocs_pof2;
    int32_t *mask = &op->allreduce.rabenseifner.mask;
    int32_t *step_idx = &op->allreduce.rabenseifner.step_index;
    ucg_rank_t new_rank = op->allreduce.rabenseifner.new_rank;
    int32_t *window_size = &op->allreduce.rabenseifner.window_size;
    int32_t *sindex = op->allreduce.rabenseifner.send_index;
    int32_t *rindex = op->allreduce.rabenseifner.recv_index;
    int32_t *scount = op->allreduce.rabenseifner.send_count;
    int32_t *rcount = op->allreduce.rabenseifner.recv_count;

    while (*mask < nprocs_pof2) {
        ucg_rank_t new_peer = new_rank ^ *mask;
        ucg_rank_t peer = (new_peer < nprocs_rem) ? new_peer * 2 : new_peer + nprocs_rem;
        int32_t step = *step_idx;
        int32_t wsize = *window_size;
        if (myrank < peer) {
            rcount[step] = wsize / 2;
            scount[step] = wsize - rcount[step];
            sindex[step] = rindex[step] + rcount[step];
        } else {
            scount[step] = wsize / 2;
            rcount[step] = wsize - scount[step];
            rindex[step] = sindex[step] + scount[step];
        }

        if (step + 1 < nstep) {
            rindex[step + 1] = rindex[step];
            sindex[step + 1] = rindex[step];
            *window_size = rcount[step];
            ++(*step_idx);
        }
        *mask <<= 1;
    }
    *mask = nprocs_pof2 >> 1;
    *step_idx = nstep - 1;
    return status;
}

static
ucg_status_t ucg_planc_ucx_allreduce_allgatherv_op_base_progress(ucg_planc_ucx_op_t *op)
{
    ucg_status_t status = UCG_OK;
    ucg_vgroup_t *vgroup = op->super.vgroup;
    uint32_t size = vgroup->size;
    ucg_coll_allreduce_args_t *args = &op->super.super.args.allreduce;
    void *recvbuf = args->recvbuf;
    uint32_t extent = ucg_dt_extent(args->dt);
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(op, &params);

    int32_t nstep = ucg_ilog2(size);
    int32_t nprocs_pof2 = UCG_BIT(nstep);
    int32_t nprocs_rem = size - nprocs_pof2;
    int32_t *mask = &op->allreduce.rabenseifner.mask;
    int32_t *step_idx = &op->allreduce.rabenseifner.step_index;
    ucg_rank_t new_rank = op->allreduce.rabenseifner.new_rank;
    int32_t *sindex = op->allreduce.rabenseifner.send_index;
    int32_t *rindex = op->allreduce.rabenseifner.recv_index;
    int32_t *scount = op->allreduce.rabenseifner.send_count;
    int32_t *rcount = op->allreduce.rabenseifner.recv_count;

    while (*mask > 0) {
        ucg_rank_t new_peer = new_rank ^ *mask;
        ucg_rank_t peer = (new_peer < nprocs_rem) ? new_peer * 2 : new_peer + nprocs_rem;
        int32_t step = *step_idx;
        if (ucg_test_and_clear_flags(&op->flags, UCG_BASE_ALLGATHERV_SEND)) {
            status = ucg_planc_ucx_p2p_isend(recvbuf + (int64_t)rindex[step] * extent,
                                             rcount[step], args->dt, peer,
                                             op->tag, vgroup, &params);
            UCG_CHECK_GOTO(status, out);
        }
        if (ucg_test_and_clear_flags(&op->flags, UCG_BASE_ALLGATHERV_RECV)) {
            status = ucg_planc_ucx_p2p_irecv(recvbuf + (int64_t)sindex[step] * extent,
                                             scount[step], args->dt, peer,
                                             op->tag, vgroup, &params);
            UCG_CHECK_GOTO(status, out);
        }
        status = ucg_planc_ucx_p2p_testall(op->ucx_group, params.state);
        UCG_CHECK_GOTO(status, out);

        --(*step_idx);
        *mask >>= 1;
        op->flags |= UCG_BASE_ALLGATHERV_SEND | UCG_BASE_ALLGATHERV_RECV;
    }
out:
    return status;
}

static ucg_status_t ucg_planc_ucx_allreduce_allgatherv_op_base(ucg_planc_ucx_op_t *op)
{
    ucg_status_t status = UCG_OK;

    if (ucg_test_flags(op->flags, UCG_BASE_REDUCE_SCATTER)) {
        status = ucg_planc_ucx_allreduce_allgatherv_op_base_init(op);
        UCG_CHECK_GOTO(status, out);
        ucg_clear_flags(&op->flags, UCG_BASE_REDUCE_SCATTER);
    }

    if (ucg_test_flags(op->flags, UCG_BASE_ALLGATHERV)) {
        status = ucg_planc_ucx_allreduce_allgatherv_op_base_progress(op);
        UCG_CHECK_GOTO(status, out);
        ucg_clear_flags(&op->flags, UCG_BASE_ALLGATHERV);
    }

out:
    op->super.super.status = status;
    return status;
}

static ucg_status_t ucg_planc_ucx_allreduce_allgatherv_op_proxy(ucg_planc_ucx_op_t *op)
{
    ucg_status_t status = UCG_OK;
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_rank_t myrank = vgroup->myrank;
    ucg_coll_allreduce_args_t *args = &op->super.super.args.allreduce;
    void *recvbuf = args->recvbuf;
    int32_t count = args->count;
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(op, &params);

    ucg_rank_t peer = myrank + 1;
    if (ucg_test_flags(op->flags, UCG_RABENSEIFNER_RANK_BASE)) {
        status = ucg_planc_ucx_allreduce_allgatherv_op_base(op);
        UCG_CHECK_GOTO(status, out);
        ucg_clear_flags(&op->flags, UCG_RABENSEIFNER_RANK_BASE);
    }
    if (ucg_test_and_clear_flags(&op->flags, UCG_PROXY_SEND_RESULT)) {
        status = ucg_planc_ucx_p2p_isend(recvbuf, count, args->dt, peer,
                                         op->tag, vgroup, &params);
        UCG_CHECK_GOTO(status, out);
    }
    status = ucg_planc_ucx_p2p_testall(op->ucx_group, params.state);
out:
    return status;
}

static ucg_status_t ucg_planc_ucx_allreduce_allgatherv_op_extra(ucg_planc_ucx_op_t *op)
{
    ucg_status_t status = UCG_OK;
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_rank_t myrank = vgroup->myrank;
    ucg_coll_allreduce_args_t *args = &op->super.super.args.allreduce;
    void *recvbuf = args->recvbuf;
    int32_t count = args->count;
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(op, &params);

    ucg_rank_t peer = myrank - 1;
    if (ucg_test_and_clear_flags(&op->flags, UCG_EXTRA_RECV_RESULT)) {
        status = ucg_planc_ucx_p2p_irecv(recvbuf, count, args->dt, peer,
                                         op->tag, vgroup, &params);
        UCG_CHECK_GOTO(status, out);
    }
    status = ucg_planc_ucx_p2p_testall(op->ucx_group, params.state);
out:
    return status;
}

static ucg_status_t ucg_planc_ucx_allreduce_allgatherv_op_progress(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status;
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    uint64_t type = op->allreduce.rabenseifner.rank_type;

    if (type == UCG_RABENSEIFNER_RANK_BASE) {
        status = ucg_planc_ucx_allreduce_allgatherv_op_base(op);
    } else if (type == UCG_RABENSEIFNER_RANK_PROXY) {
        status = ucg_planc_ucx_allreduce_allgatherv_op_proxy(op);
    } else {
        status = ucg_planc_ucx_allreduce_allgatherv_op_extra(op);
    }
    op->super.super.status = status;
    return status;
}

static ucg_status_t ucg_planc_ucx_allreduce_allgatherv_op_trigger(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status;
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_planc_ucx_op_reset(op);
    ucg_planc_ucx_allreduce_rabenseifner_common_op_reset(op);

    uint64_t type = op->allreduce.rabenseifner.rank_type;
    switch (type) {
        case UCG_RABENSEIFNER_RANK_BASE:
            op->flags = UCG_RABENSEIFNER_BASE_FLAGS;
            break;
        case UCG_RABENSEIFNER_RANK_PROXY:
            op->flags = UCG_RABENSEIFNER_BASE_FLAGS | UCG_RABENSEIFNER_PROXY_FLAGS;
            break;
        case UCG_RABENSEIFNER_RANK_EXTRA:
            op->flags = UCG_RABENSEIFNER_EXTRA_FLAGS;
            break;
        default:
            ucg_error("Unknown rank type %lu", type);
            break;
    }

    status = ucg_planc_ucx_allreduce_allgatherv_op_progress(ucg_op);
    return status == UCG_INPROGRESS ? UCG_OK : status;
}

ucg_planc_ucx_op_t *ucg_planc_ucx_allreduce_allgatherv_op_new(ucg_planc_ucx_group_t *ucx_group,
                                                              ucg_vgroup_t *vgroup,
                                                              const ucg_coll_args_t *args)
{
    UCG_CHECK_NULL(NULL, ucx_group, vgroup, args);

    ucg_planc_ucx_op_t *op = ucg_mpool_get(&ucx_group->context->op_mp);
    if (op == NULL) {
        goto err;
    }

    ucg_status_t status;
    status = UCG_CLASS_CONSTRUCT(ucg_plan_op_t, &op->super, vgroup,
                                 ucg_planc_ucx_allreduce_allgatherv_op_trigger,
                                 ucg_planc_ucx_allreduce_allgatherv_op_progress,
                                 ucg_planc_ucx_allreduce_rabenseifner_common_op_discard,
                                 args);
    if (status != UCG_OK) {
        ucg_error("Failed to initialize super of ucx op");
        goto err_free_op;
    }
    ucg_planc_ucx_op_init(op, ucx_group);

    status = ucg_planc_ucx_allreduce_rabenseifner_common_op_init(op);
    if (status != UCG_OK) {
        ucg_error("Failed to initialize allreduce allgatherv ucx op");
        goto err_destruct;
    }
    return op;

err_destruct:
    UCG_CLASS_DESTRUCT(ucg_plan_op_t, &op->super);
err_free_op:
    ucg_mpool_put(op);
err:
    return NULL;
}

ucg_plan_meta_op_t* ucg_planc_ucx_allreduce_rabenseifner_op_new(ucg_planc_ucx_group_t* ucx_group,
                                                                ucg_vgroup_t *vgroup,
                                                                const ucg_coll_args_t* args)
{
    UCG_CHECK_NULL(NULL, ucx_group, vgroup, args);

    ucg_plan_meta_op_t* meta_op = ucg_plan_meta_op_new(vgroup->group, vgroup, args);
    if (meta_op == NULL) {
        goto err;
    }

    ucg_status_t status;
    ucg_coll_args_t *meta_args = &meta_op->super.super.args;

    status = ucg_planc_ucx_allreduce_add_reduce_scatter_op(meta_op, ucx_group,
                                                           vgroup, meta_args,
                                                           UCG_TOPO_GROUP_TYPE_NET);
    UCG_CHECK_GOTO(status, err_free_meta_op);

    status = ucg_planc_ucx_allreduce_add_allgatherv_op(meta_op, ucx_group,
                                                       vgroup, meta_args,
                                                       UCG_TOPO_GROUP_TYPE_NET);
    UCG_CHECK_GOTO(status, err_free_meta_op);

    return meta_op;

err_free_meta_op:
    meta_op->super.discard(&meta_op->super);
err:
    return NULL;
}

ucg_status_t ucg_planc_ucx_allreduce_rabenseifner_prepare(ucg_vgroup_t *vgroup,
                                                          const ucg_coll_args_t *args,
                                                          ucg_plan_op_t **op)
{
    UCG_CHECK_NULL_INVALID(vgroup, args, op);

    ucg_status_t status;
    status = ucg_planc_ucx_allreduce_rabenseifner_check(vgroup, args);
    if (status != UCG_OK) {
        return UCG_ERR_UNSUPPORTED;
    }

    ucg_planc_ucx_group_t *ucx_group = ucg_derived_of(vgroup, ucg_planc_ucx_group_t);
    ucg_plan_meta_op_t *meta_op;
    meta_op = ucg_planc_ucx_allreduce_rabenseifner_op_new(ucx_group, vgroup, args);
    if (meta_op == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    *op = &meta_op->super;
    return UCG_OK;
}