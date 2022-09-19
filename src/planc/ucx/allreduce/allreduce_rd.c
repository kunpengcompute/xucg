/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "allreduce.h"
#include "planc_ucx_plan.h"
#include "planc_ucx_p2p.h"
#include "planc_ucx_global.h"
#include "core/ucg_group.h"
#include "core/ucg_dt.h"
#include "util/algo/ucg_rd.h"
#include "util/ucg_log.h"
#include "util/ucg_malloc.h"
/* op flags needed by allreduce rd. */
enum {
    UCG_RD_BASE_SEND = UCG_BIT(0), /* send to peer base */
    UCG_RD_BASE_RECV = UCG_BIT(1), /* receive from peer base */
    UCG_RD_PROXY_RECV = UCG_BIT(2), /* receive from extra */
    UCG_RD_PROXY_REDUCE = UCG_BIT(3), /* reduce proxy and extra */
    UCG_RD_PROXY_BASE = UCG_BIT(4), /* base loop */
    UCG_RD_PROXY_SEND = UCG_BIT(5), /* send result to extra */
    UCG_RD_EXTRA_SEND = UCG_BIT(6), /* send to peer proxy */
    UCG_RD_EXTRA_RECV = UCG_BIT(7), /* receive from peer proxy */
};

#define UCG_RD_BASE_FLAGS UCG_RD_BASE_SEND | UCG_RD_BASE_RECV
#define UCG_RD_PROXY_FLAGS UCG_RD_PROXY_RECV | UCG_RD_PROXY_REDUCE | \
                           UCG_RD_PROXY_BASE | UCG_RD_PROXY_SEND
#define UCG_RD_EXTRA_FLAGS UCG_RD_EXTRA_SEND | UCG_RD_EXTRA_RECV

static ucg_status_t ucg_planc_ucx_allreduce_rd_op_base(ucg_planc_ucx_op_t *op)
{
    ucg_status_t status = UCG_OK;
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_coll_allreduce_args_t *args = &op->super.super.args.allreduce;
    void *temp_staging_area = args->staging_area_stored;
    void *temp_recvbuf = args->recvbuf_stored;
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(op, &params);
    ucg_algo_rd_iter_t *iter = &op->allreduce.rd_iter;
    ucg_rank_t peer;
    ucg_rank_t my_rank = vgroup->myrank;

    while ((peer = ucg_algo_rd_iter_base_value(iter)) != UCG_INVALID_RANK) {
        if (ucg_test_and_clear_flags(&op->flags, UCG_RD_BASE_SEND)) {
            status = ucg_planc_ucx_p2p_isend(temp_recvbuf, args->count, args->dt,
                                             peer, op->tag, vgroup, &params);
            UCG_CHECK_GOTO(status, out);
        }

        if (ucg_test_and_clear_flags(&op->flags, UCG_RD_BASE_RECV)) {
            status = ucg_planc_ucx_p2p_irecv(temp_staging_area, args->count, args->dt,
                                             peer, op->tag, vgroup, &params);
            UCG_CHECK_GOTO(status, out);
        }
        status = ucg_planc_ucx_p2p_testall(op->ucx_group, params.state);
        UCG_CHECK_GOTO(status, out);

        if (my_rank < peer) {
            status = ucg_op_reduce(args->op, temp_recvbuf, temp_staging_area, args->count, args->dt);
            args->staging_area_stored = temp_recvbuf;
            args->recvbuf_stored = temp_staging_area;
            void *temp = temp_recvbuf;
            temp_recvbuf = temp_staging_area;
            temp_staging_area = temp;
        } else {
            status = ucg_op_reduce(args->op, temp_staging_area, temp_recvbuf, args->count, args->dt);
        }

        UCG_CHECK_GOTO(status, out);
        /* increase iterator to enter next loop */
        ucg_algo_rd_iter_inc(iter);
        op->flags |= UCG_RD_BASE_SEND | UCG_RD_BASE_RECV;
    }
    if (temp_recvbuf != args->recvbuf) {
        ucg_dt_memcpy(args->recvbuf, args->count, args->dt, temp_recvbuf, args->count, args->dt);
    }
out:
    return status;
}

static ucg_status_t ucg_planc_ucx_allreduce_rd_op_proxy(ucg_planc_ucx_op_t *op)
{
    ucg_status_t status = UCG_OK;
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_coll_allreduce_args_t *args = &op->super.super.args.allreduce;
    void *temp_staging_area = args->staging_area_stored;
    void *recvbuf = args->recvbuf;
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(op, &params);
    ucg_algo_rd_iter_t *iter = &op->allreduce.rd_iter;
    ucg_rank_t peer;

    if (ucg_test_and_clear_flags(&op->flags, UCG_RD_PROXY_RECV)) {
        peer = ucg_algo_rd_iter_value_inc(iter);
        status = ucg_planc_ucx_p2p_irecv(temp_staging_area, args->count, args->dt,
                                         peer, op->tag, vgroup, &params);
        UCG_CHECK_GOTO(status, out);
    }
    status = ucg_planc_ucx_p2p_testall(op->ucx_group, params.state);
    UCG_CHECK_GOTO(status, out);

    if (ucg_test_and_clear_flags(&op->flags, UCG_RD_PROXY_REDUCE)) {
        status = ucg_op_reduce(args->op, temp_staging_area, recvbuf, args->count, args->dt);
        UCG_CHECK_GOTO(status, out);
    }

    if (ucg_test_flags(op->flags, UCG_RD_PROXY_BASE)) {
        status = ucg_planc_ucx_allreduce_rd_op_base(op);
        UCG_CHECK_GOTO(status, out);
        ucg_clear_flags(&op->flags, UCG_RD_PROXY_BASE);
    }

    if (ucg_test_and_clear_flags(&op->flags, UCG_RD_PROXY_SEND)) {
        peer = ucg_algo_rd_iter_value_inc(iter);
        status = ucg_planc_ucx_p2p_isend(recvbuf, args->count, args->dt,
                                         peer, op->tag, vgroup, &params);
        UCG_CHECK_GOTO(status, out);
    }
    status = ucg_planc_ucx_p2p_testall(op->ucx_group, params.state);
out:
    return status;
}

static ucg_status_t ucg_planc_ucx_allreduce_rd_op_extra(ucg_planc_ucx_op_t *op)
{
    ucg_status_t status = UCG_OK;
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_coll_allreduce_args_t *args = &op->super.super.args.allreduce;
    const void *sendbuf = (args->sendbuf != UCG_IN_PLACE) ? args->sendbuf : args->recvbuf;
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(op, &params);
    ucg_algo_rd_iter_t *iter = &op->allreduce.rd_iter;
    ucg_rank_t peer;
    if (ucg_test_and_clear_flags(&op->flags, UCG_RD_EXTRA_SEND)) {
        peer = ucg_algo_rd_iter_value_inc(iter);
        status = ucg_planc_ucx_p2p_isend(sendbuf, args->count, args->dt,
                                         peer, op->tag, vgroup, &params);
        UCG_CHECK_GOTO(status, out);
    }

    if (ucg_test_and_clear_flags(&op->flags, UCG_RD_EXTRA_RECV)) {
        peer = ucg_algo_rd_iter_value_inc(iter);
        status = ucg_planc_ucx_p2p_irecv(args->recvbuf, args->count, args->dt,
                                         peer, op->tag, vgroup, &params);
        UCG_CHECK_GOTO(status, out);
    }
    status = ucg_planc_ucx_p2p_testall(op->ucx_group, params.state);
out:
    return status;
}

static ucg_status_t ucg_planc_ucx_allreduce_rd_op_progress(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status;
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_algo_rd_iter_t *iter = &op->allreduce.rd_iter;
    ucg_algo_rd_iter_type_t type = ucg_algo_rd_iter_type(iter);

    if (type == UCG_ALGO_RD_ITER_BASE) {
        status = ucg_planc_ucx_allreduce_rd_op_base(op);
    } else if (type == UCG_ALGO_RD_ITER_PROXY) {
        status = ucg_planc_ucx_allreduce_rd_op_proxy(op);
    } else {
        status = ucg_planc_ucx_allreduce_rd_op_extra(op);
    }
    op->super.super.status = status;
    return status;
}

static ucg_status_t ucg_planc_ucx_allreduce_rd_op_trigger(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status;
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_planc_ucx_op_reset(op);

    ucg_algo_rd_iter_t *iter = &op->allreduce.rd_iter;
    ucg_algo_rd_iter_reset(iter);
    ucg_algo_rd_iter_type_t type = ucg_algo_rd_iter_type(iter);
    if (type == UCG_ALGO_RD_ITER_BASE || type == UCG_ALGO_RD_ITER_PROXY) {
        op->flags = UCG_RD_BASE_FLAGS;
        if (type == UCG_ALGO_RD_ITER_PROXY) {
            op->flags |= UCG_RD_PROXY_FLAGS;
        }
        ucg_coll_allreduce_args_t *args = &ucg_op->super.args.allreduce;
        args->staging_area_stored = op->staging_area - args->dt->true_lb;
        args->recvbuf_stored = args->recvbuf;
        if (args->sendbuf != UCG_IN_PLACE) {
            status = ucg_dt_memcpy(args->recvbuf, args->count, args->dt,
                                   args->sendbuf, args->count, args->dt);
            if (status != UCG_OK) {
                return status;
            }
        }
    } else {
        op->flags = UCG_RD_EXTRA_FLAGS;
    }

    status = ucg_planc_ucx_allreduce_rd_op_progress(ucg_op);
    return status == UCG_INPROGRESS ? UCG_OK : status;
}

static ucg_status_t ucg_planc_ucx_allreduce_rd_op_discard(ucg_plan_op_t *ucg_op)
{
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    if (op->staging_area != NULL) {
        ucg_free(op->staging_area);
    }
    UCG_CLASS_DESTRUCT(ucg_plan_op_t, &op->super);
    ucg_mpool_put(op);
    return UCG_OK;
}

ucg_planc_ucx_op_t *ucg_planc_ucx_allreduce_rd_op_new(ucg_planc_ucx_group_t *ucx_group,
                                                      ucg_vgroup_t *vgroup,
                                                      const ucg_coll_args_t *args)
{
    UCG_CHECK_NULL(NULL, ucx_group, vgroup, args);

    ucg_status_t status;
    ucg_planc_ucx_op_t *op = ucg_mpool_get(&ucx_group->context->op_mp);
    if (op == NULL) {
        goto err;
    }
    status = UCG_CLASS_CONSTRUCT(ucg_plan_op_t, &op->super, vgroup,
                                 ucg_planc_ucx_allreduce_rd_op_trigger,
                                 ucg_planc_ucx_allreduce_rd_op_progress,
                                 ucg_planc_ucx_allreduce_rd_op_discard,
                                 args);
    if (status != UCG_OK) {
        ucg_error("Failed to initialize super of ucx op");
        goto err_free_op;
    }
    ucg_planc_ucx_op_init(op, ucx_group);

    ucg_algo_rd_iter_t *iter = &op->allreduce.rd_iter;
    ucg_algo_rd_iter_init(iter, vgroup->size, vgroup->myrank);
    ucg_algo_rd_iter_type_t type = ucg_algo_rd_iter_type(iter);
    if (type == UCG_ALGO_RD_ITER_BASE || type == UCG_ALGO_RD_ITER_PROXY) {
        ucg_dt_t *dt = args->allreduce.dt;
        int32_t count = args->allreduce.count;
        int64_t data_size = dt->true_extent + (int64_t)dt->extent * (count - 1);
        ucg_assert(op->staging_area == NULL);
        op->staging_area = ucg_malloc(data_size, "allreduce rd op staging area");
        if (op->staging_area == NULL) {
            goto err_destruct;
        }
    }
    return op;

err_destruct:
    UCG_CLASS_DESTRUCT(ucg_plan_op_t, &op->super);
err_free_op:
    ucg_mpool_put(op);
err:
    return NULL;
}

ucg_status_t ucg_planc_ucx_allreduce_rd_prepare(ucg_vgroup_t *vgroup,
                                                const ucg_coll_args_t *args,
                                                ucg_plan_op_t **op)
{
    UCG_CHECK_NULL_INVALID(vgroup, args, op);

    ucg_planc_ucx_group_t *ucx_group = ucg_derived_of(vgroup, ucg_planc_ucx_group_t);
    ucg_planc_ucx_op_t *rd_op = ucg_planc_ucx_allreduce_rd_op_new(ucx_group, vgroup, args);
    if (rd_op == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    *op = &rd_op->super;
    return UCG_OK;
}