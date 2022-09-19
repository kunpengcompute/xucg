/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "barrier.h"
#include "planc_ucx_plan.h"
#include "planc_ucx_p2p.h"
#include "planc_ucx_global.h"
#include "core/ucg_group.h"
#include "core/ucg_dt.h"
#include "util/algo/ucg_rd.h"
#include "util/ucg_log.h"
#include "util/ucg_malloc.h"

/* op flags needed by barrier rd. */
enum {
    UCG_RD_BASE_SEND = UCG_BIT(0), /* send to peer base */
    UCG_RD_BASE_RECV = UCG_BIT(1), /* receive from peer base */
    UCG_RD_PROXY_RECV = UCG_BIT(2), /* receive from extra */
    UCG_RD_PROXY_BASE = UCG_BIT(3), /* base loop */
    UCG_RD_PROXY_SEND = UCG_BIT(4), /* send result to extra */
    UCG_RD_EXTRA_SEND = UCG_BIT(5), /* send to peer proxy */
    UCG_RD_EXTRA_RECV = UCG_BIT(6), /* receive from peer proxy */
};

#define UCG_RD_BASE_FLAGS UCG_RD_BASE_SEND | UCG_RD_BASE_RECV
#define UCG_RD_PROXY_FLAGS UCG_RD_PROXY_RECV | UCG_RD_PROXY_BASE | UCG_RD_PROXY_SEND
#define UCG_RD_EXTRA_FLAGS UCG_RD_EXTRA_SEND | UCG_RD_EXTRA_RECV

static ucg_status_t ucg_planc_ucx_barrier_rd_op_base(ucg_planc_ucx_op_t *op)
{
    ucg_status_t status = UCG_OK;
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(op, &params);
    ucg_algo_rd_iter_t *iter = &op->barrier.rd_iter;
    ucg_rank_t peer;
    ucg_dt_t *dt = ucg_dt_get_predefined(UCG_DT_TYPE_UINT8);
    while ((peer = ucg_algo_rd_iter_base_value(iter)) != UCG_INVALID_RANK) {
        if (ucg_test_and_clear_flags(&op->flags, UCG_RD_BASE_SEND)) {
            status = ucg_planc_ucx_p2p_isend(NULL, 0, dt, peer, op->tag, vgroup, &params);
            UCG_CHECK_GOTO(status, out);
        }
        if (ucg_test_and_clear_flags(&op->flags, UCG_RD_BASE_RECV)) {
            status = ucg_planc_ucx_p2p_irecv(NULL, 0, dt, peer, op->tag, vgroup, &params);
            UCG_CHECK_GOTO(status, out);
        }
        status = ucg_planc_ucx_p2p_testall(op->ucx_group, params.state);
        UCG_CHECK_GOTO(status, out);
        /* increase iterator to enter next loop */
        ucg_algo_rd_iter_inc(iter);
        op->flags |= UCG_RD_BASE_SEND | UCG_RD_BASE_RECV;
    }
out:
    return status;
}

static ucg_status_t ucg_planc_ucx_barrier_rd_op_proxy(ucg_planc_ucx_op_t *op)
{
    ucg_status_t status = UCG_OK;
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(op, &params);
    ucg_algo_rd_iter_t *iter = &op->barrier.rd_iter;
    ucg_rank_t peer;
    ucg_dt_t *dt = ucg_dt_get_predefined(UCG_DT_TYPE_UINT8);
    if (ucg_test_and_clear_flags(&op->flags, UCG_RD_PROXY_RECV)) {
        peer = ucg_algo_rd_iter_value_inc(iter);
        status = ucg_planc_ucx_p2p_irecv(NULL, 0, dt, peer, op->tag, vgroup, &params);
        UCG_CHECK_GOTO(status, out);
    }
    status = ucg_planc_ucx_p2p_testall(op->ucx_group, params.state);
    UCG_CHECK_GOTO(status, out);

    if (ucg_test_flags(op->flags, UCG_RD_PROXY_BASE)) {
        status = ucg_planc_ucx_barrier_rd_op_base(op);
        UCG_CHECK_GOTO(status, out);
        ucg_clear_flags(&op->flags, UCG_RD_PROXY_BASE);
    }

    if (ucg_test_and_clear_flags(&op->flags, UCG_RD_PROXY_SEND)) {
        peer = ucg_algo_rd_iter_value_inc(iter);
        status = ucg_planc_ucx_p2p_isend(NULL, 0, dt, peer, op->tag, vgroup, &params);
        UCG_CHECK_GOTO(status, out);
    }
    status = ucg_planc_ucx_p2p_testall(op->ucx_group, params.state);
out:
    return status;
}

static ucg_status_t ucg_planc_ucx_barrier_rd_op_extra(ucg_planc_ucx_op_t *op)
{
    ucg_status_t status = UCG_OK;
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(op, &params);
    ucg_algo_rd_iter_t *iter = &op->barrier.rd_iter;
    ucg_rank_t peer;
    ucg_dt_t *dt = ucg_dt_get_predefined(UCG_DT_TYPE_UINT8);
    if (ucg_test_and_clear_flags(&op->flags, UCG_RD_EXTRA_SEND)) {
        peer = ucg_algo_rd_iter_value_inc(iter);
        status = ucg_planc_ucx_p2p_isend(NULL, 0, dt, peer, op->tag, vgroup, &params);
        UCG_CHECK_GOTO(status, out);
    }

    if (ucg_test_and_clear_flags(&op->flags, UCG_RD_EXTRA_RECV)) {
        peer = ucg_algo_rd_iter_value_inc(iter);
        status = ucg_planc_ucx_p2p_irecv(NULL, 0, dt, peer, op->tag, vgroup, &params);
        UCG_CHECK_GOTO(status, out);
    }
    status = ucg_planc_ucx_p2p_testall(op->ucx_group, params.state);
out:
    return status;
}

static ucg_status_t ucg_planc_ucx_barrier_rd_op_progress(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status;
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_algo_rd_iter_t *iter = &op->barrier.rd_iter;
    ucg_algo_rd_iter_type_t type = ucg_algo_rd_iter_type(iter);
    if (type == UCG_ALGO_RD_ITER_BASE) {
        status = ucg_planc_ucx_barrier_rd_op_base(op);
    } else if (type == UCG_ALGO_RD_ITER_PROXY) {
        status = ucg_planc_ucx_barrier_rd_op_proxy(op);
    } else {
        status = ucg_planc_ucx_barrier_rd_op_extra(op);
    }
    op->super.super.status = status;
    return status;
}

static ucg_status_t ucg_planc_ucx_barrier_rd_op_trigger(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status;
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_planc_ucx_op_reset(op);

    ucg_algo_rd_iter_t *iter = &op->barrier.rd_iter;
    ucg_algo_rd_iter_reset(iter);
    ucg_algo_rd_iter_type_t type = ucg_algo_rd_iter_type(iter);
    if (type == UCG_ALGO_RD_ITER_BASE || type == UCG_ALGO_RD_ITER_PROXY) {
        op->flags = UCG_RD_BASE_FLAGS;
        if (type == UCG_ALGO_RD_ITER_PROXY) {
            op->flags |= UCG_RD_PROXY_FLAGS;
        }
    } else {
        op->flags = UCG_RD_EXTRA_FLAGS;
    }

    status = ucg_planc_ucx_barrier_rd_op_progress(ucg_op);
    return status == UCG_INPROGRESS ? UCG_OK : status;
}

static ucg_status_t ucg_planc_ucx_barrier_rd_op_discard(ucg_plan_op_t *ucg_op)
{
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    UCG_CLASS_DESTRUCT(ucg_plan_op_t, &op->super);
    ucg_mpool_put(op);
    return UCG_OK;
}

ucg_planc_ucx_op_t *ucg_planc_ucx_barrier_rd_op_new(ucg_planc_ucx_group_t *ucx_group,
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
                                 ucg_planc_ucx_barrier_rd_op_trigger,
                                 ucg_planc_ucx_barrier_rd_op_progress,
                                 ucg_planc_ucx_barrier_rd_op_discard,
                                 args);
    if (status != UCG_OK) {
        ucg_error("Failed to initialize super of ucx op");
        goto err_free_op;
    }
    ucg_planc_ucx_op_init(op, ucx_group);
    ucg_algo_rd_iter_init(&op->barrier.rd_iter, vgroup->size, vgroup->myrank);

    return op;

err_free_op:
    ucg_mpool_put(op);
err:
    return NULL;
}

ucg_status_t ucg_planc_ucx_barrier_rd_prepare(ucg_vgroup_t *vgroup,
                                                const ucg_coll_args_t *args,
                                                ucg_plan_op_t **op)
{
    UCG_CHECK_NULL_INVALID(vgroup, args, op);

    ucg_planc_ucx_group_t *ucx_group = ucg_derived_of(vgroup, ucg_planc_ucx_group_t);
    ucg_planc_ucx_op_t *rd_op = ucg_planc_ucx_barrier_rd_op_new(ucx_group, vgroup, args);
    if (rd_op == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    *op = &rd_op->super;
    return UCG_OK;
}
