/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "reduce.h"
#include "planc_ucx_plan.h"
#include "util/ucg_log.h"
#include "util/ucg_math.h"

enum {
    UCG_REDUCE_RECV_FROM_CHILD = UCG_BIT(0),
    UCG_REDUCE_RECV_FROM_CHILD_RECV = UCG_BIT(1),
    UCG_REDUCE_SEND_TO_PARENT = UCG_BIT(2),
    UCG_REDUCE_SEND_TO_PARENT_SEND = UCG_BIT(3),
};

#define UCG_REDUCE_KNTREE_FLAGS UCG_REDUCE_RECV_FROM_CHILD | \
                                UCG_REDUCE_RECV_FROM_CHILD_RECV | \
                                UCG_REDUCE_SEND_TO_PARENT | \
                                UCG_REDUCE_SEND_TO_PARENT_SEND

static ucg_status_t ucg_planc_ucx_reduce_kntree_check(ucg_vgroup_t *vgroup,
                                                      const ucg_coll_args_t *args)
{
    ucg_op_flag_t flags = args->reduce.op->flags;
    if (!(flags & UCG_OP_FLAG_IS_COMMUTATIVE)) {
        ucg_info("Reduce kntree don't support non-commutative op");
        return UCG_ERR_UNSUPPORTED;
    }
    return UCG_OK;
}

static ucg_status_t ucg_planc_ucx_reduce_kntree_op_recv_and_reduce(ucg_planc_ucx_op_t *op)
{
    ucg_status_t status = UCG_OK;
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_coll_reduce_args_t *args = &op->super.super.args.reduce;
    void *recvbuf = args->recvbuf;
    ucg_dt_t *dt = args->dt;
    int32_t count = args->count;
    void *staging_area;
    uint64_t data_size = dt->true_extent + (uint64_t)dt->extent * (count - 1);
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(op, &params);
    ucg_algo_kntree_iter_t *iter = &op->reduce.kntree_iter;
    ucg_rank_t peer;
    int idx = 0;

    if (ucg_test_and_clear_flags(&op->flags, UCG_REDUCE_RECV_FROM_CHILD_RECV)) {
        while ((peer = ucg_algo_kntree_iter_child_value(iter)) != UCG_INVALID_RANK) {
            params.request = &op->reduce.requests[idx];
            staging_area = op->staging_area + idx * data_size;
            status = ucg_planc_ucx_p2p_irecv(staging_area, args->count, args->dt,
                                             peer, op->tag, vgroup, &params);
            UCG_CHECK_GOTO(status, out);
            /* increase iterator to enter next loop */
            ucg_algo_kntree_iter_child_inc(iter);
            idx++;
        }
    }

    for (idx = 0; idx < op->reduce.requests_count; idx++) {
        if (op->reduce.req_bitmap & UCG_BIT(idx)) {
            continue;
        }
        status = ucg_planc_ucx_p2p_test(op->ucx_group, &op->reduce.requests[idx]);
        if (status == UCG_INPROGRESS) {
            continue;
        }
        UCG_CHECK_GOTO(status, out);
        staging_area = op->staging_area + idx * data_size;
        status = ucg_op_reduce(args->op, staging_area, recvbuf, args->count, args->dt);
        UCG_CHECK_GOTO(status, out);
        op->reduce.req_bitmap |= UCG_BIT(idx);
    }

    status = (ucg_popcount(op->reduce.req_bitmap) == op->reduce.requests_count) ? UCG_OK : UCG_INPROGRESS;

out:
    return status;
}

static ucg_status_t ucg_planc_ucx_reduce_kntree_op_send(ucg_planc_ucx_op_t *op)
{
    ucg_status_t status = UCG_OK;
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_coll_reduce_args_t *args = &op->super.super.args.reduce;
    const void *sendbuf = args->recvbuf;
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(op, &params);
    ucg_algo_kntree_iter_t *iter = &op->reduce.kntree_iter;
    ucg_rank_t peer;

    peer = ucg_algo_kntree_iter_parent_value(iter);
    if (peer == UCG_INVALID_RANK) {
        return UCG_OK;
    }

    if (ucg_test_and_clear_flags(&op->flags, UCG_REDUCE_SEND_TO_PARENT_SEND)) {
        status = ucg_planc_ucx_p2p_isend(sendbuf, args->count, args->dt,
                                         peer, op->tag, vgroup, &params);
        UCG_CHECK_GOTO(status, out);
    }
    status = ucg_planc_ucx_p2p_testall(op->ucx_group, params.state);
out:
    return status;
}

static ucg_status_t ucg_planc_ucx_reduce_kntree_op_progress(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);

    /* 1. receive & reduce from child */
    if (ucg_test_flags(op->flags, UCG_REDUCE_RECV_FROM_CHILD)) {
        status = ucg_planc_ucx_reduce_kntree_op_recv_and_reduce(op);
        UCG_CHECK_GOTO(status, out);
        ucg_clear_flags(&op->flags, UCG_REDUCE_RECV_FROM_CHILD);
    }

    /* 2. send to my parent */
    if (ucg_test_flags(op->flags, UCG_REDUCE_SEND_TO_PARENT)) {
        status = ucg_planc_ucx_reduce_kntree_op_send(op);
        UCG_CHECK_GOTO(status, out);
        ucg_clear_flags(&op->flags, UCG_REDUCE_SEND_TO_PARENT);
    }

out:
    op->super.super.status = status;
    return status;
}

static ucg_status_t ucg_planc_ucx_reduce_kntree_op_trigger(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status;
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_planc_ucx_op_reset(op);

    ucg_algo_kntree_iter_t *iter = &op->reduce.kntree_iter;
    ucg_algo_kntree_iter_reset(iter);
    op->reduce.req_bitmap = 0;
    for (int i = 0; i < op->reduce.requests_count; i++) {
        op->reduce.requests[i] = NULL;
    }
    op->flags = UCG_REDUCE_KNTREE_FLAGS;

    ucg_coll_reduce_args_t *args = &ucg_op->super.args.reduce;
    if (args->sendbuf != UCG_IN_PLACE) {
        status = ucg_dt_memcpy(args->recvbuf, args->count, args->dt,
                               args->sendbuf, args->count, args->dt);
        if (status != UCG_OK) {
            return status;
        }
    }

    status = ucg_planc_ucx_reduce_kntree_op_progress(ucg_op);
    return status == UCG_INPROGRESS ? UCG_OK : status;
}

static ucg_status_t ucg_planc_ucx_reduce_kntree_op_discard(ucg_plan_op_t *ucg_op)
{
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);

    if (op->reduce.requests != NULL) {
        ucg_free(op->reduce.requests);
    }

    if (op->staging_area != NULL) {
        ucg_free(op->staging_area);
    }

    UCG_CLASS_DESTRUCT(ucg_plan_op_t, &op->super);
    ucg_mpool_put(op);
    return UCG_OK;
}

static inline
ucg_status_t ucg_planc_ucx_reduce_kntree_op_init(ucg_planc_ucx_op_t *op,
                                                 ucg_planc_ucx_group_t *ucx_group,
                                                 ucg_planc_ucx_reduce_config_t *config)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_op_init(op, ucx_group);

    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_algo_kntree_iter_t *iter = &op->reduce.kntree_iter;
    const ucg_coll_reduce_args_t *coll_args = &op->super.super.args.reduce;
    ucg_algo_kntree_iter_init(iter, vgroup->size, config->kntree_degree,
                              coll_args->root, vgroup->myrank, 0);

    ucg_dt_t *dt = coll_args->dt;
    int32_t count = coll_args->count;
    int64_t data_size = dt->true_extent + (int64_t)dt->extent * (count - 1);
    int32_t requests_count = 0;
    while (ucg_algo_kntree_iter_child_value(iter) != UCG_INVALID_RANK) {
        requests_count++;
        ucg_algo_kntree_iter_child_inc(iter);
    }
    op->staging_area = ucg_malloc(data_size * requests_count, "reduce kntree op staging area");
    if (op->staging_area == NULL) {
        status = UCG_ERR_NO_MEMORY;
    }

    op->reduce.requests = ucg_malloc(sizeof(ucg_planc_ucx_p2p_req_t *) * requests_count,
                                     "reduce kntree requests");
    if (op->reduce.requests == NULL) {
        ucg_free(op->staging_area);
        status = UCG_ERR_NO_MEMORY;
    }

    op->reduce.requests_count = requests_count;

    return status;
}

ucg_planc_ucx_op_t *ucg_planc_ucx_reduce_kntree_op_new(ucg_planc_ucx_group_t *ucx_group,
                                                       ucg_vgroup_t *vgroup,
                                                       const ucg_coll_args_t *args,
                                                       ucg_planc_ucx_reduce_config_t *config)
{
    UCG_CHECK_NULL(NULL, ucx_group, vgroup, args, config);

    ucg_planc_ucx_op_t *op = ucg_mpool_get(&ucx_group->context->op_mp);
    if (op == NULL) {
        goto err;
    }

    ucg_status_t status;
    status = UCG_CLASS_CONSTRUCT(ucg_plan_op_t, &op->super, vgroup,
                                 ucg_planc_ucx_reduce_kntree_op_trigger,
                                 ucg_planc_ucx_reduce_kntree_op_progress,
                                 ucg_planc_ucx_reduce_kntree_op_discard,
                                 args);
    if (status != UCG_OK) {
        ucg_error("Failed to initialize super of ucx op");
        goto err_free_op;
    }

    status = ucg_planc_ucx_reduce_kntree_op_init(op, ucx_group, config);
    if (status != UCG_OK) {
        ucg_error("Failed to initialize ucx op");
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

ucg_status_t ucg_planc_ucx_reduce_kntree_prepare(ucg_vgroup_t *vgroup,
                                                 const ucg_coll_args_t *args,
                                                 ucg_plan_op_t **op)
{
    UCG_CHECK_NULL_INVALID(vgroup, args, op);

    ucg_status_t status;
    status = ucg_planc_ucx_reduce_kntree_check(vgroup, args);
    if (status != UCG_OK) {
        return UCG_ERR_UNSUPPORTED;
    }

    ucg_planc_ucx_group_t *ucx_group = ucg_derived_of(vgroup, ucg_planc_ucx_group_t);
    ucg_planc_ucx_reduce_config_t *config;
    config = UCG_PLANC_UCX_CONTEXT_BUILTIN_CONFIG_BUNDLE(ucx_group->context, reduce,
                                                         UCG_COLL_TYPE_REDUCE);
    ucg_planc_ucx_op_t *ucx_op = ucg_planc_ucx_reduce_kntree_op_new(ucx_group,
                                                                    vgroup,
                                                                    args, config);
    if (ucx_op == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    *op = &ucx_op->super;
    return UCG_OK;
}