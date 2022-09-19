/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "bcast.h"
#include "planc_ucx_plan.h"
#include "planc_ucx_p2p.h"
#include "core/ucg_dt.h"
#include "core/ucg_group.h"
#include "util/algo/ucg_kntree.h"
#include "util/ucg_log.h"
#include "util/ucg_malloc.h"

enum {
    UCG_BCAST_ADJUST_ROOT = UCG_BIT(0),
    UCG_BCAST_ADJUST_ROOT_SEND = UCG_BIT(1),
    UCG_BCAST_ADJUST_ROOT_RECV = UCG_BIT(2),
    UCG_BCAST_RECV_FROM_PARENT = UCG_BIT(3),
    UCG_BCAST_RECV_FROM_PARENT_RECV = UCG_BIT(4),
};

#define UCG_BCAST_KNTREE_FLAGS UCG_BCAST_ADJUST_ROOT | \
                               UCG_BCAST_ADJUST_ROOT_SEND | \
                               UCG_BCAST_ADJUST_ROOT_RECV | \
                               UCG_BCAST_RECV_FROM_PARENT | \
                               UCG_BCAST_RECV_FROM_PARENT_RECV

static ucg_status_t ucg_planc_ucx_bcast_kntree_op_adjust_root(ucg_planc_ucx_op_t *op)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_group_t *ucx_group = op->ucx_group;
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_rank_t myrank = vgroup->myrank;
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(op, &params);
    ucg_coll_bcast_args_t *args = &op->super.super.args.bcast;
    void *buffer = args->buffer;
    ucg_algo_kntree_iter_t *iter = &op->bcast.kntree_iter;
    ucg_rank_t iter_root = ucg_algo_kntree_iter_root_value(iter);

    if (iter_root == args->root || (myrank != args->root && myrank != iter_root)) {
        return UCG_OK;
    }

    if (myrank == args->root &&
        ucg_test_and_clear_flags(&op->flags, UCG_BCAST_ADJUST_ROOT_SEND)) {
        status = ucg_planc_ucx_p2p_isend(buffer, args->count, args->dt,
                                         iter_root, op->tag, vgroup, &params);
        UCG_CHECK_GOTO(status, out);
    }

    if (myrank == iter_root &&
        ucg_test_and_clear_flags(&op->flags, UCG_BCAST_ADJUST_ROOT_RECV)) {
        status = ucg_planc_ucx_p2p_irecv(buffer, args->count, args->dt,
                                         args->root, op->tag, vgroup, &params);
        UCG_CHECK_GOTO(status, out);
    }
    status = ucg_planc_ucx_p2p_testall(ucx_group, params.state);
out:
    return status;
}

static ucg_status_t ucg_planc_ucx_bcast_kntree_op_recv_from_parent(ucg_planc_ucx_op_t *op)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_group_t *ucx_group = op->ucx_group;
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(op, &params);
    ucg_coll_bcast_args_t *args = &op->super.super.args.bcast;
    ucg_algo_kntree_iter_t *iter = &op->bcast.kntree_iter;
    ucg_rank_t peer;

    peer = ucg_algo_kntree_iter_parent_value(iter);
    if (peer == UCG_INVALID_RANK) {
        return UCG_OK;
    }

    if (ucg_test_and_clear_flags(&op->flags, UCG_BCAST_RECV_FROM_PARENT_RECV)) {
        status = ucg_planc_ucx_p2p_irecv(args->buffer, args->count, args->dt,
                                         peer, op->tag, vgroup, &params);
        UCG_CHECK_GOTO(status, out);
    }
    status = ucg_planc_ucx_p2p_testall(ucx_group, params.state);
out:
    return status;
}

static ucg_status_t ucg_planc_ucx_bcast_kntree_op_progress(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_planc_ucx_group_t *ucx_group = op->ucx_group;
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(op, &params);
    ucg_algo_kntree_iter_t *iter = &op->bcast.kntree_iter;
    ucg_coll_bcast_args_t *args = &ucg_op->super.args.bcast;
    ucg_rank_t peer;

    /* 1. adjust root */
    if (ucg_test_flags(op->flags, UCG_BCAST_ADJUST_ROOT)) {
        status = ucg_planc_ucx_bcast_kntree_op_adjust_root(op);
        UCG_CHECK_GOTO(status, out);
        ucg_clear_flags(&op->flags, UCG_BCAST_ADJUST_ROOT);
    }
    /* 2. receive from parent */
    if (ucg_test_flags(op->flags, UCG_BCAST_RECV_FROM_PARENT)) {
        status = ucg_planc_ucx_bcast_kntree_op_recv_from_parent(op);
        UCG_CHECK_GOTO(status, out);
        ucg_clear_flags(&op->flags, UCG_BCAST_RECV_FROM_PARENT);
    }
    /* 3. send to my children */
    while ((peer = ucg_algo_kntree_iter_child_value(iter)) != UCG_INVALID_RANK) {
        status = ucg_planc_ucx_p2p_isend(args->buffer, args->count, args->dt,
                                         peer, op->tag, vgroup, &params);
        UCG_CHECK_GOTO(status, out);
        ucg_algo_kntree_iter_child_inc(iter);
    }
    status = ucg_planc_ucx_p2p_testall(ucx_group, params.state);
out:
    op->super.super.status = status;
    return status;
}

static ucg_status_t ucg_planc_ucx_bcast_kntree_op_trigger(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status;
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_planc_ucx_op_reset(op);
    ucg_algo_kntree_iter_reset(&op->bcast.kntree_iter);
    op->flags = UCG_BCAST_KNTREE_FLAGS;
    status = ucg_planc_ucx_bcast_kntree_op_progress(ucg_op);
    return status == UCG_INPROGRESS ? UCG_OK : status;
}

static ucg_status_t ucg_planc_ucx_bcast_kntree_op_discard(ucg_plan_op_t *ucg_op)
{
    UCG_CLASS_DESTRUCT(ucg_plan_op_t, ucg_op);
    ucg_mpool_put(ucg_op);
    return UCG_OK;
}

ucg_planc_ucx_op_t *ucg_planc_ucx_bcast_kntree_op_new(ucg_planc_ucx_group_t *ucx_group,
                                                      ucg_vgroup_t *vgroup,
                                                      const ucg_coll_args_t *args,
                                                      const ucg_planc_ucx_bcast_config_t *config)
{
    UCG_CHECK_NULL(NULL, ucx_group, vgroup, args, config);

    ucg_planc_ucx_op_t *ucx_op = ucg_mpool_get(&ucx_group->context->op_mp);
    if (ucx_op == NULL) {
        goto err;
    }

    ucg_status_t status = UCG_CLASS_CONSTRUCT(ucg_plan_op_t, &ucx_op->super, vgroup,
                                              ucg_planc_ucx_bcast_kntree_op_trigger,
                                              ucg_planc_ucx_bcast_kntree_op_progress,
                                              ucg_planc_ucx_bcast_kntree_op_discard,
                                              args);
    if (status != UCG_OK) {
        ucg_error("Failed to initialize super of ucx op");
        goto err_free_op;
    }
    ucg_planc_ucx_op_init(ucx_op, ucx_group);
    ucg_algo_kntree_iter_t *iter = &ucx_op->bcast.kntree_iter;
    if (config->root_adjust) {
        /* Using a fixed root avoids tree changes that will reduce the number of
           connections. We chose to use rank 0 as the fixed root. */
        ucg_algo_kntree_iter_init(iter, vgroup->size, config->kntree_degree,
                                  0, vgroup->myrank, 1);
    } else {
        ucg_algo_kntree_iter_init(iter, vgroup->size, config->kntree_degree,
                                  args->bcast.root, vgroup->myrank, 1);
    }

    return ucx_op;

err_free_op:
    ucg_mpool_put(ucx_op);
err:
    return NULL;
}

ucg_status_t ucg_planc_ucx_bcast_kntree_prepare(ucg_vgroup_t *vgroup,
                                                const ucg_coll_args_t *args,
                                                ucg_plan_op_t **op)
{
    UCG_CHECK_NULL_INVALID(vgroup, args, op);

    ucg_planc_ucx_group_t *ucx_group = ucg_derived_of(vgroup, ucg_planc_ucx_group_t);
    ucg_planc_ucx_bcast_config_t *config;
    config = UCG_PLANC_UCX_CONTEXT_BUILTIN_CONFIG_BUNDLE(ucx_group->context, bcast,
                                                 UCG_COLL_TYPE_BCAST);
    ucg_planc_ucx_op_t *ucx_op = ucg_planc_ucx_bcast_kntree_op_new(ucx_group,
                                                                   vgroup,
                                                                   args, config);
    if (ucx_op == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    *op = &ucx_op->super;
    return UCG_OK;
}