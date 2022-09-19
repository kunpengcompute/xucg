/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "barrier.h"
#include "core/ucg_group.h"
#include "core/ucg_topo.h"
#include "planc_ucx_meta.h"
#include "util/ucg_log.h"

enum {
    UCG_FANIN_RECV_FROM_CHILD = UCG_BIT(0),
    UCG_FANIN_RECV_FROM_CHILD_RECV = UCG_BIT(1),
    UCG_FANIN_SEND_TO_PARENT = UCG_BIT(2),
    UCG_FANIN_SEND_TO_PARENT_SEND = UCG_BIT(3),
};

#define UCG_FANIN_KNTREE_FLAGS UCG_FANIN_RECV_FROM_CHILD | \
                               UCG_FANIN_RECV_FROM_CHILD_RECV | \
                               UCG_FANIN_SEND_TO_PARENT | \
                               UCG_FANIN_SEND_TO_PARENT_SEND

static ucg_status_t ucg_planc_ucx_fanin_kntree_op_recv(ucg_planc_ucx_op_t *op)
{
    ucg_status_t status = UCG_OK;
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(op, &params);
    ucg_algo_kntree_iter_t *iter = &op->barrier.fanin_iter;
    ucg_rank_t peer;
    while ((peer = ucg_algo_kntree_iter_child_value(iter)) != UCG_INVALID_RANK) {
        if (ucg_test_and_clear_flags(&op->flags, UCG_FANIN_RECV_FROM_CHILD_RECV)) {
            status = ucg_planc_ucx_p2p_irecv(NULL, 0, ucg_dt_get_predefined(UCG_DT_TYPE_UINT8),
                                             peer, op->tag, vgroup, &params);
            UCG_CHECK_GOTO(status, out);
        }
        /* increase iterator to enter next loop */
        ucg_algo_kntree_iter_child_inc(iter);
        op->flags |= UCG_FANIN_RECV_FROM_CHILD_RECV;
    }
    status = ucg_planc_ucx_p2p_testall(op->ucx_group, params.state);
out:
    return status;
}

static ucg_status_t ucg_planc_ucx_fanin_kntree_op_send(ucg_planc_ucx_op_t *op)
{
    ucg_status_t status = UCG_OK;
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(op, &params);
    ucg_algo_kntree_iter_t *iter = &op->barrier.fanin_iter;
    ucg_rank_t peer;
    peer = ucg_algo_kntree_iter_parent_value(iter);
    if (peer == UCG_INVALID_RANK) {
        return UCG_OK;
    }

    if (ucg_test_and_clear_flags(&op->flags, UCG_FANIN_SEND_TO_PARENT_SEND)) {
        status = ucg_planc_ucx_p2p_isend(NULL, 0, ucg_dt_get_predefined(UCG_DT_TYPE_UINT8),
                                         peer, op->tag, vgroup, &params);
        UCG_CHECK_GOTO(status, out);
    }
    status = ucg_planc_ucx_p2p_testall(op->ucx_group, params.state);
out:
    return status;
}

static ucg_status_t ucg_planc_ucx_fanin_kntree_op_progress(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(op, &params);

    /* 1. receive from child */
    if (ucg_test_flags(op->flags, UCG_FANIN_RECV_FROM_CHILD)) {
        status = ucg_planc_ucx_fanin_kntree_op_recv(op);
        UCG_CHECK_GOTO(status, out);
        ucg_clear_flags(&op->flags, UCG_FANIN_RECV_FROM_CHILD);
    }

    /* 2. send to my parent */
    if (ucg_test_flags(op->flags, UCG_FANIN_SEND_TO_PARENT)) {
        status = ucg_planc_ucx_fanin_kntree_op_send(op);
        UCG_CHECK_GOTO(status, out);
        ucg_clear_flags(&op->flags, UCG_FANIN_SEND_TO_PARENT);
    }

out:
    op->super.super.status = status;
    return status;
}

static ucg_status_t ucg_planc_ucx_fanin_kntree_op_trigger(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status;
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_planc_ucx_op_reset(op);

    ucg_algo_kntree_iter_t *iter = &op->barrier.fanin_iter;
    ucg_algo_kntree_iter_reset(iter);
    op->flags = UCG_FANIN_KNTREE_FLAGS;

    status = ucg_planc_ucx_fanin_kntree_op_progress(ucg_op);
    return status == UCG_INPROGRESS ? UCG_OK : status;
}

ucg_planc_ucx_op_t *ucg_planc_ucx_fanin_kntree_op_new(ucg_planc_ucx_group_t *ucx_group,
                                                      ucg_vgroup_t *vgroup,
                                                      const ucg_coll_args_t *args,
                                                      ucg_planc_ucx_fanin_config_t *config)
{
    UCG_CHECK_NULL(NULL, ucx_group, vgroup, config);

    ucg_planc_ucx_op_t *op = ucg_mpool_get(&ucx_group->context->op_mp);
    if (op == NULL) {
        goto err;
    }

    ucg_status_t status;
    status = UCG_CLASS_CONSTRUCT(ucg_plan_op_t, &op->super, vgroup,
                                 ucg_planc_ucx_fanin_kntree_op_trigger,
                                 ucg_planc_ucx_fanin_kntree_op_progress,
                                 ucg_planc_ucx_op_discard,
                                 args);
    if (status != UCG_OK) {
        ucg_error("Failed to initialize super of ucx op");
        goto err_free_op;
    }
    ucg_planc_ucx_op_init(op, ucx_group);

    ucg_algo_kntree_iter_t *iter = &op->barrier.fanin_iter;
    ucg_algo_kntree_iter_init(iter, vgroup->size, config->kntree_degree,
                              0, vgroup->myrank, 0);

    return op;

err_free_op:
    ucg_mpool_put(op);
err:
    return NULL;
}

static ucg_status_t ucg_planc_ucx_barrier_add_bcast_topo_group_op(ucg_plan_meta_op_t *meta_op,
                                                                  ucg_planc_ucx_group_t *ucx_group,
                                                                  ucg_vgroup_t *vgroup,
                                                                  const ucg_coll_args_t *args,
                                                                  const ucg_planc_ucx_bcast_config_t *config,
                                                                  ucg_topo_group_type_t type)
{
    ucg_topo_group_t *topo_group;
    topo_group = ucg_topo_get_group(vgroup->group->topo, type);
    if (topo_group->state == UCG_TOPO_GROUP_STATE_DISABLE) {
        /* I'm not in the topo group. */
        return ucg_planc_ucx_add_empty_op(meta_op, ucx_group, vgroup);
    }

    if (topo_group->state != UCG_TOPO_GROUP_STATE_ENABLE) {
        /* The group state is incorrect. */
        return UCG_ERR_NO_RESOURCE;
    }

    ucg_planc_ucx_op_t *ucx_op;
    ucx_op = ucg_planc_ucx_bcast_kntree_op_new(ucx_group,
                                               &topo_group->super,
                                               args, config);
    if (ucx_op == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    return ucg_plan_meta_op_add(meta_op, &ucx_op->super);
}

static ucg_status_t ucg_planc_ucx_barrier_add_fanin_topo_group_op(ucg_plan_meta_op_t *meta_op,
                                                                  ucg_planc_ucx_group_t *ucx_group,
                                                                  ucg_vgroup_t *vgroup,
                                                                  const ucg_coll_args_t *args,
                                                                  ucg_planc_ucx_fanin_config_t *config,
                                                                  ucg_topo_group_type_t type)
{
    ucg_topo_group_t *topo_group;
    topo_group = ucg_topo_get_group(vgroup->group->topo, type);
    if (topo_group->state == UCG_TOPO_GROUP_STATE_DISABLE) {
        /* I'm not in the topo group. */
        return ucg_planc_ucx_add_empty_op(meta_op, ucx_group, vgroup);
    }

    if (topo_group->state != UCG_TOPO_GROUP_STATE_ENABLE) {
        /* The group state is incorrect. */
        return UCG_ERR_NO_RESOURCE;
    }

    ucg_planc_ucx_op_t *ucx_op;
    ucx_op = ucg_planc_ucx_fanin_kntree_op_new(ucx_group, &topo_group->super,
                                               args, config);
    if (ucx_op == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    return ucg_plan_meta_op_add(meta_op, &ucx_op->super);
}

ucg_status_t ucg_planc_ucx_barrier_add_fanin_kntree_op(ucg_plan_meta_op_t *meta_op,
                                                       ucg_planc_ucx_group_t *ucx_group,
                                                       ucg_vgroup_t *vgroup,
                                                       const ucg_coll_args_t *args,
                                                       const ucg_planc_ucx_barrier_config_t *config,
                                                       ucg_topo_group_type_t group_type)
{
    ucg_planc_ucx_fanin_config_t fanin_config;
    if ((group_type == UCG_TOPO_GROUP_TYPE_NODE) ||
        (group_type == UCG_TOPO_GROUP_TYPE_SOCKET) ||
        (group_type == UCG_TOPO_GROUP_TYPE_SOCKET_LEADER)) {
        fanin_config.kntree_degree = config->fanin_intra_degree;
    } else {
        fanin_config.kntree_degree = config->fanin_inter_degree;
    }

    ucg_coll_args_t dummy_args;

    return ucg_planc_ucx_barrier_add_fanin_topo_group_op(meta_op, ucx_group, vgroup,
                                                         &dummy_args, &fanin_config, group_type);
}

ucg_status_t ucg_planc_ucx_barrier_add_barrier_rd_op(ucg_plan_meta_op_t *meta_op,
                                                     ucg_planc_ucx_group_t *ucx_group,
                                                     ucg_vgroup_t *vgroup,
                                                     const ucg_coll_args_t *args,
                                                     ucg_topo_group_type_t group_type)
{
    ucg_topo_group_t *topo_group;
    topo_group = ucg_topo_get_group(vgroup->group->topo, group_type);
    if (topo_group->state == UCG_TOPO_GROUP_STATE_DISABLE) {
        /* I'm not in the topo group. */
        return ucg_planc_ucx_add_empty_op(meta_op, ucx_group, vgroup);
    }

    if (topo_group->state != UCG_TOPO_GROUP_STATE_ENABLE) {
        /* The group state is incorrect. */
        return UCG_ERR_NO_RESOURCE;
    }

    ucg_planc_ucx_op_t *ucx_op;
    ucx_op = ucg_planc_ucx_barrier_rd_op_new(ucx_group, &topo_group->super, args);
    if (ucx_op == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    return ucg_plan_meta_op_add(meta_op, &ucx_op->super);
}

ucg_status_t ucg_planc_ucx_barrier_add_fanout_kntree_op(ucg_plan_meta_op_t *meta_op,
                                                        ucg_planc_ucx_group_t *ucx_group,
                                                        ucg_vgroup_t *vgroup,
                                                        const ucg_coll_args_t *args,
                                                        const ucg_planc_ucx_barrier_config_t *config,
                                                        ucg_topo_group_type_t group_type)
{
    ucg_planc_ucx_bcast_config_t kntree_config;
    if ((group_type == UCG_TOPO_GROUP_TYPE_NODE) ||
        (group_type == UCG_TOPO_GROUP_TYPE_SOCKET) ||
        (group_type == UCG_TOPO_GROUP_TYPE_SOCKET_LEADER)) {
        kntree_config.kntree_degree = config->fanout_intra_degree;
    } else {
        kntree_config.kntree_degree = config->fanout_inter_degree;
    }

    ucg_coll_args_t bcast_args;
    bcast_args.bcast.buffer = NULL;
    bcast_args.bcast.count = 0;
    bcast_args.bcast.dt = ucg_dt_get_predefined(UCG_DT_TYPE_UINT8);
    bcast_args.bcast.root = UCG_TOPO_GROUP_LEADER;

    return ucg_planc_ucx_barrier_add_bcast_topo_group_op(meta_op, ucx_group, vgroup,
                                                         &bcast_args, &kntree_config,
                                                         group_type);
}