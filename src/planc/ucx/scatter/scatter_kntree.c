/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "scatter.h"
#include "planc_ucx_plan.h"

enum {
    UCG_SCATTER_KNTREE_PARAMS = UCG_BIT(0),
    UCG_SCATTER_KNTREE_PARAMS_ALLOC_STAGING = UCG_BIT(1),
    UCG_SCATTER_KNTREE_DATA = UCG_BIT(2),
    UCG_SCATTER_KNTREE_DATA_INIT = UCG_BIT(3),
    UCG_SCATTER_KNTREE_DATA_RECV_FROM_PARENT = UCG_BIT(4),
    UCG_SCATTER_KNTREE_DATA_SEND_TO_CHILD = UCG_BIT(5),
    UCG_SCATTER_KNTREE_DATA_RECV = UCG_BIT(6),
    UCG_SCATTER_KNTREE_DATA_RECV_WAIT = UCG_BIT(7),
    UCG_SCATTER_KNTREE_DATA_SEND = UCG_BIT(8),
};

#define UCG_SCATTER_KNTREE_PARAMS_FLAGS UCG_SCATTER_KNTREE_PARAMS | \
                                         UCG_SCATTER_KNTREE_PARAMS_ALLOC_STAGING

#define UCG_SCATTER_KNTREE_DATA_FLAGS UCG_SCATTER_KNTREE_DATA | \
                                       UCG_SCATTER_KNTREE_DATA_INIT | \
                                       UCG_SCATTER_KNTREE_DATA_RECV_FROM_PARENT | \
                                       UCG_SCATTER_KNTREE_DATA_SEND_TO_CHILD | \
                                       UCG_SCATTER_KNTREE_DATA_RECV | \
                                       UCG_SCATTER_KNTREE_DATA_RECV_WAIT | \
                                       UCG_SCATTER_KNTREE_DATA_SEND

#define UCG_SCATTER_KNTREE_FLAGS UCG_SCATTER_KNTREE_PARAMS_FLAGS | \
                                  UCG_SCATTER_KNTREE_DATA_FLAGS

static ucg_status_t ucg_planc_ucx_scatter_kntree_op_data_recv(ucg_planc_ucx_op_t *op)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_group_t *ucx_group = op->ucx_group;
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_rank_t myrank = vgroup->myrank;
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(op, &params);
    ucg_algo_kntree_iter_t *iter = &op->scatter.kntree.kntree_iter;
    ucg_coll_scatter_args_t *args = &op->super.super.args.scatter;
    ucg_rank_t peer;

    peer = ucg_algo_kntree_iter_parent_value(iter);
    if (peer == UCG_INVALID_RANK) {
        ucg_assert(myrank == args->root);
        if (args->recvbuf != UCG_IN_PLACE) {
            const void *sbuf = args->sendbuf + args->sendcount * myrank * ucg_dt_extent(args->sendtype);
            int32_t scount = args->sendcount;
            status = ucg_dt_memcpy(args->recvbuf, args->recvcount, args->recvtype,
                                   sbuf, scount, args->sendtype);
            UCG_CHECK_GOTO(status, out);
        }
        return UCG_OK;
    }

    if (ucg_test_and_clear_flags(&op->flags, UCG_SCATTER_KNTREE_DATA_RECV)) {
        if (op->scatter.kntree.staging_count > 0) {
            //Recv myself data and my children's data
            int64_t recv_len = op->scatter.kntree.sendcount * op->scatter.kntree.sdtype_size * (op->scatter.kntree.staging_count + 1);
            status = ucg_planc_ucx_p2p_irecv(op->staging_area, recv_len,
                                             ucg_dt_get_predefined(UCG_DT_TYPE_UINT8),
                                             peer, op->tag, vgroup, &params);
            UCG_CHECK_GOTO(status, out);
        } else {
            //Recv myself data
            status = ucg_planc_ucx_p2p_irecv(args->recvbuf, args->recvcount * op->scatter.kntree.sdtype_size,
                                             ucg_dt_get_predefined(UCG_DT_TYPE_UINT8),
                                             peer, op->tag, vgroup, &params);
            UCG_CHECK_GOTO(status, out);
        }
    }

    if (ucg_test_flags(op->flags, UCG_SCATTER_KNTREE_DATA_RECV_WAIT)) {
        status = ucg_planc_ucx_p2p_testall(ucx_group, params.state);
        UCG_CHECK_GOTO(status, out);
        ucg_clear_flags(&op->flags, UCG_SCATTER_KNTREE_DATA_RECV_WAIT);
    }

    //Copy myself data
    if (op->scatter.kntree.staging_count > 0) {
        status = ucg_dt_memcpy(args->recvbuf, args->recvcount, args->recvtype,
                               op->staging_area, op->scatter.kntree.sendcount, args->sendtype);
    }

out:
    return status;
}

static ucg_status_t ucg_planc_ucx_scatter_kntree_op_data_send(ucg_planc_ucx_op_t *op)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_group_t *ucx_group = op->ucx_group;
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_rank_t myrank = vgroup->myrank;
    uint32_t group_size = vgroup->size;
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(op, &params);
    ucg_coll_scatter_args_t *args = &op->super.super.args.scatter;
    ucg_algo_kntree_iter_t *iter = &op->scatter.kntree.kntree_iter;
    ucg_rank_t peer;

    if (ucg_test_flags(op->flags, UCG_SCATTER_KNTREE_DATA_SEND)) {
        while ((peer = ucg_algo_kntree_iter_child_value(iter)) != UCG_INVALID_RANK) {
            if (myrank == args->root) {
                int32_t peer_subtree_size = ucg_algo_kntree_get_subtree_size(iter, peer);
                int32_t idx = (peer - myrank) % group_size;
                int64_t offset = args->sendcount * idx * ucg_dt_extent(args->sendtype);
                if (myrank != 0) {
                    memcpy(op->staging_area, args->sendbuf + myrank * args->sendcount * ucg_dt_extent(args->sendtype), 
                           (group_size - myrank) * args->sendcount * ucg_dt_extent(args->sendtype));
                    memcpy(op->staging_area + (group_size - myrank) * args->sendcount * ucg_dt_extent(args->sendtype),
                           args->sendbuf, myrank * args->sendcount * ucg_dt_extent(args->sendtype));

                    idx = (peer - myrank) % group_size;
                    offset = args->sendcount * idx * ucg_dt_extent(args->sendtype);
                    int64_t send_len = args->sendcount * ucg_dt_extent(args->sendtype) * peer_subtree_size;
                    status = ucg_planc_ucx_p2p_isend(op->staging_area + offset, send_len,
                                                     ucg_dt_get_predefined(UCG_DT_TYPE_UINT8),
                                                     peer, op->tag, vgroup,
                                                     &params);
                    UCG_CHECK_GOTO(status, out);
                } else {
                    idx = peer % group_size;
                    offset = args->sendcount * idx * ucg_dt_extent(args->sendtype);
                    int64_t send_len = args->sendcount * ucg_dt_extent(args->sendtype) * peer_subtree_size;
                    status = ucg_planc_ucx_p2p_isend(args->sendbuf + offset, send_len,
                                                     ucg_dt_get_predefined(UCG_DT_TYPE_UINT8),
                                                     peer, op->tag, vgroup,
                                                     &params);
                    UCG_CHECK_GOTO(status, out);
                }
            } else {
                int32_t peer_subtree_size = ucg_algo_kntree_get_subtree_size(iter, peer);
                UCG_CHECK_GOTO(status, out);
                int64_t send_len = op->scatter.kntree.sendcount * op->scatter.kntree.sdtype_size * peer_subtree_size;
                int64_t offset = op->scatter.kntree.sendcount * ((peer - myrank) % group_size)  * op->scatter.kntree.sdtype_size;
                status = ucg_planc_ucx_p2p_isend(op->staging_area + offset, send_len,
                                                 ucg_dt_get_predefined(UCG_DT_TYPE_UINT8),
                                                 peer, op->tag, vgroup, &params);
                UCG_CHECK_GOTO(status, out);
            }
            ucg_algo_kntree_iter_child_inc(iter);
        }
        ucg_clear_flags(&op->flags, UCG_SCATTER_KNTREE_DATA_SEND);
    }
    status = ucg_planc_ucx_p2p_testall(ucx_group, params.state);
out:
    return status;
}

static ucg_status_t ucg_planc_ucx_scatter_kntree_op_params(ucg_planc_ucx_op_t *op)
{
    ucg_status_t status = UCG_OK;

    if (ucg_test_and_clear_flags(&op->flags, UCG_SCATTER_KNTREE_PARAMS_ALLOC_STAGING)) {
        ucg_coll_scatter_args_t *args = &op->super.super.args.scatter;
        ucg_vgroup_t *vgroup = op->super.vgroup;
        ucg_rank_t myrank = vgroup->myrank;
        if ((myrank != args->root) && (op->scatter.kntree.staging_count > 0)) {
            int64_t size = 0;
            size = op->scatter.kntree.staging_count * op->scatter.kntree.sendcount * op->scatter.kntree.sdtype_size;
            op->staging_area = ucg_malloc(size, "scatter kntree staging area");
            if (op->staging_area == NULL) {
                return UCG_ERR_NO_MEMORY;
            }
        }
        if (myrank == args->root && myrank != 0) {
            int64_t size = 0;
            size = vgroup->size * op->scatter.kntree.sendcount * op->scatter.kntree.sdtype_size;
            op->staging_area = ucg_malloc(size, "scatter kntree staging area");
            if (op->staging_area == NULL) {
                return UCG_ERR_NO_MEMORY;
            }
        }
    }

    return status;
}

static ucg_status_t ucg_planc_ucx_scatter_kntree_op_data(ucg_planc_ucx_op_t *op)
{
    ucg_status_t status = UCG_OK;

    if (ucg_test_and_clear_flags(&op->flags, UCG_SCATTER_KNTREE_DATA_INIT)) {
        //kntree iter should be reset here for op data
        ucg_algo_kntree_iter_reset(&op->scatter.kntree.kntree_iter);
    }

    if (ucg_test_flags(op->flags, UCG_SCATTER_KNTREE_DATA_RECV_FROM_PARENT)) {
        status = ucg_planc_ucx_scatter_kntree_op_data_recv(op);
        UCG_CHECK_GOTO(status, out);
        ucg_clear_flags(&op->flags, UCG_SCATTER_KNTREE_DATA_RECV_FROM_PARENT);
    }

    if (ucg_test_flags(op->flags, UCG_SCATTER_KNTREE_DATA_SEND_TO_CHILD)) {
        status = ucg_planc_ucx_scatter_kntree_op_data_send(op);
        UCG_CHECK_GOTO(status, out);
        ucg_clear_flags(&op->flags, UCG_SCATTER_KNTREE_DATA_SEND_TO_CHILD);
    }

out:
    return status;
}

static ucg_status_t ucg_planc_ucx_scatter_kntree_op_progress(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    if (ucg_test_flags(op->flags, UCG_SCATTER_KNTREE_PARAMS)) {
        status = ucg_planc_ucx_scatter_kntree_op_params(op);
        UCG_CHECK_GOTO(status, out);
        ucg_clear_flags(&op->flags, UCG_SCATTER_KNTREE_PARAMS);
    }

    if (ucg_test_flags(op->flags, UCG_SCATTER_KNTREE_DATA)) {
        status = ucg_planc_ucx_scatter_kntree_op_data(op);
        UCG_CHECK_GOTO(status, out);
        op->scatter.kntree.first_trigger = 0;
        ucg_clear_flags(&op->flags, UCG_SCATTER_KNTREE_DATA);
    }

out:
    op->super.super.status = status;
    return status;
}

static ucg_status_t ucg_planc_ucx_scatter_kntree_op_trigger(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status;
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_planc_ucx_op_reset(op);
    ucg_algo_kntree_iter_reset(&op->scatter.kntree.kntree_iter);
    // for second trigger, we only need do op data
    if (op->scatter.kntree.first_trigger) {
        op->flags = UCG_SCATTER_KNTREE_FLAGS;
    } else {
        op->flags = UCG_SCATTER_KNTREE_DATA_FLAGS;
    }
    status = ucg_planc_ucx_scatter_kntree_op_progress(ucg_op);
    return status == UCG_INPROGRESS ? UCG_OK : status;
}

static inline ucg_status_t ucg_planc_ucx_scatter_kntree_op_discard(ucg_plan_op_t *ucg_op)
{
    return ucg_planc_ucx_op_discard(ucg_op);
}

static inline
ucg_status_t ucg_planc_ucx_scatter_kntree_op_init(ucg_planc_ucx_op_t *op,
                                                   ucg_planc_ucx_group_t *ucx_group,
                                                   const ucg_planc_ucx_scatter_config_t *config)
{
    ucg_planc_ucx_op_init(op, ucx_group);
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_rank_t myrank = vgroup->myrank;
    ucg_coll_scatter_args_t *args = &op->super.super.args.scatter;
    ucg_algo_kntree_iter_t *iter = &op->scatter.kntree.kntree_iter;

    ucg_algo_kntree_iter_init(iter, vgroup->size, config->kntree_degree,
                              args->root, vgroup->myrank, 1);
    op->scatter.kntree.staging_count = ucg_algo_kntree_get_subtree_size(iter, myrank) - 1;
    op->scatter.kntree.sdtype_size = ucg_dt_size(args->sendtype);
    op->scatter.kntree.sendcount = args->sendcount;
    op->scatter.kntree.first_trigger = 1;

    return UCG_OK;
}

ucg_planc_ucx_op_t *ucg_planc_ucx_scatter_kntree_op_new(ucg_planc_ucx_group_t *ucx_group,
                                                         ucg_vgroup_t *vgroup,
                                                         const ucg_coll_args_t *args,
                                                         const ucg_planc_ucx_scatter_config_t *config)
{
    UCG_CHECK_NULL(NULL, ucx_group, vgroup, args);

    ucg_planc_ucx_op_t *ucx_op = ucg_mpool_get(&ucx_group->context->op_mp);
    if (ucx_op == NULL) {
        goto err;
    }

    ucg_status_t status = UCG_CLASS_CONSTRUCT(ucg_plan_op_t, &ucx_op->super, vgroup,
                                              ucg_planc_ucx_scatter_kntree_op_trigger,
                                              ucg_planc_ucx_scatter_kntree_op_progress,
                                              ucg_planc_ucx_scatter_kntree_op_discard,
                                              args);
    if (status != UCG_OK) {
        ucg_error("Failed to initialize super of ucx op");
        goto err_free_op;
    }

    status = ucg_planc_ucx_scatter_kntree_op_init(ucx_op, ucx_group, config);
    if (status != UCG_OK) {
        ucg_error("Failed to initialize scatter ucx op");
        goto err_destruct_op;
    }

    return ucx_op;

err_destruct_op:
    UCG_CLASS_DESTRUCT(ucg_plan_op_t, &ucx_op->super);
err_free_op:
    ucg_mpool_put(ucx_op);
err:
    return NULL;
}

ucg_status_t ucg_planc_ucx_scatter_kntree_prepare(ucg_vgroup_t *vgroup,
                                                   const ucg_coll_args_t *args,
                                                   ucg_plan_op_t **op)
{
    UCG_CHECK_NULL_INVALID(vgroup, args, op);

    ucg_planc_ucx_group_t *ucx_group = ucg_derived_of(vgroup, ucg_planc_ucx_group_t);
    ucg_planc_ucx_scatter_config_t *config;
    config = UCG_PLANC_UCX_CONTEXT_BUILTIN_CONFIG_BUNDLE(ucx_group->context, scatter,
                                                         UCG_COLL_TYPE_SCATTER);
    ucg_planc_ucx_op_t *kntree_op = ucg_planc_ucx_scatter_kntree_op_new(ucx_group,
                                                                         vgroup,
                                                                         args,
                                                                         config);
    if (kntree_op == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    *op = &kntree_op->super;
    return UCG_OK;
}