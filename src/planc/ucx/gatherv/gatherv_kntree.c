/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2023. All rights reserved.
 */

#include "gatherv.h"
#include "planc_ucx_plan.h"

enum {
    UCG_GATHERV_KNTREE_PARAMS = UCG_BIT(0),
    UCG_GATHERV_KNTREE_PARAMS_RECV_FROM_PARENT = UCG_BIT(1),
    UCG_GATHERV_KNTREE_PARAMS_SEND_TO_CHILD = UCG_BIT(2),
    UCG_GATHERV_KNTREE_PARAMS_RECV = UCG_BIT(3),
    UCG_GATHERV_KNTREE_PARAMS_SEND = UCG_BIT(4),
    UCG_GATHERV_KNTREE_PARAMS_ALLOC_STAGING = UCG_BIT(5),
    UCG_GATHERV_KNTREE_DATA = UCG_BIT(6),
    UCG_GATHERV_KNTREE_DATA_INIT = UCG_BIT(7),
    UCG_GATHERV_KNTREE_DATA_RECV_FROM_PARENT = UCG_BIT(8),
    UCG_GATHERV_KNTREE_DATA_SEND_TO_CHILD = UCG_BIT(9),
    UCG_GATHERV_KNTREE_DATA_RECV = UCG_BIT(10),
    UCG_GATHERV_KNTREE_DATA_RECV_WAIT = UCG_BIT(11),
    UCG_GATHERV_KNTREE_DATA_SEND = UCG_BIT(12),
    UCG_GATHERV_KNTREE_DATA_ROOT_COPY = UCG_BIT(13),
};

#define UCG_GATHERV_KNTREE_PARAMS_FLAGS UCG_GATHERV_KNTREE_PARAMS | \
                                        UCG_GATHERV_KNTREE_PARAMS_RECV_FROM_PARENT | \
                                        UCG_GATHERV_KNTREE_PARAMS_SEND_TO_CHILD | \
                                        UCG_GATHERV_KNTREE_PARAMS_RECV | \
                                        UCG_GATHERV_KNTREE_PARAMS_SEND | \
                                        UCG_GATHERV_KNTREE_PARAMS_ALLOC_STAGING

#define UCG_GATHERV_KNTREE_DATA_FLAGS UCG_GATHERV_KNTREE_DATA | \
                                      UCG_GATHERV_KNTREE_DATA_INIT | \
                                      UCG_GATHERV_KNTREE_DATA_RECV_FROM_PARENT | \
                                      UCG_GATHERV_KNTREE_DATA_SEND_TO_CHILD | \
                                      UCG_GATHERV_KNTREE_DATA_RECV | \
                                      UCG_GATHERV_KNTREE_DATA_RECV_WAIT | \
                                      UCG_GATHERV_KNTREE_DATA_SEND | \
                                      UCG_GATHERV_KNTREE_DATA_ROOT_COPY

#define UCG_GATHERV_KNTREE_FLAGS UCG_GATHERV_KNTREE_PARAMS_FLAGS | \
                                  UCG_GATHERV_KNTREE_DATA_FLAGS

static ucg_status_t ucg_planc_ucx_gatherv_kntree_op_params_recv(ucg_planc_ucx_op_t *op)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_group_t *ucx_group = op->ucx_group;
    ucg_vgroup_t *vgroup = op->super.vgroup;
    uint32_t group_size = vgroup->size;
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(op, &params);
    ucg_algo_kntree_iter_t *iter = &op->gatherv.kntree.kntree_iter;
    ucg_rank_t peer;

    peer = ucg_algo_kntree_iter_parent_value(iter);
    if (peer == UCG_INVALID_RANK) {
        return UCG_OK;
    }

    if (ucg_test_and_clear_flags(&op->flags, UCG_GATHERV_KNTREE_PARAMS_RECV)) {
        status = ucg_planc_ucx_p2p_irecv(op->gatherv.kntree.recvcounts, group_size,
                                         ucg_dt_get_predefined(UCG_DT_TYPE_INT32),
                                         peer, op->tag, vgroup, &params);
        UCG_CHECK_GOTO(status, out);
        status = ucg_planc_ucx_p2p_irecv(&op->gatherv.kntree.rctype_size, 1,
                                         ucg_dt_get_predefined(UCG_DT_TYPE_INT32),
                                         peer, op->tag, vgroup, &params);
        UCG_CHECK_GOTO(status, out);
    }
    status = ucg_planc_ucx_p2p_testall(ucx_group, params.state);
out:
    return status;
}

static ucg_status_t ucg_planc_ucx_gatherv_kntree_op_params_send(ucg_planc_ucx_op_t *op)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_group_t *ucx_group = op->ucx_group;
    ucg_vgroup_t *vgroup = op->super.vgroup;
    uint32_t group_size = vgroup->size;
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(op, &params);
    ucg_coll_gatherv_args_t *args = &op->super.super.args.gatherv;
    ucg_algo_kntree_iter_t *iter = &op->gatherv.kntree.kntree_iter;
    ucg_rank_t peer;

    if (ucg_test_flags(op->flags, UCG_GATHERV_KNTREE_PARAMS_SEND)){
        while ((peer = ucg_algo_kntree_iter_child_value(iter)) != UCG_INVALID_RANK) {
            const void *sbuf = op->gatherv.kntree.recvcounts;
            if (op->gatherv.kntree.recvcounts == NULL) {
                sbuf = args->recvcounts;
            }
            status = ucg_planc_ucx_p2p_isend(sbuf, group_size,
                                            ucg_dt_get_predefined(UCG_DT_TYPE_INT32),
                                            peer, op->tag, vgroup, &params);
            UCG_CHECK_GOTO(status, out);
            status = ucg_planc_ucx_p2p_isend(&op->gatherv.kntree.rctype_size, 1,
                                            ucg_dt_get_predefined(UCG_DT_TYPE_INT32),
                                            peer, op->tag, vgroup, &params);
            UCG_CHECK_GOTO(status, out);
            ucg_algo_kntree_iter_child_inc(iter);
        }
        ucg_clear_flags(&op->flags, UCG_GATHERV_KNTREE_PARAMS_SEND);
    }
    status = ucg_planc_ucx_p2p_testall(ucx_group, params.state);
out:
    return status;
}

static ucg_status_t ucg_planc_ucx_gatherv_kntree_op_data_recv(ucg_planc_ucx_op_t *op)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_group_t *ucx_group = op->ucx_group;
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_rank_t myrank = vgroup->myrank;
    uint32_t group_size = vgroup->size;
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(op, &params);
    ucg_algo_kntree_iter_t *iter = &op->gatherv.kntree.kntree_iter;
    ucg_coll_gatherv_args_t *args = &op->super.super.args.gatherv;
    ucg_rank_t peer;
    ucg_rank_t rpeer;

    if (ucg_test_and_clear_flags(&op->flags, UCG_GATHERV_KNTREE_DATA_RECV)) {

        int64_t offset = 0;
        int64_t recv_len = op->gatherv.kntree.recvcounts[myrank] * op->gatherv.kntree.rctype_size;
        offset += recv_len;
        //Recv myself data 这里会多做一次copy,但是逻辑简单
        if (args->sendcount > 0 && recv_len > 0) { // 这里如果sendcount或者recvcount小于0则不进行memcpy
            status = ucg_dt_memcpy(op->staging_area, recv_len, ucg_dt_get_predefined(UCG_DT_TYPE_UINT8),
                                    args->sendbuf, args->sendcount, args->sendtype);
            UCG_CHECK_GOTO(status, out); 
        }
        recv_len = 0;
        for (int32_t i = 0; i< op->gatherv.kntree.child_count; i++) {
            peer = op->gatherv.kntree.childlist[i]; //虚拟的rankid
            rpeer = (peer + args->root) % group_size;
            int64_t peer_recv_len = 0;
            int32_t peer_subtree_size = ucg_algo_kntree_get_subtree_size(iter, rpeer);
            for (int32_t j = 0; j < peer_subtree_size; j++) {
                int32_t idx = (j + rpeer) % group_size;
                peer_recv_len += op->gatherv.kntree.recvcounts[idx] * op->gatherv.kntree.rctype_size;
            }
            status = ucg_planc_ucx_p2p_irecv(op->staging_area + offset, 
                                                peer_recv_len,
                                                ucg_dt_get_predefined(UCG_DT_TYPE_UINT8), 
                                                rpeer, 
                                                op->tag,
                                                vgroup, 
                                                &params);
            UCG_CHECK_GOTO(status, out);
            offset += peer_recv_len;
        }
    }

    if (ucg_test_flags(op->flags, UCG_GATHERV_KNTREE_DATA_RECV_WAIT)) {
        status = ucg_planc_ucx_p2p_testall(ucx_group, params.state);
        UCG_CHECK_GOTO(status, out);
        ucg_clear_flags(&op->flags, UCG_GATHERV_KNTREE_DATA_RECV_WAIT);
    }

out:
    return status;
}

static ucg_status_t ucg_planc_ucx_gatherv_kntree_op_data_send(ucg_planc_ucx_op_t *op)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_group_t *ucx_group = op->ucx_group;
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_rank_t myrank = vgroup->myrank;
    uint32_t group_size = vgroup->size;
    ucg_coll_gatherv_args_t *args = &op->super.super.args.gatherv;
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(op, &params);
    ucg_algo_kntree_iter_t *iter = &op->gatherv.kntree.kntree_iter;
    ucg_rank_t peer;

    peer = ucg_algo_kntree_iter_parent_value(iter);
    if (peer == UCG_INVALID_RANK) {
        return UCG_OK;
    }
    if (ucg_test_flags(op->flags, UCG_GATHERV_KNTREE_DATA_SEND)) {

        int64_t send_count = op->gatherv.kntree.recvcounts[myrank] * op->gatherv.kntree.rctype_size;
        for (int32_t i = 0; i< op->gatherv.kntree.child_count; i++) {
            int mychild = op->gatherv.kntree.childlist[i];
            int rmychild = (mychild + args->root) % group_size;
            int32_t peer_subtree_size = ucg_algo_kntree_get_subtree_size(iter, rmychild);
            for (int32_t j = 0; j < peer_subtree_size; j++) {
                int32_t idx = (j + rmychild) % group_size;
                send_count += op->gatherv.kntree.recvcounts[idx] * op->gatherv.kntree.rctype_size;
            }
        }
        status = ucg_planc_ucx_p2p_isend(op->staging_area, send_count,
                                        ucg_dt_get_predefined(UCG_DT_TYPE_UINT8),
                                        peer, op->tag, vgroup,
                                        &params);
        UCG_CHECK_GOTO(status, out);

        ucg_clear_flags(&op->flags, UCG_GATHERV_KNTREE_DATA_SEND);
    }
    status = ucg_planc_ucx_p2p_testall(ucx_group, params.state);
out:
    return status;
}

static ucg_status_t ucg_planc_ucx_gatherv_kntree_op_data_root_copy(ucg_planc_ucx_op_t *op)
{
    ucg_status_t status = UCG_OK;
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_rank_t myrank = vgroup->myrank;
    uint32_t group_size = vgroup->size;
    ucg_coll_gatherv_args_t *args = &op->super.super.args.gatherv;

    if (myrank == args->root) {
        int64_t offset = 0;
        for (int j = 0; j < group_size; j++) {
            int i = (j + args->root) % group_size;
            int64_t sendcount = args->recvcounts[i] * op->gatherv.kntree.rctype_size;
            void *rbuf = args->recvbuf + args->displs[i] * ucg_dt_extent(args->recvtype);
            if ((i == args->root && args->sendcount <= 0) || sendcount <= 0 || args->recvcounts[i] <= 0) {
                offset += sendcount;
                continue;
            }
            status = ucg_dt_memcpy(rbuf, args->recvcounts[i], args->recvtype,
                                   op->staging_area + offset, sendcount, ucg_dt_get_predefined(UCG_DT_TYPE_UINT8));
            UCG_CHECK_GOTO(status, out);
            offset += sendcount;
        }
    }

out:
    return status;
}

static ucg_status_t ucg_planc_ucx_gatherv_kntree_op_params(ucg_planc_ucx_op_t *op)
{
    ucg_status_t status = UCG_OK;
    if (ucg_test_flags(op->flags, UCG_GATHERV_KNTREE_PARAMS_RECV_FROM_PARENT)) {
        status = ucg_planc_ucx_gatherv_kntree_op_params_recv(op);
        UCG_CHECK_GOTO(status, out);
        ucg_clear_flags(&op->flags, UCG_GATHERV_KNTREE_PARAMS_RECV_FROM_PARENT);
    }

    if (ucg_test_flags(op->flags, UCG_GATHERV_KNTREE_PARAMS_SEND_TO_CHILD)) {
        status = ucg_planc_ucx_gatherv_kntree_op_params_send(op);
        UCG_CHECK_GOTO(status, out);
        ucg_clear_flags(&op->flags, UCG_GATHERV_KNTREE_PARAMS_SEND_TO_CHILD);
    }

    if (ucg_test_and_clear_flags(&op->flags, UCG_GATHERV_KNTREE_PARAMS_ALLOC_STAGING)) {
        ucg_vgroup_t *vgroup = op->super.vgroup;
        ucg_rank_t myrank = vgroup->myrank;
        uint32_t group_size = vgroup->size;
        if (op->gatherv.kntree.staging_count >= 0) {
            int64_t size = 0;
            for (int32_t i = 0; i < op->gatherv.kntree.staging_count + 1; i++) {
                int32_t idx = (myrank + i) % group_size;
                size += (int64_t)op->gatherv.kntree.recvcounts[idx] * op->gatherv.kntree.rctype_size;
            }

            if (op->staging_area != NULL) {
                ucg_free(op->staging_area);
                op->staging_area = NULL;
            }
            op->staging_area = ucg_malloc(size, "gatherv kntree staging area");
            if (op->staging_area == NULL) {
                return UCG_ERR_NO_MEMORY;
            }
        }
    }

out:
    return status;
}

static ucg_status_t ucg_planc_ucx_gatherv_kntree_op_data(ucg_planc_ucx_op_t *op)
{
    ucg_status_t status = UCG_OK;

    if (ucg_test_and_clear_flags(&op->flags, UCG_GATHERV_KNTREE_DATA_INIT)) {
        //kntree iter should be reset here for op data
        ucg_algo_kntree_iter_reset(&op->gatherv.kntree.kntree_iter);
    }

    if (ucg_test_flags(op->flags, UCG_GATHERV_KNTREE_DATA_RECV_FROM_PARENT)) {
        status = ucg_planc_ucx_gatherv_kntree_op_data_recv(op);
        UCG_CHECK_GOTO(status, out);
        ucg_clear_flags(&op->flags, UCG_GATHERV_KNTREE_DATA_RECV_FROM_PARENT);
    }

    if (ucg_test_flags(op->flags, UCG_GATHERV_KNTREE_DATA_SEND_TO_CHILD)) {
        status = ucg_planc_ucx_gatherv_kntree_op_data_send(op);
        UCG_CHECK_GOTO(status, out);
        ucg_clear_flags(&op->flags, UCG_GATHERV_KNTREE_DATA_SEND_TO_CHILD);
    }

    if (ucg_test_flags(op->flags, UCG_GATHERV_KNTREE_DATA_ROOT_COPY)) {
        status = ucg_planc_ucx_gatherv_kntree_op_data_root_copy(op);
        UCG_CHECK_GOTO(status, out);
        ucg_clear_flags(&op->flags, UCG_GATHERV_KNTREE_DATA_ROOT_COPY);
    }

out:
    return status;
}

static ucg_status_t ucg_planc_ucx_gatherv_kntree_op_progress(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    if (ucg_test_flags(op->flags, UCG_GATHERV_KNTREE_PARAMS)) {
        status = ucg_planc_ucx_gatherv_kntree_op_params(op);
        UCG_CHECK_GOTO(status, out);
        ucg_clear_flags(&op->flags, UCG_GATHERV_KNTREE_PARAMS);
    }

    if (ucg_test_flags(op->flags, UCG_GATHERV_KNTREE_DATA)) {
        status = ucg_planc_ucx_gatherv_kntree_op_data(op);
        UCG_CHECK_GOTO(status, out);
        op->gatherv.kntree.first_trigger = 0;
        ucg_clear_flags(&op->flags, UCG_GATHERV_KNTREE_DATA);
    }

out:
    op->super.super.status = status;
    return status;
}

static ucg_status_t ucg_planc_ucx_gatherv_kntree_op_trigger(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status;
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_planc_ucx_op_reset(op);
    ucg_algo_kntree_iter_reset(&op->gatherv.kntree.kntree_iter);
    // for second trigger, we only need do op data
    if (op->gatherv.kntree.first_trigger) {
        op->flags = UCG_GATHERV_KNTREE_FLAGS;
    } else {
        op->flags = UCG_GATHERV_KNTREE_DATA_FLAGS;
    }
    status = ucg_planc_ucx_gatherv_kntree_op_progress(ucg_op);
    return status == UCG_INPROGRESS ? UCG_OK : status;
}

static inline ucg_status_t ucg_planc_ucx_gatherv_kntree_op_discard(ucg_plan_op_t *ucg_op)
{
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_rank_t myrank = vgroup->myrank;
    ucg_coll_gatherv_args_t *args = &op->super.super.args.gatherv;
    if (myrank != args->root) {
        if (op->gatherv.kntree.recvcounts != NULL) {
            ucg_free(op->gatherv.kntree.recvcounts);
        }
        op->gatherv.kntree.recvcounts = NULL;    
    }

    if (op->gatherv.kntree.childlist != NULL) {
        ucg_free(op->gatherv.kntree.childlist);
    }
    op->gatherv.kntree.childlist = NULL;
    return ucg_planc_ucx_op_discard(ucg_op);
}

static inline
ucg_status_t ucg_planc_ucx_gatherv_kntree_op_init(ucg_planc_ucx_op_t *op,
                                                   ucg_planc_ucx_group_t *ucx_group,
                                                   const ucg_planc_ucx_gatherv_config_t *config)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_op_init(op, ucx_group);
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_rank_t myrank = vgroup->myrank;
    int32_t group_size = vgroup->size;
    ucg_coll_gatherv_args_t *args = &op->super.super.args.gatherv;
    ucg_algo_kntree_iter_t *iter = &op->gatherv.kntree.kntree_iter;

    ucg_algo_kntree_iter_init(iter, vgroup->size, config->kntree_degree,
                              args->root, vgroup->myrank, 1);
    op->gatherv.kntree.staging_count = ucg_algo_kntree_get_subtree_size(iter, myrank) - 1;
    op->gatherv.kntree.recvcounts = NULL;
    op->gatherv.kntree.childlist = NULL;
    op->gatherv.kntree.childlist = (int32_t *)ucg_malloc((int64_t)group_size * sizeof(int32_t),
                                            "gatherv childlist");
    if (op->gatherv.kntree.childlist == NULL) {
        status = UCG_ERR_NO_MEMORY;
        goto err;
    }    
    if (myrank == args->root) {
        op->gatherv.kntree.rctype_size = ucg_dt_size(args->recvtype);
        op->gatherv.kntree.recvcounts = (int32_t *)args->recvcounts;
    } else {
        op->gatherv.kntree.recvcounts = ucg_malloc((int64_t)group_size * sizeof(int32_t),
                                             "gatherv recvcounts");
        if (op->gatherv.kntree.recvcounts == NULL) {
            status = UCG_ERR_NO_MEMORY;
            goto err_free_childlist;
        }
    }
    ucg_rank_t peer;
    uint32_t child_count = 0;
    ucg_algo_kntree_iter_reset(iter);
    while((peer = ucg_algo_kntree_iter_child_value(iter)) != UCG_INVALID_RANK) {
        op->gatherv.kntree.childlist[child_count++] = (peer + group_size - args->root) % group_size; //存放虚拟的rankid 
        ucg_algo_kntree_iter_child_inc(iter);
    }
    op->gatherv.kntree.child_count = child_count;

    for (uint32_t i = 0; i < child_count; i++) {
        for (uint32_t j = 0; j < child_count - i - 1; j++) {
            if (op->gatherv.kntree.childlist[j] > op->gatherv.kntree.childlist[j + 1]) {
                peer = op->gatherv.kntree.childlist[j];
                op->gatherv.kntree.childlist[j] = op->gatherv.kntree.childlist[j + 1];
                op->gatherv.kntree.childlist[j + 1] = peer;
            }
        }
    }
    op->gatherv.kntree.first_trigger = 1;

    return UCG_OK;
err_free_childlist:
    ucg_free(op->reduce_scatter.ring.displs);
err:
    return status;
}

ucg_planc_ucx_op_t *ucg_planc_ucx_gatherv_kntree_op_new(ucg_planc_ucx_group_t *ucx_group,
                                                         ucg_vgroup_t *vgroup,
                                                         const ucg_coll_args_t *args,
                                                         const ucg_planc_ucx_gatherv_config_t *config)
{
    UCG_CHECK_NULL(NULL, ucx_group, vgroup, args);

    ucg_planc_ucx_op_t *ucx_op = ucg_mpool_get(&ucx_group->context->op_mp);
    if (ucx_op == NULL) {
        goto err;
    }

    ucg_status_t status = UCG_CLASS_CONSTRUCT(ucg_plan_op_t, &ucx_op->super, vgroup,
                                              ucg_planc_ucx_gatherv_kntree_op_trigger,
                                              ucg_planc_ucx_gatherv_kntree_op_progress,
                                              ucg_planc_ucx_gatherv_kntree_op_discard,
                                              args);
    if (status != UCG_OK) {
        ucg_error("Failed to initialize super of ucx op");
        goto err_free_op;
    }

    status = ucg_planc_ucx_gatherv_kntree_op_init(ucx_op, ucx_group, config);
    if (status != UCG_OK) {
        ucg_error("Failed to initialize gatherv ucx op");
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

ucg_status_t ucg_planc_ucx_gatherv_kntree_prepare(ucg_vgroup_t *vgroup,
                                                   const ucg_coll_args_t *args,
                                                   ucg_plan_op_t **op)
{
    UCG_CHECK_NULL_INVALID(vgroup, args, op);
    ucg_planc_ucx_group_t *ucx_group = ucg_derived_of(vgroup, ucg_planc_ucx_group_t);
    ucg_planc_ucx_gatherv_config_t *config;
    config = UCG_PLANC_UCX_CONTEXT_BUILTIN_CONFIG_BUNDLE(ucx_group->context, gatherv,
                                                         UCG_COLL_TYPE_GATHERV);
    ucg_planc_ucx_op_t *kntree_op = ucg_planc_ucx_gatherv_kntree_op_new(ucx_group,
                                                                         vgroup,
                                                                         args,
                                                                         config);
    if (kntree_op == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    *op = &kntree_op->super;
    return UCG_OK;
}