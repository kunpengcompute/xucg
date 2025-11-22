/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */
#include "../scatterv/scatterv.h"
#include "scatter.h"
#include "planc_ucx_plan.h"

enum {
    UCG_KNTREE_RECV = UCG_BIT(0),
    UCG_KNTREE_SEND = UCG_BIT(1),
    UCG_KNTREE_ADJUST_ROOT_PARAMS = UCG_BIT(2),
    UCG_KNTREE_ADJUST_ROOT_DATA = UCG_BIT(3),
    UCG_KNTREE_INTER_NODE_PARAMS = UCG_BIT(4),
    UCG_KNTREE_INTER_NODE_DATA = UCG_BIT(5),
    UCG_KNTREE_INTRA_NODE_PARAMS = UCG_BIT(6),
    UCG_KNTREE_INTRA_NODE_DATA = UCG_BIT(7),
    UCG_KNTREE_RECV_FROM_PARENT = UCG_BIT(8),
    UCG_KNTREE_SEND_TO_CHILD = UCG_BIT(9),
    UCG_KNTREE_SEND_SENDCOUNTS = UCG_BIT(10),
    UCG_KNTREE_SEND_DTSIZE = UCG_BIT(11),
    UCG_KNTREE_SEND_SENDBUF = UCG_BIT(12),
    UCG_KNTREE_RECV_SENDCOUNTS = UCG_BIT(13),
    UCG_KNTREE_RECV_DTSIZE = UCG_BIT(14),
    UCG_KNTREE_RECV_SENDBUF = UCG_BIT(15),
};
/* adjust root: send and recv params */
#define UCG_KNTREE_ADJUST_ROOT_SEND_PARAMS_FLAGS (UCG_KNTREE_ADJUST_ROOT_PARAMS | \
                                                  UCG_KNTREE_SEND)
#define UCG_KNTREE_ADJUST_ROOT_RECV_PARAMS_FLAGS (UCG_KNTREE_ADJUST_ROOT_PARAMS | \
                                                  UCG_KNTREE_RECV)

/* adjust root: send and recv data */
#define UCG_KNTREE_ADJUST_ROOT_SEND_DATA_FLAGS (UCG_KNTREE_ADJUST_ROOT_DATA | \
                                                 UCG_KNTREE_SEND)
#define UCG_KNTREE_ADJUST_ROOT_RECV_DATA_FLAGS (UCG_KNTREE_ADJUST_ROOT_DATA | \
                                                 UCG_KNTREE_RECV)

/* inter node: send and recv params */
#define UCG_KNTREE_INTER_NODE_PARAMS_FLAGS (UCG_KNTREE_INTER_NODE_PARAMS | \
                                            UCG_KNTREE_SEND_TO_CHILD | \
                                            UCG_KNTREE_SEND_SENDCOUNTS | \
                                            UCG_KNTREE_SEND_DTSIZE | \
                                            UCG_KNTREE_RECV_FROM_PARENT | \
                                            UCG_KNTREE_RECV_SENDCOUNTS | \
                                            UCG_KNTREE_RECV_DTSIZE)
/* inter node: send and recv data */
#define UCG_KNTREE_INTER_NODE_DATA_FLAGS (UCG_KNTREE_INTER_NODE_DATA | \
                                          UCG_KNTREE_SEND_TO_CHILD | \
                                          UCG_KNTREE_SEND_SENDBUF | \
                                          UCG_KNTREE_RECV_FROM_PARENT | \
                                          UCG_KNTREE_RECV_SENDBUF)

/* intra node: send and recv params */
#define UCG_KNTREE_INTRA_NODE_PARAMS_FLAGS (UCG_KNTREE_INTRA_NODE_PARAMS | \
                                            UCG_KNTREE_SEND_TO_CHILD | \
                                            UCG_KNTREE_SEND_SENDCOUNTS | \
                                            UCG_KNTREE_SEND_DTSIZE | \
                                            UCG_KNTREE_RECV_FROM_PARENT | \
                                            UCG_KNTREE_RECV_SENDCOUNTS | \
                                            UCG_KNTREE_RECV_DTSIZE)
/* intra node: send and recv data */
#define UCG_KNTREE_INTRA_NODE_DATA_FLAGS (UCG_KNTREE_INTRA_NODE_DATA | \
                                          UCG_KNTREE_SEND_TO_CHILD | \
                                          UCG_KNTREE_SEND_SENDBUF | \
                                          UCG_KNTREE_RECV_FROM_PARENT | \
                                          UCG_KNTREE_RECV_SENDBUF)

#define ucg_planc_ucx_free_ptr(ptr) \
    do { \
        if (*ptr) { \
            ucg_free(*ptr); \
        } \
        *ptr = NULL; \
    } while(0)

static inline void scatterv_free_intra_sendbuf(ucg_coll_scatterv_args_t *inter_args,
                                               ucg_coll_scatterv_args_t *intra_args,
                                               ucg_planc_ucx_scatterv_na_kntree_args_t *na_args)
{
    UCG_CHECK_NULL_VOID(inter_args, intra_args, na_args);
    if (na_args->node_leader_group->state == UCG_TOPO_GROUP_STATE_ENABLE) {
        ucg_planc_ucx_free_ptr((void **)&inter_args->sendbuf);
    } else {
        ucg_planc_ucx_free_ptr((void **)&intra_args->sendbuf);
    }
    return;
}

static inline void scatterv_free_intra_sendcounts(ucg_coll_scatterv_args_t *inter_args,
                                                  ucg_coll_scatterv_args_t *intra_args,
                                                  ucg_planc_ucx_scatterv_na_kntree_args_t *na_args)
{
    UCG_CHECK_NULL_VOID(inter_args, intra_args, na_args);
    if (na_args->node_leader_group->state == UCG_TOPO_GROUP_STATE_ENABLE) {
        ucg_planc_ucx_free_ptr((void **)&inter_args->sendcounts);
    } else {
        ucg_planc_ucx_free_ptr((void **)&intra_args->sendcounts);
    }
    return;
}

static inline void scatterv_free_root_adjust_root(ucg_coll_scatterv_args_t *inter_args,
                                                  ucg_coll_scatterv_args_t *args,
                                                  ucg_rank_t myrank)
{
    UCG_CHECK_NULL_VOID(inter_args, args);
    if (myrank == args->root) {
        ucg_planc_ucx_free_ptr((void **)&inter_args->sendbuf);
        ucg_planc_ucx_free_ptr((void **)&inter_args->sendcounts);
    }
    return;
}

static inline void scatterv_free_temp_buf(ucg_planc_ucx_scatterv_na_kntree_args_t *na_args)
{
    UCG_CHECK_NULL_VOID(na_args);
    ucg_planc_ucx_free_ptr((void **)&na_args->displs);
    ucg_planc_ucx_free_ptr((void **)&na_args->sendcounts);
    return;
}

static inline void scatterv_free_buf(void **buf)
{
    ucg_planc_ucx_free_ptr(buf);
    return;
}

static inline ucg_status_t ucg_planc_ucx_scatterv_na_kntree_op_discard(ucg_plan_op_t *ucg_op)
{
    ucg_planc_ucx_op_t *ucx_op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_coll_scatterv_args_t *args = &ucx_op->super.super.args.scatterv;
    ucg_planc_ucx_free_ptr((void **)&args->displs);
    ucg_planc_ucx_free_ptr((void **)&args->sendcounts);
    return ucg_planc_ucx_op_discard(ucg_op);
}

static ucg_status_t ucg_planc_ucx_scatterv_data_op_progress(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_op_t *ucx_op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_planc_ucx_group_t *ucx_group = ucx_op->ucx_group;
    ucg_vgroup_t *vgroup = ucx_op->super.vgroup;
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(ucx_op, &params);
    ucg_coll_scatterv_args_t *args = &ucx_op->super.super.args.scatterv;
    ucg_planc_ucx_scatterv_na_kntree_args_t *na_args = &ucx_op->scatterv.na_kntree;
    ucg_algo_kntree_iter_t *iter = &ucx_op->scatterv.na_kntree.kntree_iter;
    ucg_rank_t peer;

    if (ucg_test_flags(ucx_op->flags, UCG_KNTREE_RECV_FROM_PARENT)) {
        ucg_rank_t peer = ucg_algo_kntree_iter_parent_value(iter);
        if (peer != UCG_INVALID_RANK &&
            ucg_test_and_clear_flags(&ucx_op->flags, UCG_KNTREE_RECV_SENDBUF)) {
            status = ucg_planc_ucx_p2p_irecv((void *)args->sendbuf,
                                             na_args->sendbuf_size,
                                             ucg_dt_get_predefined(UCG_DT_TYPE_UINT8),
                                             peer, ucx_op->tag, vgroup, &params);
            UCG_CHECK_GOTO(status, out);
        }
        status = ucg_planc_ucx_p2p_testall(ucx_group, params.state);
        UCG_CHECK_GOTO(status, out);
        ucg_clear_flags(&ucx_op->flags, UCG_KNTREE_RECV_FROM_PARENT);
    }

    if (ucg_test_flags(ucx_op->flags, UCG_KNTREE_SEND_TO_CHILD)){
        while ((peer = ucg_algo_kntree_iter_child_value(iter)) != UCG_INVALID_RANK) {
            if (ucg_test_and_clear_flags(&ucx_op->flags, UCG_KNTREE_SEND_SENDBUF)) {
                uint64_t offset = (na_args->displs[peer] - na_args->displs[iter->myrank]) * na_args->sdtype_size;
                status = ucg_planc_ucx_p2p_isend(args->sendbuf + offset,
                                                 na_args->sendcounts[peer] * na_args->sdtype_size,
                                                 ucg_dt_get_predefined(UCG_DT_TYPE_UINT8),
                                                 peer, ucx_op->tag, vgroup, &params);
                UCG_CHECK_GOTO(status, out);
            }
            status = ucg_planc_ucx_p2p_testall(ucx_group, params.state);
            UCG_CHECK_GOTO(status, out);
            ucg_algo_kntree_iter_child_inc(iter);
            ucx_op->flags |= UCG_KNTREE_SEND_SENDBUF;
        }
        ucg_clear_flags(&ucx_op->flags, UCG_KNTREE_SEND_TO_CHILD);
    }
    status = ucg_planc_ucx_p2p_testall(ucx_group, params.state);
    UCG_CHECK_GOTO(status, out);
out:
    return status;
}

static ucg_status_t ucg_planc_ucx_scatterv_params_op_progress(ucg_plan_op_t *ucg_op, uint32_t nprocs)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_op_t *ucx_op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_planc_ucx_group_t *ucx_group = ucx_op->ucx_group;
    ucg_vgroup_t *vgroup = ucx_op->super.vgroup;
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(ucx_op, &params);
    ucg_planc_ucx_scatterv_na_kntree_args_t *na_args = &ucx_op->scatterv.na_kntree;
    ucg_coll_scatterv_args_t *args = &ucx_op->super.super.args.scatterv;
    ucg_algo_kntree_iter_t *iter = &ucx_op->scatterv.na_kntree.kntree_iter;
    ucg_rank_t peer;

    if (ucg_test_flags(ucx_op->flags, UCG_KNTREE_RECV_FROM_PARENT)) {
        if ((peer = ucg_algo_kntree_iter_parent_value(iter)) != UCG_INVALID_RANK) {
            if (ucg_test_and_clear_flags(&ucx_op->flags, UCG_KNTREE_RECV_SENDCOUNTS)) {
                status = ucg_planc_ucx_p2p_irecv((int32_t *)args->sendcounts,
                                                 nprocs,
                                                 ucg_dt_get_predefined(UCG_DT_TYPE_INT32),
                                                 peer, ucx_op->tag, vgroup, &params);
                UCG_CHECK_GOTO(status, out);
            }
            if (ucg_test_and_clear_flags(&ucx_op->flags, UCG_KNTREE_RECV_DTSIZE)) {
                status = ucg_planc_ucx_p2p_irecv(&na_args->sdtype_size, 1,
                                                ucg_dt_get_predefined(UCG_DT_TYPE_INT32),
                                                peer, ucx_op->tag, vgroup, &params);
                UCG_CHECK_GOTO(status, out);
            }
            status = ucg_planc_ucx_p2p_testall(ucx_group, params.state);
            UCG_CHECK_GOTO(status, out);
        }
        ucg_clear_flags(&ucx_op->flags, UCG_KNTREE_RECV_FROM_PARENT);
    }

    if (ucg_test_flags(ucx_op->flags, UCG_KNTREE_SEND_TO_CHILD)){
        while ((peer = ucg_algo_kntree_iter_child_value(iter)) != UCG_INVALID_RANK) {
            if (ucg_test_and_clear_flags(&ucx_op->flags, UCG_KNTREE_SEND_SENDCOUNTS)) {
                status = ucg_planc_ucx_p2p_isend(args->sendcounts, nprocs,
                                                 ucg_dt_get_predefined(UCG_DT_TYPE_INT32),
                                                 peer, ucx_op->tag, vgroup, &params);
                UCG_CHECK_GOTO(status, out);
            }
            if (ucg_test_and_clear_flags(&ucx_op->flags, UCG_KNTREE_SEND_DTSIZE)) {
                status = ucg_planc_ucx_p2p_isend(&na_args->sdtype_size, 1,
                                                 ucg_dt_get_predefined(UCG_DT_TYPE_INT32),
                                                 peer, ucx_op->tag, vgroup, &params);
                UCG_CHECK_GOTO(status, out);
            }
            status = ucg_planc_ucx_p2p_testall(ucx_group, params.state);
            UCG_CHECK_GOTO(status, out);
            ucg_algo_kntree_iter_child_inc(iter);
            ucx_op->flags |= UCG_KNTREE_SEND_SENDCOUNTS | UCG_KNTREE_SEND_DTSIZE;
        }
        ucg_clear_flags(&ucx_op->flags, UCG_KNTREE_SEND_TO_CHILD);
    }
    status = ucg_planc_ucx_p2p_testall(ucx_group, params.state);
    UCG_CHECK_GOTO(status, out);
out:
    return status;
}

static ucg_status_t ucg_planc_ucx_scatterv_adjust_root_data_op_progress(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_op_t *ucx_op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_planc_ucx_group_t *ucx_group = ucx_op->ucx_group;
    ucg_vgroup_t *vgroup = ucx_op->super.vgroup;
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(ucx_op, &params);
    ucg_coll_scatterv_args_t *args = &ucx_op->super.super.args.scatterv;
    int32_t sendbuf_size = ucx_op->scatterv.na_kntree.sendbuf_size;
    if (ucg_test_and_clear_flags(&ucx_op->flags, UCG_KNTREE_SEND)) {
        status = ucg_planc_ucx_p2p_isend(args->sendbuf,
                                         sendbuf_size,
                                         ucg_dt_get_predefined(UCG_DT_TYPE_UINT8),
                                         0, ucx_op->tag, vgroup, &params);
        UCG_CHECK_GOTO(status, out);
    } else if (ucg_test_and_clear_flags(&ucx_op->flags, UCG_KNTREE_RECV)) {
        status = ucg_planc_ucx_p2p_irecv((void *)args->sendbuf,
                                         sendbuf_size,
                                         ucg_dt_get_predefined(UCG_DT_TYPE_UINT8),
                                         args->root,
                                         ucx_op->tag, vgroup, &params);
        UCG_CHECK_GOTO(status, out);
    }
    status = ucg_planc_ucx_p2p_testall(ucx_group, params.state);
out:
    ucx_op->super.super.status = status;
    return status;
}

static ucg_status_t ucg_planc_ucx_scatterv_adjust_root_params_op_progress(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_op_t *ucx_op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_planc_ucx_group_t *ucx_group = ucx_op->ucx_group;
    ucg_vgroup_t *vgroup = ucx_op->super.vgroup;
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(ucx_op, &params);
    uint32_t group_size = vgroup->size;
    ucg_coll_scatterv_args_t *args = &ucx_op->super.super.args.scatterv;

    if (ucg_test_and_clear_flags(&ucx_op->flags, UCG_KNTREE_SEND)) {
        status = ucg_planc_ucx_p2p_isend(args->sendcounts, group_size,
                                         ucg_dt_get_predefined(UCG_DT_TYPE_INT32),
                                         0, ucx_op->tag, vgroup, &params);
        UCG_CHECK_GOTO(status, out);
        status = ucg_planc_ucx_p2p_isend(&ucx_op->scatterv.na_kntree.sdtype_size, 1,
                                         ucg_dt_get_predefined(UCG_DT_TYPE_INT32),
                                         0, ucx_op->tag, vgroup, &params);
        UCG_CHECK_GOTO(status, out);
    } else if (ucg_test_and_clear_flags(&ucx_op->flags, UCG_KNTREE_RECV)) {
        status = ucg_planc_ucx_p2p_irecv((int32_t *)args->sendcounts,
                                         group_size,
                                         ucg_dt_get_predefined(UCG_DT_TYPE_INT32),
                                         args->root,
                                         ucx_op->tag, vgroup, &params);
        status = ucg_planc_ucx_p2p_irecv(&ucx_op->scatterv.na_kntree.sdtype_size,
                                         1,
                                         ucg_dt_get_predefined(UCG_DT_TYPE_INT32),
                                         args->root,
                                         ucx_op->tag, vgroup, &params);
    }
    status = ucg_planc_ucx_p2p_testall(ucx_group, params.state);
    UCG_CHECK_GOTO(status, out);
out:
    ucx_op->super.super.status = status;
    return status;
}

static ucg_status_t ucg_planc_ucx_scatterv_intra_node_data_op_prepare(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_op_t *ucx_op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_planc_ucx_scatterv_na_kntree_args_t *na_args = &ucx_op->scatterv.na_kntree;
    ucg_vgroup_t *vgroup = &na_args->intra_node_group->super;
    ucg_rank_t myrank_topo = vgroup->myrank;
    uint32_t nprocs = vgroup->size;
    ucg_coll_scatterv_args_t *inter_args = &na_args->scatterv_inter.scatterv;
    ucg_coll_scatterv_args_t *intra_args = &na_args->scatterv_intra.scatterv;
    ucg_algo_kntree_iter_t *iter = &na_args->kntree_iter;
    if (myrank_topo == UCG_TOPO_GROUP_LEADER) {
        intra_args->sendbuf = inter_args->sendbuf;
    } else {
        /* intra group sendbuf */
        int32_t subtree_size = ucg_algo_kntree_get_subtree_size(iter, myrank_topo);
        uint32_t total_len = 0;
        for (int32_t i = myrank_topo; i < subtree_size + myrank_topo; ++i) {
            total_len += intra_args->sendcounts[i];
        }
        uint64_t total_bufsize = total_len * na_args->sdtype_size;
        intra_args->sendbuf = ucg_malloc(total_bufsize,
                                         "scatterv intra node data sendbuf");
        if (intra_args->sendbuf == NULL) {
            return UCG_ERR_NO_MEMORY;
        }
        na_args->sendbuf_size = total_bufsize;
    }

    /* intra group temp sendcounts */
    na_args->sendcounts = ucg_calloc(nprocs, sizeof(int32_t),
                                     "scatterv inter temp sendcounts");
    if (na_args->sendcounts == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    ucg_algo_kntree_iter_reset(iter);
    ucg_rank_t peer;
    while ((peer = ucg_algo_kntree_iter_child_value(iter)) != UCG_INVALID_RANK) {
        int32_t subtree_size = ucg_algo_kntree_get_subtree_size(iter, peer);
        for (int32_t i = 0; i < subtree_size; ++i) {
            na_args->sendcounts[peer] += intra_args->sendcounts[peer + i];
        }
        ucg_algo_kntree_iter_child_inc(iter);
    }

    /* intra group temp displs */
    na_args->displs = ucg_calloc(nprocs, sizeof(int32_t),
                                 "scatterv inter temp displs");
    if (na_args->displs == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    na_args->displs[0] = 0;
    for (uint32_t i = 1; i < nprocs; ++i) {
        na_args->displs[i] = na_args->displs[i - 1] + intra_args->sendcounts[i - 1];
    }

    intra_args->recvtype = na_args->scatterv_origin.scatterv.recvtype;
    intra_args->recvcount = na_args->scatterv_origin.scatterv.recvcount;
    intra_args->root = 0;
    return status;
}

static ucg_status_t ucg_planc_ucx_scatterv_intra_node_params_op_prepare(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_op_t *ucx_op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_planc_ucx_group_t *ucx_group = ucx_op->ucx_group;
    ucg_planc_ucx_scatterv_config_t *config;
    config = UCG_PLANC_UCX_CONTEXT_BUILTIN_CONFIG_BUNDLE(ucx_group->context, scatterv,
                                                         UCG_COLL_TYPE_SCATTERV);
    ucg_topo_group_t *intra_node_group = ucx_op->scatterv.na_kntree.intra_node_group;
    ucg_vgroup_t *vgroup = &intra_node_group->super;
    ucg_rank_t myrank = vgroup->myrank;
    ucg_rank_t global_myrank = ucx_op->scatterv.na_kntree.vgroup_origin->myrank;
    ucg_planc_ucx_scatterv_na_kntree_args_t *na_args = &ucx_op->scatterv.na_kntree;
    ucg_coll_scatterv_args_t *inter_args = &na_args->scatterv_inter.scatterv;
    ucg_coll_scatterv_args_t *intra_args = &na_args->scatterv_intra.scatterv;
    ucg_algo_kntree_iter_t *iter = &ucx_op->scatterv.na_kntree.kntree_iter;
    uint32_t group_nprocs = vgroup->size;
    ucg_algo_kntree_iter_init(iter, vgroup->size, config->na_kntree_intra_degree,
                              0, vgroup->myrank, 1);
    if (myrank == UCG_TOPO_GROUP_LEADER) {
        intra_args->sendcounts = inter_args->sendcounts + global_myrank;
        intra_args->sendtype = ucg_dt_get_predefined(UCG_DT_TYPE_UINT8);
    } else {
        intra_args->sendcounts = ucg_calloc(group_nprocs,
                                            sizeof(int32_t),
                                            "scatterv inter node sendcounts");
        if (intra_args->sendcounts == NULL) {
            return UCG_ERR_NO_MEMORY;
        }
    }
    intra_args->root = 0;
    return status;
}

static ucg_status_t ucg_planc_ucx_scatterv_inter_node_data_op_prepare(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_op_t *ucx_op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_planc_ucx_scatterv_na_kntree_args_t *na_args = &ucx_op->scatterv.na_kntree;
    ucg_coll_scatterv_args_t *inter_args = &na_args->scatterv_inter.scatterv;
    uint32_t nprocs = na_args->intra_node_group->super.size;
    uint32_t nnodes = na_args->node_leader_group->super.size;
    ucg_algo_kntree_iter_t *iter = &na_args->kntree_iter;

    ucg_rank_t myrank_topo = na_args->node_leader_group->super.myrank;
    if (myrank_topo != UCG_TOPO_GROUP_LEADER) {
        ucg_rank_t myrank = na_args->vgroup_origin->myrank;
        int32_t subtree_size = ucg_algo_kntree_get_subtree_size(iter, myrank_topo);
        uint32_t total_len = 0;
        for (uint32_t i = myrank; i < subtree_size * nprocs + myrank; ++i) {
            total_len += inter_args->sendcounts[i];
        }
        uint64_t total_bufsize = total_len * na_args->sdtype_size;
        inter_args->sendbuf = ucg_malloc(total_bufsize,
                                         "scatterv inter node sendbuf");
        if (inter_args->sendbuf == NULL) {
            return UCG_ERR_NO_MEMORY;
        }
        na_args->sendbuf_size = total_bufsize;
    }

    /* inter group temp sendcounts */
    ucg_algo_kntree_iter_reset(iter);
    na_args->sendcounts = ucg_calloc(nnodes, sizeof(int32_t),
                                     "scatterv inter temp sendcounts");
    if (na_args->sendcounts == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    ucg_rank_t peer;
    while ((peer = ucg_algo_kntree_iter_child_value(iter)) != UCG_INVALID_RANK) {
        int32_t subtree_size = ucg_algo_kntree_get_subtree_size(iter, peer);
        for (int32_t i = 0; i < subtree_size; ++i) {
            for (uint32_t j = 0; j < nprocs; ++j) {
                na_args->sendcounts[peer] += inter_args->sendcounts[(peer + i) * nprocs + j];
            }
        }
        ucg_algo_kntree_iter_child_inc(iter);
    }

    /* inter group temp displs */
    na_args->displs = ucg_calloc(nnodes, sizeof(int32_t),
                                 "scatterv inter temp displs");
    if (na_args->displs == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    na_args->displs[0] = 0;
    for (uint32_t i = 1; i < nnodes; ++i) {
        na_args->displs[i] = na_args->displs[i - 1];
        for (uint32_t j = 0; j < nprocs; ++j) {
            na_args->displs[i] += inter_args->sendcounts[(i - 1) * nprocs + j];
        }
    }

    inter_args->root = UCG_TOPO_GROUP_LEADER;
    return status;
}

static ucg_status_t ucg_planc_ucx_scatterv_inter_node_params_op_prepare(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_op_t *ucx_op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_planc_ucx_group_t *ucx_group = ucx_op->ucx_group;
    ucg_planc_ucx_scatterv_config_t *config;
    config = UCG_PLANC_UCX_CONTEXT_BUILTIN_CONFIG_BUNDLE(ucx_group->context, scatterv,
                                                         UCG_COLL_TYPE_SCATTERV);
    ucg_vgroup_t *vgroup = &ucx_op->scatterv.na_kntree.node_leader_group->super;
    ucg_rank_t myrank = vgroup->myrank;
    ucg_coll_scatterv_args_t *inter_args = &ucx_op->scatterv.na_kntree.scatterv_inter.scatterv;
    ucg_algo_kntree_iter_t *iter = &ucx_op->scatterv.na_kntree.kntree_iter;
    uint32_t global_nprocs = ucx_op->scatterv.na_kntree.global_nprocs;

    ucg_algo_kntree_iter_init(iter, vgroup->size, config->na_kntree_inter_degree,
                              0, vgroup->myrank, 1);
    if (myrank != UCG_TOPO_GROUP_LEADER) {
        inter_args->sendcounts = ucg_calloc(global_nprocs,
                                            sizeof(int32_t),
                                            "scatterv inter node sendcounts");
        if (inter_args->sendcounts == NULL) {
            return UCG_ERR_NO_MEMORY;
        }
    }
    inter_args->root = 0;
    return status;
}

static ucg_status_t ucg_planc_ucx_scatterv_adjust_root_data_op_prepare(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_op_t *ucx_op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_rank_t myrank = ucg_op->vgroup->myrank;
    ucg_planc_ucx_scatterv_na_kntree_args_t *na_args = &ucx_op->scatterv.na_kntree;
    ucg_coll_scatterv_args_t *inter_args = &na_args->scatterv_inter.scatterv;
    int32_t total_count = 0;
    if (myrank == UCG_TOPO_GROUP_LEADER) {
        for (uint32_t i = 0; i < na_args->global_nprocs; ++i) {
            total_count += inter_args->sendcounts[i];
        }
        uint64_t bufsize = ucx_op->scatterv.na_kntree.sdtype_size * total_count;
        /* rank 0 set sendbuf size */
        na_args->sendbuf_size = bufsize;
        inter_args->sendbuf = ucg_malloc(bufsize,
                                         "scatterv adjust root data sendbuf");
        if (inter_args->sendbuf == NULL) {
            return UCG_ERR_NO_MEMORY;
        }
    }
    return status;
}

/* root process need to send array sendcounts and integer send_datatype_size to vrank 0*/
static ucg_status_t ucg_planc_ucx_scatterv_adjust_root_params_op_prepare(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_op_t *ucx_op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_rank_t myrank = ucx_op->super.vgroup->myrank;
    ucg_coll_scatterv_args_t *inter_args = &ucx_op->scatterv.na_kntree.scatterv_inter.scatterv;
    uint32_t group_size = ucx_op->scatterv.na_kntree.global_nprocs;
    if (myrank == UCG_TOPO_GROUP_LEADER) {
        inter_args->sendcounts = ucg_calloc(group_size,
                                            sizeof(int32_t),
                                            "scatterv adjust root params sendcounts");
        if (inter_args->sendcounts == NULL) {
            return UCG_ERR_NO_MEMORY;
        }
    }
    inter_args->root = ucx_op->super.super.args.scatterv.root;
    return status;
}

static ucg_status_t ucg_planc_ucx_scatterv_na_kntree_op_progress(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_op_t *ucx_op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_planc_ucx_scatterv_na_kntree_args_t *na_args = &ucx_op->scatterv.na_kntree;
    ucg_coll_scatterv_args_t *inter_args = &na_args->scatterv_inter.scatterv;
    ucg_coll_scatterv_args_t *intra_args = &na_args->scatterv_intra.scatterv;
    ucg_coll_scatterv_args_t *args = &na_args->scatterv_origin.scatterv;
    ucg_rank_t myrank = na_args->vgroup_origin->myrank;
    ucg_rank_t myrank_intra = na_args->intra_node_group->super.myrank;

    if (ucg_test_flags(ucx_op->flags, UCG_KNTREE_ADJUST_ROOT_PARAMS)) {
        if (!na_args->is_initialized) {
            status = ucg_planc_ucx_scatterv_adjust_root_params_op_prepare(ucg_op);
            UCG_PLANC_UCX_NA_KNTREE_CHECK_GOTO(status, out, free_adjust_root_params);
            ucx_op->super.super.args = na_args->scatterv_inter;
            ucx_op->super.vgroup = na_args->vgroup_origin;
            na_args->is_initialized = 1;
        }
        status = ucg_planc_ucx_scatterv_adjust_root_params_op_progress(ucg_op);
        UCG_PLANC_UCX_NA_KNTREE_CHECK_GOTO(status, out, free_adjust_root_params);
        ucg_clear_flags(&ucx_op->flags, UCG_KNTREE_ADJUST_ROOT_PARAMS);
        ucx_op->flags = ucx_op->super.vgroup->myrank
                        ? UCG_KNTREE_ADJUST_ROOT_SEND_DATA_FLAGS
                        : UCG_KNTREE_ADJUST_ROOT_RECV_DATA_FLAGS;
        na_args->is_initialized = 0;
    }

    if (ucg_test_flags(ucx_op->flags, UCG_KNTREE_ADJUST_ROOT_DATA)) {
        if (!na_args->is_initialized) {
            status = ucg_planc_ucx_scatterv_adjust_root_data_op_prepare(ucg_op);
            UCG_PLANC_UCX_NA_KNTREE_CHECK_GOTO(status, out, free_adjust_root_data);
            ucx_op->super.super.args = na_args->scatterv_inter;
            ucx_op->super.vgroup = na_args->vgroup_origin;
            na_args->is_initialized = 1;
        }
        status = ucg_planc_ucx_scatterv_adjust_root_data_op_progress(ucg_op);
        UCG_PLANC_UCX_NA_KNTREE_CHECK_GOTO(status, out, free_adjust_root_data);
        ucg_clear_flags(&ucx_op->flags, UCG_KNTREE_ADJUST_ROOT_DATA);
        /* free non-zero root sendcounts and sendbuf */
        scatterv_free_root_adjust_root(inter_args, args, myrank);

        ucx_op->flags = na_args->node_leader_group->state == UCG_TOPO_GROUP_STATE_ENABLE
                        ? UCG_KNTREE_INTER_NODE_PARAMS_FLAGS
                        : UCG_KNTREE_INTRA_NODE_PARAMS_FLAGS;
        na_args->is_initialized = 0;
    }

    if (ucg_test_flags(ucx_op->flags, UCG_KNTREE_INTER_NODE_PARAMS)) {
        if (!na_args->is_initialized) {
            status = ucg_planc_ucx_scatterv_inter_node_params_op_prepare(ucg_op);
            UCG_PLANC_UCX_NA_KNTREE_CHECK_GOTO(status, out, free_inter_params);
            ucg_algo_kntree_iter_reset(&ucx_op->scatterv.na_kntree.kntree_iter);
            ucx_op->super.super.args = na_args->scatterv_inter;
            ucx_op->super.vgroup = &na_args->node_leader_group->super;
            na_args->is_initialized = 1;
        }
        status = ucg_planc_ucx_scatterv_params_op_progress(ucg_op, na_args->global_nprocs);
        UCG_PLANC_UCX_NA_KNTREE_CHECK_GOTO(status, out, free_inter_params);
        ucg_clear_flags(&ucx_op->flags, UCG_KNTREE_INTER_NODE_PARAMS);
        ucx_op->flags = UCG_KNTREE_INTER_NODE_DATA_FLAGS;
        na_args->is_initialized = 0;
    }

    if (ucg_test_flags(ucx_op->flags, UCG_KNTREE_INTER_NODE_DATA)) {
        if (!na_args->is_initialized) {
            status = ucg_planc_ucx_scatterv_inter_node_data_op_prepare(ucg_op);
            UCG_PLANC_UCX_NA_KNTREE_CHECK_GOTO(status, out, free_inter_data);
            ucg_algo_kntree_iter_reset(&ucx_op->scatterv.na_kntree.kntree_iter);
            ucx_op->super.super.args = na_args->scatterv_inter;
            ucx_op->super.vgroup = &na_args->node_leader_group->super;
            na_args->is_initialized = 1;
        }
        status = ucg_planc_ucx_scatterv_data_op_progress(ucg_op);
        UCG_PLANC_UCX_NA_KNTREE_CHECK_GOTO(status, out, free_inter_data);
        ucg_clear_flags(&ucx_op->flags, UCG_KNTREE_INTER_NODE_DATA);
        ucx_op->flags = UCG_KNTREE_INTRA_NODE_PARAMS_FLAGS;
        na_args->is_initialized = 0;
    }

    if (ucg_test_flags(ucx_op->flags, UCG_KNTREE_INTRA_NODE_PARAMS)) {
        if (!na_args->is_initialized) {
            status = ucg_planc_ucx_scatterv_intra_node_params_op_prepare(ucg_op);
            UCG_PLANC_UCX_NA_KNTREE_CHECK_GOTO(status, out, free_intra_params);
            ucx_op->super.super.args = na_args->scatterv_intra;
            ucx_op->super.vgroup = &na_args->intra_node_group->super;
            ucg_algo_kntree_iter_reset(&ucx_op->scatterv.na_kntree.kntree_iter);
            na_args->is_initialized = 1;
        }
        status = ucg_planc_ucx_scatterv_params_op_progress(ucg_op, na_args->group_nprocs);
        UCG_PLANC_UCX_NA_KNTREE_CHECK_GOTO(status, out, free_intra_params);
        ucg_clear_flags(&ucx_op->flags, UCG_KNTREE_INTRA_NODE_PARAMS);
        ucx_op->flags = UCG_KNTREE_INTRA_NODE_DATA_FLAGS;
        na_args->is_initialized = 0;
    }

    if (ucg_test_flags(ucx_op->flags, UCG_KNTREE_INTRA_NODE_DATA)) {
        if (!na_args->is_initialized) {
            status = ucg_planc_ucx_scatterv_intra_node_data_op_prepare(ucg_op);
            UCG_PLANC_UCX_NA_KNTREE_CHECK_GOTO(status, out, free_intra_data);
            ucx_op->super.super.args = na_args->scatterv_intra;
            ucx_op->super.vgroup = &na_args->intra_node_group->super;
            ucg_algo_kntree_iter_reset(&ucx_op->scatterv.na_kntree.kntree_iter);
            na_args->is_initialized = 1;
        }
        status = ucg_planc_ucx_scatterv_data_op_progress(ucg_op);
        UCG_PLANC_UCX_NA_KNTREE_CHECK_GOTO(status, out, free_intra_data);
        ucg_clear_flags(&ucx_op->flags, UCG_KNTREE_INTRA_NODE_DATA);
        ucx_op->flags = 0;
        na_args->is_initialized = 0;
        ucg_op->super.args = na_args->scatterv_origin;
        ucg_op->vgroup = na_args->vgroup_origin;
    }

    /* sendcounts isn't equal to recvcount*/
    int32_t recv_len = intra_args->sendcounts[myrank_intra] * na_args->sdtype_size >
                       args->recvcount * ucg_dt_size(args->recvtype) ?
                       args->recvcount * ucg_dt_size(args->recvtype) :
                       intra_args->sendcounts[myrank_intra] * na_args->sdtype_size;
    memcpy(args->recvbuf, intra_args->sendbuf, recv_len);

free_intra_data:
    scatterv_free_intra_sendbuf(inter_args, intra_args, na_args);
    scatterv_free_temp_buf(na_args);
free_intra_params:
    scatterv_free_intra_sendcounts(inter_args, intra_args, na_args);
free_inter_data:
    scatterv_free_temp_buf(na_args);
    scatterv_free_buf((void**)&inter_args->sendbuf);
free_inter_params:
    scatterv_free_buf((void**)&inter_args->sendcounts);
free_adjust_root_data:
    scatterv_free_buf((void**)&inter_args->sendbuf);
free_adjust_root_params:
    scatterv_free_buf((void**)&inter_args->sendcounts);
out:
    ucx_op->super.super.status = status;
    return status;
}

static ucg_status_t ucg_planc_ucx_scatterv_na_kntree_op_init(ucg_planc_ucx_op_t *ucx_op)
{
    ucg_vgroup_t *vgroup = ucx_op->super.vgroup;
    ucg_topo_group_t *node_leader_group;
    node_leader_group = ucg_topo_get_group(vgroup->group->topo, UCG_TOPO_GROUP_TYPE_NODE_LEADER);
    UCG_CHECK_NULL(UCG_ERR_NO_MEMORY, node_leader_group);
    if ((node_leader_group->state != UCG_TOPO_GROUP_STATE_ENABLE) &&
        (node_leader_group->state != UCG_TOPO_GROUP_STATE_DISABLE)) {
        return UCG_ERR_NO_MEMORY;
    }

    ucg_topo_group_t *intra_node_group;
    UCG_CHECK_NULL(UCG_ERR_NO_MEMORY, vgroup);
    intra_node_group = ucg_topo_get_group(vgroup->group->topo, UCG_TOPO_GROUP_TYPE_NODE);
    UCG_CHECK_NULL(UCG_ERR_NO_MEMORY, intra_node_group);
    if (intra_node_group->state != UCG_TOPO_GROUP_STATE_ENABLE) {
        return UCG_ERR_NO_MEMORY;
    }

    ucg_planc_ucx_scatterv_na_kntree_args_t *na_args = &ucx_op->scatterv.na_kntree;
    na_args->global_nprocs = vgroup->size;
    na_args->group_nprocs = intra_node_group->super.size;
    na_args->is_initialized = 0;
    na_args->sendbuf_size = 0;
    na_args->sdtype_size = 0;
    na_args->displs = NULL;
    na_args->sendcounts = NULL;
    na_args->intra_node_group = intra_node_group;
    na_args->node_leader_group = node_leader_group;
    na_args->scatterv_origin = ucx_op->super.super.args;
    na_args->vgroup_origin = vgroup;

    ucg_coll_scatterv_args_t scatterv_inter_args = {
        .sendbuf = NULL,
        .displs = NULL,
        .sendcounts = NULL,
        .sendtype = NULL,
        .recvbuf = NULL,
        .recvcount = 0,
        .recvtype = NULL,
        .root = 0
    };

    ucg_coll_scatterv_args_t scatterv_intra_args = {
        .sendbuf = NULL,
        .displs = NULL,
        .sendcounts = NULL,
        .sendtype = NULL,
        .recvbuf = NULL,
        .recvcount = 0,
        .recvtype = NULL,
        .root = 0
    };

    na_args->scatterv_inter.scatterv = scatterv_inter_args;
    na_args->scatterv_intra.scatterv = scatterv_intra_args;
    return UCG_OK;
}

static ucg_status_t ucg_planc_ucx_scatterv_na_kntree_op_trigger(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status;
    ucg_planc_ucx_op_t *ucx_op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_coll_scatterv_args_t *args = &ucg_op->super.args.scatterv;

    ucg_vgroup_t *vgroup = ucx_op->super.vgroup;
    UCG_CHECK_NULL(UCG_ERR_NO_MEMORY, vgroup);

    uint32_t group_size = vgroup->size;
    ucg_planc_ucx_op_reset(ucx_op);
    ucg_planc_ucx_scatterv_na_kntree_op_init(ucx_op);
    ucg_planc_ucx_scatterv_na_kntree_args_t *na_args = &ucx_op->scatterv.na_kntree;
    ucg_coll_scatterv_args_t *inter_args = &na_args->scatterv_inter.scatterv;

    /* if root == 0, rank 0 need to copy origin_args to inter_args
     * 1. aggregated sendbuf
     * 2. sendcounts
     * 3. sendtype
     */
    if (ucg_op->vgroup->myrank == args->root) {
        uint64_t sdt_size = ucg_dt_size(args->sendtype);
        /* 1. set sendbuffer */
        int32_t total_count = 0;
        for (uint32_t i = 0; i < group_size; ++i) {
            total_count += args->sendcounts[i];
        }
        uint32_t bufsize = sdt_size * total_count;
        inter_args->sendbuf = ucg_malloc(bufsize,
                                         "scatterv adjust root data sendbuffer");
        if (inter_args->sendbuf == NULL) {
            return UCG_ERR_NO_MEMORY;
        }
        int32_t offset = 0;
        for (uint32_t i = 0; i < group_size; ++i) {
            status = ucg_dt_memcpy((void*)inter_args->sendbuf + offset * sdt_size,
                                   args->sendcounts[i], args->sendtype,
                                   args->sendbuf + args->displs[i] * sdt_size,
                                   args->sendcounts[i], args->sendtype);
            if (status != UCG_OK) {
                return status;
            }
            offset += args->sendcounts[i];
        }
        /* set sendcounts */
        inter_args->sendcounts = ucg_calloc(group_size, sizeof(int32_t),
                                 "scatterv inter temp sendcounts");
        ucg_dt_memcpy((int32_t *)inter_args->sendcounts, group_size,
                      ucg_dt_get_predefined(UCG_DT_TYPE_INT32),
                      args->sendcounts, group_size,
                      ucg_dt_get_predefined(UCG_DT_TYPE_INT32));
        inter_args->sendtype = args->sendtype;
        na_args->sendbuf_size = bufsize;
        na_args->sdtype_size = sdt_size;
    }

    if (na_args->intra_node_group->state == UCG_TOPO_GROUP_STATE_ENABLE) {
        ucx_op->flags = UCG_KNTREE_INTRA_NODE_PARAMS_FLAGS;
    }
    if (na_args->node_leader_group->state == UCG_TOPO_GROUP_STATE_ENABLE) {
        ucx_op->flags = UCG_KNTREE_INTER_NODE_PARAMS_FLAGS;
    }
    if (args->root != UCG_TOPO_GROUP_LEADER) {
        if (vgroup->myrank == UCG_TOPO_GROUP_LEADER) {
            ucx_op->flags = UCG_KNTREE_ADJUST_ROOT_RECV_PARAMS_FLAGS;
        } else if (vgroup->myrank == args->root) {
            ucx_op->flags = UCG_KNTREE_ADJUST_ROOT_SEND_PARAMS_FLAGS;
        }
    }
    status = ucg_planc_ucx_scatterv_na_kntree_op_progress(ucg_op);
    return (status == UCG_INPROGRESS) ? UCG_OK : status;
}

ucg_planc_ucx_op_t* ucg_planc_ucx_scatter_na_kntree_op_new(ucg_planc_ucx_group_t *ucx_group,
                                                            ucg_vgroup_t *vgroup,
                                                            const ucg_coll_args_t *args,
                                                            const ucg_planc_ucx_scatter_config_t *config)
{
    UCG_CHECK_NULL(NULL, ucx_group, vgroup, args, config);
    int *sendcounts = (int *)malloc(sizeof(int32_t) * vgroup->size);
    for (int i = 0; i < vgroup->size; i++) {
        sendcounts[i] = args->scatter.sendcount;
    }
    int *displs = (int *)malloc(sizeof(int32_t) * vgroup->size);
    for (int i = 0; i < vgroup->size; i++) {
        displs[i] = i * args->scatter.sendcount;
    }
    ucg_coll_args_t args_scatterv = {
        .scatterv = {
            .sendbuf = args->scatter.sendbuf,
            .sendcounts = sendcounts,
            .displs = displs,
            .sendtype = args->scatter.sendtype,
            .recvbuf = args->scatter.recvbuf,
            .recvcount = args->scatter.recvcount,
            .recvtype = args->scatter.recvtype,
            .root = args->scatter.root
        },
        .type = UCG_COLL_TYPE_SCATTERV,
        .info = args->info
    };

    ucg_planc_ucx_op_t *ucx_op = ucg_mpool_get(&ucx_group->context->op_mp);
    if (ucx_op == NULL) {
        goto err;
    }

    ucg_status_t status;
    status = UCG_CLASS_CONSTRUCT(ucg_plan_op_t, &ucx_op->super, vgroup,
                                 ucg_planc_ucx_scatterv_na_kntree_op_trigger,
                                 ucg_planc_ucx_scatterv_na_kntree_op_progress,
                                 ucg_planc_ucx_scatterv_na_kntree_op_discard,
                                 &args_scatterv);
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

static ucg_status_t ucg_planc_ucx_scatter_na_kntree_check(ucg_vgroup_t *vgroup,
                                                           const ucg_coll_args_t *args)
{
    int32_t ppn = vgroup->group->topo->ppn;
    if (ppn == UCG_TOPO_PPX_UNKNOWN) {
        ucg_info("Scatter na_kntree don't support unknown ppn");
        return UCG_ERR_UNSUPPORTED;
    }
    if (ppn == UCG_TOPO_PPX_UNBALANCED) {
        ucg_info("Scatter na_kntree don't support unbalanced ppn");
        return UCG_ERR_UNSUPPORTED;
    }
    if (ppn == 1) {
        ucg_info("Scatter na_kntree don't support ppn==1");
        return UCG_ERR_UNSUPPORTED;
    }
    if (!vgroup->group->topo->detail.nrank_continuous) {
        ucg_info("Scatter na_kntree don't support node ranks discontinuous");
        return UCG_ERR_UNSUPPORTED;
    }
    uint32_t group_size = vgroup->size;
    if ((uint32_t)ppn == group_size) {
        ucg_info("Scatter na_kntree don't support single node");
        return UCG_ERR_UNSUPPORTED;
    }
    const ucg_coll_scatter_args_t *scatter_args = &args->scatter;
    if (scatter_args->sendbuf == UCG_IN_PLACE) {
        ucg_info("Scatter na_kntree don't support in place sendbuf");
        return UCG_ERR_UNSUPPORTED;
    }

    return UCG_OK;
}

ucg_status_t ucg_planc_ucx_scatter_na_kntree_prepare(ucg_vgroup_t *vgroup,
                                                      const ucg_coll_args_t *args,
                                                      ucg_plan_op_t **op)
{
    UCG_CHECK_NULL_INVALID(vgroup, args, op);

    ucg_status_t status;
    status = ucg_planc_ucx_scatter_na_kntree_check(vgroup, args);
    if (status != UCG_OK) {
        return UCG_ERR_UNSUPPORTED;
    }

    ucg_planc_ucx_group_t *ucx_group = ucg_derived_of(vgroup, ucg_planc_ucx_group_t);
    ucg_planc_ucx_scatter_config_t *config;
    config = UCG_PLANC_UCX_CONTEXT_BUILTIN_CONFIG_BUNDLE(ucx_group->context, scatter,
                                                         UCG_COLL_TYPE_SCATTER);
    ucg_planc_ucx_op_t *ucx_op;
    ucx_op = ucg_planc_ucx_scatter_na_kntree_op_new(ucx_group, vgroup, args, config);
    if (ucx_op == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    *op = &ucx_op->super;
    return UCG_OK;
}
