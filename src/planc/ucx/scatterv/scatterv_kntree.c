/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "scatterv.h"
#include "planc_ucx_plan.h"
#include "planc_ucx_p2p.h"
#include "core/ucg_dt.h"
#include "core/ucg_group.h"
#include "util/ucg_log.h"
#include "util/ucg_malloc.h"

enum {
    UCG_SCATTERV_KNTREE_PARAMS = UCG_BIT(0),
    UCG_SCATTERV_KNTREE_PARAMS_RECV_FROM_PARENT = UCG_BIT(1),
    UCG_SCATTERV_KNTREE_PARAMS_SEND_TO_CHILD = UCG_BIT(2),
    UCG_SCATTERV_KNTREE_PARAMS_RECV = UCG_BIT(3),
    UCG_SCATTERV_KNTREE_PARAMS_SEND = UCG_BIT(4),
    UCG_SCATTERV_KNTREE_PARAMS_ALLOC_STAGING = UCG_BIT(5),
    UCG_SCATTERV_KNTREE_DATA = UCG_BIT(6),
    UCG_SCATTERV_KNTREE_DATA_INIT = UCG_BIT(7),
    UCG_SCATTERV_KNTREE_DATA_RECV_FROM_PARENT = UCG_BIT(8),
    UCG_SCATTERV_KNTREE_DATA_SEND_TO_CHILD = UCG_BIT(9),
    UCG_SCATTERV_KNTREE_DATA_RECV = UCG_BIT(10),
    UCG_SCATTERV_KNTREE_DATA_RECV_WAIT = UCG_BIT(11),
    UCG_SCATTERV_KNTREE_DATA_SEND = UCG_BIT(12),
};

#define UCG_SCATTERV_KNTREE_PARAMS_FLAGS UCG_SCATTERV_KNTREE_PARAMS | \
                                         UCG_SCATTERV_KNTREE_PARAMS_RECV_FROM_PARENT | \
                                         UCG_SCATTERV_KNTREE_PARAMS_SEND_TO_CHILD | \
                                         UCG_SCATTERV_KNTREE_PARAMS_RECV | \
                                         UCG_SCATTERV_KNTREE_PARAMS_SEND | \
                                         UCG_SCATTERV_KNTREE_PARAMS_ALLOC_STAGING

#define UCG_SCATTERV_KNTREE_DATA_FLAGS UCG_SCATTERV_KNTREE_DATA | \
                                       UCG_SCATTERV_KNTREE_DATA_INIT | \
                                       UCG_SCATTERV_KNTREE_DATA_RECV_FROM_PARENT | \
                                       UCG_SCATTERV_KNTREE_DATA_SEND_TO_CHILD | \
                                       UCG_SCATTERV_KNTREE_DATA_RECV | \
                                       UCG_SCATTERV_KNTREE_DATA_RECV_WAIT | \
                                       UCG_SCATTERV_KNTREE_DATA_SEND

#define UCG_SCATTERV_KNTREE_FLAGS UCG_SCATTERV_KNTREE_PARAMS_FLAGS | \
                                  UCG_SCATTERV_KNTREE_DATA_FLAGS

static ucg_status_t ucg_planc_ucx_scatterv_kntree_op_params_recv(ucg_planc_ucx_op_t *op)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_group_t *ucx_group = op->ucx_group;
    ucg_vgroup_t *vgroup = op->super.vgroup;
    uint32_t group_size = vgroup->size;
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(op, &params);
    ucg_algo_kntree_iter_t *iter = &op->scatterv.kntree.kntree_iter;
    ucg_rank_t peer;

    peer = ucg_algo_kntree_iter_parent_value(iter);
    if (peer == UCG_INVALID_RANK) {
        return UCG_OK;
    }

    if (ucg_test_and_clear_flags(&op->flags, UCG_SCATTERV_KNTREE_PARAMS_RECV)) {
        status = ucg_planc_ucx_p2p_irecv(op->scatterv.kntree.sendcounts, group_size,
                                         ucg_dt_get_predefined(UCG_DT_TYPE_INT32),
                                         peer, op->tag, vgroup, &params);
        UCG_CHECK_GOTO(status, out);
        status = ucg_planc_ucx_p2p_irecv(&op->scatterv.kntree.sdtype_size, 1,
                                         ucg_dt_get_predefined(UCG_DT_TYPE_INT32),
                                         peer, op->tag, vgroup, &params);
        UCG_CHECK_GOTO(status, out);
    }
    status = ucg_planc_ucx_p2p_testall(ucx_group, params.state);
out:
    return status;
}

static ucg_status_t ucg_planc_ucx_scatterv_kntree_op_params_send(ucg_planc_ucx_op_t *op)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_group_t *ucx_group = op->ucx_group;
    ucg_vgroup_t *vgroup = op->super.vgroup;
    uint32_t group_size = vgroup->size;
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(op, &params);
    ucg_coll_scatterv_args_t *args = &op->super.super.args.scatterv;
    ucg_algo_kntree_iter_t *iter = &op->scatterv.kntree.kntree_iter;
    ucg_rank_t peer;

    if (ucg_test_flags(op->flags, UCG_SCATTERV_KNTREE_PARAMS_SEND)) {
        while ((peer = ucg_algo_kntree_iter_child_value(iter)) != UCG_INVALID_RANK) {
            const void *sbuf = op->scatterv.kntree.sendcounts;
            if (op->scatterv.kntree.sendcounts == NULL) {
                sbuf = args->sendcounts;
            }
            status = ucg_planc_ucx_p2p_isend(sbuf, group_size,
                                             ucg_dt_get_predefined(UCG_DT_TYPE_INT32),
                                             peer, op->tag, vgroup, &params);
            UCG_CHECK_GOTO(status, out);
            status = ucg_planc_ucx_p2p_isend(&op->scatterv.kntree.sdtype_size, 1,
                                             ucg_dt_get_predefined(UCG_DT_TYPE_INT32),
                                             peer, op->tag, vgroup, &params);
            UCG_CHECK_GOTO(status, out);
            ucg_algo_kntree_iter_child_inc(iter);
        }
        ucg_clear_flags(&op->flags, UCG_SCATTERV_KNTREE_PARAMS_SEND);
    }
    status = ucg_planc_ucx_p2p_testall(ucx_group, params.state);
out:
    return status;
}

static ucg_status_t ucg_planc_ucx_scatterv_kntree_op_data_recv(ucg_planc_ucx_op_t *op)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_group_t *ucx_group = op->ucx_group;
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_rank_t myrank = vgroup->myrank;
    uint32_t group_size = vgroup->size;
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(op, &params);
    ucg_algo_kntree_iter_t *iter = &op->scatterv.kntree.kntree_iter;
    ucg_coll_scatterv_args_t *args = &op->super.super.args.scatterv;
    ucg_rank_t peer;

    peer = ucg_algo_kntree_iter_parent_value(iter);
    if (peer == UCG_INVALID_RANK) {
        ucg_assert(myrank == args->root);
        if (args->recvbuf != UCG_IN_PLACE) {
            const void *sbuf = args->sendbuf + args->displs[myrank] * ucg_dt_extent(args->sendtype);
            int32_t scount = args->sendcounts[myrank];
            status = ucg_dt_memcpy(args->recvbuf, args->recvcount, args->recvtype,
                                   sbuf, scount, args->sendtype);
            UCG_CHECK_GOTO(status, out);
        }
        return UCG_OK;
    }

    if (ucg_test_and_clear_flags(&op->flags, UCG_SCATTERV_KNTREE_DATA_RECV)) {
        //Recv myself data
        status = ucg_planc_ucx_p2p_irecv(args->recvbuf, args->recvcount,
                                         args->recvtype, peer, op->tag,
                                         vgroup, &params);
        UCG_CHECK_GOTO(status, out);

        //Recv my children's data
        if (op->scatterv.kntree.staging_count > 0) {
            int64_t offset = 0;
            for (int32_t i = 0; i < op->scatterv.kntree.staging_count; i++) {
                int32_t idx = (myrank + i + 1) % group_size;
                int32_t recv_len = op->scatterv.kntree.sendcounts[idx] * op->scatterv.kntree.sdtype_size;
                op->scatterv.kntree.staging_displs[i] = offset;
                status = ucg_planc_ucx_p2p_irecv(op->staging_area + offset, recv_len,
                                                 ucg_dt_get_predefined(UCG_DT_TYPE_UINT8),
                                                 peer, op->tag, vgroup, &params);
                UCG_CHECK_GOTO(status, out);
                offset += recv_len;
            }
        }
    }

    if (ucg_test_flags(op->flags, UCG_SCATTERV_KNTREE_DATA_RECV_WAIT)) {
        status = ucg_planc_ucx_p2p_testall(ucx_group, params.state);
        UCG_CHECK_GOTO(status, out);
        ucg_clear_flags(&op->flags, UCG_SCATTERV_KNTREE_DATA_RECV_WAIT);
    }

out:
    return status;
}

static ucg_status_t ucg_planc_ucx_scatterv_kntree_op_data_send(ucg_planc_ucx_op_t *op)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_group_t *ucx_group = op->ucx_group;
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_rank_t myrank = vgroup->myrank;
    uint32_t group_size = vgroup->size;
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(op, &params);
    ucg_coll_scatterv_args_t *args = &op->super.super.args.scatterv;
    ucg_algo_kntree_iter_t *iter = &op->scatterv.kntree.kntree_iter;
    ucg_rank_t peer;

    if (ucg_test_flags(op->flags, UCG_SCATTERV_KNTREE_DATA_SEND)) {
        while ((peer = ucg_algo_kntree_iter_child_value(iter)) != UCG_INVALID_RANK) {
            if (myrank == args->root) {
                int32_t peer_subtree_size = ucg_algo_kntree_get_subtree_size(iter, peer);
                for (int32_t i = 0; i < peer_subtree_size; i++) {
                    int32_t idx = (i + peer) % group_size;
                    int64_t offset = (int64_t)args->displs[idx] * ucg_dt_extent(args->sendtype);
                    status = ucg_planc_ucx_p2p_isend(args->sendbuf + offset,
                                                     args->sendcounts[idx],
                                                     args->sendtype,
                                                     peer, op->tag, vgroup,
                                                     &params);
                    UCG_CHECK_GOTO(status, out);
                }
            } else {
                int32_t peer_subtree_size = ucg_algo_kntree_get_subtree_size(iter, peer);
                for (int32_t i = 0; i < peer_subtree_size; i++) {
                    int32_t idx = (i + peer) % group_size;
                    int32_t send_len = op->scatterv.kntree.sendcounts[idx] * op->scatterv.kntree.sdtype_size;
                    int32_t staging_displs_idx = (myrank < idx) ?
                                                 (idx - myrank - 1) :
                                                 (idx + group_size - myrank - 1);
                    int64_t offset = op->scatterv.kntree.staging_displs[staging_displs_idx];
                    status = ucg_planc_ucx_p2p_isend(op->staging_area + offset,
                                                     send_len,
                                                     ucg_dt_get_predefined(UCG_DT_TYPE_UINT8),
                                                     peer, op->tag, vgroup, &params);
                    UCG_CHECK_GOTO(status, out);
                }
            }
            ucg_algo_kntree_iter_child_inc(iter);
        }
        ucg_clear_flags(&op->flags, UCG_SCATTERV_KNTREE_DATA_SEND);
    }
    status = ucg_planc_ucx_p2p_testall(ucx_group, params.state);
out:
    return status;
}

static ucg_status_t ucg_planc_ucx_scatterv_kntree_op_params(ucg_planc_ucx_op_t *op)
{
    ucg_status_t status = UCG_OK;
    if (ucg_test_flags(op->flags, UCG_SCATTERV_KNTREE_PARAMS_RECV_FROM_PARENT)) {
        status = ucg_planc_ucx_scatterv_kntree_op_params_recv(op);
        UCG_CHECK_GOTO(status, out);
        ucg_clear_flags(&op->flags, UCG_SCATTERV_KNTREE_PARAMS_RECV_FROM_PARENT);
    }

    if (ucg_test_flags(op->flags, UCG_SCATTERV_KNTREE_PARAMS_SEND_TO_CHILD)) {
        status = ucg_planc_ucx_scatterv_kntree_op_params_send(op);
        UCG_CHECK_GOTO(status, out);
        ucg_clear_flags(&op->flags, UCG_SCATTERV_KNTREE_PARAMS_SEND_TO_CHILD);
    }

    if (ucg_test_and_clear_flags(&op->flags, UCG_SCATTERV_KNTREE_PARAMS_ALLOC_STAGING)) {
        ucg_coll_scatterv_args_t *args = &op->super.super.args.scatterv;
        ucg_vgroup_t *vgroup = op->super.vgroup;
        ucg_rank_t myrank = vgroup->myrank;
        uint32_t group_size = vgroup->size;
        if ((myrank != args->root) && (op->scatterv.kntree.staging_count > 0)) {
            int64_t size = 0;
            for (int32_t i = 0; i < op->scatterv.kntree.staging_count; i++) {
                int32_t idx = (myrank + i + 1) % group_size;
                size += (int64_t)op->scatterv.kntree.sendcounts[idx] * op->scatterv.kntree.sdtype_size;
            }
            op->staging_area = ucg_malloc(size, "scatterv kntree staging area");
            if (op->staging_area == NULL) {
                return UCG_ERR_NO_MEMORY;
            }
        }
    }

out:
    return status;
}

static ucg_status_t ucg_planc_ucx_scatterv_kntree_op_data(ucg_planc_ucx_op_t *op)
{
    ucg_status_t status = UCG_OK;

    if (ucg_test_and_clear_flags(&op->flags, UCG_SCATTERV_KNTREE_DATA_INIT)) {
        //kntree iter should be reset here for op data
        ucg_algo_kntree_iter_reset(&op->scatterv.kntree.kntree_iter);
    }

    if (ucg_test_flags(op->flags, UCG_SCATTERV_KNTREE_DATA_RECV_FROM_PARENT)) {
        status = ucg_planc_ucx_scatterv_kntree_op_data_recv(op);
        UCG_CHECK_GOTO(status, out);
        ucg_clear_flags(&op->flags, UCG_SCATTERV_KNTREE_DATA_RECV_FROM_PARENT);
    }

    if (ucg_test_flags(op->flags, UCG_SCATTERV_KNTREE_DATA_SEND_TO_CHILD)) {
        status = ucg_planc_ucx_scatterv_kntree_op_data_send(op);
        UCG_CHECK_GOTO(status, out);
        ucg_clear_flags(&op->flags, UCG_SCATTERV_KNTREE_DATA_SEND_TO_CHILD);
    }

out:
    return status;
}

static ucg_status_t ucg_planc_ucx_scatterv_kntree_op_progress(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    if (ucg_test_flags(op->flags, UCG_SCATTERV_KNTREE_PARAMS)) {
        status = ucg_planc_ucx_scatterv_kntree_op_params(op);
        UCG_CHECK_GOTO(status, out);
        ucg_clear_flags(&op->flags, UCG_SCATTERV_KNTREE_PARAMS);
    }

    if (ucg_test_flags(op->flags, UCG_SCATTERV_KNTREE_DATA)) {
        status = ucg_planc_ucx_scatterv_kntree_op_data(op);
        UCG_CHECK_GOTO(status, out);
        op->scatterv.kntree.first_trigger = 0;
        ucg_clear_flags(&op->flags, UCG_SCATTERV_KNTREE_DATA);
    }

out:
    op->super.super.status = status;
    return status;
}

static ucg_status_t ucg_planc_ucx_scatterv_kntree_op_trigger(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status;
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_planc_ucx_op_reset(op);
    ucg_algo_kntree_iter_reset(&op->scatterv.kntree.kntree_iter);
    // for second trigger, we only need do op data
    if (op->scatterv.kntree.first_trigger) {
        op->flags = UCG_SCATTERV_KNTREE_FLAGS;
    } else {
        op->flags = UCG_SCATTERV_KNTREE_DATA_FLAGS;
    }
    status = ucg_planc_ucx_scatterv_kntree_op_progress(ucg_op);
    return status == UCG_INPROGRESS ? UCG_OK : status;
}

static inline ucg_status_t ucg_planc_ucx_scatterv_kntree_op_discard(ucg_plan_op_t *ucg_op)
{
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    if (op->scatterv.kntree.sendcounts != NULL) {
        ucg_free(op->scatterv.kntree.sendcounts);
    }
    if (op->scatterv.kntree.staging_displs != NULL) {
        ucg_free(op->scatterv.kntree.staging_displs);
    }
    return ucg_planc_ucx_op_discard(ucg_op);
}

static inline
ucg_status_t ucg_planc_ucx_scatterv_kntree_op_init(ucg_planc_ucx_op_t *op,
                                                   ucg_planc_ucx_group_t *ucx_group,
                                                   const ucg_planc_ucx_scatterv_config_t *config)
{
    ucg_planc_ucx_op_init(op, ucx_group);
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_rank_t myrank = vgroup->myrank;
    int32_t group_size = vgroup->size;
    ucg_coll_scatterv_args_t *args = &op->super.super.args.scatterv;
    ucg_algo_kntree_iter_t *iter = &op->scatterv.kntree.kntree_iter;

    ucg_algo_kntree_iter_init(iter, vgroup->size, config->kntree_degree,
                              args->root, vgroup->myrank, 1);
    op->scatterv.kntree.staging_count = ucg_algo_kntree_get_subtree_size(iter, myrank) - 1;
    op->scatterv.kntree.sendcounts = NULL;
    op->scatterv.kntree.staging_displs = NULL;
    if (myrank == args->root) {
        op->scatterv.kntree.sdtype_size = ucg_dt_size(args->sendtype);
    } else {
        op->scatterv.kntree.sendcounts = ucg_malloc((int64_t)group_size * sizeof(int32_t),
                                                    "scatterv sendcounts");
        if (op->scatterv.kntree.sendcounts == NULL) {
            return UCG_ERR_NO_MEMORY;
        }
        op->scatterv.kntree.staging_displs = ucg_malloc((int64_t)op->scatterv.kntree.staging_count * sizeof(int32_t),
                                                        "scatterv staging_displs");
        if (op->scatterv.kntree.staging_displs == NULL) {
            return UCG_ERR_NO_MEMORY;
        }
    }
    op->scatterv.kntree.first_trigger = 1;

    return UCG_OK;
}

ucg_planc_ucx_op_t *ucg_planc_ucx_scatterv_kntree_op_new(ucg_planc_ucx_group_t *ucx_group,
                                                         ucg_vgroup_t *vgroup,
                                                         const ucg_coll_args_t *args,
                                                         const ucg_planc_ucx_scatterv_config_t *config)
{
    UCG_CHECK_NULL(NULL, ucx_group, vgroup, args);

    ucg_planc_ucx_op_t *ucx_op = ucg_mpool_get(&ucx_group->context->op_mp);
    if (ucx_op == NULL) {
        goto err;
    }

    ucg_status_t status = UCG_CLASS_CONSTRUCT(ucg_plan_op_t, &ucx_op->super, vgroup,
                                              ucg_planc_ucx_scatterv_kntree_op_trigger,
                                              ucg_planc_ucx_scatterv_kntree_op_progress,
                                              ucg_planc_ucx_scatterv_kntree_op_discard,
                                              args);
    if (status != UCG_OK) {
        ucg_error("Failed to initialize super of ucx op");
        goto err_free_op;
    }

    status = ucg_planc_ucx_scatterv_kntree_op_init(ucx_op, ucx_group, config);
    if (status != UCG_OK) {
        ucg_error("Failed to initialize scatterv ucx op");
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

ucg_status_t ucg_planc_ucx_scatterv_kntree_prepare(ucg_vgroup_t *vgroup,
                                                 const ucg_coll_args_t *args,
                                                 ucg_plan_op_t **op)
{
    UCG_CHECK_NULL_INVALID(vgroup, args, op);

    ucg_planc_ucx_group_t *ucx_group = ucg_derived_of(vgroup, ucg_planc_ucx_group_t);
    ucg_planc_ucx_scatterv_config_t *config;
    config = UCG_PLANC_UCX_CONTEXT_BUILTIN_CONFIG_BUNDLE(ucx_group->context, scatterv,
                                                         UCG_COLL_TYPE_SCATTERV);
    ucg_planc_ucx_op_t *kntree_op = ucg_planc_ucx_scatterv_kntree_op_new(ucx_group,
                                                                         vgroup,
                                                                         args,
                                                                         config);
    if (kntree_op == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    *op = &kntree_op->super;
    return UCG_OK;
}