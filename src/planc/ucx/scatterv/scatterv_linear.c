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
    UCG_SCATTERV_LINEAR_RECV = UCG_BIT(0),
    UCG_SCATTERV_LINEAR_SEND = UCG_BIT(1),
};

#define UCG_SCATTERV_LINEAR_FLAGS UCG_SCATTERV_LINEAR_RECV | UCG_SCATTERV_LINEAR_SEND

static ucg_status_t ucg_planc_ucx_scatterv_linear_op_root(ucg_planc_ucx_op_t *op)
{
    ucg_status_t status = UCG_OK;
    ucg_coll_scatterv_args_t *args = &op->super.super.args.scatterv;
    ucg_vgroup_t *vgroup = op->super.vgroup;
    uint32_t group_size = vgroup->size;
    int *idx = &op->scatterv.linear.idx;
    uint32_t sendtype_extent = ucg_dt_extent(args->sendtype);
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(op, &params);

    while (*idx < group_size) {
        if (ucg_test_and_clear_flags(&op->flags, UCG_SCATTERV_LINEAR_SEND)) {
            void *sbuf = (char *)args->sendbuf + (int64_t)args->displs[*idx] * sendtype_extent;
            int32_t scount = args->sendcounts[*idx];
            if (*idx == args->root) {
                if (scount > 0 && args->recvbuf != UCG_IN_PLACE) {
                    status = ucg_dt_memcpy(args->recvbuf, args->recvcount, args->recvtype,
                                           sbuf, scount, args->sendtype);
                }
            } else {
                if (scount > 0) {
                    status = ucg_planc_ucx_p2p_isend(sbuf, scount, args->sendtype, *idx,
                                                     op->tag, vgroup, &params);
                }
            }
            UCG_CHECK_GOTO(status, out);
        }
        status = ucg_planc_ucx_p2p_testall(op->ucx_group, params.state);
        UCG_CHECK_GOTO(status, out);
        op->flags |= UCG_SCATTERV_LINEAR_SEND;
        (*idx)++;
    }
out:
    return status;
}

static ucg_status_t ucg_planc_ucx_scatterv_linear_op_non_root(ucg_planc_ucx_op_t *op)
{
    ucg_status_t status = UCG_OK;
    ucg_coll_scatterv_args_t *args = &op->super.super.args.scatterv;
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(op, &params);

    if (ucg_test_and_clear_flags(&op->flags, UCG_SCATTERV_LINEAR_RECV)) {
        if (args->recvcount > 0) {
            status = ucg_planc_ucx_p2p_irecv(args->recvbuf, args->recvcount,
                                             args->recvtype, args->root, op->tag,
                                             vgroup, &params);
            UCG_CHECK_GOTO(status, out);
        }
    }
    status = ucg_planc_ucx_p2p_testall(op->ucx_group, params.state);
out:
    return status;
}

ucg_status_t ucg_planc_ucx_scatterv_linear_op_progress(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_coll_scatterv_args_t *args = &op->super.super.args.scatterv;
    ucg_rank_t myrank = op->super.vgroup->myrank;
    if (myrank == args->root) {
        status = ucg_planc_ucx_scatterv_linear_op_root(op);
    } else {
        status = ucg_planc_ucx_scatterv_linear_op_non_root(op);
    }
    op->super.super.status = status;
    return status;
}

static ucg_status_t ucg_planc_ucx_scatterv_linear_op_trigger(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status;
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_planc_ucx_op_reset(op);
    op->scatterv.linear.idx = 0;
    op->flags = UCG_SCATTERV_LINEAR_FLAGS;
    status = ucg_planc_ucx_scatterv_linear_op_progress(ucg_op);
    return status == UCG_INPROGRESS ? UCG_OK : status;
}

ucg_planc_ucx_op_t *ucg_planc_ucx_scatterv_linear_op_new(ucg_planc_ucx_group_t *ucx_group,
                                                         ucg_vgroup_t *vgroup,
                                                         const ucg_coll_args_t *args)
{
    UCG_CHECK_NULL(NULL, ucx_group, vgroup, args);

    ucg_planc_ucx_op_t *ucx_op = ucg_mpool_get(&ucx_group->context->op_mp);
    if (ucx_op == NULL) {
        goto err;
    }

    ucg_status_t status = UCG_CLASS_CONSTRUCT(ucg_plan_op_t, &ucx_op->super, vgroup,
                                              ucg_planc_ucx_scatterv_linear_op_trigger,
                                              ucg_planc_ucx_scatterv_linear_op_progress,
                                              ucg_planc_ucx_op_discard,
                                              args);

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

ucg_status_t ucg_planc_ucx_scatterv_linear_prepare(ucg_vgroup_t *vgroup,
                                                  const ucg_coll_args_t *args,
                                                  ucg_plan_op_t **op)
{
    UCG_CHECK_NULL_INVALID(vgroup, args, op);

    ucg_planc_ucx_group_t *ucx_group = ucg_derived_of(vgroup, ucg_planc_ucx_group_t);
    ucg_planc_ucx_op_t *linear_op = ucg_planc_ucx_scatterv_linear_op_new(ucx_group, vgroup, args);
    if (linear_op == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    *op = &linear_op->super;
    return UCG_OK;
}
