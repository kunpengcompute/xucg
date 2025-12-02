/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2024. All rights reserved.
 */

#include "gather.h"
#include "planc_ucx_plan.h"
#include "gather_meta.h"

enum {
    UCG_GATHER_LINEAR_RECV = UCG_BIT(0),
    UCG_GATHER_LINEAR_SEND = UCG_BIT(1),
};

#define UCG_GATHER_LINEAR_FLAGS UCG_GATHER_LINEAR_RECV | UCG_GATHER_LINEAR_SEND

static ucg_status_t ucg_planc_ucx_gather_kntree_op_discard(ucg_plan_op_t *ucg_op)
{
    ucg_planc_ucx_op_t *ucx_op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_coll_args_t *gatherv_args = &ucx_op->gather.gatherv.args;

    ucg_plan_op_t *op;
    op = &ucx_op->gather.gatherv.op->super;
    op->discard(op);

    void *recvcounts = (void *)gatherv_args->gatherv.recvcounts;
    void *disps = (void *)gatherv_args->gatherv.displs;
    ucx_op->gather.gatherv.op = NULL; 
    if (recvcounts != NULL) {
        ucg_free(recvcounts);
    }
    gatherv_args->gatherv.recvcounts = NULL;
    if (disps != NULL) {
        ucg_free(disps);
    }
    gatherv_args->gatherv.displs = NULL;

    UCG_CLASS_DESTRUCT(ucg_plan_op_t, ucg_op);
    ucg_mpool_put(ucg_op);
    return UCG_OK;
}


ucg_status_t ucg_planc_ucx_gather_kntree_op_progress(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_op_t *ucx_op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_plan_op_t *curr_ucg_op = NULL;

    if (ucx_op->gather.gatherv.op_trigged == 0) {
            curr_ucg_op = &ucx_op->gather.gatherv.op->super;
            status = curr_ucg_op->trigger(curr_ucg_op);
            ucx_op->gather.gatherv.op_trigged = 1;
    }
    curr_ucg_op = &ucx_op->gather.gatherv.op->super;
    status = curr_ucg_op->progress(curr_ucg_op);
    UCG_CHECK_GOTO(status, out);

out:
    ucx_op->super.super.status = status;
    return status;
}

static ucg_status_t ucg_planc_ucx_gather_kntree_op_trigger(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status;
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    op->gather.gatherv.op_trigged = 0;
    ucg_planc_ucx_op_reset(op);
    op->flags = UCG_GATHER_LINEAR_FLAGS;
    status = ucg_planc_ucx_gather_kntree_op_progress(ucg_op);
    return status == UCG_INPROGRESS ? UCG_OK : status;
}

static void ucg_planc_ucx_gather_kntree_op_init(ucg_planc_ucx_op_t *ucx_op)
{
    ucg_vgroup_t *vgroup = ucx_op->super.vgroup;
    ucg_planc_ucx_group_t *ucx_group = ucg_derived_of(vgroup, ucg_planc_ucx_group_t);
    ucg_coll_args_t *args = &ucx_op->super.super.args;
    ucg_planc_ucx_op_t *curr_ucx_op;
    ucg_coll_args_t *gatherv_args = &ucx_op->gather.gatherv.args;
    ucx_op->gather.gatherv.op_trigged = 0;
    ucx_op->gather.gatherv.op = NULL;


    gatherv_args->type = UCG_COLL_TYPE_GATHERV;
    gatherv_args->gatherv.root = args->gather.root;
    gatherv_args->gatherv.sendbuf = args->gather.sendbuf;
    gatherv_args->gatherv.sendcount = args->gather.sendcount;
    gatherv_args->gatherv.sendtype = args->gather.sendtype;
    gatherv_args->gatherv.recvbuf = args->gather.recvbuf;
    gatherv_args->gatherv.recvtype = args->gather.recvtype;
    int32_t *recvcounts = (int32_t *)ucg_malloc(sizeof(int32_t) * (int32_t)vgroup->size, "gatherv recvcounts");
    int32_t *disps = (int32_t *)ucg_malloc(sizeof(int32_t) * (int32_t)vgroup->size, "gatherv disps");
    for (int i = 0 ; i < vgroup->size; i++) {
        recvcounts[i] = args->gather.recvcount;
        disps[i] = i * args->gather.recvcount;
    }
    gatherv_args->gatherv.displs = disps;
    gatherv_args->gatherv.recvcounts = recvcounts;
    curr_ucx_op = ucg_planc_ucx_gather_build_topo_group_op(ucx_group,
                                                             vgroup,
                                                             gatherv_args,
                                                             UCG_GATHER_KNTREE,
                                                             UCG_TOPO_GROUP_TYPE_NET);
    /* To ensure that requests of multiple members in the same collection op
     **         can be matched, all subops must have the same request ID. */
    curr_ucx_op->super.super.id = ucx_op->super.super.id;
    ucx_op->gather.gatherv.op = curr_ucx_op;
    return;
}

ucg_planc_ucx_op_t *ucg_planc_ucx_gather_kntree_op_new(ucg_planc_ucx_group_t *ucx_group,
                                                        ucg_vgroup_t *vgroup,
                                                        const ucg_coll_args_t *args)
{
    UCG_CHECK_NULL(NULL, ucx_group, vgroup, args);

    ucg_planc_ucx_op_t *ucx_op = ucg_mpool_get(&ucx_group->context->op_mp);
    if (ucx_op == NULL) {
        goto err;
    }

    ucg_status_t status = UCG_CLASS_CONSTRUCT(ucg_plan_op_t, &ucx_op->super, vgroup,
                                              ucg_planc_ucx_gather_kntree_op_trigger,
                                              ucg_planc_ucx_gather_kntree_op_progress,
                                              ucg_planc_ucx_gather_kntree_op_discard,
                                              args);

    if (status != UCG_OK) {
        ucg_error("Failed to initialize super of ucx op");
        goto err_free_op;
    }

    ucg_planc_ucx_op_init(ucx_op, ucx_group);
    ucg_planc_ucx_gather_kntree_op_init(ucx_op);
    return ucx_op;

err_free_op:
    ucg_mpool_put(ucx_op);
err:
    return NULL;
}

static ucg_status_t ucg_planc_ucx_gather_kntree_check(const ucg_coll_args_t *args)
{
    if (args->gather.root != 0) {
        ucg_info("Node-aware kntree gather does not support root != 0");
        return UCG_ERR_UNSUPPORTED;
    }
    return UCG_OK;
}

ucg_status_t ucg_planc_ucx_gather_kntree_prepare(ucg_vgroup_t *vgroup,
                                                  const ucg_coll_args_t *args,
                                                  ucg_plan_op_t **op)
{
    UCG_CHECK_NULL_INVALID(vgroup, args, op);
    ucg_status_t status;
    status = ucg_planc_ucx_gather_kntree_check(args);
    if (status != UCG_OK) {
        return status;
    }
    ucg_planc_ucx_group_t *ucx_group = ucg_derived_of(vgroup, ucg_planc_ucx_group_t);
    ucg_planc_ucx_op_t *kntree_op = ucg_planc_ucx_gather_kntree_op_new(ucx_group, vgroup, args);
    if (kntree_op == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    *op = &kntree_op->super;
    return UCG_OK;
}
