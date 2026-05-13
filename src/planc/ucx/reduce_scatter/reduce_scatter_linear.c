/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

#include "reduce_scatter.h"
#include "planc_ucx_plan.h"
#include "planc_ucx_meta.h"

typedef enum {
    UCG_REDUCE_SCATTER_OP_EMPTY,
    UCG_REDUCE_SCATTER_OP_REDUCE,
    UCG_REDUCE_SCATTER_OP_SCATTER,
} ucg_reduce_scatter_op_type_t;

static void ucg_planc_ucx_reduce_scatter_init_reduce_args(const ucg_coll_args_t *args,
                                                          ucg_coll_args_t *reduce_args,
                                                          void *tmpbuf, uint32_t size)
{
    int64_t total_count = 0;
    for (int i = 0; i < size; ++i){
        total_count += args->reduce_scatter.recvcounts[i];
    }
    reduce_args->type = UCG_COLL_TYPE_REDUCE;
    reduce_args->info = args->info;
    if (args->reduce_scatter.sendbuf == UCG_IN_PLACE) {
        reduce_args->reduce.sendbuf = args->reduce_scatter.recvbuf;
    } else {
        reduce_args->reduce.sendbuf = args->reduce_scatter.sendbuf;
    }
    reduce_args->reduce.recvbuf = tmpbuf;
    reduce_args->reduce.count = total_count;
    reduce_args->reduce.dt = args->reduce_scatter.dt;
    reduce_args->reduce.op = args->reduce_scatter.op;
    reduce_args->reduce.root = UCG_TOPO_GROUP_LEADER;
    return;
}

static void ucg_planc_ucx_reduce_scatter_init_scatterv_args(const ucg_coll_args_t *args,
                                                            ucg_coll_args_t *scatterv_args,
                                                            void *tmpbuf, uint32_t size, ucg_rank_t rank)
{
    int32_t *displs = (int32_t *)ucg_malloc(size * sizeof(int32_t), "reduce_scatter displs");
    int64_t total_count = 0;
    for (int i = 0; i < size; ++i){
        displs[i] = total_count;
        total_count += args->reduce_scatter.recvcounts[i];
    }
    scatterv_args->type = UCG_COLL_TYPE_SCATTERV;
    scatterv_args->info = args->info;
    scatterv_args->scatterv.sendbuf = tmpbuf;
    scatterv_args->scatterv.recvbuf = args->reduce_scatter.recvbuf;
    scatterv_args->scatterv.sendcounts = args->reduce_scatter.recvcounts;
    scatterv_args->scatterv.displs = displs;
    scatterv_args->scatterv.recvcount = args->reduce_scatter.recvcounts[rank];
    scatterv_args->scatterv.sendtype = args->reduce_scatter.dt;
    scatterv_args->scatterv.recvtype = args->reduce_scatter.dt;
    scatterv_args->scatterv.root = UCG_TOPO_GROUP_LEADER;
    return;
}

static ucg_status_t ucg_planc_ucx_reduce_scatter_empty_op_progress(ucg_plan_op_t *ucg_op)
{
    return UCG_OK;
}

static ucg_status_t ucg_planc_ucx_reduce_scatter_empty_op_trigger(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status;
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_planc_ucx_op_reset(op);
    status = ucg_planc_ucx_reduce_scatter_empty_op_progress(ucg_op);
    return status == UCG_INPROGRESS ? UCG_OK : status;
}

static ucg_status_t ucg_planc_ucx_reduce_scatter_empty_op_discard(ucg_plan_op_t *ucg_op)
{
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);

    if(ucg_op->vgroup->myrank == UCG_TOPO_GROUP_LEADER) {
        ucg_free(op->staging_area);
    }
    UCG_CLASS_DESTRUCT(ucg_plan_op_t, &op->super);
    ucg_mpool_put(op);
    return UCG_OK;
}

static inline ucg_status_t ucg_planc_ucx_reduce_scatter_empty_op_init(ucg_planc_ucx_op_t *op,
                                                                      ucg_planc_ucx_group_t *ucx_group)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_op_init(op, ucx_group);

    ucg_vgroup_t *vgroup = op->super.vgroup;
    const ucg_coll_reduce_scatter_args_t *coll_args = &op->super.super.args.reduce_scatter;
    int64_t total_count = 0;
    for (int i = 0; i < vgroup->size; ++i){
        total_count += coll_args->recvcounts[i];
    }
    op->staging_area = coll_args->recvbuf;
    if(vgroup->myrank == UCG_TOPO_GROUP_LEADER) {
        op->staging_area = ucg_malloc(total_count * coll_args->dt->extent, "reduce_scatter tmpbuf");
        if (op->staging_area == NULL) {
            status = UCG_ERR_NO_MEMORY;
        }
    }

    return status;
}

static ucg_planc_ucx_op_t *ucg_planc_ucx_reduce_scatter_empty_op_new(ucg_planc_ucx_group_t *ucx_group,
                                                                     ucg_vgroup_t *vgroup,
                                                                     const ucg_coll_args_t *args)
{
    UCG_CHECK_NULL(NULL, ucx_group, vgroup, args);

    ucg_planc_ucx_op_t *op = ucg_mpool_get(&ucx_group->context->op_mp);
    if (op == NULL) {
        goto err;
    }

    ucg_status_t status;
    status = UCG_CLASS_CONSTRUCT(ucg_plan_op_t, &op->super, vgroup,
                                 ucg_planc_ucx_reduce_scatter_empty_op_trigger,
                                 ucg_planc_ucx_reduce_scatter_empty_op_progress,
                                 ucg_planc_ucx_reduce_scatter_empty_op_discard,
                                 args);
    if (status != UCG_OK) {
        ucg_error("Failed to initialize super of ucx op");
        goto err_free_op;
    }
    
    status = ucg_planc_ucx_reduce_scatter_empty_op_init(op, ucx_group);
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

static ucg_status_t ucg_planc_ucx_reduce_scatter_add_op(ucg_planc_ucx_op_t **empty_op,
                                                        ucg_plan_meta_op_t *meta_op,
                                                        ucg_topo_t *topo,
                                                        ucg_planc_ucx_group_t *ucx_group,
                                                        ucg_vgroup_t *vgroup,
                                                        const ucg_coll_args_t *args,
                                                        ucg_planc_ucx_reduce_config_t *reduce_config,
                                                        ucg_planc_ucx_scatterv_config_t *scatterv_config,
                                                        ucg_reduce_scatter_op_type_t type)
{
    ucg_planc_ucx_op_t *ucx_op = NULL;
    ucg_topo_group_t *topo_group;
    topo_group = ucg_topo_get_group(topo, UCG_TOPO_GROUP_TYPE_NET);
    if (topo_group == NULL) {
        return UCG_ERR_UNSUPPORTED;
    }

    if (topo_group->state == UCG_TOPO_GROUP_STATE_DISABLE) {
        /* I'm not in the topo group. */
        return ucg_planc_ucx_add_empty_op(meta_op, ucx_group, vgroup);
    }

    if (topo_group->state != UCG_TOPO_GROUP_STATE_ENABLE) {
        /* The group state is incorrect. */
        return UCG_ERR_NO_RESOURCE;
    }

    if (type == UCG_REDUCE_SCATTER_OP_REDUCE) {
        ucg_coll_args_t reduce_args;
        ucg_planc_ucx_reduce_scatter_init_reduce_args(args, &reduce_args, (*empty_op)->staging_area, vgroup->size);
        ucx_op = ucg_planc_ucx_reduce_kntree_op_new(ucx_group, &topo_group->super,
                                                    &reduce_args, reduce_config);
    } else if (type == UCG_REDUCE_SCATTER_OP_SCATTER) {
        ucg_coll_args_t scatterv_args;
        ucg_planc_ucx_reduce_scatter_init_scatterv_args(args, &scatterv_args, (*empty_op)->staging_area, vgroup->size, vgroup->myrank);
        ucx_op = ucg_planc_ucx_scatterv_kntree_op_new(ucx_group, &topo_group->super,
                                                     &scatterv_args, scatterv_config);
    } else {
        *empty_op = ucg_planc_ucx_reduce_scatter_empty_op_new(ucx_group, &topo_group->super, args);
        if (*empty_op == NULL) {
            return UCG_ERR_NO_MEMORY;
        }
        return ucg_plan_meta_op_add(meta_op, &(*empty_op)->super);
    }

    if (ucx_op == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    return ucg_plan_meta_op_add(meta_op, &ucx_op->super);
}

ucg_plan_meta_op_t *ucg_planc_ucx_reduce_scatter_linear_op_new(ucg_planc_ucx_group_t *ucx_group,
                                                               ucg_vgroup_t *vgroup,
                                                               const ucg_coll_args_t *args,
                                                               ucg_planc_ucx_reduce_config_t *reduce_config,
                                                               ucg_planc_ucx_scatterv_config_t *scatterv_config)
{
    UCG_CHECK_NULL(NULL, ucx_group, vgroup, args, reduce_config);
    UCG_CHECK_NULL(NULL, ucx_group, vgroup, args, scatterv_config);

    ucg_plan_meta_op_t *meta_op = ucg_plan_meta_op_new(vgroup->group, vgroup, args);
    if (meta_op == NULL) {
        goto err;
    }

    ucg_status_t status;
    ucg_topo_t *topo = vgroup->group->topo;
    ucg_coll_args_t *meta_args = &meta_op->super.super.args;

    ucg_planc_ucx_op_t *empty_op = NULL;

    /* 0. empty. */
    status = ucg_planc_ucx_reduce_scatter_add_op(&empty_op, meta_op, topo, ucx_group, vgroup,
                                                 meta_args, reduce_config, scatterv_config,
                                                 UCG_REDUCE_SCATTER_OP_EMPTY);
    UCG_CHECK_GOTO(status, err_free_meta_op);

    /* 1. reduce. */
    status = ucg_planc_ucx_reduce_scatter_add_op(&empty_op, meta_op, topo, ucx_group, vgroup,
                                                 meta_args, reduce_config, scatterv_config,
                                                 UCG_REDUCE_SCATTER_OP_REDUCE);
    UCG_CHECK_GOTO(status, err_free_meta_op);

    /* 2. scatterv. */
    status = ucg_planc_ucx_reduce_scatter_add_op(&empty_op, meta_op, topo, ucx_group, vgroup,
                                                 meta_args, reduce_config, scatterv_config,
                                                 UCG_REDUCE_SCATTER_OP_SCATTER);
    UCG_CHECK_GOTO(status, err_free_meta_op);

    return meta_op;

err_free_meta_op:
    meta_op->super.discard(&meta_op->super);
err:
    return NULL;
}

static ucg_status_t ucg_planc_ucx_reduce_scatter_linear_check(ucg_vgroup_t *vgroup,
                                                              const ucg_coll_args_t *args)
{
    if (args->reduce_scatter.op->type != UCG_OP_TYPE_SUM) {
        ucg_info("Reduce_scatterv linear op type is not MPI_SUM, so roll back to OpenMPI");
        return UCG_ERR_UNSUPPORTED;
    }
    return UCG_OK;
}

ucg_status_t ucg_planc_ucx_reduce_scatter_linear_prepare(ucg_vgroup_t *vgroup,
                                                         const ucg_coll_args_t *args,
                                                         ucg_plan_op_t **op)
{
    UCG_CHECK_NULL_INVALID(vgroup, args, op);

    ucg_status_t status;
    status = ucg_planc_ucx_reduce_scatter_linear_check(vgroup, args);
    if (status != UCG_OK) {
        return UCG_ERR_UNSUPPORTED;
    }

    ucg_planc_ucx_group_t *ucx_group = ucg_derived_of(vgroup, ucg_planc_ucx_group_t);
    ucg_planc_ucx_reduce_config_t *reduce_config;
    reduce_config = UCG_PLANC_UCX_CONTEXT_BUILTIN_CONFIG_BUNDLE(ucx_group->context, reduce,
                                                                UCG_COLL_TYPE_REDUCE);
    ucg_planc_ucx_scatterv_config_t *scatterv_config;
    scatterv_config = UCG_PLANC_UCX_CONTEXT_BUILTIN_CONFIG_BUNDLE(ucx_group->context, scatterv,
                                                                  UCG_COLL_TYPE_SCATTERV);
    ucg_plan_meta_op_t *meta_op;
    meta_op = ucg_planc_ucx_reduce_scatter_linear_op_new(ucx_group, vgroup, args, reduce_config, scatterv_config);
    if (meta_op == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    *op = &meta_op->super;
    return UCG_OK;
}