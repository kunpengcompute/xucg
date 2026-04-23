/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

#include "reduce_scatter_block.h"
#include "planc_ucx_plan.h"
#include "planc_ucx_meta.h"

typedef enum {
    UCG_REDUCE_SCATTER_BLOCK_OP_REDUCE,
    UCG_REDUCE_SCATTER_BLOCK_OP_SCATTER,
} ucg_reduce_scatter_block_op_type_t;

static void ucg_planc_ucx_reduce_scatter_block_init_reduce_args(const ucg_coll_args_t *args,
                                                                ucg_coll_args_t *reduce_args,
                                                                void *tmpbuf, uint32_t size)
{
    reduce_args->type = UCG_COLL_TYPE_REDUCE;
    reduce_args->info = args->info;
    reduce_args->reduce.sendbuf = args->reduce_scatter_block.sendbuf;
    if (args->reduce_scatter_block.sendbuf == UCG_IN_PLACE) {
        reduce_args->reduce.recvbuf = args->reduce_scatter_block.recvbuf;
    } else {
        reduce_args->reduce.recvbuf = tmpbuf;
    }
    reduce_args->reduce.count = args->reduce_scatter_block.recvcount * size;
    reduce_args->reduce.dt = args->reduce_scatter_block.dt;
    reduce_args->reduce.op = args->reduce_scatter_block.op;
    reduce_args->reduce.root = UCG_TOPO_GROUP_LEADER;
    return;
}

static void ucg_planc_ucx_reduce_scatter_block_init_scatter_args(const ucg_coll_args_t *args,
                                                                 ucg_coll_args_t *scatter_args,
                                                                 void *tmpbuf)
{
    scatter_args->type = UCG_COLL_TYPE_BCAST;
    scatter_args->info = args->info;
    if (args->reduce_scatter_block.sendbuf == UCG_IN_PLACE) {
        scatter_args->reduce.sendbuf = args->reduce_scatter_block.recvbuf;
    } else {
        scatter_args->reduce.sendbuf = tmpbuf;
    }
    scatter_args->scatter.recvbuf = args->reduce_scatter_block.recvbuf;
    scatter_args->scatter.sendcount = args->reduce_scatter_block.recvcount;
    scatter_args->scatter.recvcount = args->reduce_scatter_block.recvcount;
    scatter_args->scatter.sendtype = args->reduce_scatter_block.dt;
    scatter_args->scatter.recvtype = args->reduce_scatter_block.dt;
    scatter_args->scatter.root = UCG_TOPO_GROUP_LEADER;
    return;
}

static
ucg_status_t ucg_planc_ucx_reduce_scatter_block_add_intra_subnet_op(ucg_plan_meta_op_t *meta_op,
                                                                    ucg_topo_t *topo,
                                                                    ucg_planc_ucx_group_t *ucx_group,
                                                                    ucg_vgroup_t *vgroup,
                                                                    const ucg_coll_args_t *args,
                                                                    ucg_planc_ucx_reduce_config_t *reduce_config,
                                                                    ucg_planc_ucx_scatter_config_t *scatter_config,
                                                                    ucg_reduce_scatter_block_op_type_t type,
                                                                    void *tmpbuf)
{
    ucg_planc_ucx_op_t *ucx_op;
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

    if (type == UCG_REDUCE_SCATTER_BLOCK_OP_REDUCE) {
        ucg_coll_args_t reduce_args;
        ucg_planc_ucx_reduce_scatter_block_init_reduce_args(args, &reduce_args, tmpbuf, vgroup->size);
        ucx_op = ucg_planc_ucx_reduce_kntree_op_new(ucx_group, &topo_group->super,
                                                    &reduce_args, reduce_config);
    } else {
        ucg_coll_args_t scatter_args;
        ucg_planc_ucx_reduce_scatter_block_init_scatter_args(args, &scatter_args, tmpbuf);
        ucx_op = ucg_planc_ucx_scatter_kntree_op_new(ucx_group, &topo_group->super,
                                                     &scatter_args, scatter_config);
    }

    if (ucx_op == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    return ucg_plan_meta_op_add(meta_op, &ucx_op->super);
}

ucg_plan_meta_op_t *ucg_planc_ucx_reduce_scatter_block_linear_op_new(ucg_planc_ucx_group_t *ucx_group,
                                                                     ucg_vgroup_t *vgroup,
                                                                     const ucg_coll_args_t *args,
                                                                     ucg_planc_ucx_reduce_config_t *reduce_config,
                                                                     ucg_planc_ucx_scatter_config_t *scatter_config)
{
    UCG_CHECK_NULL(NULL, ucx_group, vgroup, args, reduce_config);
    UCG_CHECK_NULL(NULL, ucx_group, vgroup, args, scatter_config);

    int64_t total_count = args->reduce_scatter_block.recvcount * vgroup->size;
    void *tmpbuf = ucg_malloc(total_count * args->reduce_scatter_block.dt->extent, "reduce_scatter_block tmpbuf");
    if (tmpbuf == NULL) {
        goto err;
    }

    ucg_plan_meta_op_t *meta_op = ucg_plan_meta_op_new(vgroup->group, vgroup, args);
    if (meta_op == NULL) {
        goto err;
    }

    ucg_status_t status;
    ucg_topo_t *topo = vgroup->group->topo;
    ucg_coll_args_t *meta_args = &meta_op->super.super.args;

    /* 1. reduce. */
    status = ucg_planc_ucx_reduce_scatter_block_add_intra_subnet_op(meta_op, topo, ucx_group, vgroup,
                                                                    meta_args, reduce_config, scatter_config,
                                                                    UCG_REDUCE_SCATTER_BLOCK_OP_REDUCE, tmpbuf);
    UCG_CHECK_GOTO(status, err_free_meta_op);

    /* 2. scatter. */
    status = ucg_planc_ucx_reduce_scatter_block_add_intra_subnet_op(meta_op, topo, ucx_group, vgroup,
                                                                    meta_args, reduce_config, scatter_config,
                                                                    UCG_REDUCE_SCATTER_BLOCK_OP_SCATTER, tmpbuf);
    UCG_CHECK_GOTO(status, err_free_meta_op);

    return meta_op;

err_free_meta_op:
    meta_op->super.discard(&meta_op->super);
err:
    return NULL;
}

static ucg_status_t ucg_planc_ucx_reduce_scatter_block_linear_check(ucg_vgroup_t *vgroup,
                                                                    const ucg_coll_args_t *args)
{
    if (args->reduce_scatter_block.op->type != UCG_OP_TYPE_SUM) {
        ucg_info("Reduce_scatter linear op type is not MPI_SUM, so roll back to OpenMPI");
        return UCG_ERR_UNSUPPORTED;
    }
    return UCG_OK;
}

ucg_status_t ucg_planc_ucx_reduce_scatter_block_linear_prepare(ucg_vgroup_t *vgroup,
                                                               const ucg_coll_args_t *args,
                                                               ucg_plan_op_t **op)
{
    UCG_CHECK_NULL_INVALID(vgroup, args, op);

    ucg_status_t status;
    status = ucg_planc_ucx_reduce_scatter_block_linear_check(vgroup, args);
    if (status != UCG_OK) {
        return UCG_ERR_UNSUPPORTED;
    }

    ucg_planc_ucx_group_t *ucx_group = ucg_derived_of(vgroup, ucg_planc_ucx_group_t);
    ucg_planc_ucx_reduce_config_t *reduce_config;
    reduce_config = UCG_PLANC_UCX_CONTEXT_BUILTIN_CONFIG_BUNDLE(ucx_group->context, reduce,
                                                                UCG_COLL_TYPE_REDUCE);
    ucg_planc_ucx_scatter_config_t *scatter_config;
    scatter_config = UCG_PLANC_UCX_CONTEXT_BUILTIN_CONFIG_BUNDLE(ucx_group->context, scatter,
                                                                 UCG_COLL_TYPE_SCATTER);
    ucg_plan_meta_op_t *meta_op;
    meta_op = ucg_planc_ucx_reduce_scatter_block_linear_op_new(ucx_group, vgroup, args, reduce_config, scatter_config);
    if (meta_op == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    *op = &meta_op->super;
    return UCG_OK;
}