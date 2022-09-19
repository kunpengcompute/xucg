/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "allreduce.h"
#include "planc_ucx_plan.h"
#include "planc_ucx_meta.h"
#include "reduce/reduce.h"
#include "bcast/bcast.h"
#include "core/ucg_topo.h"
#include "core/ucg_group.h"
#include "core/ucg_plan.h"
#include "util/ucg_log.h"

typedef enum {
    UCG_ALLREDUCE_OP_REDUCE,
    UCG_ALLREDUCE_OP_BCAST,
} ucg_allreduce_op_type_t;

static ucg_status_t ucg_planc_ucx_allreduce_nta_kntree_check(ucg_vgroup_t *vgroup,
                                                             const ucg_coll_args_t *args)
{
    ucg_op_flag_t flags = args->allreduce.op->flags;
    if (!(flags & UCG_OP_FLAG_IS_COMMUTATIVE)) {
        ucg_info("Allreduce nta_kntree don't support non-commutative op");
        return UCG_ERR_UNSUPPORTED;
    }
    return UCG_OK;
}

static void ucg_planc_ucx_allreduce_init_reduce_args(const ucg_coll_args_t *args,
                                                     ucg_coll_args_t *reduce_args)
{
    reduce_args->type = UCG_COLL_TYPE_REDUCE;
    reduce_args->info = args->info;
    reduce_args->reduce.sendbuf = args->allreduce.sendbuf;
    reduce_args->reduce.recvbuf = args->allreduce.recvbuf;
    reduce_args->reduce.count = args->allreduce.count;
    reduce_args->reduce.dt = args->allreduce.dt;
    reduce_args->reduce.op = args->allreduce.op;
    reduce_args->reduce.root = UCG_TOPO_GROUP_LEADER;
    return;
}

static void ucg_planc_ucx_allreduce_init_bcast_args(const ucg_coll_args_t *args,
                                                    ucg_coll_args_t *bcast_args)
{
    bcast_args->type = UCG_COLL_TYPE_BCAST;
    bcast_args->info = args->info;
    bcast_args->bcast.buffer = args->allreduce.recvbuf;
    bcast_args->bcast.count = args->allreduce.count;
    bcast_args->bcast.dt = args->allreduce.dt;
    bcast_args->bcast.root = UCG_TOPO_GROUP_LEADER;
    return;
}

/**
 * @brief Reduce/Bcast between processes on each subnet.
 */
static
ucg_status_t ucg_planc_ucx_allreduce_add_intra_subnet_op(ucg_plan_meta_op_t *meta_op,
                                                         ucg_topo_t *topo,
                                                         ucg_planc_ucx_group_t *ucx_group,
                                                         ucg_vgroup_t *vgroup,
                                                         const ucg_coll_args_t *args,
                                                         ucg_planc_ucx_allreduce_config_t *config,
                                                         ucg_allreduce_op_type_t type)
{
    ucg_planc_ucx_op_t *ucx_op;
    ucg_topo_group_t *topo_group;
    topo_group = ucg_topo_get_group(topo, UCG_TOPO_GROUP_TYPE_SUBNET);
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

    if (type == UCG_ALLREDUCE_OP_REDUCE) {
        ucg_coll_args_t reduce_args;
        ucg_planc_ucx_reduce_config_t reduce_config;
        reduce_config.kntree_degree = config->nta_kntree_intra_degree;
        ucg_planc_ucx_allreduce_init_reduce_args(args, &reduce_args);
        ucx_op = ucg_planc_ucx_reduce_kntree_op_new(ucx_group, &topo_group->super,
                                                    &reduce_args, &reduce_config);
    } else {
        ucg_coll_args_t bcast_args;
        ucg_planc_ucx_bcast_config_t bcast_config;
        bcast_config.kntree_degree = config->nta_kntree_intra_degree;
        bcast_config.root_adjust = 0;
        ucg_planc_ucx_allreduce_init_bcast_args(args, &bcast_args);
        ucx_op = ucg_planc_ucx_bcast_kntree_op_new(ucx_group, &topo_group->super,
                                                   &bcast_args, &bcast_config);
    }

    if (ucx_op == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    return ucg_plan_meta_op_add(meta_op, &ucx_op->super);
}

/**
 * @brief Allreduce between the leader processes on each subnet.
 */
static
ucg_status_t ucg_planc_ucx_allreduce_add_inter_subnet_op(ucg_plan_meta_op_t *meta_op,
                                                         ucg_topo_t *topo,
                                                         ucg_planc_ucx_group_t *ucx_group,
                                                         ucg_vgroup_t *vgroup,
                                                         const ucg_coll_args_t *args,
                                                         ucg_planc_ucx_allreduce_config_t *config)
{
    ucg_topo_group_t *topo_group;
    topo_group = ucg_topo_get_group(topo, UCG_TOPO_GROUP_TYPE_SUBNET_LEADER);
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

    ucg_planc_ucx_op_t *ucx_op;
    ucg_coll_args_t allreduce_args = *args;
    allreduce_args.allreduce.sendbuf = UCG_IN_PLACE;
    ucx_op = ucg_planc_ucx_allreduce_rd_op_new(ucx_group, &topo_group->super,
                                               &allreduce_args);
    if (ucx_op == NULL) {
        return UCG_ERR_NO_MEMORY;
    }

    return ucg_plan_meta_op_add(meta_op, &ucx_op->super);
}

ucg_plan_meta_op_t* ucg_planc_ucx_allreduce_nta_kntree_op_new(ucg_planc_ucx_group_t* ucx_group,
                                                              ucg_vgroup_t *vgroup,
                                                              const ucg_coll_args_t* args,
                                                              ucg_planc_ucx_allreduce_config_t *config)
{
    UCG_CHECK_NULL(NULL, ucx_group, vgroup, args, config);

    ucg_plan_meta_op_t* meta_op = ucg_plan_meta_op_new(vgroup->group, vgroup, args);
    if (meta_op == NULL) {
        goto err;
    }

    ucg_status_t status;
    ucg_topo_t *topo = vgroup->group->topo;
    ucg_coll_args_t *meta_args = &meta_op->super.super.args;

    /* 1. reduce in subnet group. */
    status = ucg_planc_ucx_allreduce_add_intra_subnet_op(meta_op, topo, ucx_group,
                                                         vgroup, meta_args, config,
                                                         UCG_ALLREDUCE_OP_REDUCE);
    UCG_CHECK_GOTO(status, err_free_meta_op);

    /* 2. allreduce in subnet leader group. */
    status = ucg_planc_ucx_allreduce_add_inter_subnet_op(meta_op, topo, ucx_group,
                                                         vgroup, meta_args, config);
    UCG_CHECK_GOTO(status, err_free_meta_op);

    /* 3. broadcast in subnet group. */
    status = ucg_planc_ucx_allreduce_add_intra_subnet_op(meta_op, topo, ucx_group,
                                                         vgroup, meta_args, config,
                                                         UCG_ALLREDUCE_OP_BCAST);
    UCG_CHECK_GOTO(status, err_free_meta_op);

    return meta_op;

err_free_meta_op:
    meta_op->super.discard(&meta_op->super);
err:
    return NULL;
}

ucg_status_t ucg_planc_ucx_allreduce_nta_kntree_prepare(ucg_vgroup_t *vgroup,
                                                        const ucg_coll_args_t *args,
                                                        ucg_plan_op_t **op)
{
    UCG_CHECK_NULL_INVALID(vgroup, args, op);

    ucg_status_t status;
    status = ucg_planc_ucx_allreduce_nta_kntree_check(vgroup, args);
    if (status != UCG_OK) {
        return UCG_ERR_UNSUPPORTED;
    }

    ucg_planc_ucx_group_t *ucx_group = ucg_derived_of(vgroup, ucg_planc_ucx_group_t);
    ucg_planc_ucx_allreduce_config_t *config;
    config = UCG_PLANC_UCX_CONTEXT_BUILTIN_CONFIG_BUNDLE(ucx_group->context, allreduce,
                                                         UCG_COLL_TYPE_ALLREDUCE);
    ucg_plan_meta_op_t *meta_op;
    meta_op = ucg_planc_ucx_allreduce_nta_kntree_op_new(ucx_group, vgroup, args, config);
    if (meta_op == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    *op = &meta_op->super;
    return UCG_OK;
}