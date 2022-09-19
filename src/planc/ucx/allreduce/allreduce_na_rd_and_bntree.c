/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "allreduce.h"
#include "planc_ucx_plan.h"
#include "planc_ucx_p2p.h"
#include "core/ucg_group.h"
#include "allreduce_meta.h"
#include "util/ucg_log.h"

static ucg_status_t ucg_planc_ucx_allreduce_na_rd_and_bntree_check(ucg_vgroup_t *vgroup,
                                                                   const ucg_coll_args_t *args)
{
    ucg_op_flag_t flags = args->allreduce.op->flags;
    if (!(flags & UCG_OP_FLAG_IS_COMMUTATIVE)) {
        ucg_info("Allreduce na_rd_and_bntree don't support non-commutative op");
        return UCG_ERR_UNSUPPORTED;
    }
    return UCG_OK;
}

ucg_plan_meta_op_t* ucg_planc_ucx_allreduce_na_rd_and_bntree_op_new(ucg_planc_ucx_group_t* ucx_group,
                                                                    ucg_vgroup_t *vgroup,
                                                                    const ucg_coll_args_t* args,
                                                                    const ucg_planc_ucx_allreduce_config_t *config)
{
    UCG_CHECK_NULL(NULL, ucx_group, vgroup, args, config);

    ucg_plan_meta_op_t* meta_op = ucg_plan_meta_op_new(vgroup->group, vgroup, args);
    if (meta_op == NULL) {
        goto err;
    }

    ucg_status_t status;
    ucg_coll_args_t *meta_args = &meta_op->super.super.args;
    int32_t send_in_place = 0;

    status = ucg_planc_ucx_allreduce_add_reduce_kntree_op(meta_op, ucx_group,
                                                          vgroup, meta_args,
                                                          config, UCG_TOPO_GROUP_TYPE_NODE,
                                                          send_in_place);
    UCG_CHECK_GOTO(status, err_free_meta_op);
    ucg_planc_ucx_allreduce_set_send_in_place_flag(vgroup, UCG_TOPO_GROUP_TYPE_NODE, &send_in_place);

    status = ucg_planc_ucx_allreduce_add_allreduce_rd_op(meta_op, ucx_group,
                                                         vgroup, meta_args,
                                                         UCG_TOPO_GROUP_TYPE_NODE_LEADER,
                                                         send_in_place);
    UCG_CHECK_GOTO(status, err_free_meta_op);

    status = ucg_planc_ucx_allreduce_add_bcast_kntree_op(meta_op, ucx_group,
                                                         vgroup, meta_args,
                                                         config, UCG_TOPO_GROUP_TYPE_NODE);
    UCG_CHECK_GOTO(status, err_free_meta_op);

    return meta_op;

err_free_meta_op:
    meta_op->super.discard(&meta_op->super);
err:
    return NULL;
}

ucg_status_t ucg_planc_ucx_allreduce_na_rd_and_bntree_prepare(ucg_vgroup_t *vgroup,
                                                             const ucg_coll_args_t *args,
                                                             ucg_plan_op_t **op)
{
    UCG_CHECK_NULL_INVALID(vgroup, args, op);

    ucg_status_t status;
    status = ucg_planc_ucx_allreduce_na_rd_and_bntree_check(vgroup, args);
    if (status != UCG_OK) {
        return UCG_ERR_UNSUPPORTED;
    }

    ucg_planc_ucx_group_t *ucx_group = ucg_derived_of(vgroup, ucg_planc_ucx_group_t);
    ucg_planc_ucx_allreduce_config_t config;
    config.fanin_intra_degree = 2;
    config.fanout_intra_degree = 2;

    ucg_plan_meta_op_t *meta_op;
    meta_op = ucg_planc_ucx_allreduce_na_rd_and_bntree_op_new(ucx_group, vgroup, args, &config);
    if (meta_op == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    *op = &meta_op->super;
    return UCG_OK;
}