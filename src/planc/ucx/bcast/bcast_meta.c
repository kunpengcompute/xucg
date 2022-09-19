/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "bcast.h"
#include "planc_ucx_plan.h"
#include "planc_ucx_p2p.h"
#include "planc_ucx_meta.h"
#include "core/ucg_group.h"
#include "util/ucg_log.h"

enum {
    UCG_BCAST_ADJUST_ROOT_SEND = UCG_BIT(0),
    UCG_BCAST_ADJUST_ROOT_RECV = UCG_BIT(1),
};


static ucg_status_t ucg_planc_ucx_bcast_adjust_root_op_progress(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status;
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_planc_ucx_group_t *ucx_group = op->ucx_group;
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_planc_ucx_p2p_params_t params;
    ucg_planc_ucx_op_set_p2p_params(op, &params);
    ucg_coll_bcast_args_t *args = &op->super.super.args.bcast;
    void *buffer = args->buffer;

    if (ucg_test_and_clear_flags(&op->flags, UCG_BCAST_ADJUST_ROOT_SEND)) {
        status = ucg_planc_ucx_p2p_isend(buffer, args->count, args->dt, 0,
                                         op->tag, vgroup, &params);
    } else if (ucg_test_and_clear_flags(&op->flags, UCG_BCAST_ADJUST_ROOT_RECV)) {
        status = ucg_planc_ucx_p2p_irecv(buffer, args->count, args->dt, args->root,
                                         op->tag, vgroup, &params);
    }
    status = ucg_planc_ucx_p2p_testall(ucx_group, params.state);
    UCG_CHECK_GOTO(status, out);
out:
    op->super.super.status = status;
    return status;
}

static ucg_status_t ucg_planc_ucx_bcast_adjust_root_op_trigger(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status;
    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_planc_ucx_op_reset(op);
    ucg_rank_t myrank = op->super.vgroup->myrank;
    ucg_coll_bcast_args_t *args = &op->super.super.args.bcast;
    ucg_assert(args->root != UCG_TOPO_GROUP_LEADER);
    if (myrank == UCG_TOPO_GROUP_LEADER) {
        op->flags = UCG_BCAST_ADJUST_ROOT_RECV;
    } else if (myrank == args->root) {
        op->flags = UCG_BCAST_ADJUST_ROOT_SEND;
    } else {
        /* never happen */
        ucg_assert(0);
    }

    status = ucg_planc_ucx_bcast_adjust_root_op_progress(ucg_op);
    return status == UCG_INPROGRESS ? UCG_OK : status;
}

static ucg_status_t ucg_planc_ucx_bcast_adjust_root_op_discard(ucg_plan_op_t *ucg_op)
{
    UCG_CLASS_DESTRUCT(ucg_plan_op_t, ucg_op);
    ucg_mpool_put(ucg_op);
    return UCG_OK;
}

static ucg_planc_ucx_op_t* ucg_planc_ucx_bcast_adjust_root_op_new(ucg_planc_ucx_group_t *ucx_group,
                                                                  ucg_vgroup_t *vgroup,
                                                                  const ucg_coll_args_t *args)
{
    ucg_assert(args->bcast.root != UCG_TOPO_GROUP_LEADER);
    ucg_planc_ucx_op_t *ucx_op = ucg_mpool_get(&ucx_group->context->op_mp);
    if (ucx_op == NULL) {
        return NULL;
    }

    ucg_status_t status = UCG_CLASS_CONSTRUCT(ucg_plan_op_t, &ucx_op->super, vgroup,
                                              ucg_planc_ucx_bcast_adjust_root_op_trigger,
                                              ucg_planc_ucx_bcast_adjust_root_op_progress,
                                              ucg_planc_ucx_bcast_adjust_root_op_discard,
                                              args);
    if (status != UCG_OK) {
        ucg_error("Failed to initialize super of ucx op");
        goto err_free_op;
    }

    ucg_planc_ucx_op_init(ucx_op, ucx_group);
    return ucx_op;

err_free_op:
    ucg_mpool_put(ucx_op);
    return NULL;
}

ucg_status_t ucg_planc_ucx_bcast_add_adjust_root_op(ucg_plan_meta_op_t *meta_op,
                                                    ucg_planc_ucx_group_t *ucx_group,
                                                    ucg_vgroup_t *vgroup,
                                                    const ucg_coll_args_t *args,
                                                    const ucg_planc_ucx_bcast_config_t *config)
{
    const ucg_coll_bcast_args_t *coll_args = &args->bcast;
    if (coll_args->root == UCG_TOPO_GROUP_LEADER || config->root_adjust == 0) {
        return UCG_OK;
    }
    ucg_rank_t myrank = vgroup->myrank;
    if (myrank != UCG_TOPO_GROUP_LEADER && myrank != coll_args->root) {
        return ucg_planc_ucx_add_empty_op(meta_op, ucx_group, vgroup);
    }

    ucg_planc_ucx_op_t *ucx_op = NULL;
    ucx_op = ucg_planc_ucx_bcast_adjust_root_op_new(ucx_group, vgroup, args);
    if (ucx_op == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    return ucg_plan_meta_op_add(meta_op, &ucx_op->super);
}

ucg_status_t ucg_planc_ucx_bcast_add_topo_group_kntree_op(ucg_plan_meta_op_t *meta_op,
                                                          ucg_planc_ucx_group_t *ucx_group,
                                                          ucg_vgroup_t *vgroup,
                                                          const ucg_coll_args_t *args,
                                                          const ucg_planc_ucx_bcast_config_t *config,
                                                          ucg_topo_group_type_t type)
{
    ucg_topo_group_t *topo_group;
    topo_group = ucg_topo_get_group(vgroup->group->topo, type);
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

    ucg_coll_args_t *adjust_args = (ucg_coll_args_t*)args;
    ucg_rank_t old_root = args->bcast.root;
    if (old_root != UCG_TOPO_GROUP_LEADER && config->root_adjust) {
        adjust_args->bcast.root = UCG_TOPO_GROUP_LEADER;
    }
    ucg_planc_ucx_op_t *ucx_op;
    ucx_op = ucg_planc_ucx_bcast_kntree_op_new(ucx_group,
                                               &topo_group->super,
                                               adjust_args, config);
    adjust_args->bcast.root = old_root;
    if (ucx_op == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    return ucg_plan_meta_op_add(meta_op, &ucx_op->super);
}