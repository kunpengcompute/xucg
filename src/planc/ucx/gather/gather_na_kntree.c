/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */
#include <unistd.h>
#include "gather.h"
#include "planc_ucx_plan.h"
#include "planc_ucx_p2p.h"
#include "core/ucg_dt.h"
#include "core/ucg_group.h"
#include "util/ucg_log.h"
#include "util/ucg_malloc.h"
#include "planc/ucx/gather/gather_meta.h"

enum {
    UCG_NA_GATHER_SENDCOUNT_INTER_PHASE = UCG_BIT(0),
    UCG_NA_GATHER_SENDCOUNT_INTRA_PHASE = UCG_BIT(1),
    UCG_NA_GATHER_INTRA_PHASE = UCG_BIT(2),
    UCG_NA_GATHER_INTER_PHASE = UCG_BIT(3),
};

static inline void ucg_plan_ucx_free_ptr(void **ptr)
{
    if (*ptr) {
        ucg_free(*ptr);
    }
    *ptr = NULL;
    return;
}

static ucg_status_t ucg_planc_ucx_gather_na_kntree_inter_gather_recvcount_prepare(ucg_plan_op_t *ucg_op)
{
    ucg_planc_ucx_op_t *ucx_op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_vgroup_t *vgroup = ucx_op->super.vgroup;
    ucg_planc_ucx_group_t *ucx_group = ucx_op->ucx_group;
    ucg_coll_args_t *args = &ucx_op->super.super.args;
    ucg_planc_ucx_op_t *curr_ucx_op;
    ucg_coll_args_t inter_bcast_args;
    int32_t is_node_leader = ucx_op->gather.topo_aware.is_node_leader;
    uint32_t is_root = vgroup->myrank == args->gather.root;

    /* inter-gather */
    // set bcast arguments
    inter_bcast_args.type = UCG_COLL_TYPE_BCAST;
    if (is_root) {
        ucx_op->gather.topo_aware.recvcount_type[0] = args->gather.recvcount;
        ucx_op->gather.topo_aware.recvcount_type[1] = ucg_dt_size(args->gather.recvtype);
    }
    if (is_node_leader) {
        inter_bcast_args.bcast.root = args->gather.root / ucx_op->gather.topo_aware.ppn;
        inter_bcast_args.bcast.buffer = (void *)ucx_op->gather.topo_aware.recvcount_type;
        inter_bcast_args.bcast.count = 2;
        inter_bcast_args.bcast.dt = ucg_dt_get_predefined(UCG_DT_TYPE_INT32);

    }
    if (ucx_op->gather.topo_aware.inter_sendcount_op == NULL) {
    curr_ucx_op = ucg_planc_ucx_gather_na_kntree_build_topo_group_op(ucx_group,
                                                                     vgroup,
                                                                     &inter_bcast_args,
                                                                     UCG_GATHER_BCAST_KNTREE,
                                                                     UCG_TOPO_GROUP_TYPE_NODE_LEADER,
                                                                     1);
    /* To ensure that requests of multiple members in the same collection op
     **         can be matched, all subops must have the same request ID. */
    curr_ucx_op->super.super.id = ucx_op->super.super.id;
    ucx_op->gather.topo_aware.inter_sendcount_op = curr_ucx_op;
    }

    return UCG_OK;
}


static ucg_status_t ucg_planc_ucx_gather_na_kntree_intra_gather_prepare(ucg_plan_op_t *ucg_op)
{
    ucg_planc_ucx_op_t *ucx_op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_vgroup_t *vgroup = ucx_op->super.vgroup;
    ucg_planc_ucx_group_t *ucx_group = ucx_op->ucx_group;
    ucg_coll_args_t *args = &ucx_op->super.super.args;
    ucg_planc_ucx_op_t *curr_ucx_op;
    ucg_coll_args_t intra_gather_args;
    int32_t is_node_leader = ucx_op->gather.topo_aware.is_node_leader;
    int32_t ppn = ucx_op->gather.topo_aware.ppn;

    /* intra-gather */
    // set gather arguments
    intra_gather_args.type = UCG_COLL_TYPE_GATHER;
    intra_gather_args.gather.root = (vgroup->myrank / ppn == args->gather.root / ppn) ? (args->gather.root % ppn) : 0;// root is the node_leader
    intra_gather_args.gather.sendbuf = args->gather.sendbuf;
    intra_gather_args.gather.sendcount = args->gather.sendcount;
    intra_gather_args.gather.sendtype = args->gather.sendtype;
    if (is_node_leader) {
        int32_t recvcount = ucx_op->gather.topo_aware.recvcount_type[0] * ucx_op->gather.topo_aware.recvcount_type[1];

        intra_gather_args.gather.recvtype = ucg_dt_get_predefined(UCG_DT_TYPE_UINT8);
        intra_gather_args.gather.recvcount = recvcount; //decided dynamically
        intra_gather_args.gather.recvbuf = ucx_op->gather.topo_aware.intra_rbuf;

    }
    if (ucx_op->gather.topo_aware.intra_op == NULL) {
    curr_ucx_op = ucg_planc_ucx_gather_na_kntree_build_topo_group_op(ucx_group,
                                                                      vgroup,
                                                                      &intra_gather_args,
                                                                      UCG_GATHER_KNTREE,
                                                                      UCG_TOPO_GROUP_TYPE_NODE,
                                                                      0);
    /* To ensure that requests of multiple members in the same collection op
     **         can be matched, all subops must have the same request ID. */
    curr_ucx_op->super.super.id = ucx_op->super.super.id;
    ucx_op->gather.topo_aware.intra_op = curr_ucx_op;
    }


    return UCG_OK;
}

static ucg_status_t ucg_planc_ucx_gather_na_kntree_inter_gather_prepare(ucg_plan_op_t *ucg_op)
{
    ucg_planc_ucx_op_t *ucx_op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_vgroup_t *vgroup = ucx_op->super.vgroup;
    ucg_planc_ucx_group_t *ucx_group = ucx_op->ucx_group;
    ucg_coll_args_t *args = &ucx_op->super.super.args;
    ucg_planc_ucx_op_t *curr_ucx_op;
    ucg_coll_args_t inter_gather_args;
    int32_t is_node_leader = ucx_op->gather.topo_aware.is_node_leader;
    uint32_t is_root = vgroup->myrank == args->gather.root;
    int32_t ppn = ucx_op->gather.topo_aware.ppn;

    /* inter-gather */
    // set gather arguments
    inter_gather_args.type = UCG_COLL_TYPE_GATHER;
    if (is_node_leader) {
        inter_gather_args.gather.root = args->gather.root / ucx_op->gather.topo_aware.ppn;
        inter_gather_args.gather.sendbuf = ucx_op->gather.topo_aware.intra_rbuf; //decided dynamically
        inter_gather_args.gather.sendcount = ppn * ucx_op->gather.topo_aware.recvcount_type[0] * ucx_op->gather.topo_aware.recvcount_type[1];//decided dynamically
        inter_gather_args.gather.sendtype = ucg_dt_get_predefined(UCG_DT_TYPE_UINT8);
        if (is_root) {
            inter_gather_args.gather.recvcount = args->gather.recvcount * ppn;
            inter_gather_args.gather.recvbuf = args->gather.recvbuf;
            inter_gather_args.gather.recvtype = args->gather.recvtype;
        }
    }
    if (ucx_op->gather.topo_aware.inter_op == NULL) {
        curr_ucx_op = ucg_planc_ucx_gather_na_kntree_build_topo_group_op(ucx_group,
                                                            vgroup,
                                                            &inter_gather_args,
                                                                UCG_GATHER_KNTREE,
                                                            UCG_TOPO_GROUP_TYPE_NODE_LEADER,
                                                            1);
        /* To ensure that requests of multiple members in the same collection op
        **         can be matched, all subops must have the same request ID. */
        curr_ucx_op->super.super.id = ucx_op->super.super.id;
        ucx_op->gather.topo_aware.inter_op = curr_ucx_op;
    }

    return UCG_OK;
}

ucg_status_t ucg_planc_ucx_gather_na_kntree_op_progress(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_op_t *ucx_op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_plan_op_t *curr_ucg_op = NULL;

    /* inter-node gather recvcount and recvtype size */
    if (ucg_test_flags(ucx_op->flags, UCG_NA_GATHER_SENDCOUNT_INTER_PHASE)) {
        if (ucx_op->gather.topo_aware.inter_sendcount_op_trigged == 0) {

            curr_ucg_op = &ucx_op->gather.topo_aware.inter_sendcount_op->super;
            status = curr_ucg_op->trigger(curr_ucg_op);
            ucx_op->gather.topo_aware.inter_sendcount_op_trigged = 1;
        }
        curr_ucg_op = &ucx_op->gather.topo_aware.inter_sendcount_op->super;
        status = curr_ucg_op->progress(curr_ucg_op);
        UCG_CHECK_GOTO(status, out);
        ucx_op->flags = UCG_NA_GATHER_INTRA_PHASE;
    }

    /* intra-node gather */
    if (ucg_test_flags(ucx_op->flags, UCG_NA_GATHER_INTRA_PHASE)) {
        if (ucx_op->gather.topo_aware.is_intra_op_trigged == 0) {
            status = ucg_planc_ucx_gather_na_kntree_intra_gather_prepare(ucg_op);
            if (status != UCG_OK) {
                return status;
            }
            curr_ucg_op = &ucx_op->gather.topo_aware.intra_op->super;
            status = curr_ucg_op->trigger(curr_ucg_op);
            ucx_op->gather.topo_aware.is_intra_op_trigged = 1;
        }
        curr_ucg_op = &ucx_op->gather.topo_aware.intra_op->super;
        status = curr_ucg_op->progress(curr_ucg_op);
        UCG_CHECK_GOTO(status, out);
        ucx_op->flags = UCG_NA_GATHER_INTER_PHASE;
    }


    /* inter-node gather */
    if (ucg_test_flags(ucx_op->flags, UCG_NA_GATHER_INTER_PHASE)) {
        if (ucx_op->gather.topo_aware.is_inter_op_trigged == 0) {
            status = ucg_planc_ucx_gather_na_kntree_inter_gather_prepare(ucg_op);
            if (status != UCG_OK) {
                return status;
            }
            curr_ucg_op = &ucx_op->gather.topo_aware.inter_op->super;
            status = curr_ucg_op->trigger(curr_ucg_op);
            ucx_op->gather.topo_aware.is_inter_op_trigged = 1;
        }
        curr_ucg_op = &ucx_op->gather.topo_aware.inter_op->super;
        status = curr_ucg_op->progress(curr_ucg_op);
        UCG_CHECK_GOTO(status, out);
        ucg_clear_flags(&ucx_op->flags, UCG_NA_GATHER_INTER_PHASE);
    }
out:
    ucx_op->super.super.status = status;
    return status;
}

static ucg_status_t ucg_planc_ucx_gather_na_kntree_op_discard(ucg_plan_op_t *ucg_op)
{
    ucg_planc_ucx_op_t *ucx_op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    int32_t is_node_leader = ucx_op->gather.topo_aware.is_node_leader;

    ucg_plan_op_t *op;
    op = &ucx_op->gather.topo_aware.inter_sendcount_op->super;
    op->discard(op);
    op = &ucx_op->gather.topo_aware.intra_op->super;
    op->discard(op);
    op = &ucx_op->gather.topo_aware.inter_op->super;
    op->discard(op);
    ucx_op->gather.topo_aware.inter_sendcount_op = NULL; 
    ucx_op->gather.topo_aware.inter_op = NULL; 
    ucx_op->gather.topo_aware.intra_op = NULL; 

    if (is_node_leader) {
        ucg_plan_ucx_free_ptr((void **)&(ucx_op->gather.topo_aware.intra_rbuf));
    }

    UCG_CLASS_DESTRUCT(ucg_plan_op_t, ucg_op);
    ucg_mpool_put(ucg_op);
    return UCG_OK;
}

static ucg_status_t ucg_planc_ucx_gather_na_kntree_op_trigger(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status;

    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    op->gather.topo_aware.is_inter_op_trigged = 0;
    op->gather.topo_aware.is_intra_op_trigged = 0;
    op->gather.topo_aware.inter_sendcount_op_trigged = 0;
    ucg_planc_ucx_op_reset(op);
    op->flags = UCG_NA_GATHER_SENDCOUNT_INTER_PHASE;
    status = ucg_planc_ucx_gather_na_kntree_op_progress(ucg_op);
    return status == UCG_INPROGRESS ? UCG_OK : status;
}


static ucg_status_t ucg_planc_ucx_gather_na_kntree_op_init(ucg_planc_ucx_op_t *ucx_op, ucg_planc_ucx_gather_config_t *config)
{
    ucg_vgroup_t *vgroup = ucx_op->super.vgroup;
    ucg_coll_args_t *args = &ucx_op->super.super.args;

    ucx_op->gather.topo_aware.is_inter_op_trigged = 0;
    ucx_op->gather.topo_aware.is_intra_op_trigged = 0;
    ucx_op->gather.topo_aware.inter_sendcount_op_trigged = 0;
    ucx_op->gather.topo_aware.inter_op = NULL;
    ucx_op->gather.topo_aware.intra_op = NULL;
    ucx_op->gather.topo_aware.inter_sendcount_op = NULL;

    
    /* decide node leader */
    ucg_topo_group_t *node_leader_group, *node_member_group;
    vgroup->group->topo->myroot = args->gather.root;
    node_leader_group = ucg_topo_get_group(vgroup->group->topo, UCG_TOPO_GROUP_TYPE_NODE_LEADER);
    uint32_t is_node_leader = node_leader_group->state == UCG_TOPO_GROUP_STATE_ENABLE;
    ucx_op->gather.topo_aware.is_node_leader = is_node_leader;
    uint32_t node_cnt = node_leader_group->super.size;
    ucx_op->gather.topo_aware.node_cnt = node_cnt;
    node_member_group = ucg_topo_get_group(vgroup->group->topo, UCG_TOPO_GROUP_TYPE_NODE);
    uint32_t ppn = node_member_group->super.size;
    ucx_op->gather.topo_aware.ppn = ppn;
    if (is_node_leader) {
        if (vgroup->myrank == args->gather.root) {
            ucx_op->gather.topo_aware.intra_rbuf = (int32_t *)ucg_malloc(args->gather.recvcount * ppn * ucg_dt_extent(args->gather.recvtype), "intra gather recvcounts recvbuf");
        } else {
            ucx_op->gather.topo_aware.intra_rbuf = (int32_t *)ucg_malloc(args->gather.sendcount * ppn * ucg_dt_extent(args->gather.sendtype), "intra gather recvcounts recvbuf");
        }

        if (ucx_op->gather.topo_aware.intra_rbuf == NULL) {
            return UCG_ERR_NO_MEMORY;
        }    
    }
    ucg_plan_op_t *ucg_op = &ucx_op->super;
    ucg_planc_ucx_gather_na_kntree_inter_gather_recvcount_prepare(ucg_op); //原来在progress,会出现重复调用，放这里防止重复调用
    return UCG_OK;
}

ucg_planc_ucx_op_t *ucg_planc_ucx_gather_na_kntree_op_new(ucg_planc_ucx_group_t *ucx_group,
                                                           ucg_vgroup_t *vgroup,
                                                           const ucg_coll_args_t *args,
                                                           ucg_planc_ucx_gather_config_t *config)
{
    UCG_CHECK_NULL(NULL, ucx_group, vgroup, args);

    ucg_planc_ucx_op_t *ucx_op = ucg_mpool_get(&ucx_group->context->op_mp);
    if (ucx_op == NULL) {
        goto err;
    }

    ucg_status_t status = UCG_CLASS_CONSTRUCT(ucg_plan_op_t, &ucx_op->super, vgroup,
                                              ucg_planc_ucx_gather_na_kntree_op_trigger,
                                              ucg_planc_ucx_gather_na_kntree_op_progress,
                                              ucg_planc_ucx_gather_na_kntree_op_discard,
                                              args);

    if (status != UCG_OK) {
        ucg_error("Failed to initialize super of ucx op");
        goto err_free_op;
    }

    ucg_planc_ucx_op_init(ucx_op, ucx_group);
    status = ucg_planc_ucx_gather_na_kntree_op_init(ucx_op, config);
    if (status != UCG_OK) {
        ucg_error("Failed to initialize na_kntree");
        goto err_free_op;
    }
    return ucx_op;

err_free_op:
    ucg_mpool_put(ucx_op);
err:
    return NULL;
}

static ucg_status_t ucg_planc_ucx_gather_na_kntree_check(ucg_vgroup_t *vgroup,
                                                          const ucg_coll_args_t *args)
{
    int32_t ppn = vgroup->group->topo->ppn;
    int32_t group_size = vgroup->size;

    if (ppn == group_size) {
        ucg_info("Node-aware kntree gather does not support single node");
        return UCG_ERR_UNSUPPORTED;
    }
    if (ppn == UCG_TOPO_PPX_UNKNOWN) {
        ucg_info("Node-aware kntree gather don't support unknown ppn");
        return UCG_ERR_UNSUPPORTED;
    }
    if (ppn == UCG_TOPO_PPX_UNBALANCED) {
        ucg_info("Node-aware kntree gather don't support unbalanced ppn");
        return UCG_ERR_UNSUPPORTED;
    }
    if (ppn == 1) {
        ucg_info("Node-aware kntree gather don't support ppn==1");
        return UCG_ERR_UNSUPPORTED;
    }
    for (int i = 0;i < vgroup->size;i++) {
        ucg_location_t location;
        vgroup->group->topo->get_location(vgroup->group->topo->group, i, &location);
        if (location.node_id != i / ppn) {
            ucg_info("Node-aware kntree gather does not support node is not order");
            return UCG_ERR_UNSUPPORTED;
        }
    }
    return UCG_OK;
}

ucg_status_t ucg_planc_ucx_gather_na_kntree_prepare(ucg_vgroup_t *vgroup,
                                                     const ucg_coll_args_t *args,
                                                     ucg_plan_op_t **op)
{
    UCG_CHECK_NULL_INVALID(vgroup, args, op);

    ucg_status_t status = ucg_planc_ucx_gather_na_kntree_check(vgroup, args);
    if (status != UCG_OK) {
        return status;
    }

    ucg_planc_ucx_group_t *ucx_group = ucg_derived_of(vgroup, ucg_planc_ucx_group_t);
    ucg_planc_ucx_gather_config_t *config;
    config = UCG_PLANC_UCX_CONTEXT_BUILTIN_CONFIG_BUNDLE(ucx_group->context, gather,
                                                         UCG_COLL_TYPE_GATHER);

    ucg_planc_ucx_op_t *ucx_op = ucg_planc_ucx_gather_na_kntree_op_new(ucx_group, vgroup, args, config);
    if (ucx_op == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    *op = &ucx_op->super;
    return UCG_OK;
}