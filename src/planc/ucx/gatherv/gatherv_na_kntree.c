/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */
#include <unistd.h>
#include "gatherv.h"
#include "planc_ucx_plan.h"
#include "planc_ucx_p2p.h"
#include "core/ucg_dt.h"
#include "core/ucg_group.h"
#include "util/ucg_log.h"
#include "util/ucg_malloc.h"
#include "planc/ucx/gatherv/gatherv_meta.h"

enum {
    UCG_NA_GATHERV_SENDCOUNT_INTER_PHASE = UCG_BIT(0),
    UCG_NA_GATHERV_SENDCOUNT_INTRA_PHASE = UCG_BIT(1),
    UCG_NA_GATHERV_INTRA_PHASE = UCG_BIT(2),
    UCG_NA_GATHERV_INTER_PHASE = UCG_BIT(3),
};

static inline void ucg_plan_ucx_free_ptr(void **ptr)
{
    if (*ptr) {
        ucg_free(*ptr);
    }
    *ptr = NULL;
    return;
}

static inline int32_t *ucg_planc_ucx_create_displs(const int32_t *counts, int32_t n)
{
    int32_t *displs = (int32_t *)ucg_malloc(sizeof(int32_t) * n, "displs");
    if (displs == NULL) {
        return NULL;
    }

    displs[0] = 0;
    for (int32_t i = 1; i < n; i++) {
        displs[i] = displs[i - 1] + counts[i - 1];
    }

    return displs;
}

static ucg_status_t ucg_planc_ucx_gatherv_na_kntree_inter_gatherv_recvcounts_prepare(ucg_plan_op_t *ucg_op)
{
    ucg_planc_ucx_op_t *ucx_op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_vgroup_t *vgroup = ucx_op->super.vgroup;
    ucg_planc_ucx_group_t *ucx_group = ucx_op->ucx_group;
    ucg_coll_args_t *args = &ucx_op->super.super.args;
    ucg_planc_ucx_op_t *curr_ucx_op;
    ucg_coll_args_t *inter_bcast_args = &ucx_op->gatherv.topo_aware.inter_gatherv_sc_args;
    int32_t is_node_leader = ucx_op->gatherv.topo_aware.is_node_leader;
    uint32_t is_root = vgroup->myrank == args->gatherv.root;

    /* inter-gatherv */
    // set scatterv arguments
    inter_bcast_args->type = UCG_COLL_TYPE_BCAST;
    if (is_node_leader && !is_root) {
        int32_t *recvcounts = (int32_t *)ucg_malloc(sizeof(int32_t) * (int32_t)vgroup->size, "node leader gatherv recvcounts");
        inter_bcast_args->bcast.root = 0;
        inter_bcast_args->bcast.buffer = recvcounts;
        inter_bcast_args->bcast.count = vgroup->size; 
        inter_bcast_args->bcast.dt = ucg_dt_get_predefined(UCG_DT_TYPE_INT32);

    }
    if (is_root) {
        inter_bcast_args->bcast.root = 0;

        int32_t *recvcounts = (int32_t *)args->gatherv.recvcounts; // 这里使用用户传入的参数，不能自己去释放内存
        inter_bcast_args->bcast.buffer = recvcounts;
        inter_bcast_args->bcast.count = vgroup->size; 
        inter_bcast_args->bcast.dt = ucg_dt_get_predefined(UCG_DT_TYPE_INT32);
    }
    if (ucx_op->gatherv.topo_aware.inter_sendcount_op == NULL) {
    curr_ucx_op = ucg_planc_ucx_bcast_build_topo_group_op(ucx_group,
                                                             vgroup,
                                                             inter_bcast_args,
                                                             UCG_GATHERV_BCAST_KNTREE,
                                                             UCG_TOPO_GROUP_TYPE_NODE_LEADER,
                                                             1);
    /* To ensure that requests of multiple members in the same collection op
     **         can be matched, all subops must have the same request ID. */
    curr_ucx_op->super.super.id = ucx_op->super.super.id;
    ucx_op->gatherv.topo_aware.inter_sendcount_op = curr_ucx_op;
    }

    return UCG_OK;
}

static ucg_status_t ucg_planc_ucx_gatherv_na_kntree_inter_gatherv_recvtype_prepare(ucg_plan_op_t *ucg_op)
{
    ucg_planc_ucx_op_t *ucx_op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_vgroup_t *vgroup = ucx_op->super.vgroup;
    ucg_planc_ucx_group_t *ucx_group = ucx_op->ucx_group;
    ucg_coll_args_t *args = &ucx_op->super.super.args;
    ucg_planc_ucx_op_t *curr_ucx_op;
    ucg_coll_args_t *inter_bcast_args = &ucx_op->gatherv.topo_aware.intra_gatherv_sc_args;
    int32_t is_node_leader = ucx_op->gatherv.topo_aware.is_node_leader;
    uint32_t is_root = vgroup->myrank == args->gatherv.root;

    /* inter-gatherv */
    // set scatterv arguments

    inter_bcast_args->type = UCG_COLL_TYPE_BCAST;
    if (is_node_leader && !is_root) {
        int32_t *recvtype_size = (int32_t *)ucg_malloc(sizeof(int32_t) , "node leader gatherv recvtype_size");
        inter_bcast_args->bcast.root = 0;
        inter_bcast_args->bcast.buffer = recvtype_size;
        inter_bcast_args->bcast.count = 1; 
        inter_bcast_args->bcast.dt = ucg_dt_get_predefined(UCG_DT_TYPE_INT32);

    }
    if (is_root) {
        int32_t *sendbuf = (int32_t *)ucg_malloc(sizeof(int32_t), "root gatherv recvtype_size");
        sendbuf[0] = ucg_dt_size(args->gatherv.recvtype);
        inter_bcast_args->bcast.root = 0;
        inter_bcast_args->bcast.buffer = sendbuf;
        inter_bcast_args->bcast.count = 1;
        inter_bcast_args->bcast.dt = ucg_dt_get_predefined(UCG_DT_TYPE_INT32);
    }
    if (ucx_op->gatherv.topo_aware.intra_sendcount_op == NULL) {
    curr_ucx_op = ucg_planc_ucx_bcast_build_topo_group_op(ucx_group,
                                                          vgroup,
                                                          inter_bcast_args,
                                                          UCG_GATHERV_BCAST_KNTREE,
                                                          UCG_TOPO_GROUP_TYPE_NODE_LEADER,
                                                          1);
    /* To ensure that requests of multiple members in the same collection op
     **         can be matched, all subops must have the same request ID. */
    curr_ucx_op->super.super.id = ucx_op->super.super.id;
    ucx_op->gatherv.topo_aware.intra_sendcount_op = curr_ucx_op;
    }

    return UCG_OK;
}

static ucg_status_t ucg_planc_ucx_gatherv_na_kntree_intra_gatherv_prepare(ucg_plan_op_t *ucg_op)
{
    ucg_planc_ucx_op_t *ucx_op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_vgroup_t *vgroup = ucx_op->super.vgroup;
    ucg_planc_ucx_group_t *ucx_group = ucx_op->ucx_group;
    ucg_coll_args_t *args = &ucx_op->super.super.args;
    ucg_planc_ucx_op_t *curr_ucx_op;
    ucg_coll_args_t *intra_gatherv_args = &ucx_op->gatherv.topo_aware.intra_gatherv_args;
    ucg_coll_args_t *intra_gatherv_sc_args = &ucx_op->gatherv.topo_aware.intra_gatherv_sc_args;
    ucg_coll_args_t *inter_gatherv_sc_args = &ucx_op->gatherv.topo_aware.inter_gatherv_sc_args;
    int32_t is_node_leader = ucx_op->gatherv.topo_aware.is_node_leader;
    int32_t ppn = ucx_op->gatherv.topo_aware.ppn;
    int32_t intra_total_count = 0;


    /* intra-gatherv */
    // set gatherv arguments
    intra_gatherv_args->type = UCG_COLL_TYPE_GATHERV;
    intra_gatherv_args->gatherv.root = 0;// root is the node_leader
    intra_gatherv_args->gatherv.sendbuf = args->gatherv.sendbuf;
    intra_gatherv_args->gatherv.sendcount = args->gatherv.sendcount;
    intra_gatherv_args->gatherv.sendtype = args->gatherv.sendtype;
    if (is_node_leader) {
        int32_t * recvcounts = (int32_t *)ucg_malloc(sizeof(int32_t) * ppn, "intra gatherv recvcounts");
        ucg_topo_group_t *node_intra_group = ucg_topo_get_group(vgroup->group->topo, UCG_TOPO_GROUP_TYPE_NODE);
        ucg_rank_t node_intra_rank;
        for (int32_t i = 0; i < ppn; i++) {
            node_intra_rank = ucg_rank_map_eval(&node_intra_group->super.rank_map, i);
            recvcounts[i] = ((int32_t*)inter_gatherv_sc_args->bcast.buffer)[node_intra_rank] * ((int32_t*)intra_gatherv_sc_args->bcast.buffer)[0];
            intra_total_count += recvcounts[i];
        }
        intra_gatherv_args->gatherv.recvtype = ucg_dt_get_predefined(UCG_DT_TYPE_UINT8);
        uint32_t recvtype_extent = ucg_dt_extent(intra_gatherv_args->gatherv.recvtype);
        intra_gatherv_args->gatherv.recvcounts = recvcounts;//decided dynamically
        intra_gatherv_args->gatherv.displs = ucg_planc_ucx_create_displs(intra_gatherv_args->gatherv.recvcounts, ppn);//decided dynamically

        uint64_t recvbuf_len = recvtype_extent * intra_total_count;
        intra_gatherv_args->gatherv.recvbuf = (int32_t *)ucg_malloc(recvbuf_len, "intra gatherv recvcounts recvbuf");//decided dynamically

        ucx_op->gatherv.topo_aware.intra_total_count = intra_total_count;
    }
    if (ucx_op->gatherv.topo_aware.intra_op == NULL) {
    curr_ucx_op = ucg_planc_ucx_gatherv_na_kntree_build_topo_group_op(ucx_group,
                                                                      vgroup,
                                                                      intra_gatherv_args,
                                                                      UCG_GATHERV_KNTREE,
                                                                      UCG_TOPO_GROUP_TYPE_NODE,
                                                                      0);
    /* To ensure that requests of multiple members in the same collection op
     **         can be matched, all subops must have the same request ID. */
    curr_ucx_op->super.super.id = ucx_op->super.super.id;
    ucx_op->gatherv.topo_aware.intra_op = curr_ucx_op;
    }


    return UCG_OK;
}

static ucg_status_t ucg_planc_ucx_gatherv_na_kntree_inter_gatherv_prepare(ucg_plan_op_t *ucg_op)
{
    ucg_planc_ucx_op_t *ucx_op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_vgroup_t *vgroup = ucx_op->super.vgroup;
    ucg_planc_ucx_group_t *ucx_group = ucx_op->ucx_group;
    ucg_coll_args_t *args = &ucx_op->super.super.args;
    ucg_planc_ucx_op_t *curr_ucx_op;
    ucg_coll_args_t *inter_gatherv_args = &ucx_op->gatherv.topo_aware.inter_gatherv_args;
    ucg_coll_args_t *intra_gatherv_args = &ucx_op->gatherv.topo_aware.intra_gatherv_args;
    int32_t is_node_leader = ucx_op->gatherv.topo_aware.is_node_leader;
    int32_t node_cnt = ucx_op->gatherv.topo_aware.node_cnt;
    uint32_t is_root = vgroup->myrank == args->gatherv.root;

    /* inter-gatherv */
    // set gatherv arguments
    inter_gatherv_args->type = UCG_COLL_TYPE_GATHERV;
    if (is_node_leader) {
        inter_gatherv_args->gatherv.root = 0;
        inter_gatherv_args->gatherv.sendbuf = intra_gatherv_args->gatherv.recvbuf;//decided dynamically
        inter_gatherv_args->gatherv.sendcount = ucx_op->gatherv.topo_aware.intra_total_count;//decided dynamically
        inter_gatherv_args->gatherv.sendtype = ucg_dt_get_predefined(UCG_DT_TYPE_UINT8);
        if (is_root) {
            int32_t *recvcounts = (int32_t *)ucg_malloc(sizeof(int32_t) * node_cnt, "inter gatherv recvcounts");
            ucg_topo_group_t *node_leader_group = ucg_topo_get_group(vgroup->group->topo, UCG_TOPO_GROUP_TYPE_NODE_LEADER);
            ucg_rank_t node_leader_rank, last_node_leader_rank;
            last_node_leader_rank = 0;
            for (int32_t i = 0; i < node_cnt; i++) {
                if (i == node_cnt - 1) {
                    node_leader_rank = vgroup->size;
                } else {
                    node_leader_rank = ucg_rank_map_eval(&node_leader_group->super.rank_map, i + 1);
                }

                int32_t intra_recvcounts_num = 0;
                for (int32_t j = last_node_leader_rank; j < node_leader_rank; j++) {
                    intra_recvcounts_num += args->gatherv.recvcounts[j];
                }
                recvcounts[i] = intra_recvcounts_num;
                last_node_leader_rank = node_leader_rank;
            }
            inter_gatherv_args->gatherv.recvcounts = recvcounts;
            inter_gatherv_args->gatherv.displs = ucg_planc_ucx_create_displs(inter_gatherv_args->gatherv.recvcounts, node_cnt);
            inter_gatherv_args->gatherv.recvbuf = args->gatherv.recvbuf;
            inter_gatherv_args->gatherv.recvtype = args->gatherv.recvtype;
        }
    }
    if (ucx_op->gatherv.topo_aware.inter_op == NULL) {
    curr_ucx_op = ucg_planc_ucx_gatherv_na_kntree_build_topo_group_op(ucx_group,
                                                           vgroup,
                                                           inter_gatherv_args,
                                                            UCG_GATHERV_KNTREE,
                                                           UCG_TOPO_GROUP_TYPE_NODE_LEADER,
                                                           1);
    /* To ensure that requests of multiple members in the same collection op
     **         can be matched, all subops must have the same request ID. */
    curr_ucx_op->super.super.id = ucx_op->super.super.id;
    ucx_op->gatherv.topo_aware.inter_op = curr_ucx_op;
    }

    return UCG_OK;
}

ucg_status_t ucg_planc_ucx_gatherv_na_kntree_op_progress(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_op_t *ucx_op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_plan_op_t *curr_ucg_op = NULL;

    /* inter-node gatherv recvcounts */
    if (ucg_test_flags(ucx_op->flags, UCG_NA_GATHERV_SENDCOUNT_INTER_PHASE)) {
        if (ucx_op->gatherv.topo_aware.inter_sendcount_op_trigged == 0) {
            // status =  ucg_planc_ucx_gatherv_na_kntree_inter_gatherv_recvcounts_prepare(ucg_op);
            // if (status != UCG_OK) {
            //     return status;
            // }
            curr_ucg_op = &ucx_op->gatherv.topo_aware.inter_sendcount_op->super;
            status = curr_ucg_op->trigger(curr_ucg_op);
            ucx_op->gatherv.topo_aware.inter_sendcount_op_trigged = 1;
        }
        curr_ucg_op = &ucx_op->gatherv.topo_aware.inter_sendcount_op->super;
        status = curr_ucg_op->progress(curr_ucg_op);
        UCG_CHECK_GOTO(status, out);
        ucx_op->flags = UCG_NA_GATHERV_SENDCOUNT_INTRA_PHASE;
    }

    /* inter-node gatherv recvtype */
    if (ucg_test_flags(ucx_op->flags, UCG_NA_GATHERV_SENDCOUNT_INTRA_PHASE)) {
        if (ucx_op->gatherv.topo_aware.intra_sendcount_op_trigged == 0) {
            // status =  ucg_planc_ucx_gatherv_na_kntree_inter_gatherv_recvtype_prepare(ucg_op);
            // if (status != UCG_OK) {
            //     return status;
            // }
            curr_ucg_op = &ucx_op->gatherv.topo_aware.intra_sendcount_op->super;
            status = curr_ucg_op->trigger(curr_ucg_op);
            ucx_op->gatherv.topo_aware.intra_sendcount_op_trigged = 1;
        }
        curr_ucg_op = &ucx_op->gatherv.topo_aware.intra_sendcount_op->super;
        status = curr_ucg_op->progress(curr_ucg_op);
        UCG_CHECK_GOTO(status, out);
        ucx_op->flags = UCG_NA_GATHERV_INTRA_PHASE;
    }

    /* intra-node gatherv */
    if (ucg_test_flags(ucx_op->flags, UCG_NA_GATHERV_INTRA_PHASE)) {
        if (ucx_op->gatherv.topo_aware.is_intra_op_trigged == 0) {
            status = ucg_planc_ucx_gatherv_na_kntree_intra_gatherv_prepare(ucg_op);
            if (status != UCG_OK) {
                return status;
            }
            curr_ucg_op = &ucx_op->gatherv.topo_aware.intra_op->super;
            status = curr_ucg_op->trigger(curr_ucg_op);
            ucx_op->gatherv.topo_aware.is_intra_op_trigged = 1;
        }
        curr_ucg_op = &ucx_op->gatherv.topo_aware.intra_op->super;
        status = curr_ucg_op->progress(curr_ucg_op);
        UCG_CHECK_GOTO(status, out);
        ucx_op->flags = UCG_NA_GATHERV_INTER_PHASE;
    }


    /* inter-node gatherv */
    if (ucg_test_flags(ucx_op->flags, UCG_NA_GATHERV_INTER_PHASE)) {
        if (ucx_op->gatherv.topo_aware.is_inter_op_trigged == 0) {
            status = ucg_planc_ucx_gatherv_na_kntree_inter_gatherv_prepare(ucg_op);
            if (status != UCG_OK) {
                return status;
            }
            curr_ucg_op = &ucx_op->gatherv.topo_aware.inter_op->super;
            status = curr_ucg_op->trigger(curr_ucg_op);
            ucx_op->gatherv.topo_aware.is_inter_op_trigged = 1;
        }
        curr_ucg_op = &ucx_op->gatherv.topo_aware.inter_op->super;
        status = curr_ucg_op->progress(curr_ucg_op);
        UCG_CHECK_GOTO(status, out);
        ucg_clear_flags(&ucx_op->flags, UCG_NA_GATHERV_INTER_PHASE);
    }
out:
    ucx_op->super.super.status = status;
    return status;
}

static ucg_status_t ucg_planc_ucx_gatherv_na_kntree_op_discard(ucg_plan_op_t *ucg_op)
{
    ucg_planc_ucx_op_t *ucx_op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    ucg_vgroup_t *vgroup = ucx_op->super.vgroup;
    ucg_coll_args_t *intra_gatherv_sc_args = &ucx_op->gatherv.topo_aware.intra_gatherv_sc_args;
    ucg_coll_args_t *inter_gatherv_sc_args = &ucx_op->gatherv.topo_aware.inter_gatherv_sc_args;
    ucg_coll_args_t *intra_gatherv_args = &ucx_op->gatherv.topo_aware.intra_gatherv_args;
    ucg_coll_args_t *inter_gatherv_args = &ucx_op->gatherv.topo_aware.inter_gatherv_args;
    int32_t is_node_leader = ucx_op->gatherv.topo_aware.is_node_leader;
    ucg_coll_args_t *args = &ucx_op->super.super.args;
    uint32_t is_root = vgroup->myrank == args->gatherv.root;

    ucg_plan_op_t *op;
    op = &ucx_op->gatherv.topo_aware.intra_sendcount_op->super;
    op->discard(op);

    op = &ucx_op->gatherv.topo_aware.inter_sendcount_op->super;
    op->discard(op);

    op = &ucx_op->gatherv.topo_aware.intra_op->super;
    op->discard(op);

    op = &ucx_op->gatherv.topo_aware.inter_op->super;
    op->discard(op);


    ucx_op->gatherv.topo_aware.intra_sendcount_op = NULL; 
    ucx_op->gatherv.topo_aware.inter_sendcount_op = NULL; 
    ucx_op->gatherv.topo_aware.inter_op = NULL; 
    ucx_op->gatherv.topo_aware.intra_op = NULL; 
    if (is_node_leader && !is_root) {
        ucg_plan_ucx_free_ptr((void **)&(inter_gatherv_sc_args->bcast.buffer));
        ucg_plan_ucx_free_ptr((void **)&(intra_gatherv_sc_args->bcast.buffer));
    }
    if (is_root) {
        ucg_plan_ucx_free_ptr((void **)&(intra_gatherv_sc_args->bcast.buffer));
    }
    if (is_node_leader) {
        ucg_plan_ucx_free_ptr((void **)&(intra_gatherv_args->gatherv.recvbuf));

        ucg_plan_ucx_free_ptr((void **)&(intra_gatherv_args->gatherv.displs));
        if (is_root) {
            ucg_plan_ucx_free_ptr((void **)&(inter_gatherv_args->gatherv.recvcounts));
            ucg_plan_ucx_free_ptr((void **)&(inter_gatherv_args->gatherv.displs));
        }
    }


    UCG_CLASS_DESTRUCT(ucg_plan_op_t, ucg_op);
    ucg_mpool_put(ucg_op);
    return UCG_OK;
}

static ucg_status_t ucg_planc_ucx_gatherv_na_kntree_op_trigger(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status;

    ucg_planc_ucx_op_t *op = ucg_derived_of(ucg_op, ucg_planc_ucx_op_t);
    op->gatherv.topo_aware.is_inter_op_trigged = 0;
    op->gatherv.topo_aware.is_intra_op_trigged = 0;
    op->gatherv.topo_aware.intra_sendcount_op_trigged = 0;
    op->gatherv.topo_aware.inter_sendcount_op_trigged = 0;
    ucg_planc_ucx_op_reset(op);
    op->flags = UCG_NA_GATHERV_SENDCOUNT_INTER_PHASE;
    status = ucg_planc_ucx_gatherv_na_kntree_op_progress(ucg_op);
    return status == UCG_INPROGRESS ? UCG_OK : status;
}


static void ucg_planc_ucx_gatherv_na_kntree_op_init(ucg_planc_ucx_op_t *ucx_op, ucg_planc_ucx_gatherv_config_t *config)
{
    ucg_vgroup_t *vgroup = ucx_op->super.vgroup;

    ucx_op->gatherv.topo_aware.is_inter_op_trigged = 0;
    ucx_op->gatherv.topo_aware.is_intra_op_trigged = 0;
    ucx_op->gatherv.topo_aware.intra_sendcount_op_trigged = 0;
    ucx_op->gatherv.topo_aware.inter_sendcount_op_trigged = 0;
    ucx_op->gatherv.topo_aware.inter_op = NULL;
    ucx_op->gatherv.topo_aware.intra_op = NULL;
    ucx_op->gatherv.topo_aware.intra_sendcount_op = NULL;
    ucx_op->gatherv.topo_aware.inter_sendcount_op = NULL;

    /* decide node leader */
    ucg_topo_group_t *node_leader_group, *node_member_group;
    node_leader_group = ucg_topo_get_group(vgroup->group->topo, UCG_TOPO_GROUP_TYPE_NODE_LEADER);
    uint32_t is_node_leader = node_leader_group->state == UCG_TOPO_GROUP_STATE_ENABLE;
    ucx_op->gatherv.topo_aware.is_node_leader = is_node_leader;
    uint32_t node_cnt = node_leader_group->super.size;
    ucx_op->gatherv.topo_aware.node_cnt = node_cnt;
    node_member_group = ucg_topo_get_group(vgroup->group->topo, UCG_TOPO_GROUP_TYPE_NODE);
    uint32_t ppn = node_member_group->super.size;
    ucx_op->gatherv.topo_aware.ppn = ppn;

    ucg_plan_op_t *ucg_op = &ucx_op->super;
    ucg_planc_ucx_gatherv_na_kntree_inter_gatherv_recvcounts_prepare(ucg_op); //原来在progress,会出现重复调用，放这里防止重复调用
    ucg_planc_ucx_gatherv_na_kntree_inter_gatherv_recvtype_prepare(ucg_op); //原来在progress,会出现重复调用，放这里防止重复调用
}

ucg_planc_ucx_op_t *ucg_planc_ucx_gatherv_na_kntree_op_new(ucg_planc_ucx_group_t *ucx_group,
                                                           ucg_vgroup_t *vgroup,
                                                           const ucg_coll_args_t *args,
                                                           ucg_planc_ucx_gatherv_config_t *config)
{
    UCG_CHECK_NULL(NULL, ucx_group, vgroup, args);

    ucg_planc_ucx_op_t *ucx_op = ucg_mpool_get(&ucx_group->context->op_mp);
    if (ucx_op == NULL) {
        goto err;
    }

    ucg_status_t status = UCG_CLASS_CONSTRUCT(ucg_plan_op_t, &ucx_op->super, vgroup,
                                              ucg_planc_ucx_gatherv_na_kntree_op_trigger,
                                              ucg_planc_ucx_gatherv_na_kntree_op_progress,
                                              ucg_planc_ucx_gatherv_na_kntree_op_discard,
                                              args);

    if (status != UCG_OK) {
        ucg_error("Failed to initialize super of ucx op");
        goto err_free_op;
    }

    ucg_planc_ucx_op_init(ucx_op, ucx_group);
    ucg_planc_ucx_gatherv_na_kntree_op_init(ucx_op, config);
    return ucx_op;

err_free_op:
    ucg_mpool_put(ucx_op);
err:
    return NULL;
}

static ucg_status_t ucg_planc_ucx_gatherv_na_kntree_check(ucg_vgroup_t *vgroup,
                                                          const ucg_coll_args_t *args)
{
    int32_t ppn = vgroup->group->topo->ppn;
    int32_t group_size = vgroup->size;

    if (ppn == group_size) {
        ucg_info("Node-aware kntree gatherv does not support single node");
        return UCG_ERR_UNSUPPORTED;
    }

    if (args->gatherv.root != 0) {
        ucg_info("Node-aware kntree gatherv does not support root != 0");
        return UCG_ERR_UNSUPPORTED;
    }
    return UCG_OK;
}

ucg_status_t ucg_planc_ucx_gatherv_na_kntree_prepare(ucg_vgroup_t *vgroup,
                                                     const ucg_coll_args_t *args,
                                                     ucg_plan_op_t **op)
{
    UCG_CHECK_NULL_INVALID(vgroup, args, op);

    ucg_status_t status = ucg_planc_ucx_gatherv_na_kntree_check(vgroup, args);
    if (status != UCG_OK) {
        return status;
    }

    ucg_planc_ucx_group_t *ucx_group = ucg_derived_of(vgroup, ucg_planc_ucx_group_t);
    ucg_planc_ucx_gatherv_config_t *config;
    config = UCG_PLANC_UCX_CONTEXT_BUILTIN_CONFIG_BUNDLE(ucx_group->context, gatherv,
                                                         UCG_COLL_TYPE_GATHERV);

    ucg_planc_ucx_op_t *ucx_op = ucg_planc_ucx_gatherv_na_kntree_op_new(ucx_group, vgroup, args, config);
    if (ucx_op == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    *op = &ucx_op->super;
    return UCG_OK;
}