/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "allreduce.h"
#include "core/ucg_group.h"
#include "core/ucg_topo.h"
#include "reduce/reduce.h"
#include "planc_ucx_meta.h"
#include "util/ucg_log.h"

static ucg_status_t ucg_planc_ucx_allreduce_add_bcast_topo_group_op(ucg_plan_meta_op_t *meta_op,
                                                                    ucg_planc_ucx_group_t *ucx_group,
                                                                    ucg_vgroup_t *vgroup,
                                                                    const ucg_coll_args_t *args,
                                                                    const ucg_planc_ucx_bcast_config_t *config,
                                                                    ucg_topo_group_type_t type)
{
    ucg_topo_group_t *topo_group;
    topo_group = ucg_topo_get_group(vgroup->group->topo, type);
    if (topo_group->state == UCG_TOPO_GROUP_STATE_DISABLE) {
        /* I'm not in the topo group. */
        return ucg_planc_ucx_add_empty_op(meta_op, ucx_group, vgroup);
    }

    if (topo_group->state != UCG_TOPO_GROUP_STATE_ENABLE) {
        /* The group state is incorrect. */
        return UCG_ERR_NO_RESOURCE;
    }

    ucg_planc_ucx_op_t *ucx_op;
    ucx_op = ucg_planc_ucx_bcast_kntree_op_new(ucx_group,
                                               &topo_group->super,
                                               args, config);
    if (ucx_op == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    return ucg_plan_meta_op_add(meta_op, &ucx_op->super);
}

static ucg_status_t ucg_planc_ucx_allreduce_add_rd_topo_group_op(ucg_plan_meta_op_t *meta_op,
                                                                 ucg_planc_ucx_group_t *ucx_group,
                                                                 ucg_vgroup_t *vgroup,
                                                                 const ucg_coll_args_t *args,
                                                                 ucg_topo_group_type_t type)
{
    ucg_topo_group_t *topo_group;
    topo_group = ucg_topo_get_group(vgroup->group->topo, type);
    if (topo_group->state == UCG_TOPO_GROUP_STATE_DISABLE) {
        /* I'm not in the topo group. */
        return ucg_planc_ucx_add_empty_op(meta_op, ucx_group, vgroup);
    }

    if (topo_group->state != UCG_TOPO_GROUP_STATE_ENABLE) {
        /* The group state is incorrect. */
        return UCG_ERR_NO_RESOURCE;
    }

    ucg_planc_ucx_op_t *ucx_op;
    ucx_op = ucg_planc_ucx_allreduce_rd_op_new(ucx_group, &topo_group->super, args);
    if (ucx_op == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    return ucg_plan_meta_op_add(meta_op, &ucx_op->super);
}

static ucg_status_t ucg_planc_ucx_allreduce_add_reduce_topo_group_op(ucg_plan_meta_op_t *meta_op,
                                                                     ucg_planc_ucx_group_t *ucx_group,
                                                                     ucg_vgroup_t *vgroup,
                                                                     const ucg_coll_args_t *args,
                                                                     ucg_planc_ucx_reduce_config_t *config,
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

    ucg_planc_ucx_op_t *ucx_op;
    ucx_op = ucg_planc_ucx_reduce_kntree_op_new(ucx_group, &topo_group->super,
                                                args, config);
    if (ucx_op == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    return ucg_plan_meta_op_add(meta_op, &ucx_op->super);
}

ucg_status_t ucg_planc_ucx_allreduce_add_reduce_kntree_op(ucg_plan_meta_op_t *meta_op,
                                                          ucg_planc_ucx_group_t *ucx_group,
                                                          ucg_vgroup_t *vgroup,
                                                          const ucg_coll_args_t *args,
                                                          const ucg_planc_ucx_allreduce_config_t *config,
                                                          ucg_topo_group_type_t group_type,
                                                          int32_t send_in_place)
{
    ucg_planc_ucx_reduce_config_t reduce_config;
    if ((group_type == UCG_TOPO_GROUP_TYPE_NODE) ||
        (group_type == UCG_TOPO_GROUP_TYPE_SOCKET) ||
        (group_type == UCG_TOPO_GROUP_TYPE_SOCKET_LEADER)) {
        reduce_config.kntree_degree = config->fanin_intra_degree;
    } else {
        reduce_config.kntree_degree = config->fanin_inter_degree;
    }

    ucg_coll_args_t reduce_args;
    if (send_in_place) {
        reduce_args.reduce.sendbuf = UCG_IN_PLACE;
    } else {
        reduce_args.reduce.sendbuf = args->allreduce.sendbuf;
    }
    reduce_args.reduce.recvbuf = args->allreduce.recvbuf;
    reduce_args.reduce.count = args->allreduce.count;
    reduce_args.reduce.dt = args->allreduce.dt;
    reduce_args.reduce.op = args->allreduce.op;
    reduce_args.reduce.root = UCG_TOPO_GROUP_LEADER;

    return ucg_planc_ucx_allreduce_add_reduce_topo_group_op(meta_op, ucx_group, vgroup,
                                                            &reduce_args, &reduce_config,
                                                            group_type);
}

ucg_status_t ucg_planc_ucx_allreduce_add_allreduce_rd_op(ucg_plan_meta_op_t *meta_op,
                                                         ucg_planc_ucx_group_t *ucx_group,
                                                         ucg_vgroup_t *vgroup,
                                                         const ucg_coll_args_t *args,
                                                         ucg_topo_group_type_t group_type,
                                                         int32_t send_in_place)
{
    ucg_coll_args_t rd_args = *args;
    if (send_in_place) {
        rd_args.allreduce.sendbuf = UCG_IN_PLACE;
    }

    return ucg_planc_ucx_allreduce_add_rd_topo_group_op(meta_op, ucx_group, vgroup,
                                                        &rd_args, group_type);
}

ucg_status_t ucg_planc_ucx_allreduce_add_bcast_kntree_op(ucg_plan_meta_op_t *meta_op,
                                                         ucg_planc_ucx_group_t *ucx_group,
                                                         ucg_vgroup_t *vgroup,
                                                         const ucg_coll_args_t *args,
                                                         const ucg_planc_ucx_allreduce_config_t *config,
                                                         ucg_topo_group_type_t group_type)
{
    ucg_planc_ucx_bcast_config_t kntree_config;
    if ((group_type == UCG_TOPO_GROUP_TYPE_NODE) ||
        (group_type == UCG_TOPO_GROUP_TYPE_SOCKET) ||
        (group_type == UCG_TOPO_GROUP_TYPE_SOCKET_LEADER)) {
        kntree_config.kntree_degree = config->fanout_intra_degree;
    } else {
        kntree_config.kntree_degree = config->fanout_inter_degree;
    }

    ucg_coll_args_t bcast_args;
    bcast_args.bcast.buffer = args->allreduce.recvbuf;
    bcast_args.bcast.count = args->allreduce.count;
    bcast_args.bcast.dt = args->allreduce.dt;
    bcast_args.bcast.root = UCG_TOPO_GROUP_LEADER;

    return ucg_planc_ucx_allreduce_add_bcast_topo_group_op(meta_op, ucx_group, vgroup,
                                                           &bcast_args, &kntree_config,
                                                           group_type);
}

ucg_status_t ucg_planc_ucx_allreduce_add_reduce_scatter_op(ucg_plan_meta_op_t *meta_op,
                                                           ucg_planc_ucx_group_t *ucx_group,
                                                           ucg_vgroup_t *vgroup,
                                                           const ucg_coll_args_t *args,
                                                           ucg_topo_group_type_t group_type)
{
    ucg_topo_group_t *topo_group;
    topo_group = ucg_topo_get_group(vgroup->group->topo, group_type);
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
    ucx_op = ucg_planc_ucx_allreduce_reduce_scatter_op_new(ucx_group, &topo_group->super, args);
    if (ucx_op == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    return ucg_plan_meta_op_add(meta_op, &ucx_op->super);
}

static ucg_status_t ucg_planc_ucx_allreduce_init_rd_args(ucg_vgroup_t *vgroup,
                                                         const ucg_coll_args_t *args,
                                                         ucg_topo_group_type_t topo_type,
                                                         int32_t *offset, int32_t *count)
{
    ucg_status_t status = UCG_OK;
    ucg_topo_group_t *topo_group;
    topo_group = ucg_topo_get_group(vgroup->group->topo, topo_type);
    ucg_rank_t myrank = topo_group->super.myrank;
    int32_t size = topo_group->super.size;

    int32_t nstep = ucg_ilog2(size);
    int32_t nprocs_pof2 = UCG_BIT(nstep);
    int32_t nprocs_rem = size - nprocs_pof2;
    int32_t mask = 1;
    int32_t step = 0;
    ucg_rank_t new_rank;
    if (myrank < 2 * nprocs_rem) {
        if (myrank % 2 == 0) {
            new_rank = myrank / 2;
        } else {
            new_rank = -1;
        }
    } else {
        new_rank = myrank - nprocs_rem;
    }
    int32_t wsize = args->allreduce.count;
    int32_t *sindex = ucg_malloc(nstep * sizeof(int32_t), "allreduce send index");
    if (sindex == NULL) {
        status = UCG_ERR_NO_MEMORY;
        goto err;
    }
    int32_t *rindex = ucg_malloc(nstep * sizeof(int32_t), "allreduce recv index");
    if (rindex == NULL) {
        status = UCG_ERR_NO_MEMORY;
        goto err_free_sindex;
    }
    int32_t *scount = ucg_malloc(nstep * sizeof(int32_t), "allreduce send count");
    if (scount == NULL) {
        status = UCG_ERR_NO_MEMORY;
        goto err_free_rindex;
    }
    int32_t *rcount = ucg_malloc(nstep * sizeof(int32_t), "allreduce recv count");
    if (rcount == NULL) {
        status = UCG_ERR_NO_MEMORY;
        goto err_free_scount;
    }
    sindex[0] = rindex[0] = rcount[0] = 0;

    while (mask < nprocs_pof2) {
        ucg_rank_t new_peer = new_rank ^ mask;
        ucg_rank_t peer = (new_peer < nprocs_rem) ? new_peer * 2 : new_peer + nprocs_rem;
        if (myrank < peer) {
            rcount[step] = wsize / 2;
            scount[step] = wsize - rcount[step];
            sindex[step] = rindex[step] + rcount[step];
        } else {
            scount[step] = wsize / 2;
            rcount[step] = wsize - scount[step];
            rindex[step] = sindex[step] + scount[step];
        }

        if (step + 1 < nstep) {
            rindex[step + 1] = rindex[step];
            sindex[step + 1] = rindex[step];
            wsize = rcount[step];
            ++step;
        }
        mask <<= 1;
    }
    *offset = rindex[step] * ucg_dt_extent(args->allreduce.dt);
    *count = rcount[step];

    ucg_free(rcount);
err_free_scount:
    ucg_free(scount);
err_free_rindex:
    ucg_free(rindex);
err_free_sindex:
    ucg_free(sindex);
err:
    return status;
}

ucg_status_t ucg_planc_ucx_allreduce_add_allreduce_op(ucg_plan_meta_op_t *meta_op,
                                                      ucg_planc_ucx_group_t *ucx_group,
                                                      ucg_vgroup_t *vgroup,
                                                      const ucg_coll_args_t *args,
                                                      ucg_topo_group_type_t topo_type,
                                                      ucg_planc_ucx_algo_group_type_t group_type)
{
    ucg_status_t status;
    ucg_coll_args_t rd_args = *args;
    int32_t offset, count;
    status = ucg_planc_ucx_allreduce_init_rd_args(vgroup, args, topo_type, &offset, &count);
    if (status != UCG_OK) {
        return UCG_ERR_NO_MEMORY;
    }
    if (count > 0) { // has added reduce_scatter op
        rd_args.allreduce.sendbuf = args->allreduce.recvbuf + offset;
        rd_args.allreduce.recvbuf = args->allreduce.recvbuf + offset;
        rd_args.allreduce.count = count;
    }

    ucg_planc_ucx_algo_group_t *algo_group = &ucx_group->groups[group_type];
    if (algo_group->state == UCG_ALGO_GROUP_STATE_DISABLE) {
        /* I'm not in the algo group. */
        return ucg_planc_ucx_add_empty_op(meta_op, ucx_group, vgroup);
    }

    if (algo_group->state != UCG_ALGO_GROUP_STATE_ENABLE) {
        /* The group state is incorrect. */
        return UCG_ERR_NO_RESOURCE;
    }

    ucg_planc_ucx_op_t *ucx_op;
    ucx_op = ucg_planc_ucx_allreduce_rd_op_new(ucx_group, &algo_group->super, &rd_args);
    if (ucx_op == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    return ucg_plan_meta_op_add(meta_op, &ucx_op->super);
}

ucg_status_t ucg_planc_ucx_allreduce_add_allgatherv_op(ucg_plan_meta_op_t *meta_op,
                                                       ucg_planc_ucx_group_t *ucx_group,
                                                       ucg_vgroup_t *vgroup,
                                                       const ucg_coll_args_t *args,
                                                       ucg_topo_group_type_t group_type)
{
    ucg_topo_group_t *topo_group;
    topo_group = ucg_topo_get_group(vgroup->group->topo, group_type);
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
    ucx_op = ucg_planc_ucx_allreduce_allgatherv_op_new(ucx_group, &topo_group->super, args);
    if (ucx_op == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    return ucg_plan_meta_op_add(meta_op, &ucx_op->super);
}

void ucg_planc_ucx_allreduce_set_send_in_place_flag(ucg_vgroup_t *vgroup,
                                                    ucg_topo_group_type_t pre_group_type,
                                                    int32_t *send_in_place)
{
    if (vgroup->group->topo->groups[pre_group_type].state == UCG_TOPO_GROUP_STATE_ENABLE) {
        *send_in_place = 1;
    }
    return;
}