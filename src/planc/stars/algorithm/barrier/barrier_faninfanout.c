/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#include "barrier_faninfanout.h"

static ucg_status_t UCG_STARS_ALGO_FUN(barrier_faninfanout, iter_init)(ucg_planc_stars_op_t *op,
                                                                       const ucg_planc_stars_barrier_config_t *config)
{
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_algo_kntree_iter_t *iter;
    iter = &op->barrier.kntree.fanin_iter;
    ucg_algo_kntree_iter_init(iter, vgroup->size, config->fanin_degree, KNTREE_ITER_ROOT, vgroup->myrank,
                              0);  // right-most
    iter = &op->barrier.kntree.fanout_iter;
    ucg_algo_kntree_iter_init(iter, vgroup->size, config->fanout_degree, KNTREE_ITER_ROOT, vgroup->myrank,
                              1);  // left-most
    ucg_debug("Set the degree of barrier fanin : %d, fanout : %d", config->fanin_degree, config->fanout_degree);
    return UCG_OK;
}

static ucg_status_t ucg_planc_stars_barrier_faninfanout_ofd_put_req(ucg_planc_stars_op_t *meta_op,
                                                                    ucg_planc_stars_op_t *op,
                                                                    stars_rank_info_h peer_rank)
{
    scp_ofd_req_elem_h request = ucg_planc_stars_op_get_ofd_req_elem(op);
    UCG_ASSERT_RET(request != NULL, UCG_ERR_NO_MEMORY);

    request->lbuf = op->staging_area;
    request->llen = 1;
    request->rbuf = (void *)peer_rank->rbuf_desc->addr;
    request->lmemh = op->plan.lsmemh;
    request->type = OFFLOAD_PUT;

    ucg_status_t status = ucg_planc_stars_fill_ofd_put_req_elem(EID_IDX, peer_rank, request);
    UCG_ASSERT_CODE_RET(status);
    if (meta_op == NULL) {
        ucg_planc_stars_op_push_ofd_req_elem(op, request);
    } else {
        ucg_planc_stars_op_push_ofd_req_elem(meta_op, request);
    }

    return UCG_OK;
}

static ucg_status_t ucg_planc_stars_barrier_faninfanout_ofd_wait_req(ucg_planc_stars_op_t *meta_op,
                                                                     ucg_planc_stars_op_t *op,
                                                                     stars_rank_info_h peer_rank)
{
    scp_ofd_req_elem_h request = ucg_planc_stars_op_get_ofd_req_elem(op);
    UCG_ASSERT_RET(request != NULL, UCG_ERR_NO_MEMORY);

    ucg_planc_stars_fill_ofd_wait_req_elem(EID_IDX, peer_rank, request, op->plan.event_elem);
    if (meta_op == NULL) {
        ucg_planc_stars_op_push_ofd_req_elem(op, request);
    } else {
        ucg_planc_stars_op_push_ofd_req_elem(meta_op, request);
    }
    return UCG_OK;
}

static ucg_status_t UCG_STARS_ALGO_FUN(barrier_faninfanout, meta_submit_op)(ucg_planc_stars_op_t *meta_op,
                                                                            ucg_planc_stars_op_t *op)
{
    stars_comm_plan_t *plan = &op->plan;
    ucg_rank_t myrank = op->super.vgroup->myrank;
    ucg_status_t status;
    stars_rank_info_h peer_rank;

    ucg_algo_kntree_iter_t *fanin_iter = &op->barrier.kntree.fanin_iter;
    if (ucg_algo_kntree_iter_child_value(fanin_iter) != UCG_INVALID_RANK) {
        int32_t idx = (myrank == KNTREE_ITER_ROOT) ? 0 : 1;
        for (; idx < plan->comm_dep.get_rank_num; ++idx) {
            peer_rank = &plan->comm_dep.get_ranks[idx];
            status = ucg_planc_stars_barrier_faninfanout_ofd_wait_req(meta_op, op, peer_rank);
            UCG_CHECK_GOTO(status, out);
        }
    }

    if (myrank != KNTREE_ITER_ROOT) {
        peer_rank = &plan->comm_dep.put_ranks[0];
        status = ucg_planc_stars_barrier_faninfanout_ofd_put_req(meta_op, op, peer_rank);
        UCG_CHECK_GOTO(status, out);
    }
    UCG_CHECK_GOTO_ERR(status, out, "put with notify");
    if (myrank != KNTREE_ITER_ROOT) {
        peer_rank = &plan->comm_dep.get_ranks[0];
        status = ucg_planc_stars_barrier_faninfanout_ofd_wait_req(meta_op, op, peer_rank);
        UCG_CHECK_GOTO(status, out);
    }

    ucg_algo_kntree_iter_t *fanout_iter = &op->barrier.kntree.fanout_iter;
    if (ucg_algo_kntree_iter_child_value(fanout_iter) != UCG_INVALID_RANK) {
        int32_t idx = (myrank == KNTREE_ITER_ROOT) ? 0 : 1;
        for (; idx < plan->comm_dep.put_rank_num; ++idx) {
            peer_rank = &plan->comm_dep.put_ranks[idx];
            status = ucg_planc_stars_barrier_faninfanout_ofd_put_req(meta_op, op, peer_rank);
            UCG_CHECK_GOTO(status, out);
        }
    }

out:
    return status;
}


static ucg_status_t ucg_planc_stars_barrier_faninfanout_submit_stars_op(ucg_planc_stars_op_t *op)
{
    stars_comm_plan_t *plan = &op->plan;
    ucg_rank_t myrank = op->super.vgroup->myrank;
    ucg_status_t status;
    stars_rank_info_h peer_rank;

    ucg_algo_kntree_iter_t *fanin_iter = &op->barrier.kntree.fanin_iter;
    if (ucg_algo_kntree_iter_child_value(fanin_iter) != UCG_INVALID_RANK) {
        int32_t idx = (myrank == KNTREE_ITER_ROOT) ? 0 : 1;
        for (; idx < plan->comm_dep.get_rank_num; ++idx) {
            peer_rank = &plan->comm_dep.get_ranks[idx];
            status = ucg_planc_stars_barrier_faninfanout_ofd_wait_req(NULL, op, peer_rank);
            UCG_CHECK_GOTO(status, out);
        }
    }

    if (myrank != KNTREE_ITER_ROOT) {
        peer_rank = &plan->comm_dep.put_ranks[0];
        status = ucg_planc_stars_barrier_faninfanout_ofd_put_req(NULL, op, peer_rank);
        UCG_CHECK_GOTO(status, out);
    }
    UCG_CHECK_GOTO_ERR(status, out, "put with notify");
    if (myrank != KNTREE_ITER_ROOT) {
        peer_rank = &plan->comm_dep.get_ranks[0];
        status = ucg_planc_stars_barrier_faninfanout_ofd_wait_req(NULL, op, peer_rank);
        UCG_CHECK_GOTO(status, out);
    }

    ucg_algo_kntree_iter_t *fanout_iter = &op->barrier.kntree.fanout_iter;
    if (ucg_algo_kntree_iter_child_value(fanout_iter) != UCG_INVALID_RANK) {
        int32_t idx = (myrank == KNTREE_ITER_ROOT) ? 0 : 1;
        for (; idx < plan->comm_dep.put_rank_num; ++idx) {
            peer_rank = &plan->comm_dep.put_ranks[idx];
            status = ucg_planc_stars_barrier_faninfanout_ofd_put_req(NULL, op, peer_rank);
            UCG_CHECK_GOTO(status, out);
        }
    }
    return scp_submit_ofd_req(op->stars_group->context->scp_worker,
                              &op->stars_group->stars_stream,
                              &op->ofd_req, op->stats.cur_elem);

out:
    return status;
}

static inline size_t UCG_STARS_ALGO_FUN(barrier_faninfanout, put_max_size)(ucg_planc_stars_op_t *op,
                                                                           ucg_coll_args_t *coll_args)
{
    return SEND_RECV_BYTE;
}

static ucg_status_t UCG_STARS_ALGO_FUN(barrier_faninfanout, init_rbuf)(ucg_planc_stars_op_t *op,
                                                                       ucg_coll_args_t *coll_args)
{
    stars_comm_plan_t *plan = &op->plan;
    ucg_status_t status;
    // todo check get rank num max uint16_t
    status = ucg_planc_stars_rbuf_init(op, op->staging_area, SEND_RECV_BYTE);
    UCG_ASSERT_CODE_RET(status);

    for (uint32_t idx = 0; idx < plan->comm_dep.get_rank_num; ++idx) {
        plan->comm_dep.get_ranks[idx].rbuf_desc = plan->lrbuf_desc;
    }

    return UCG_OK;
}

static ucg_status_t UCG_STARS_ALGO_FUN(barrier_faninfanout, init_sbuf)(ucg_planc_stars_op_t *op,
                                                                       ucg_coll_args_t *coll_args)
{
    stars_comm_plan_t *plan = &op->plan;
    plan->lsbuf_desc = plan->lrbuf_desc;
    plan->lsmemh = plan->lrmemh;
    return UCG_OK;
}

static inline ucg_status_t UCG_STARS_ALGO_FUN(barrier_faninfanout, check)(ucg_vgroup_t *vgroup,
                                                                          const ucg_coll_args_t *coll_args)
{
    return UCG_OK;
}

static ucg_status_t UCG_STARS_ALGO_FUN(barrier_faninfanout, ofd_plan)(ucg_planc_stars_op_t *op)
{
    stars_comm_dep_h comm_dep = &op->plan.comm_dep;
    comm_dep->get_ranks    = comm_dep->put_ranks    = NULL;
    comm_dep->get_rank_num = comm_dep->put_rank_num = 0;

    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_rank_t myrank = vgroup->myrank, peer = UCG_INVALID_RANK;
    uint32_t group_size = vgroup->size;
    uint32_t peer_put[group_size + 1], peer_get[group_size + 1];
    ucg_algo_kntree_iter_t *fanout_iter = &op->barrier.kntree.fanout_iter;
    ucg_algo_kntree_iter_t *fanin_iter = &op->barrier.kntree.fanin_iter;
    if (myrank != KNTREE_ITER_ROOT) {
        peer = ucg_algo_kntree_iter_parent_value(fanout_iter);
        peer_get[comm_dep->get_rank_num++] = peer;
        peer = ucg_algo_kntree_iter_parent_value(fanin_iter);
        peer_put[comm_dep->put_rank_num++] = peer;
    }

    while ((peer = ucg_algo_kntree_iter_child_value(fanout_iter)) != UCG_INVALID_RANK) {
        peer_put[comm_dep->put_rank_num++] = peer;
        ucg_algo_kntree_iter_child_inc(fanout_iter);
    }
    ucg_algo_kntree_iter_reset(fanout_iter);
    while ((peer = ucg_algo_kntree_iter_child_value(fanin_iter)) != UCG_INVALID_RANK) {
        peer_get[comm_dep->get_rank_num++] = peer;
        ucg_algo_kntree_iter_child_inc(fanin_iter);
    }
    ucg_algo_kntree_iter_reset(fanin_iter);

    ucg_status_t status;
    status = ucg_planc_stars_rank_dep_alloc(comm_dep);
    UCG_ASSERT_CODE_RET(status);

    ucg_planc_stars_context_t *context = op->stars_group->context;
    for (uint32_t idx = 0; idx < comm_dep->put_rank_num; ++idx) {
        peer = peer_put[idx];
        comm_dep->put_ranks[idx].peer_id = peer;
        status = ucg_planc_stars_rank_dep_init(op, &comm_dep->put_ranks[idx], peer, 1);
        UCG_ASSERT_CODE_GOTO(status, err_free_memory);
    }

    for (uint32_t idx = 0; idx < comm_dep->get_rank_num; ++idx) {
        peer = peer_get[idx];
        comm_dep->get_ranks[idx].peer_id = peer;
        status = ucg_planc_stars_rank_dep_init(op, &comm_dep->get_ranks[idx], peer, 1);
        UCG_ASSERT_CODE_GOTO(status, err_free_memory);
    }
    return UCG_OK;

err_free_memory:
    ucg_free(comm_dep->get_ranks);
    ucg_free(comm_dep->put_ranks);
    return status;
}

static ucg_status_t UCG_STARS_ALGO_FUN(barrier_faninfanout, init)(ucg_plan_op_t *ucg_op)
{
    ucg_planc_stars_op_t *op = ucg_derived_of(ucg_op, ucg_planc_stars_op_t);
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_planc_stars_group_t *stars_group = ucg_derived_of(vgroup, ucg_planc_stars_group_t);
    ucg_planc_stars_barrier_config_t *config =
        UCG_PLANC_STARS_CONTEXT_BUILTIN_CONFIG_BUNDLE(stars_group->context, barrier, UCG_COLL_TYPE_BARRIER);

    ucg_status_t status;
    status = UCG_STARS_ALGO_FUN(barrier_faninfanout, iter_init)(op, config);
    UCG_ASSERT_CODE_GOTO(status, err_destruct_op);

    op->staging_area = ucg_malloc(SEND_RECV_BYTE, "init send recv buffer");
    if (op->staging_area == NULL) {
        ucg_error("Failed to initialize staging area");
        return UCG_ERR_NO_MEMORY;
    }
    memset(op->staging_area, 0, SEND_RECV_BYTE);

    ucg_coll_args_t *coll_args = &ucg_op->super.args;
    status = ucg_planc_stars_algo_prepare_plan(op, coll_args, UCG_STARS_ALGO_FUN(barrier_faninfanout, put_max_size),
                                               UCG_STARS_ALGO_FUN(barrier_faninfanout, ofd_plan));
    UCG_CHECK_GOTO_ERR(status, out, "prepare barrier faninfanout plan");

    status = ucg_planc_stars_algo_exch_buf_addr(op, coll_args, UCG_STARS_ALGO_FUN(barrier_faninfanout, init_rbuf),
                                                UCG_STARS_ALGO_FUN(barrier_faninfanout, init_sbuf));
    UCG_CHECK_GOTO_ERR(status, out, "exchange buffer address");

    return UCG_OK;
err_destruct_op:
    UCG_CLASS_DESTRUCT(ucg_plan_op_t, &op->super);
out:
    ucg_free(op->staging_area);
    op->super.super.status = status;
    return status;
}

ucg_status_t UCG_STARS_ALGO_FUN(barrier_faninfanout, meta_trigger)(ucg_plan_op_t *meta_op, ucg_plan_op_t *ucg_op)
{
    ucg_planc_stars_op_t *op = ucg_derived_of(ucg_op, ucg_planc_stars_op_t);
    ucg_planc_stars_op_t *meta_stars_op = ucg_derived_of(meta_op, ucg_planc_stars_op_t);
    ucg_planc_stars_op_reset(op);
    ucg_algo_kntree_iter_reset(&op->barrier.kntree.fanin_iter);
    ucg_algo_kntree_iter_reset(&op->barrier.kntree.fanout_iter);
    op->super.super.status = UCG_ERR_IO_ERROR;

    ucg_status_t status = UCG_STARS_ALGO_FUN(barrier_faninfanout, meta_submit_op)(meta_stars_op, op);
    if (status != UCG_OK) {
        op->super.super.status = status;
        ucg_error("Failed to submit barrier faninfanout stars operation.");
        return status;
    }

    op->super.super.status = UCG_INPROGRESS;
    return UCG_OK;
}

static ucg_status_t UCG_STARS_ALGO_FUN(barrier_faninfanout, trigger)(ucg_plan_op_t *ucg_op)
{
    ucg_planc_stars_op_t *op = ucg_derived_of(ucg_op, ucg_planc_stars_op_t);
    ucg_planc_stars_op_reset(op);
    ucg_algo_kntree_iter_reset(&op->barrier.kntree.fanin_iter);
    ucg_algo_kntree_iter_reset(&op->barrier.kntree.fanout_iter);
    op->super.super.status = UCG_ERR_IO_ERROR;

    ucg_status_t status = ucg_planc_stars_barrier_faninfanout_submit_stars_op(op);
    if (status != UCG_OK) {
        op->super.super.status = status;
        ucg_error("Failed to submit barrier faninfanout stars operation.");
        return status;
    }

    op->super.super.status = UCG_INPROGRESS;
    return UCG_OK;
}

inline static ucg_status_t UCG_STARS_ALGO_FUN(barrier_faninfanout, progress)(ucg_plan_op_t *ucg_op)
{
    return ucg_planc_stars_algo_progress(ucg_op, NULL);
}

static ucg_status_t UCG_STARS_ALGO_FUN(barrier_faninfanout, discard)(ucg_plan_op_t *ucg_op)
{
    ucg_planc_stars_op_t *op = ucg_derived_of(ucg_op, ucg_planc_stars_op_t);
    ucg_planc_stars_buf_cleanup(op);
    if (op->staging_area != NULL) {
        ucg_free(op->staging_area);
    }
    ucg_planc_stars_op_discard(ucg_op);
    UCG_CLASS_DESTRUCT(ucg_plan_op_t, ucg_op);
    return UCG_OK;
}

UCG_STARS_ALGO_PRE_DEFINE(barrier_faninfanout)
