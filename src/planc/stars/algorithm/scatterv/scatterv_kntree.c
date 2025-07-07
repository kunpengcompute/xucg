/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#include "scatterv.h"

#include "planc_stars_algo.h"

#define KNTREE_EID_IDX 0

static ucg_status_t UCG_STARS_ALGO_FUN(scatterv_kntree, get_scounts)(ucg_planc_stars_op_t *op,
                                                                     const ucg_planc_stars_scatterv_config_t *config)
{
    ucg_planc_stars_p2p_params_t params;
    ucg_coll_scatterv_args_t *args = &op->super.super.args.scatterv;
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_rank_t root = args->root;
    ucg_rank_t myrank = vgroup->myrank;
    uint32_t group_size = vgroup->size;
    ucg_algo_kntree_iter_t *iter = &op->scatterv.kntree.kntree_iter;
    ucg_algo_kntree_iter_init(iter, group_size, config->kntree_degree, root, myrank, 1);
    ucg_status_t status;
    ucg_planc_stars_set_p2p_params(op, &params);

    op->scatterv.kntree.sendtype_size = ucg_dt_extent(args->sendtype);
    if (myrank == root) {
        op->scatterv.kntree.sendcounts = (int32_t *)args->sendcounts;
    } else {
        op->scatterv.kntree.sendcounts = ucg_calloc(group_size, sizeof(int32_t),
                                                    "scatterv staging sendcounts");
        if (op->scatterv.kntree.sendcounts == NULL) {
            return UCG_ERR_NO_MEMORY;
        }
    }

    int32_t *sendcounts = op->scatterv.kntree.sendcounts;
    int32_t *sendtype_size = &op->scatterv.kntree.sendtype_size;

    /* Root proc doesn't need to receive scounts array */
    ucg_rank_t parent = ucg_algo_kntree_iter_parent_value(iter);
    if (parent != UCG_INVALID_RANK) {
        ucp_datatype_t ucp_dt = ucp_dt_make_contig(1);
        status = ucg_planc_stars_oob_irecv(parent, sendcounts,
                                           group_size * sizeof(int32_t),
                                           myrank, vgroup, &params, ucp_dt);
        UCG_CHECK_GOTO_ERR(status, err_free_scnts, "p2p irecv sendcounts");
        status = ucg_planc_stars_oob_irecv(parent, sendtype_size,
                                           sizeof(int32_t),
                                           myrank, vgroup, &params, ucp_dt);
        UCG_CHECK_GOTO_ERR(status, err_free_scnts, "p2p irecv sendtype_size");
        status = ucg_planc_stars_oob_waitall(params.state);
        UCG_CHECK_GOTO_ERR(status, err_free_scnts, "p2p wait all");
    }

    ucg_rank_t child;
    while ((child = ucg_algo_kntree_iter_child_value(iter)) != UCG_INVALID_RANK) {
        ucp_datatype_t ucp_dt = ucp_dt_make_contig(1);
        status = ucg_planc_stars_oob_isend(child, sendcounts,
                                           group_size * sizeof(int32_t),
                                           child, vgroup, &params, ucp_dt);
        UCG_CHECK_GOTO_ERR(status, err_free_scnts, "p2p isend sendcounts");
        status = ucg_planc_stars_oob_isend(child, sendtype_size,
                                           sizeof(int32_t),
                                           child, vgroup, &params, ucp_dt);
        UCG_CHECK_GOTO_ERR(status, err_free_scnts, "p2p isend sendtype_size");
        ucg_algo_kntree_iter_child_inc(iter);
    }
    ucg_algo_kntree_iter_reset(iter);
    status = ucg_planc_stars_oob_waitall(params.state);
    UCG_CHECK_GOTO_ERR(status, err_free_scnts, "p2p wait all");

    return status;
err_free_scnts:
    if (myrank != root) {
        ucg_free(sendcounts);
    }
    return status;
}

/* Remove the processes whose recvcount is 0. */
static ucg_status_t UCG_STARS_ALGO_FUN(scatterv_kntree, remove_zero)(ucg_planc_stars_op_t *op, int32_t *map,
                                                                     const ucg_planc_stars_scatterv_config_t *config)
{
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_coll_scatterv_args_t *args = &op->super.super.args.scatterv;
    ucg_rank_t v_myrank = vgroup->myrank;
    ucg_rank_t v_root = args->root;
    uint32_t v_group_size = vgroup->size;
    int32_t cur = 0;

    for (uint32_t i = 0; i < vgroup->size; ++i) {
        if (op->scatterv.kntree.sendcounts[i] != 0 || i == args->root) {
            map[cur++] = i;
            continue;
        }
        if (vgroup->myrank > i) {
            --v_myrank;
        }
        if (args->root > i) {
            --v_root;
        }

        --v_group_size; /* The group_size after procs of recv_cnt==0 are excluded. */
    }

    /* The process whose recv_cnt is 0 exits directly without executing trigger and progress. */
    if (args->recvcount == 0 && vgroup->myrank != args->root) {
        op->scatterv.kntree.is_empty = 1;
        ucg_debug("Rank %d doesn't need to receive data", vgroup->myrank);
        return UCG_OK;
    }
    if (vgroup->myrank == args->root && v_group_size == 1) {
        ucg_debug("Only root %d needs to receive data", args->root);
        op->scatterv.kntree.is_empty = 1;
        return UCG_OK;
    }

    ucg_algo_kntree_iter_t *iter = &op->scatterv.kntree.kntree_iter;
    ucg_algo_kntree_iter_init(iter, v_group_size, config->kntree_degree, v_root, v_myrank, 1);

    op->scatterv.kntree.v_group_size = v_group_size; /* virtual group_size */
    op->scatterv.kntree.v_myrank = v_myrank; /* virtual myrank */

    /* real parent */
    ucg_rank_t parent = ucg_algo_kntree_iter_parent_value(iter);
    op->scatterv.kntree.parent_rank = (parent == UCG_INVALID_RANK) ?
                                          UCG_INVALID_RANK : map[parent];

    uint32_t subsize = ucg_algo_kntree_get_subtree_size(iter, v_myrank);
    op->scatterv.kntree.staging_count = subsize - 1;
    if (op->scatterv.kntree.staging_count == 0) {
        /* leaf procs don't have child */
        return UCG_OK;
    }

    /* immediate child num */
    uint32_t child_num = 0;
    ucg_rank_t child, child_rank[subsize - 1];
    while ((child = ucg_algo_kntree_iter_child_value(iter)) != UCG_INVALID_RANK) {
        child_rank[child_num++] = map[child];
        ucg_algo_kntree_iter_child_inc(iter);
    }
    ucg_algo_kntree_iter_reset(iter);

    op->scatterv.kntree.child_num = child_num;

    /* real child */
    op->scatterv.kntree.child_rank =
        ucg_calloc(child_num, sizeof(ucg_rank_t), "real children rank");
    if (op->scatterv.kntree.child_rank == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    memcpy(op->scatterv.kntree.child_rank, child_rank, child_num * sizeof(ucg_rank_t));

    /* The root proc needs to know the real rank of each process. */
    if (v_myrank == v_root) {
        op->scatterv.kntree.rank_map =
            ucg_calloc(v_group_size, sizeof(int32_t), "tempory rank map");
        if (op->scatterv.kntree.rank_map == NULL) {
            ucg_free(op->scatterv.kntree.child_rank);
            return UCG_ERR_NO_MEMORY;
        }
        memcpy(op->scatterv.kntree.rank_map, map, v_group_size * sizeof(int32_t));
    }
    return UCG_OK;
}

/* alloc and init staging_displs */
static ucg_status_t UCG_STARS_ALGO_FUN(scatterv_kntree, init_staging)(ucg_planc_stars_op_t *op, int32_t *map)
{
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_coll_scatterv_args_t *args = &op->super.super.args.scatterv;
    ucg_rank_t myrank = vgroup->myrank, v_myrank = op->scatterv.kntree.v_myrank;
    ucg_rank_t root = args->root;
    uint32_t v_group_size = op->scatterv.kntree.v_group_size;
    ucg_algo_kntree_iter_t *iter = &op->scatterv.kntree.kntree_iter;
    ucg_algo_kntree_iter_reset(iter);
    uint32_t subsize = ucg_algo_kntree_get_subtree_size(iter, v_myrank);
    uint32_t child_num = op->scatterv.kntree.child_num;
    int32_t child_idx = 0, idx, idx_peer;
    uint32_t child_subsize;
    ucg_rank_t child, peer;

    /* Leaf procs don't need to init staging_displs and staing_area */
    if (op->scatterv.kntree.staging_count == 0) {
        return UCG_OK;
    }

    uint32_t displs_size = (myrank == root) ? subsize : child_num;
    op->scatterv.kntree.staging_displs =
        ucg_calloc(displs_size, sizeof(int32_t), "temporary staging displs");
    if (op->scatterv.kntree.staging_displs == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    memset(op->scatterv.kntree.staging_displs, 0, displs_size * sizeof(int32_t));
    int32_t *staging_displs = op->scatterv.kntree.staging_displs;

    if (myrank == root) {
        /* The staging_displs of root proc stores the offset of each child
         * of root to store its own child's data in the staging_area.
         */
        while ((child = ucg_algo_kntree_iter_child_value(iter)) != UCG_INVALID_RANK) {
            child_subsize = ucg_algo_kntree_get_subtree_size(iter, child);
            for (uint32_t i = 1; i < child_subsize; ++i) {
                idx = (i + child - root + v_group_size) % v_group_size;
                idx_peer = (idx - 1 + v_group_size) % v_group_size;
                staging_displs[idx] = staging_displs[idx_peer] +
                                      args->sendcounts[map[(idx_peer + root) % v_group_size]];
            }
            ucg_algo_kntree_iter_child_inc(iter);
        }
        ucg_algo_kntree_iter_reset(iter);
        return UCG_OK;
    }

    /* Total count of data of non-root procs sent to the child each time. */
    op->scatterv.kntree.child_total_size =
        ucg_calloc(child_num, sizeof(int32_t), "child total size");
    if (op->scatterv.kntree.child_total_size == NULL) {
        return UCG_ERR_NO_MEMORY;
    }

    int32_t buf_size = args->recvcount;
    while ((child = ucg_algo_kntree_iter_child_value(iter)) != UCG_INVALID_RANK) {
        child_subsize = ucg_algo_kntree_get_subtree_size(iter, child);
        for (uint32_t i = 0; i < child_subsize; ++i) {
            peer = map[(i + child) % v_group_size];
            op->scatterv.kntree.child_total_size[child_idx] += op->scatterv.kntree.sendcounts[peer];
        }
        buf_size += op->scatterv.kntree.child_total_size[child_idx++];
        ucg_algo_kntree_iter_child_inc(iter);
    }
    ucg_algo_kntree_iter_reset(iter);

    op->staging_area = ucg_calloc(buf_size, op->scatterv.kntree.sendtype_size, "temporary staging area");
    if (op->staging_area == NULL) {
        return UCG_ERR_NO_MEMORY;
    }

    /* The staging_displs of non-root proc stores the offset of each child
     * in the staging_area.
     */
    staging_displs[child_num - 1] = args->recvcount;
    for (int32_t i = child_num - 2; i >= 0; --i) {
        staging_displs[i] = staging_displs[i + 1] + op->scatterv.kntree.child_total_size[i + 1];
    }

    return UCG_OK;
}

static ucg_status_t UCG_STARS_ALGO_FUN(scatterv_kntree, put_req_root)(ucg_planc_stars_op_t *op,
                                                                      int32_t *child_idx,
                                                                      int32_t idx)
{
    ucg_coll_scatterv_args_t *args = &op->super.super.args.scatterv;
    int32_t *map = op->scatterv.kntree.rank_map;
    ucg_rank_t v_myrank = op->scatterv.kntree.v_myrank;
    uint32_t v_group_size = op->scatterv.kntree.v_group_size;
    ucg_rank_t peer = map[(idx + v_myrank) % v_group_size];

    ucg_algo_kntree_iter_t *iter = &op->scatterv.kntree.kntree_iter;

    /* Leaf procs who don't have child can directly receive data to its recvbuf.
     * Other procs need to receive data to the staging_area.
     */
    ucg_rank_t child = ucg_algo_kntree_iter_child_value(iter);
    if (idx < (child + v_group_size - v_myrank) % v_group_size) {
        ucg_algo_kntree_iter_child_inc(iter);
        child = ucg_algo_kntree_iter_child_value(iter);
        *child_idx += 1;
    }

    /* buffer offset in sendbuf of root */
    int64_t dtsize = ucg_dt_extent(args->sendtype);
    int64_t send_offset = args->displs[peer] * dtsize;
    int64_t length = args->sendcounts[peer] * dtsize;
    int64_t recv_offset = op->scatterv.kntree.staging_displs[idx] * dtsize;

    stars_comm_plan_t *plan = &op->plan;
    stars_rank_info_h peer_rank = &plan->comm_dep.put_ranks[*child_idx];

    scp_ofd_req_elem_h request = ucg_planc_stars_op_get_ofd_req_elem(op);
    UCG_ASSERT_RET(request != NULL, UCG_ERR_NO_MEMORY);

    request->unnotify           = (idx == (child + v_group_size - v_myrank) % v_group_size) ? 0 : 1;
    request->lbuf               = (void *)args->sendbuf + send_offset;
    request->llen               = length;
    request->rbuf               = (void *)peer_rank->rbuf_desc->addr + recv_offset;
    request->lmemh              = plan->lsmemh;
    request->type               = OFFLOAD_PUT;
    request->scp_event          = NULL;

    ucg_status_t status =
        ucg_planc_stars_fill_ofd_put_req_elem(KNTREE_EID_IDX, peer_rank, request);
    UCG_ASSERT_CODE_RET(status);

    ucg_planc_stars_op_push_ofd_req_elem(op, request);
    UCG_ASSERT_CODE_GOTO(status, free);

    return UCG_OK;
free:
    ucg_mpool_put(request);
    return status;
}

static ucg_status_t UCG_STARS_ALGO_FUN(scatterv_kntree, put_req_nonroot)(ucg_planc_stars_op_t *op,
                                                                         ucg_rank_t child,
                                                                         uint32_t child_idx)
{
    stars_comm_plan_t *plan = &op->plan;

    /* Leaf procs who don't have child can directly receive data to its recvbuf.
     * Other procs need to receive data to the staging_area. */
    int64_t dtsize = (int64_t)op->scatterv.kntree.sendtype_size;
    int64_t send_offset = op->scatterv.kntree.staging_displs[child_idx] * dtsize;
    int64_t length = op->scatterv.kntree.child_total_size[child_idx] * dtsize;
    stars_rank_info_h peer_rank = &plan->comm_dep.put_ranks[child_idx];

    scp_ofd_req_elem_h request = ucg_planc_stars_op_get_ofd_req_elem(op);
    UCG_ASSERT_RET(request != NULL, UCG_ERR_NO_MEMORY);

    request->lbuf               = (void *)plan->lrbuf_desc->addr + send_offset;
    request->llen               = length;
    request->rbuf               = (void *)peer_rank->rbuf_desc->addr;
    request->lmemh              = plan->lsmemh;
    request->type               = OFFLOAD_PUT;
    request->scp_event          = NULL;

    ucg_status_t status =
        ucg_planc_stars_fill_ofd_put_req_elem(KNTREE_EID_IDX, peer_rank, request);
    UCG_ASSERT_CODE_RET(status);
    ucg_planc_stars_op_push_ofd_req_elem(op, request);
    UCG_ASSERT_CODE_GOTO(status, free);

    return UCG_OK;
free:
    ucg_mpool_put(request);
    return status;
}

static ucg_status_t UCG_STARS_ALGO_FUN(scatterv_kntree, barrier_req)(ucg_planc_stars_op_t *op,
                                                                     stars_rank_info_h peer_rank)
{
    scp_ofd_req_elem_h request = ucg_planc_stars_op_get_ofd_req_elem(op);
    UCG_ASSERT_RET(request != NULL, UCG_ERR_NO_MEMORY);
    ucg_status_t status = UCG_OK;

    status =
            ucg_planc_stars_fill_ofd_barrier_req_elem(peer_rank, request);
    UCG_ASSERT_CODE_GOTO(status, free);
    ucg_planc_stars_fill_ofd_barrier_req_elem(peer_rank, request);
    ucg_planc_stars_op_push_ofd_req_elem(op, request);
    return UCG_OK;
free:
    ucg_mpool_put(request);
    return status;
}

static ucg_status_t UCG_STARS_ALGO_FUN(scatterv_kntree, wait_req)(ucg_planc_stars_op_t *op,
                                                                  stars_rank_info_h peer_rank)
{
    scp_ofd_req_elem_h request = ucg_planc_stars_op_get_ofd_req_elem(op);
    UCG_ASSERT_RET(request != NULL, UCG_ERR_NO_MEMORY);
    ucg_status_t status = UCG_OK;

    status =
            ucg_planc_stars_fill_ofd_wait_req_elem(KNTREE_EID_IDX, peer_rank, request, op->plan.event_elem);
    UCG_ASSERT_CODE_GOTO(status, free);
    ucg_planc_stars_fill_ofd_wait_req_elem(KNTREE_EID_IDX, peer_rank, request, op->plan.event_elem);
    ucg_planc_stars_op_push_ofd_req_elem(op, request);
    return UCG_OK;
free:
    ucg_mpool_put(request);
    return status;
}

/* Arrange tasks and submit tasks to Stars. */
static ucg_status_t UCG_STARS_ALGO_FUN(scatterv_kntree, submit_stars_op)(ucg_plan_op_t *ucg_op)
{
    ucg_planc_stars_op_t *op = ucg_derived_of(ucg_op, ucg_planc_stars_op_t);
    ucg_coll_scatterv_args_t *args = &op->super.super.args.scatterv;
    ucg_vgroup_t *vgroup = op->super.vgroup;
    stars_comm_plan_t *plan = &op->plan;
    ucg_algo_kntree_iter_t *iter = &op->scatterv.kntree.kntree_iter;
    ucg_rank_t root = args->root;
    ucg_rank_t myrank = vgroup->myrank;
    ucg_status_t status;

    if (myrank != root) {
        /* Root just needs to put data */
        stars_rank_info_h peer_rank = &plan->comm_dep.get_ranks[0];
        status = UCG_STARS_ALGO_FUN(scatterv_kntree, wait_req)(op, peer_rank);
        UCG_CHECK_GOTO(status, out);
        status = UCG_STARS_ALGO_FUN(scatterv_kntree, barrier_req)(op, peer_rank);
        UCG_CHECK_GOTO(status, out);
    }

    /* Put data to child */
    /* 1. Root: one by one */
    int32_t child_idx;
    if (myrank == root) {
        child_idx = 0;
        int32_t put_idx = op->scatterv.kntree.staging_count;
        for (; put_idx > 0; --put_idx) {
            status = UCG_STARS_ALGO_FUN(scatterv_kntree, put_req_root)(op, &child_idx, put_idx);
            UCG_CHECK_GOTO(status, out);
        }
        ucg_algo_kntree_iter_reset(iter);

    /* 2. Non-root: batch */
    } else if (op->scatterv.kntree.staging_count > 0) {
        ucg_rank_t child;
        for (child_idx = 0; child_idx < op->scatterv.kntree.child_num; ++child_idx) {
            child = op->scatterv.kntree.child_rank[child_idx];
            status = UCG_STARS_ALGO_FUN(scatterv_kntree, put_req_nonroot)(op, child, child_idx);
            UCG_CHECK_GOTO(status, out);
        }
    }

    return scp_submit_ofd_req(op->stars_group->context->scp_worker,
                              &op->stars_group->stars_stream,
                              &op->ofd_req, op->stats.cur_elem);

out:
    return status;
}

static ucg_status_t UCG_STARS_ALGO_FUN(scatterv_kntree, init_rbuf)(ucg_planc_stars_op_t *op,
                                                                   ucg_coll_args_t *coll_args)
{
    ucg_coll_scatterv_args_t *args = &coll_args->scatterv;
    ucg_vgroup_t *vgroup = op->super.vgroup;
    stars_comm_plan_t *plan = &op->plan;

    if (args->root == vgroup->myrank) {
        return UCG_OK;
    }

    /* Leaf procs recvive data to args->recvbuf.
     * Non-Leaf procs recvive data to op->staging_area.
     */
    void *lrbuf = (op->scatterv.kntree.staging_count > 0) ? op->staging_area : args->recvbuf;
    int64_t length = op->plan.sbuf_size;
    ucg_status_t status = ucg_planc_stars_rbuf_init(op, lrbuf, length);
    UCG_ASSERT_CODE_RET(status);

    for (uint32_t idx = 0; idx < plan->comm_dep.get_rank_num; ++idx) {
        plan->comm_dep.get_ranks[idx].rbuf_desc = plan->lrbuf_desc;
    }

    return UCG_OK;
}

static ucg_status_t UCG_STARS_ALGO_FUN(scatterv_kntree, init_sbuf)(ucg_planc_stars_op_t *op,
                                                                   ucg_coll_args_t *coll_args)
{
    ucg_coll_scatterv_args_t *args = &coll_args->scatterv;
    ucg_vgroup_t *vgroup = op->super.vgroup;
    stars_comm_plan_t *plan = &op->plan;
    ucg_status_t status = UCG_OK;

    if (op->scatterv.kntree.staging_count == 0) {
        return UCG_OK;
    } else if (args->root != vgroup->myrank) {
        /* send buffer and receive buffer are both op->staging_area */
        plan->lsbuf_desc    = plan->lrbuf_desc;
        plan->lsmemh        = plan->lrmemh;
        return UCG_OK;
    } else {
        status = ucg_planc_stars_sbuf_init(op, (void *)args->sendbuf, plan->sbuf_size);
        UCG_ASSERT_CODE_RET(status);
    }

    return status;
}

static inline size_t UCG_STARS_ALGO_FUN(scatterv_kntree, put_max_size)(ucg_planc_stars_op_t *op,
                                                                       ucg_coll_args_t *coll_args)
{
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_coll_scatterv_args_t *args = &coll_args->scatterv;
    ucg_rank_t myrank = vgroup->myrank;
    int32_t max_count = 0, cur_count = 0;

    if (myrank == args->root) {
        for (uint32_t i = 0; i < vgroup->size; i++) {
            cur_count = args->sendcounts[i] + args->displs[i];
            max_count = (max_count < cur_count) ? cur_count : max_count;
        }
    } else {
        max_count = op->scatterv.kntree.sendcounts[myrank];
        for (uint32_t i = 0; i < op->scatterv.kntree.child_num; ++i) {
            max_count += op->scatterv.kntree.child_total_size[i];
        }
    }

    return max_count * op->scatterv.kntree.sendtype_size;
}

static ucg_status_t UCG_STARS_ALGO_FUN(scatterv_kntree, offload_plan)(ucg_planc_stars_op_t *op)
{
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_rank_t root = op->super.super.args.scatterv.root;
    ucg_rank_t myrank = vgroup->myrank;

    stars_comm_dep_h comm_dep = &op->plan.comm_dep;
    comm_dep->get_ranks = NULL;
    comm_dep->get_rank_num = (myrank == root) ? 0 : 1;
    comm_dep->put_ranks = NULL;
    comm_dep->put_rank_num = op->scatterv.kntree.child_num;

    ucg_debug("my real rank %d, my virtual rank %d, get_rank_num %d, put_rank_num %d",
        myrank, op->scatterv.kntree.v_myrank, comm_dep->get_rank_num, comm_dep->put_rank_num);

    ucg_status_t status = ucg_planc_stars_rank_dep_alloc(comm_dep);
    UCG_ASSERT_CODE_RET(status);

    /* Create ep and store them in get_ranks[idx]->ep and put_ranks[idx]->ep. */
    ucg_rank_t peer_rank; // child rank (put) or parent rank (get)
    ucg_planc_stars_context_t *context = op->stars_group->context;

    if (myrank != root) {
        peer_rank = op->scatterv.kntree.parent_rank;
        comm_dep->get_ranks[0].peer_id = peer_rank;
        comm_dep->get_ranks[0].barrier_flag = 1;
        status = ucg_planc_stars_rank_dep_init(op, &comm_dep->get_ranks[0], peer_rank, 1);
        UCG_ASSERT_CODE_GOTO(status, err_free_memory);
    }

    for (uint32_t idx = 0; idx < comm_dep->put_rank_num; ++idx) {
        peer_rank = op->scatterv.kntree.child_rank[idx];
        comm_dep->put_ranks[idx].peer_id = peer_rank;
        status = ucg_planc_stars_rank_dep_init(op, &comm_dep->put_ranks[idx], peer_rank, 1);
        UCG_ASSERT_CODE_GOTO(status, err_free_memory);
    }
    return UCG_OK;

err_free_memory:
    if (comm_dep->get_ranks) {
        ucg_free(comm_dep->get_ranks);
        comm_dep->get_ranks = NULL;
    }
    if (comm_dep->put_ranks) {
        ucg_free(comm_dep->put_ranks);
        comm_dep->put_ranks = NULL;
    }
    return status;
}

static inline ucg_status_t UCG_STARS_ALGO_FUN(scatterv_kntree, check)(ucg_vgroup_t *vgroup,
                                                                      const ucg_coll_args_t *coll_args)
{
    return UCG_OK;
}

static ucg_status_t UCG_STARS_ALGO_FUN(scatterv_kntree, init)(ucg_plan_op_t *ucg_op)
{
    ucg_status_t status;

    ucg_planc_stars_op_t *op = ucg_derived_of(ucg_op, ucg_planc_stars_op_t);
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_planc_stars_group_t *stars_group = ucg_derived_of(vgroup, ucg_planc_stars_group_t);
    ucg_planc_stars_scatterv_config_t *config =
            UCG_PLANC_STARS_CONTEXT_BUILTIN_CONFIG_BUNDLE(stars_group->context, scatterv,
                                                          UCG_COLL_TYPE_SCATTERV);
    op->scatterv.kntree.run_mode = !config->run_hpl;
    ucg_debug("Set scatterv kntree running mode : %s, degree : %d",
              op->scatterv.kntree.run_mode ? "COMMON" : "HPL", config->kntree_degree);

    op->staging_area                        = NULL;
    op->scatterv.kntree.staging_count       = 0;
    op->scatterv.kntree.sendcounts          = NULL;
    op->scatterv.kntree.sendtype_size       = 0;
    op->scatterv.kntree.staging_displs      = NULL;
    op->scatterv.kntree.v_group_size        = 0;
    op->scatterv.kntree.v_myrank            = UCG_INVALID_RANK;
    op->scatterv.kntree.parent_rank         = UCG_INVALID_RANK;
    op->scatterv.kntree.child_num           = 0;
    op->scatterv.kntree.child_rank          = NULL;
    op->scatterv.kntree.child_total_size    = NULL;
    op->scatterv.kntree.rank_map            = NULL;
    op->scatterv.kntree.is_empty            = 0;
    // Avoid cleaning up rkey_bundle.
    op->plan.comm_dep.put_rank_num          = 0;

    int32_t map[vgroup->size];
    if (op->scatterv.kntree.run_mode == COMMON_MODE) {
        status = UCG_STARS_ALGO_FUN(scatterv_kntree, get_scounts)(op, config);
        UCG_ASSERT_CODE_GOTO(status, err_destruct_op);
    } else {
        ucg_coll_scatterv_args_t *args = &ucg_op->super.args.scatterv;
        op->scatterv.kntree.sendcounts = (int32_t *)args->sendcounts;
        op->scatterv.kntree.sendtype_size = (int32_t)ucg_dt_extent(args->recvtype);
    }

    status = UCG_STARS_ALGO_FUN(scatterv_kntree, remove_zero)(op, map, config);
    UCG_ASSERT_CODE_GOTO(status, err_destruct_op);

    /* The process whose recv_cnt is 0 does not perform any operation. */
    UCG_MATCH_RET(op->scatterv.kntree.is_empty == 1, UCG_OK);

    status = UCG_STARS_ALGO_FUN(scatterv_kntree, init_staging)(op, map);
    UCG_ASSERT_CODE_GOTO(status, err_destruct_op);

    ucg_coll_args_t *coll_args = &ucg_op->super.args;
    status = ucg_planc_stars_algo_prepare_plan(op, coll_args,
                                               UCG_STARS_ALGO_FUN(scatterv_kntree, put_max_size),
                                               UCG_STARS_ALGO_FUN(scatterv_kntree, offload_plan));
    UCG_ASSERT_CODE_GOTO(status, out);

    status = ucg_planc_stars_algo_exch_buf_addr(op, coll_args,
                                                UCG_STARS_ALGO_FUN(scatterv_kntree, init_rbuf),
                                                UCG_STARS_ALGO_FUN(scatterv_kntree, init_sbuf));
    UCG_ASSERT_CODE_GOTO(status, out);

    op->stats.nb_type = (coll_args->type == UCG_COLL_TYPE_ISCATTERV) ? 0 : 1;
    return UCG_OK;

err_destruct_op:
    UCG_CLASS_DESTRUCT(ucg_plan_op_t, &op->super);
out:
    op->super.super.status = status;
    return status;
}

static ucg_status_t UCG_STARS_ALGO_FUN(scatterv_kntree, trigger)(ucg_plan_op_t *ucg_op)
{
    UCG_STATS_GET_TIME(start_tick);
    ucg_planc_stars_op_t *op = ucg_derived_of(ucg_op, ucg_planc_stars_op_t);
    ucg_planc_stars_op_reset(op);
    op->super.super.status = UCG_ERR_IO_ERROR;

    ucg_status_t status = ucg_planc_stars_plan_get_stats_elem(op);
    UCG_ASSERT_CODE_RET(status);

    if (op->scatterv.kntree.is_empty == 1) {
        ucg_vgroup_t *vgroup = ucg_op->vgroup;
        ucg_coll_scatterv_args_t *args = &ucg_op->super.args.scatterv;
        if (vgroup->myrank == args->root && args->recvcount > 0 &&
            args->recvbuf != UCG_IN_PLACE) {
            int64_t offset = args->displs[vgroup->myrank] * ucg_dt_extent(args->sendtype);
            status = ucg_dt_memcpy(args->recvbuf, args->recvcount, args->recvtype,
                                   args->sendbuf + offset, args->sendcounts[vgroup->myrank], args->sendtype);
        }
        op->super.super.status = UCG_OK;
        return UCG_OK;
    }

    ucg_algo_kntree_iter_reset(&op->scatterv.kntree.kntree_iter);
    UCG_STATS_GET_TIME(submit_tick);
    status = UCG_STARS_ALGO_FUN(scatterv_kntree, submit_stars_op)(&op->super);
    UCG_ASSERT_CODE_RET(status);

    op->super.super.status = UCG_INPROGRESS;
        UCG_STATS_GET_TIME(end_tick);
    UCG_STATS_SET_TIME(op->stats.cur_elem->start_tick, start_tick);
    UCG_STATS_COST_TIME(submit_tick, end_tick,
                        op->stats.cur_elem->trigger.submit.total);
    UCG_STATS_COST_TIME(start_tick, end_tick,
                        op->stats.cur_elem->trigger.total);
    return UCG_OK;
}

static ucg_status_t UCG_STARS_ALGO_FUN(scatterv_kntree, progress_done)(ucg_planc_stars_op_t *op)
{
    ucg_status_t status = UCG_OK;
    ucg_coll_scatterv_args_t *args = &op->super.super.args.scatterv;
    if (args->recvbuf == UCG_IN_PLACE || args->recvcount == 0) {
        return status;
    }

    ucg_rank_t myrank = op->super.vgroup->myrank;
    if (myrank == args->root) {
        int64_t offset = (int64_t)args->displs[myrank] * op->scatterv.kntree.sendtype_size;
        status = ucg_dt_memcpy(args->recvbuf, args->recvcount, args->recvtype,
                               args->sendbuf + offset, args->sendcounts[myrank], args->sendtype);
    } else if (op->scatterv.kntree.staging_count > 0) {
        status = ucg_dt_memcpy(args->recvbuf, args->recvcount, args->recvtype,
                               op->staging_area, args->recvcount, args->recvtype);
    }
    return status;
}

inline static ucg_status_t UCG_STARS_ALGO_FUN(scatterv_kntree, progress)(ucg_plan_op_t *ucg_op)
{
    return ucg_planc_stars_algo_progress(ucg_op, &UCG_STARS_ALGO_FUN(scatterv_kntree, progress_done));
}

static ucg_status_t UCG_STARS_ALGO_FUN(scatterv_kntree, discard)(ucg_plan_op_t *ucg_op)
{
    UCG_STATS_GET_TIME(start_tick);
    ucg_planc_stars_op_t *op = ucg_derived_of(ucg_op, ucg_planc_stars_op_t);
    ucg_vgroup_t *vgroup = op->super.vgroup;
    ucg_coll_scatterv_args_t *args = &op->super.super.args.scatterv;

    UCG_STATS_GET_TIME(buf_cleanup);
    if (op->scatterv.kntree.is_empty == 0) {
        ucg_planc_stars_buf_cleanup(op);
    }

    if (op->staging_area != NULL) {
        ucg_free(op->staging_area);
    }
    if (op->scatterv.kntree.staging_displs != NULL) {
        ucg_free(op->scatterv.kntree.staging_displs);
    }
    if (op->scatterv.kntree.rank_map != NULL) {
        ucg_free(op->scatterv.kntree.rank_map);
    }
    if (op->scatterv.kntree.child_total_size != NULL) {
        ucg_free(op->scatterv.kntree.child_total_size);
    }
    if (op->scatterv.kntree.child_rank != NULL) {
        ucg_free(op->scatterv.kntree.child_rank);
    }
    if (op->scatterv.kntree.run_mode == COMMON_MODE &&
        vgroup->myrank != args->root &&
        op->scatterv.kntree.sendcounts != NULL) {
        ucg_free(op->scatterv.kntree.sendcounts);
    }

    UCG_STATS_GET_TIME(op_cleanup);
    ucg_planc_stars_op_discard(ucg_op);
    UCG_STATS_GET_TIME(op_destruct);
    UCG_CLASS_DESTRUCT(ucg_plan_op_t, ucg_op);
    UCG_STATS_GET_TIME(mpool_put);
    UCG_STATS_GET_TIME(end_tick);

    UCG_STATS_COST_TIME(buf_cleanup, op_cleanup,
                        op->stats.discard.buf_cleanup.total);
    UCG_STATS_COST_TIME(op_cleanup, op_destruct,
                        op->stats.discard.op_cleanup);
    UCG_STATS_COST_TIME(op_destruct, mpool_put,
                        op->stats.discard.op_destruct);
    UCG_STATS_COST_TIME(mpool_put, end_tick,
                        op->stats.discard.mpool_put);
    UCG_STATS_COST_TIME(start_tick, end_tick, op->stats.discard.total);
    UCG_STATS_SET_TIME(op->stats.end_tick, end_tick);
    UCG_STATS_COST_TIME(op->stats.start_tick, end_tick, op->stats.cost_time);

    ucg_stats_dump("scatterv_kntree", &op->stats);

    return UCG_OK;
}

UCG_STARS_ALGO_PRE_DEFINE(scatterv_kntree)
