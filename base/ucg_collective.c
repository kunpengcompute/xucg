/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#include "ucg_collective.h"
#include "ucg_group.h"
#include "ucg_context.h"
#include <ucs/stats/stats.h>
#include <ucp/core/ucp_worker.h>
#include <ucs/datastruct/queue.h>
#include <ucs/datastruct/list.h>
#include <ucs/profile/profile.h>
#include <ucs/debug/memtrack.h>

static void ucg_log_coll_params(ucg_collective_params_t *params)
{
    ucs_debug("ucg_collective_create OP: "
              "params={type=%u, root=%lu, send=[%p,%i,%lu,%p,%p], "
              "recv=[%p,%i,%lu,%p,%p], cb=%p, op=%p}",
              (unsigned)params->type.modifiers, (uint64_t)params->type.root,
              params->send.buf, params->send.count, params->send.dt_len,
              params->send.dt_ext, params->send.displs,
              params->recv.buf, params->recv.count, params->recv.dt_len,
              params->recv.dt_ext, params->recv.displs,
              params->comp_cb, params->recv.op_ext);
}

static UCS_F_ALWAYS_INLINE ucs_status_t ucg_collective_trigger(ucg_group_h group, ucg_op_t *op, ucg_request_t **req)
{
    /* Barrier effect - all new collectives are pending */
    if (ucs_unlikely(op->params.type.modifiers & UCG_GROUP_COLLECTIVE_MODIFIER_BARRIER)) {
        ucs_assert(group->is_barrier_outstanding == 0);
        group->is_barrier_outstanding = 1;
    }

    /* Start the first step of the collective operation */
    ucs_status_t ret;
    UCS_PROFILE_CODE("ucg_trigger") {
        ret = op->trigger(op, group->next_id++, req);
    }

    if (ret != UCS_INPROGRESS) {
        UCS_STATS_UPDATE_COUNTER(group->stats, UCG_GROUP_STAT_OPS_IMMEDIATE, 1);
    }

    return ret;
}

static UCS_F_ALWAYS_INLINE ucs_status_t ucg_collective_start(ucg_coll_h coll, ucg_request_t **req)
{
    ucs_status_t ret;
    ucg_op_t *op = (ucg_op_t*)coll;
    ucg_group_h group = op->plan->group;

    /* Since group was created - don't need UCP_CONTEXT_CHECK_FEATURE_FLAGS */
    UCP_WORKER_THREAD_CS_ENTER_CONDITIONAL(group->worker);

    ucs_trace_req("ucg_collective_start: op=%p req=%p", coll, *req);

    if (ucs_unlikely(group->is_barrier_outstanding)) {
        ucs_list_del(&op->list);
        ucs_queue_push(&group->pending, &op->queue);
        op->pending_req = req;
        ret = UCS_INPROGRESS;
    } else {
        ret = ucg_collective_trigger(group, op, req);
    }

    UCS_STATS_UPDATE_COUNTER(group->stats, UCG_GROUP_STAT_OPS_USED, 1);
    UCP_WORKER_THREAD_CS_EXIT_CONDITIONAL(group->worker);
    return ret;
}

UCS_PROFILE_FUNC(ucs_status_t, ucg_collective_create,
        (group, params, coll), ucg_group_h group,
        ucg_collective_params_t *params, ucg_coll_h *coll)
{
    UCP_WORKER_THREAD_CS_ENTER_CONDITIONAL(group->worker);
    ucs_status_t status;
    if (group == NULL || params == NULL || coll == NULL || params->send.count < 0) {
        status = UCS_ERR_INVALID_PARAM;
        goto out;
    }

    ucg_group_member_index_t root = UCG_ROOT_RANK(params);
    if (root >= group->params.member_count) {
        status = UCS_ERR_INVALID_PARAM;
        ucs_error("Invalid root[%ld] for communication group size[%d]", root, group->params.member_count);
        goto out;
    }

    ucg_op_t *op = NULL;
    ucg_plan_t *plan = ucg_group_get_cache_plan(group, params);
    if (ucs_likely(plan != NULL)) {
        ucs_list_for_each(op, &plan->op_head, list) {
            if (!memcmp(&op->params, params, sizeof(*params))) {
                status = UCS_OK;
                goto op_found;
            }
        }

        UCS_STATS_UPDATE_COUNTER(group->stats, UCG_GROUP_STAT_PLANS_USED, 1);
        goto plan_found;
    }

    /* select which plan to use for this collective operation */
    ucg_planner_h planner = NULL;
    ucg_planner_ctx_h planner_ctx;
    status = ucg_group_select_planner(group, NULL, params, &planner, &planner_ctx);
    if (status != UCS_OK) {
        ucs_error("Failed to select planner");
        goto out;
    }

    /* create the actual plan for the collective operation */
    UCS_PROFILE_CODE("ucg_plan") {
        ucs_trace_req("ucg_collective_create PLAN: planner=%s type=%x root=%lu",
                      planner->name, params->type.modifiers, (uint64_t)params->type.root);
        status = planner->plan(planner_ctx, params, &plan);
    }
    if (status != UCS_OK) {
        goto out;
    }

    plan->planner           = planner;
    plan->group             = group;
    plan->type              = params->type;
    plan->group_id          = group->params.group_id;
    plan->am_mp             = &group->worker->am_mp;
    ucg_group_update_cache_plan(group, params, plan);
    ucs_list_head_init(&plan->op_head);
    UCS_STATS_UPDATE_COUNTER(group->stats, UCG_GROUP_STAT_PLANS_CREATED, 1);

plan_found:
    UCS_STATS_UPDATE_COUNTER(group->stats, UCG_GROUP_STAT_OPS_CREATED, 1);
    UCS_PROFILE_CODE("ucg_prepare") {
        status = plan->prepare(plan, params, &op);
    }
    if (status != UCS_OK) {
        goto out;
    }

    ucs_list_add_head(&plan->op_head, &op->list);
    memcpy(&op->params, params, sizeof(*params));
    op->plan = plan;

op_found:
    *coll = op;
    ucg_log_coll_params(params);

out: 
    UCP_WORKER_THREAD_CS_EXIT_CONDITIONAL(group->worker);
    return status;
}

UCS_PROFILE_FUNC(ucs_status_ptr_t, ucg_collective_start_nb,
                 (coll), ucg_coll_h coll)
{
    if (coll == NULL) {
        return UCS_STATUS_PTR(UCS_ERR_INVALID_PARAM);
    }
    ucs_debug("ucg_collective_start_nb %p", coll);
    ucg_request_t *req = NULL;
    ucs_status_ptr_t ret = UCS_STATUS_PTR(ucg_collective_start(coll, &req));
    return UCS_PTR_IS_ERR(ret) ? ret : req;
}

UCS_PROFILE_FUNC(ucs_status_t, ucg_collective_start_nbr,
                 (coll, request), ucg_coll_h coll, void *request)
{
    if (coll == NULL || request == NULL) {
        return UCS_ERR_INVALID_PARAM;
    }
    ucs_debug("ucg_collective_start_nbr %p", coll);
    return ucg_collective_start(coll, (ucg_request_t**)&request);
}

void ucg_collective_destroy(ucg_coll_h coll)
{
    if (coll == NULL) {
        return;
    }
    ucs_info("ucg_collective_destroy %p", coll);
    ucg_op_t *op = (ucg_op_t*)coll;
    op->discard(op);
    return;
}

ucs_status_t ucg_request_check_status(void *request)
{
    /* Depends on the implementation of op->trigger() 
     * Currently, in builtin
     * struct ucg_builtin_request_t {
     *     ucg_request_t super;
     *     ...; <-- request points here
     * }
     * so "request - 1" is the right address.
     * TODO: Extract specific check_status() from request, 
     *       then call check_status(request)
     */
    ucg_request_t *req = (ucg_request_t*)request - 1;

    if (req->flags & UCG_REQUEST_COMMON_FLAG_COMPLETED) {
        ucs_assert(req->status != UCS_INPROGRESS);
        return req->status;
    }
    return UCS_INPROGRESS;
}

void ucg_request_cancel(ucg_group_h group, void *request) 
{
    return;
}

void ucg_request_free(void *request)
{
    /* Depends on the implementation of op->trigger()
     * Currently, builtin uses the reuseable slot->req, 
     * so this function does nothing.
     * TODO: Extract specific free_request() from request, 
     *       then call free_request(request)
     */
    return;
}

ucs_status_t ucg_collective_release_barrier(ucg_group_h group)
{
    if (group->is_barrier_outstanding == 0) {
        // current operation is not barrier.
        return UCS_OK;
    }
    group->is_barrier_outstanding = 0;
    if (ucs_queue_is_empty(&group->pending)) {
        return UCS_OK;
    }

    ucs_status_t ret;
    do {
        /* Move the operation from the pending queue back to the original one */
        ucg_op_t *op = (ucg_op_t*)ucs_queue_pull_non_empty(&group->pending);
        ucg_request_t **req = op->pending_req;
        ucs_list_add_head(&op->plan->op_head, &op->list);

        /* Start this next pending operation */
        ret = ucg_collective_trigger(group, op, req);
    } while ((!ucs_queue_is_empty(&group->pending)) &&
             (!group->is_barrier_outstanding) &&
             (ret == UCS_OK));

    return ret;
}