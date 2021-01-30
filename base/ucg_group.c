/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#include "ucg_plan.h"
#include "ucg_group.h"
#include "ucg_context.h"

#include <ucs/datastruct/queue.h>
#include <ucs/datastruct/list.h>
#include <ucs/profile/profile.h>
#include <ucs/debug/memtrack.h>
#include <ucp/core/ucp_worker.h>
#include <ucp/wireup/address.h>
#include <ucg/api/ucg_mpi.h>


#define UCG_GROUP_PARAM_REQUIRED_MASK (UCG_GROUP_PARAM_FIELD_MEMBER_COUNT |\
                                       UCG_GROUP_PARAM_FIELD_MEMBER_INDEX |\
                                       UCG_GROUP_PARAM_FIELD_CB_CONTEXT)

#if ENABLE_STATS
/**
 * UCG group statistics counters
 */
enum {
    UCG_GROUP_STAT_PLANS_CREATED,
    UCG_GROUP_STAT_PLANS_USED,

    UCG_GROUP_STAT_OPS_CREATED,
    UCG_GROUP_STAT_OPS_USED,
    UCG_GROUP_STAT_OPS_IMMEDIATE,

    UCG_GROUP_STAT_LAST
};

static ucs_stats_class_t ucg_group_stats_class = {
    .name           = "ucg_group",
    .num_counters   = UCG_GROUP_STAT_LAST,
    .counter_names  = {
        [UCG_GROUP_STAT_PLANS_CREATED] = "plans_created",
        [UCG_GROUP_STAT_PLANS_USED]    = "plans_reused",
        [UCG_GROUP_STAT_OPS_CREATED]   = "ops_created",
        [UCG_GROUP_STAT_OPS_USED]      = "ops_started",
        [UCG_GROUP_STAT_OPS_IMMEDIATE] = "ops_immediate"
    }
};
#endif /* ENABLE_STATS */

#if ENABLE_MT
#define UCG_GROUP_THREAD_CS_ENTER(_obj) ucs_recursive_spin_lock(&(_obj)->lock);
#define UCG_GROUP_THREAD_CS_EXIT(_obj)  ucs_recursive_spin_unlock(&(_obj)->lock);
#else
#define UCG_GROUP_THREAD_CS_ENTER(_obj)
#define UCG_GROUP_THREAD_CS_EXIT(_obj)
#endif

void ucg_init_group_cache(struct ucg_group *new_group)
{
    unsigned idx, rank, algo_idx;
    for (algo_idx = 0; algo_idx <  UCG_GROUP_MSG_SIZE_LEVEL; algo_idx++) {
        for (rank = 0; rank < UCG_GROUP_MAX_ROOT_PARAM; rank++) {
            for (idx = 0; idx < UCG_GROUP_MAX_COLL_TYPE_BUCKETS; idx++) {
                new_group->cache[algo_idx][rank][idx] = NULL;
            }
        }
     }
}

void ucg_init_group_root_used(struct ucg_group *new_group)
{
    unsigned rank;
    /* Initalization of root_used */
    for (rank = 0; rank < UCG_GROUP_MAX_ROOT_PARAM; rank++) {
        new_group->root_used[rank] = (unsigned) -1;
    }
}

static inline ucs_status_t ucg_group_plan(ucg_group_h group,
                                          const ucg_collective_params_t *params,
                                          ucg_plan_t **plan_p)
{
    ucg_plan_t *plan;
    ucs_status_t status;
    ucg_group_ctx_h gctx;
    ucg_plan_desc_t *planner;

    UCS_PROFILE_CODE("ucg_choose") {
        status = ucg_plan_choose(params, group, &planner, &gctx);
    }
    if (ucs_unlikely(status != UCS_OK)) {
        return status;
    }

    UCS_PROFILE_CODE("ucg_plan") {
        status = planner->component->plan(gctx, params, &plan);
    }
    if (ucs_unlikely(status != UCS_OK)) {
        return status;
    }

#if ENABLE_MT
    status = ucs_recursive_spinlock_init(&plan->lock, 0);
    if (ucs_unlikely(status != UCS_OK)) {
        return status;
    }
#endif

    if (group->params.field_mask & UCG_GROUP_PARAM_FIELD_MEMBER_INDEX) {
        plan->my_index = group->params.member_index;
    }
    if (group->params.field_mask & UCG_GROUP_PARAM_FIELD_MEMBER_COUNT) {
        plan->group_size = group->params.member_count;
    }

    ucs_list_head_init(&plan->op_head);

    plan->group_id = group->params.id;
    plan->planner  = planner;
    plan->group    = group;
    *plan_p        = plan;

    return UCS_OK;
}

static ucs_status_t
ucg_group_wireup_coll_iface_calc(enum ucg_group_member_distance *distance,
                                 ucg_group_member_index_t member_count,
                                 uint32_t *proc_idx, uint32_t *proc_cnt)
{
    ucs_status_t status = UCS_ERR_INVALID_PARAM;
    ucg_group_member_index_t idx, cnt = 0;

    for (idx = 0; idx < member_count; idx++) {
        if (distance[idx] == UCG_GROUP_MEMBER_DISTANCE_NONE) {
            *proc_idx = (uint32_t)idx;
            status    = UCS_OK;
        } else if (distance[idx] <= UCG_GROUP_MEMBER_DISTANCE_HOST) {
            (*proc_cnt)++;
        }
    }

    *proc_cnt = (uint32_t)cnt;

    return status;
}

static ucs_status_t ucg_group_wireup_coll_iface_create(ucg_group_h group,
                                                       int is_bcast)
{
    ucs_status_t status;

    ucp_rsc_index_t tl_id = is_bcast ? group->context->bcast_id :
                                       group->context->incast_id;

    ucp_worker_iface_t **wiface = is_bcast ? &group->bcast_iface :
                                             &group->incast_iface;

    uct_iface_params_t iface_params = {
            .field_mask           = UCT_IFACE_PARAM_FIELD_OPEN_MODE |
                                    UCT_IFACE_PARAM_FIELD_DEVICE |
                                    UCT_IFACE_PARAM_FIELD_COLL_INFO,

            .open_mode            = UCT_IFACE_OPEN_MODE_DEVICE,

            .mode = {
                .device = {
                        .tl_name  = NULL, // TODO: resource->tl_rsc.tl_name,
                        .dev_name = NULL  // TODO: resource->tl_rsc.dev_name
                }
            },

            .global_info = {
                    .proc_cnt = group->params.member_count,
                    .proc_idx = group->params.member_index
            }
    };

    // TODO: the interface will contain all the members - should be optimized...

    ucs_assert(group->params.field_mask & UCG_GROUP_PARAM_FIELD_DISTANCES);
    status = ucg_group_wireup_coll_iface_calc(group->params.distance,
                                              group->params.member_count,
                                              &iface_params.host_info.proc_cnt,
                                              &iface_params.host_info.proc_cnt);
    if (status != UCS_OK) {
        return status;
    }

    status = ucp_worker_iface_open(group->worker, tl_id, &iface_params, wiface);
    if (status != UCS_OK) {
        return status;
    }

    return ucp_worker_iface_init(group->worker, tl_id, *wiface);
}

static ucs_status_t ucg_group_wireup_coll_iface_bcast_addresses(ucg_group_h group,
                                                                void *addrs,
                                                                size_t addr_len)
{
    int is_done;
    ucg_coll_h first_bcast;

    ucs_status_t status = ucg_coll_bcast_init(addrs, addrs, addr_len, NULL,
                                              NULL, 0, 0, group, &first_bcast);
    if (status != UCS_OK) {
        return status;
    }

    status = ucg_collective_start(first_bcast, &is_done);

    while (!is_done) {
        ucp_worker_progress(group->worker);
    }

    ucg_collective_destroy(first_bcast);

    return status;
}

static ucs_status_t ucg_group_wireup_coll_ifaces(ucg_group_h group)
{
    void *addrs;
    size_t addr_len;
    ucs_status_t status;
    ucp_unpacked_address_t unpacked;

    ucp_worker_h worker = group->worker;
    uint64_t tl_bitmap  = group->context->bcast_id |
                          group->context->incast_id;
    unsigned pack_flags = UCP_ADDRESS_PACK_FLAG_DEVICE_ADDR |
                          UCP_ADDRESS_PACK_FLAG_IFACE_ADDR;
    unsigned init_flags = UCP_EP_INIT_CREATE_AM_LANE;

    /* Create a broadcast interface for the new communicator */
    status = ucg_group_wireup_coll_iface_create(group, 1);
    if (status != UCS_OK) {
        return status;
    }

    /* Create an incast interface for the new communicator */
    status = ucg_group_wireup_coll_iface_create(group, 0);
    if (status != UCS_OK) {
        // TODO: cleanup
        return status;
    }

    /* Root prepares the address (the rest will be overwritten...) */
    status = ucp_address_pack(worker, NULL, tl_bitmap, pack_flags,
                              NULL, &addr_len, &addrs);
    if (status != UCS_OK) {
        // TODO: cleanup
        return status;
    }

    /* Broadcast the address */
    status = ucg_group_wireup_coll_iface_bcast_addresses(group, addrs, addr_len);
    if (status != UCS_OK) {
        // TODO: cleanup
        return status;
    }

    status = ucp_address_unpack(worker, addrs, pack_flags, &unpacked);
    if (status != UCS_OK) {
        // TODO: cleanup
        return status;
    }

    /* Connect to the address (for root - loopback) */
    ucp_ep_create_to_worker_addr(worker, tl_bitmap, &unpacked, init_flags,
                                 "for incast/bcast", &group->root_ep);
    if (status != UCS_OK) {
        // TODO: cleanup
        return status;
    }

    ucs_free(addrs);

    return UCS_OK;
}

ucs_status_t ucg_group_create(ucp_worker_h worker,
                              const ucg_group_params_t *params,
                              ucg_group_h *group_p)
{
    ucs_status_t status;
    struct ucg_group *group;
    ucg_context_t *ctx = ucs_container_of(worker->context, ucg_context_t, ucp_ctx);

    if (!ucs_test_all_flags(params->field_mask, UCG_GROUP_PARAM_REQUIRED_MASK)) {
        ucs_error("UCG is missing some critical group parameters");
        return UCS_ERR_INVALID_PARAM;
    }

    /* Allocate a new group */
    size_t dist_size  = sizeof(*params->distance) * params->member_count;
    size_t total_size = ctx->per_group_planners_ctx + dist_size;
    group             = UCS_ALLOC_CHECK(total_size, "communicator group");

    /* Fill in the group fields */
    group->is_barrier_outstanding = 0;
    group->is_cache_cleanup_due   = 0;
    group->context                = ctx;
    group->worker                 = worker;
    group->next_coll_id           = 1;

#if ENABLE_MT
    ucs_recursive_spinlock_init(&group->lock, 0);
#endif
    ucs_queue_head_init(&group->pending);
    memcpy((ucg_group_params_t*)&group->params, params, sizeof(*params));
    // TODO: replace memcpy with per-field copy to improve ABI compatibility
    group->params.distance = UCS_PTR_BYTE_OFFSET(group, total_size - dist_size);

    if (params->field_mask & UCG_GROUP_PARAM_FIELD_DISTANCES) {
        memcpy(group->params.distance, params->distance, dist_size);
    } else {
        /* If the user didn't specify the distances - treat as uniform */
        memset(group->params.distance, UCG_GROUP_MEMBER_DISTANCE_UNKNOWN, dist_size);
    }

    if ((params->field_mask & UCG_GROUP_PARAM_FIELD_ID) != 0) {
        ctx->next_group_id = ucs_max(ctx->next_group_id, group->params.id);
    } else {
        group->params.id = ++ctx->next_group_id;
        group->params.field_mask |= UCG_GROUP_PARAM_FIELD_ID;
    }

    status = UCS_STATS_NODE_ALLOC(&group->stats,
                                  &ucg_group_stats_class,
                                  worker->stats, "-%p", group);
    if (status != UCS_OK) {
        goto cleanup_group;
    }

    /* Initialize the planners (loadable modules) */
    status = ucg_plan_group_create(group);
    if (status != UCS_OK) {
        goto cleanup_stats;
    }

    /* Clear the cache */
    group->cache_size = 0;
    memset(group->cache, 0, sizeof(group->cache));

    ucs_list_add_tail(&ctx->groups_head, &group->list);

    if (group->params.member_count >= ctx->config.coll_iface_member_thresh) {
        status = ucg_group_wireup_coll_ifaces(group);
        if (status != UCS_OK) {
            ucg_group_destroy(group);
            return status;
        }
    }

    ucg_init_group_cache(group);
    ucg_init_group_root_used(group);

    *group_p = group;
    return UCS_OK;

cleanup_stats:
    UCS_STATS_NODE_FREE(group->stats);
cleanup_group:
    ucs_free(group);
    return status;
}

const ucg_group_params_t* ucg_group_get_params(ucg_group_h group)
{
    return &group->params;
}

static ucs_status_t ucg_group_cache_cleanup(ucg_group_h group)
{
    // TODO: implement... otherwise cache might grow too big!
    // TODO: use "op->discard_f(op);"
    return UCS_OK;
}

void ucg_group_destroy(ucg_group_h group)
{
    /* First - make sure all the collectives are completed */
    while (!ucs_queue_is_empty(&group->pending)) {
        ucp_worker_progress(group->worker);
    }

    UCG_GROUP_THREAD_CS_ENTER(group)

    ucg_plan_group_destroy(group);
    UCS_STATS_NODE_FREE(group->stats);
    ucs_list_del(&group->list);

    UCG_GROUP_THREAD_CS_EXIT(group)

#if ENABLE_MT
    ucs_recursive_spinlock_destroy(&group->lock);
#endif

    ucs_free(group);
}

ucg_collective_progress_t ucg_request_get_progress(ucg_coll_h coll)
{
    return ((ucg_op_t*)coll)->plan->planner->component->progress;
}

void ucg_request_cancel(ucg_coll_h coll)
{
    // TODO: implement
}

static int ucg_chk_noncontig_allreduce_plan(const ucg_collective_params_t *coll_params,
                                            const ucg_group_params_t *group_params,
                                            const ucg_plan_t *plan)
{
    int noncontig_allreduce;

    if (coll_params->send.type.modifiers != ucg_predefined_modifiers[UCG_PRIMITIVE_ALLREDUCE]) {
        return 0;
    }

    noncontig_allreduce = ucg_is_noncontig_allreduce(group_params, coll_params);
    if (plan->is_noncontig_allreduce) {
        return !noncontig_allreduce;
    } else {
        return noncontig_allreduce;
    }
}

void ucg_get_cache_plan(unsigned int message_size_level, unsigned int coll_root,
                        ucg_group_h group, const ucg_collective_params_t *params,
                        ucg_plan_t **cache_plan)
{
    ucg_group_member_index_t root = params->send.type.root;

    ucg_plan_t *plan = group->cache[message_size_level][coll_root][params->send.type.plan_cache_index];
    if (plan == NULL) {
        *cache_plan = NULL;
        return;
    }

    if (params->recv.op && !ucg_global_params.reduce_op.is_commutative_f(params->recv.op) && !plan->support_non_commutative) {
        *cache_plan = NULL;
        return;
    }

    if (params->recv.op && !ucg_global_params.reduce_op.is_commutative_f(params->recv.op) && params->send.count > 1
        && plan->is_ring_plan_topo_type) {
        *cache_plan = NULL;
        return;
    }

    /* TODO: (alex) need to find a good way to keep the config private, but still filter by message size... maybe new component API?
    ucg_builtin_config_t *config = (ucg_builtin_config_t *)plan->planner->config; // TODO: only available in (builtin-)bctx->config
    if (params->send.dtype > config->large_datatype_threshold && !plan->support_large_datatype) {
        *cache_plan = NULL;
        return;
    }
    */

    if (ucg_chk_noncontig_allreduce_plan(params, &group->params, plan)) {
        *cache_plan = NULL;
        return;
    }

    if (plan->is_ring_plan_topo_type && ucg_is_segmented_allreduce(params)) {
        *cache_plan = NULL;
        return;
    }

    if (plan != NULL && root != plan->type.root) {
        *cache_plan = NULL;
        return;
    }

    ucs_debug("select plan from cache: %p", plan);
    *cache_plan = plan;
}

void ucg_update_group_cache(ucg_group_h group,
                            unsigned int message_size_level,
                            unsigned int coll_root,
                            const ucg_collective_params_t *params,
                            ucg_plan_t *plan)
{
    if (group->cache[message_size_level][coll_root][params->send.type.plan_cache_index] != NULL) {
        // TODO: (alex) need to avoid memory leak without using "buildin" specifically
        // ucg_builtin_plan_t *builtin_plan = ucs_derived_of(group->cache[message_size_level][coll_root][params->plan_cache_index], ucg_builtin_plan_t);
        // (void)ucg_builtin_destroy_plan(builtin_plan, group);
        group->cache[message_size_level][coll_root][params->send.type.plan_cache_index] = NULL;
    }
    group->cache[message_size_level][coll_root][params->send.type.plan_cache_index] = plan;
}

void ucg_collective_create_choose_algorithm(unsigned msg_size, unsigned *message_size_level)
{
    /* choose algorithm due to message size */
    if (msg_size < UCG_GROUP_MED_MSG_SIZE) {
        *message_size_level = 0;
    } else {
        *message_size_level = 1;
    }
}

// TODO: (alex) remove this later...
#include <ucp/dt/dt.inl>
extern ucs_status_t ucg_builtin_convert_datatype(void *param_datatype, ucp_datatype_t *ucp_datatype);

UCS_PROFILE_FUNC(ucs_status_t, ucg_collective_create,
        (group, params, coll), ucg_group_h group,
        const ucg_collective_params_t *params, ucg_coll_h *coll)
{
    /* check the recycling/cache for this collective */
    int is_match;
    ucg_op_t *op = NULL;
    ucs_status_t status;
    if (group == NULL || params == NULL || coll == NULL || params->send.count < 0) {
        status = UCS_ERR_INVALID_PARAM;
        goto out;
    }

    /* find the plan of current root whether has been established */
    ucg_group_member_index_t root = UCG_ROOT_RANK(params);
    unsigned coll_root;
    ucp_datatype_t send_dt;
    unsigned message_size_level;
    unsigned is_coll_root_found = 1;

    status = ucg_builtin_convert_datatype(params->send.dtype, &send_dt);
    if (ucs_unlikely(status != UCS_OK)) {
        return status;
    }
    unsigned msg_size = ucp_dt_length(send_dt, params->send.count, NULL, NULL);

    if (root >= group->params.member_count) {
        status = UCS_ERR_INVALID_PARAM;
        ucs_error("Invalid root[%u] for communication group size[%u]", root, group->params.member_count);
        goto out;
    }

    /* root cache has been not found */
    if (root != group->root_used[root % UCG_GROUP_MAX_ROOT_PARAM]) {
        group->root_used[root % UCG_GROUP_MAX_ROOT_PARAM] = root;
        is_coll_root_found = 0;
    }
    coll_root = root % UCG_GROUP_MAX_ROOT_PARAM;

    ucg_collective_create_choose_algorithm(msg_size, &message_size_level);

    ucg_plan_t *plan = NULL;
    if (is_coll_root_found) {
        ucg_get_cache_plan(message_size_level, coll_root, group, params, &plan);
    }

    /* check the recycling/cache for this collective */
    if (ucs_likely(plan != NULL)) {
        UCG_GROUP_THREAD_CS_ENTER(plan)

        ucs_list_for_each(op, &plan->op_head, list) {
            /* we only need to compare the first 64 bytes of each set of cached
             * parameters against the given one (checked during compile time) */
            UCS_STATIC_ASSERT(sizeof(ucg_collective_params_t) ==
                              UCS_SYS_CACHE_LINE_SIZE);

#ifdef HAVE_UCP_EXTENSIONS
#ifdef __AVX512F__
            /* Only apply AVX512 to broadcast - otherwise risk CPU down-clocking! */
            if (plan->my_index == 0) {
#else
            if (1) {
#endif
                is_match = ucs_cpu_cache_line_is_equal(params, &op->params);
            } else
#endif
            is_match = (memcmp(params, &op->params, UCS_SYS_CACHE_LINE_SIZE) == 0);
            if (is_match && !plan->is_noncontig_allreduce) {
                ucs_list_del(&op->list);
                UCG_GROUP_THREAD_CS_EXIT(plan);

                // TODO: (alex) move to some other place - only run if needed
                ucg_builtin_update_op(plan, op, params);

                status = UCS_OK;
                goto op_found;
            }
        }

        UCS_STATS_UPDATE_COUNTER(group->stats, UCG_GROUP_STAT_PLANS_USED, 1);
    } else {
        ucs_trace_req("ucg_collective_create PLAN: type=%x root=%"PRIx64,
                      (unsigned)UCG_PARAM_TYPE(params).modifiers,
                      (uint64_t)UCG_PARAM_TYPE(params).root);

        /* create the actual plan for the collective operation */
        status = ucg_group_plan(group, params, &plan);
        if (status != UCS_OK) {
            goto out;
        }

        UCG_GROUP_THREAD_CS_ENTER(plan);

        ucg_update_group_cache(group, msg_size, coll_root, params, plan);

        UCS_STATS_UPDATE_COUNTER(group->stats, UCG_GROUP_STAT_PLANS_CREATED, 1);
    }

    UCS_STATS_UPDATE_COUNTER(group->stats, UCG_GROUP_STAT_OPS_CREATED, 1);

    UCS_PROFILE_CODE("ucg_prepare") {
        status = plan->planner->component->prepare(plan, params, &op);
    }

    UCG_GROUP_THREAD_CS_EXIT(plan);

    if (status != UCS_OK) {
        goto out;
    }

    ucs_trace_req("ucg_collective_create OP: planner=%s(%s) "
                  "params={type=%u, root=%lu, send=[%p,%lu,%p], "
                  "recv=[%p,%lu,%p], op/displs=%p}",
                  plan->planner->name, plan->planner->component->name,
                  (uint16_t)UCG_PARAM_TYPE(params).modifiers,
                  (uint64_t)UCG_PARAM_TYPE(params).root,
                  params->send.buffer, params->send.count, params->send.dtype,
                  params->recv.buffer, params->recv.count, params->recv.dtype,
                  UCG_PARAM_OP(params));

    if (ucs_unlikely(++group->cache_size >
                     group->context->config.group_cache_size_thresh)) {
        group->is_cache_cleanup_due = 1;
    }

op_found:
    *coll = op;

out:
    return status;
}

ucs_status_t static UCS_F_ALWAYS_INLINE
ucg_collective_trigger(ucg_group_h group, ucg_op_t *op, void *req)
{
    ucs_status_t ret;

    /* Start the first step of the collective operation */
    UCS_PROFILE_CODE("ucg_trigger") {
        ret = op->trigger_f(op, group->next_coll_id++, req);
    }

    if (ret != UCS_INPROGRESS) {
        UCS_STATS_UPDATE_COUNTER(group->stats, UCG_GROUP_STAT_OPS_IMMEDIATE, 1);
    }

    return ret;
}

ucs_status_t ucg_collective_acquire_barrier(ucg_group_h group)
{
    ucs_assert(group->is_barrier_outstanding == 0);
    group->is_barrier_outstanding = 1;
    return UCS_OK;
}

ucs_status_t ucg_collective_release_barrier(ucg_group_h group)
{
    ucs_status_t status;

    ucs_assert(group->is_barrier_outstanding == 1);
    group->is_barrier_outstanding = 0;

    UCG_GROUP_THREAD_CS_ENTER(group)

    if (!ucs_queue_is_empty(&group->pending)) {
        do {
            /* Start the next pending operation */
            ucg_op_t *op = (ucg_op_t*)ucs_queue_pull_non_empty(&group->pending);
            status = ucg_collective_trigger(group, op, op->pending_req);
        } while ((!ucs_queue_is_empty(&group->pending)) &&
                 (!group->is_barrier_outstanding) &&
                 (status == UCS_OK));
    } else {
        status = UCS_OK;
    }

    UCG_GROUP_THREAD_CS_EXIT(group)

    return status;
}

UCS_PROFILE_FUNC(ucs_status_t, ucg_collective_start, (coll, req),
                 ucg_coll_h coll, void *req)
{
    ucs_status_t ret;
    ucg_op_t *op = (ucg_op_t*)coll;
    ucg_group_h group = op->plan->group;

    ucs_trace_req("ucg_collective_start: op=%p req=%p", coll, req);

    UCG_GROUP_THREAD_CS_ENTER(group)

    if (ucs_unlikely(group->is_barrier_outstanding)) {
        ucs_queue_push(&group->pending, &op->queue);
        op->pending_req = req;
        ret = UCS_INPROGRESS;
    } else {
        ret = ucg_collective_trigger(group, op, req);
    }

    if (ucs_unlikely(group->is_cache_cleanup_due) && !UCS_STATUS_IS_ERR(ret)) {
        ucg_group_cache_cleanup(group);
    }

    UCS_STATS_UPDATE_COUNTER(group->stats, UCG_GROUP_STAT_OPS_USED, 1);

    UCG_GROUP_THREAD_CS_EXIT(group)

    return ret;
}

void ucg_collective_destroy(ucg_coll_h coll)
{
    ucg_op_t *op     = (ucg_op_t*)coll;
    ucg_plan_t *plan = op->plan;

    UCG_GROUP_THREAD_CS_ENTER(plan);

    ucs_list_add_head(&plan->op_head, &op->list);

    UCG_GROUP_THREAD_CS_EXIT(plan);
}
