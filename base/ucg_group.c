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
#include <ucg/api/ucg_mpi.h>

#include <ucp/core/ucp_worker.h> /* for ucp_worker_add_resource_ifaces */
#include <ucp/wireup/address.h> /* for ucp_address_pack/unpack */

#define UCG_GROUP_PARAM_REQUIRED_MASK (UCG_GROUP_PARAM_FIELD_MEMBER_COUNT |\
                                       UCG_GROUP_PARAM_FIELD_MEMBER_INDEX)

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
    new_group->cache_size = 0;
    memset(new_group->cache_by_modifiers, 0, sizeof(new_group->cache_by_modifiers));
    memset(new_group->cache_nonzero_root, 0, sizeof(new_group->cache_nonzero_root));
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

unsigned ucg_group_count_ppx(const ucg_group_params_t *group_params,
                             enum ucg_group_member_distance domain_distance,
                             unsigned *ppn)
{
    enum ucg_group_member_distance distance;
    ucg_group_member_index_t index, count = 0;

    if (ppn != NULL) {
        *ppn = 1;
    }

    switch (group_params->distance_type) {
    case UCG_GROUP_DISTANCE_TYPE_FIXED:
        distance = group_params->distance_value;
        return (distance > domain_distance) ? 1 :
                ucs_max(group_params->member_count, 1);

    case UCG_GROUP_DISTANCE_TYPE_ARRAY:
        for (index = 0; index < group_params->member_count; index++) {
            distance = group_params->distance_array[index];
            ucs_assert(distance <= UCG_GROUP_MEMBER_DISTANCE_UNKNOWN);
            if (distance <= domain_distance) {
                count++;
            }
            if ((distance <= UCG_GROUP_MEMBER_DISTANCE_HOST) && (ppn != NULL)) {
                (*ppn)++; // TODO: fix support for "non-full-nodes" allocation
            }
        }
        return count;

    case UCG_GROUP_DISTANCE_TYPE_TABLE:
    case UCG_GROUP_DISTANCE_TYPE_PLACEMENT:
        return 1;
    }

    return count;
}

#if HAVE_UCP_EXTENSIONS
static ucs_status_t
ucg_group_wireup_coll_iface_create(ucg_group_h group,
                                   uct_incast_cb_t incast_cb,
                                   unsigned *iface_id_base_p,
                                   ucp_tl_bitmap_t *coll_tl_bitmap_p)
{
    uct_iface_params_t params = {
        .field_mask = UCT_IFACE_PARAM_FIELD_COLL_INFO,
        .global_info = {
            .proc_cnt = group->params.member_count,
            .proc_idx = group->params.member_index
        }
    };

    if (incast_cb) {
        params.field_mask |= UCT_IFACE_PARAM_FIELD_INCAST_CB;
        params.incast_cb   = incast_cb;
    }

    params.host_info.proc_idx = group->params.member_index;
    params.host_info.proc_cnt = ucg_group_count_ppx(&group->params,
            UCG_GROUP_MEMBER_DISTANCE_HOST, NULL);

    return ucp_worker_add_resource_ifaces(group->worker, &params,
                                          iface_id_base_p, coll_tl_bitmap_p);
}

static void ucg_group_compreq_set1(void *req, ucs_status_t status)
{
    *(int*)req = 1;
}

static ucs_status_t
ucg_group_wireup_coll_iface_bcast_addresses(ucg_group_h group,
                                            void *addrs,
                                            size_t addr_len,
                                            ucg_group_member_index_t root)
{
    int is_done = 0;
    ucg_op_t *first_bcast = NULL;

    ucs_status_t status = ucg_coll_bcast_init(addrs, addrs, addr_len, NULL,
                                              NULL, 0, 0, group,
                                              (void**)&first_bcast);
    if (status != UCS_OK) {
        return status;
    }

    first_bcast->compreq_f = ucg_group_compreq_set1;

    status = ucg_collective_start(first_bcast, &is_done);
    if (!UCS_STATUS_IS_ERR(status)) {
        while (!is_done) {
            ucp_worker_progress(group->worker);
        }

        status = UCS_OK;
    }

    ucg_collective_destroy(first_bcast);

    /* To prevent this P2P plan from being re-used (instead of the one we can
     * now create with the help of these addresses) we must ignore this plan */
    ucg_plan_t **plan =
      &group->cache_by_modifiers[ucg_predefined_modifiers[UCG_PRIMITIVE_BCAST]];
    ucs_assert(first_bcast->plan == *plan);
    ucs_assert(first_bcast->plan->next_cb == NULL);
    *plan = NULL;

    return status;
}

ucs_status_t ucg_group_wireup_coll_ifaces(ucg_group_h group,
                                          ucg_group_member_index_t root,
                                          uct_incast_cb_t incast_cb,
                                          ucp_ep_h *ep_p)
{
    void *addrs;
    size_t addr_len;
    ucs_status_t status;
    unsigned iface_id_base;
    ucp_tl_bitmap_t coll_tl_bitmap;
    ucp_unpacked_address_t unpacked;

    ucp_worker_h worker = group->worker;
    unsigned pack_flags = UCP_ADDRESS_PACK_FLAG_DEVICE_ADDR |
                          UCP_ADDRESS_PACK_FLAG_IFACE_ADDR;

#if ENABLE_DEBUG_DATA
    pack_flags |= UCP_ADDRESS_PACK_FLAG_WORKER_NAME;
#endif

    /* Create collective interfaces for the new communicator */
    UCS_ASYNC_BLOCK(&worker->async);
    status = ucg_group_wireup_coll_iface_create(group, incast_cb,
                                                &iface_id_base,
                                                &coll_tl_bitmap);
    UCS_ASYNC_UNBLOCK(&worker->async);
    if (status != UCS_OK) {
        return status;
    }

    if (UCS_BITMAP_IS_ZERO_INPLACE(&coll_tl_bitmap)) {
        return UCS_ERR_UNREACHABLE;
    }

    /* Root prepares the address (the rest will be overwritten...) */
    status = ucp_address_pack(worker, NULL, &coll_tl_bitmap, iface_id_base,
                              pack_flags, NULL, &addr_len, &addrs);
    if (status != UCS_OK) {
        // TODO: cleanup
        return status;
    }

    /* Because address lengths aren't equal among nodes - round up and pray. */
    addr_len = ucs_roundup_pow2(addr_len); // TODO: find a way to get the same.

    /* Broadcast the address */
    status = ucg_group_wireup_coll_iface_bcast_addresses(group, addrs,
                                                         addr_len, root);
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
    UCS_ASYNC_BLOCK(&worker->async);
    status = ucp_ep_create_to_worker_addr(worker, &coll_tl_bitmap, iface_id_base,
                                          &unpacked, 0, "collective ep", ep_p);
    UCS_ASYNC_UNBLOCK(&worker->async);

    ucs_free(addrs);

    return status;
}
#else
ucs_status_t ucg_group_wireup_coll_ifaces(ucg_group_h group,
                                          ucg_group_member_index_t root,
                                          ucp_ep_h *ep_p)
{
    return UCS_OK;
}
#endif

static void ucg_group_copy_params(ucg_group_params_t *dst,
                                  const ucg_group_params_t *src,
                                  void *distance, size_t distance_size)
{
    size_t group_params_size = sizeof(src->field_mask) +
                               ucs_offsetof(ucg_params_t, field_mask);

    if (src->field_mask != 0) {
        enum ucg_group_params_field msb_flag = UCS_BIT((sizeof(uint64_t) * 8)
                - 1 - ucs_count_leading_zero_bits(src->field_mask));
        ucs_assert((msb_flag & src->field_mask) == msb_flag);

        switch (msb_flag) {
        case UCG_GROUP_PARAM_FIELD_ID:
            group_params_size = ucs_offsetof(ucg_group_params_t, member_count);
            break;

        case UCG_GROUP_PARAM_FIELD_MEMBER_COUNT:
            group_params_size = ucs_offsetof(ucg_group_params_t, member_index);
            break;

        case UCG_GROUP_PARAM_FIELD_MEMBER_INDEX:
            group_params_size = ucs_offsetof(ucg_group_params_t, cb_context);
            break;

        case UCG_GROUP_PARAM_FIELD_CB_CONTEXT:
            group_params_size = ucs_offsetof(ucg_group_params_t, distance_type);
            break;

        case UCG_GROUP_PARAM_FIELD_DISTANCES:
            group_params_size = ucs_offsetof(ucg_group_params_t, name);
            break;

        case UCG_GROUP_PARAM_FIELD_NAME:
            group_params_size = sizeof(ucg_params_t);
            break;
        }
    }

    memcpy(dst, src, group_params_size);

    if (dst->field_mask & UCG_GROUP_PARAM_FIELD_DISTANCES) {
        dst->distance_array = distance;
        if (src->distance_type != UCG_GROUP_DISTANCE_TYPE_FIXED) {
            memcpy(distance, src->distance_array, distance_size);
        }
    } else {
        /* If the user didn't specify the distances - treat as uniform */
        dst->field_mask    |= UCG_GROUP_PARAM_FIELD_DISTANCES;
        dst->distance_type  = UCG_GROUP_DISTANCE_TYPE_FIXED;
        dst->distance_value = UCG_GROUP_MEMBER_DISTANCE_UNKNOWN;
    }
}

ucs_status_t ucg_group_create(ucp_worker_h worker,
                              const ucg_group_params_t *params,
                              ucg_group_h *group_p)
{
    ucs_status_t status;
    struct ucg_group *group;
    size_t dist_size, total_size;

    ucg_context_t *ctx = ucs_container_of(worker->context, ucg_context_t, ucp_ctx);

    if (!ucs_test_all_flags(params->field_mask, UCG_GROUP_PARAM_REQUIRED_MASK)) {
        ucs_error("UCG is missing some critical group parameters");
        return UCS_ERR_INVALID_PARAM;
    }

    /* Allocate a new group */
    if (params->field_mask & UCG_GROUP_PARAM_FIELD_DISTANCES) {
        switch (params->distance_type) {
        case UCG_GROUP_DISTANCE_TYPE_FIXED:
            dist_size = 0;
            break;

        case UCG_GROUP_DISTANCE_TYPE_ARRAY:
            dist_size = params->member_count;
            break;

        case UCG_GROUP_DISTANCE_TYPE_TABLE:
            dist_size = params->member_count * params->member_count;
            break;

        case UCG_GROUP_DISTANCE_TYPE_PLACEMENT:
            dist_size = params->member_count * UCG_GROUP_MEMBER_DISTANCE_UNKNOWN;
            break;

        default:
            return UCS_ERR_INVALID_PARAM;
        }
    } else {
        dist_size = 0;
    }

    dist_size *= sizeof(enum ucg_group_member_distance);
    total_size = ctx->per_group_planners_ctx + dist_size;
    group      = UCS_ALLOC_CHECK(total_size, "communicator group");

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
    ucg_group_copy_params(&group->params, params,
                          UCS_PTR_BYTE_OFFSET(group, total_size - dist_size),
                          dist_size);

    if (params->field_mask & UCG_GROUP_PARAM_FIELD_ID) {
        ctx->next_group_id = ucs_max(ctx->next_group_id, group->params.id);
    } else {
        group->params.field_mask |= UCG_GROUP_PARAM_FIELD_ID;
        group->params.id          = ++ctx->next_group_id;
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

    ucg_init_group_cache(group);
    ucg_init_group_root_used(group);
    ucs_list_add_tail(&ctx->groups_head, &group->list);

    *group_p = group;
    return UCS_OK;

cleanup_stats:
    UCS_STATS_NODE_FREE(group->stats);
cleanup_group:
    ucs_free(group);
    return status;
}

ucs_status_t ucg_group_query(ucg_group_h group,
                             ucg_group_attr_t *attr)
{
    if (attr->field_mask & UCG_GROUP_ATTR_FIELD_NAME) {
        ucs_strncpy_safe(attr->name, group->name, UCG_GROUP_NAME_MAX);
    }

    if (attr->field_mask & UCG_GROUP_ATTR_FIELD_ID) {
        attr->id = group->params.id;
    }

    if (attr->field_mask & UCG_GROUP_ATTR_FIELD_MEMBER_COUNT) {
        attr->member_count = group->params.member_count;
    }

    if (attr->field_mask & UCG_GROUP_ATTR_FIELD_MEMBER_INDEX) {
        attr->member_index = group->params.member_index;
    }

    return UCS_OK;
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

    UCS_STATS_NODE_FREE(group->stats);

    ucg_plan_group_destroy(group);

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

ucs_status_t ucg_request_cancel(ucg_coll_h coll)
{
    // TODO: implement
    return UCS_ERR_UNSUPPORTED;
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
                        ucg_plan_t ***cache_plan)
{
    ucg_group_member_index_t root = params->send.type.root;

    ucg_plan_t **plan_p = &group->cache_nonzero_root[message_size_level][coll_root];
    ucg_plan_t *plan = *plan_p;
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

    /* TODO: fix problem - only works for builtin...
    ucg_builtin_config_t *config = (ucg_builtin_config_t *)plan->planner->component->config;
    if (params->send.dtype > config->large_datatype_threshold && !plan->support_large_datatype) {
        *cache_plan = NULL;
        return;
    }*/

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
    *cache_plan = plan_p;
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
    int is_match;
    ucg_op_t *op;
    ucg_plan_t *plan;
    ucg_plan_t **plan_p;
    unsigned coll_root;
    unsigned coll_mask;
    unsigned message_size_level;
    ucp_datatype_t send_dt;
    ucs_status_t status;

    /* Sanity check */
    if (ucs_unlikely((group == NULL) || (params == NULL) || (coll == NULL))) {
        return UCS_ERR_INVALID_PARAM;
    }

    ucg_group_member_index_t root = UCG_ROOT_RANK(params);
    if (ucs_likely(root == 0)) {
        uint16_t modifiers = UCG_PARAM_TYPE(params).modifiers;
        coll_mask          = modifiers & UCG_GROUP_CACHE_MODIFIER_MASK;
        plan_p             = &group->cache_by_modifiers[coll_mask];
    } else {
        /* find the plan of current root whether has been established */
        // TODO: unsigned is_coll_root_found = 1;

        status = ucg_builtin_convert_datatype(params->send.dtype, &send_dt);
        if (ucs_unlikely(status != UCS_OK)) {
            return status;
        }
        unsigned msg_size = ucp_dt_length(send_dt, params->send.count, NULL, NULL);

        if (root >= group->params.member_count) {
            ucs_error("Invalid root[%u] for communication group size[%u]", root, group->params.member_count);
            return UCS_ERR_INVALID_PARAM;
        }

        /* root cache has been not found */
        if (root != group->root_used[root % UCG_GROUP_MAX_ROOT_PARAM]) {
            group->root_used[root % UCG_GROUP_MAX_ROOT_PARAM] = root;
            // TODO: is_coll_root_found = 0;
        }

        ucg_collective_create_choose_algorithm(msg_size, &message_size_level);

        coll_root = root % UCG_GROUP_MAX_ROOT_PARAM;
        ucg_get_cache_plan(message_size_level, coll_root, group, params, &plan_p);
    }

    /* */
    plan = *plan_p;
    if (ucs_likely(plan != NULL) &&
        ucs_unlikely(plan->incast_cb != UCG_PLAN_INCAST_UNUSED)) {
        uct_incast_cb_t want_incast_cb;

        status = ucg_builtin_convert_datatype(params->send.dtype, &send_dt);
        if (ucs_unlikely(status != UCS_OK)) {
            return status;
        }

        status = ucg_plan_choose_incast_cb(params,
                                           ucp_dt_length(send_dt, 1, NULL, NULL),
                                           params->send.count,
                                           &want_incast_cb);
        if (ucs_unlikely(status != UCS_OK)) {
            return status;
        }

        while ((plan) && (plan->incast_cb != want_incast_cb)) {
            plan_p = &plan->next_cb;
            plan   = *plan_p;
        }
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
            if (is_match) { // TODO: && ucg_builtin_op_can_reuse(plan, op, params)) {
                ucs_list_del(&op->list);
                UCG_GROUP_THREAD_CS_EXIT(plan);
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
        status = ucg_group_plan(group, params, plan_p);
        if (status != UCS_OK) {
            goto out;
        }

        plan = *plan_p;

        UCG_GROUP_THREAD_CS_ENTER(plan);

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
