/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019-2020.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#include "ucg_group.h"
#include "ucg_context.h"
#include <ucg/api/ucg_mpi.h>

#include <ucp/core/ucp_worker.h>
#include <ucs/datastruct/queue.h>
#include <ucs/datastruct/list.h>
#include <ucs/profile/profile.h>
#include <ucs/debug/memtrack.h>
#include <ucs/stats/stats.h>
#include <ucp/core/ucp_ep.inl>
#include <ucp/core/ucp_proxy_ep.h> /* for @ref ucp_proxy_ep_test */

#if ENABLE_STATS
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
#endif

__KHASH_IMPL(ucg_groups_ep, static UCS_F_MAYBE_UNUSED inline,
             ucg_group_member_index_t, ucp_ep_h, 1, kh_int64_hash_func,
             kh_int64_hash_equal);

static ucs_status_t ucg_group_progress_add(uct_iface_h iface, ucg_group_h group)
{
    if (group->iface_cnt == UCG_GROUP_MAX_IFACES) {
        ucs_fatal("iface count(%u) reaches the limit(%d)", group->iface_cnt,
            UCG_GROUP_MAX_IFACES);
        return UCS_ERR_EXCEEDS_LIMIT;
    }

    unsigned i;
    for (i = 0; i < group->iface_cnt; ++i) {
        if (group->ifaces[i] == iface) {
            return UCS_OK;
        }
    }
    group->ifaces[group->iface_cnt] = iface;
    ++group->iface_cnt;
    return UCS_OK;
}

static void ucg_group_calc_cache_index(ucg_collective_params_t *params,
                                       unsigned *level,
                                       unsigned *coll_root)
{
    unsigned msg_size = params->send.count * params->send.dt_len;
    if (msg_size < UCG_GROUP_MED_MSG_SIZE) {
        *level = 0;
    } else {
        *level = 1;
    }

    ucg_group_member_index_t root = UCG_ROOT_RANK(params);
    *coll_root = root % UCG_GROUP_MAX_ROOT_PARAM;

    return;
}

static void* ucg_dup_one_dim_array(const void* array,
                                   int member_size,
                                   int member_count,
                                   const char *msg)
{
    int malloc_size = member_size * member_count;
    void* dup_array = ucs_malloc(malloc_size, msg);
    if (dup_array == NULL) {
        return NULL;
    }
    memcpy(dup_array, array, malloc_size);
    return dup_array;
}

static enum ucg_group_member_distance* ucg_group_dup_distance(enum ucg_group_member_distance *distance,
                                                              int member_count)
{
    return ucg_dup_one_dim_array(distance, sizeof(enum ucg_group_member_distance),
                                 member_count, "distance");
}

static uint16_t* ucg_group_dup_node_index(uint16_t *node_index,
                                              int member_count)
{
    return ucg_dup_one_dim_array(node_index, sizeof(uint16_t), member_count, "node index");
}

static ucs_status_t ucg_group_apply_params(ucg_group_h group_p,
                                           const ucg_group_params_t *params)
{
    ucs_status_t status = UCS_ERR_INVALID_PARAM;
    uint64_t field_mask = params->field_mask;
    group_p->params.field_mask = field_mask;

    UCG_CHECK_REQUIRED_FIELD(field_mask, UCG_GROUP_PARAM_FIELD_UCP_WORKER, err);
    group_p->params.ucp_worker = params->ucp_worker;

    UCG_CHECK_REQUIRED_FIELD(field_mask, UCG_GROUP_PARAM_FIELD_ID, err);
    group_p->params.group_id = params->group_id;

    UCG_CHECK_REQUIRED_FIELD(field_mask, UCG_GROUP_PARAM_FIELD_MEMBER_COUNT, err);
    group_p->params.member_count = params->member_count;

    UCG_CHECK_REQUIRED_FIELD(field_mask, UCG_GROUP_PARAM_FIELD_DISTANCE, err_free_topo_map);
    group_p->params.distance = ucg_group_dup_distance(params->distance, params->member_count);
    if (group_p->params.distance == NULL) {
        status = UCS_ERR_NO_MEMORY;
        goto err_free_topo_map;
    }

    UCG_CHECK_REQUIRED_FIELD(field_mask, UCG_GROUP_PARAM_FIELD_NODE_INDEX, err_free_distance);
    group_p->params.node_index = ucg_group_dup_node_index(params->node_index, params->member_count);
    if (group_p->params.node_index == NULL) {
        status = UCS_ERR_NO_MEMORY;
        goto err_free_distance;
    }

    UCG_CHECK_REQUIRED_FIELD(field_mask, UCG_GROUP_PARAM_FIELD_BIND_TO_NONE, err_free_node_index);
    group_p->params.is_bind_to_none   = params->is_bind_to_none;

    UCG_CHECK_REQUIRED_FIELD(field_mask, UCG_GROUP_PARAM_FIELD_CB_GROUP_IBJ, err_free_node_index);
    group_p->params.cb_group_obj      = params->cb_group_obj;

    UCG_CHECK_REQUIRED_FIELD(field_mask, UCG_GROUP_PARAM_FIELD_IS_SOCKET_BALANCE, err_free_node_index);
    group_p->params.is_socket_balance = params->is_socket_balance;

    return UCS_OK;
err_free_node_index:
    ucs_free(group_p->params.node_index);
    group_p->params.node_index = NULL;
err_free_distance:
    ucs_free(group_p->params.distance);
    group_p->params.distance = NULL;
err_free_topo_map:
err:
    return status;
}

static int ucg_chk_noncontig_allreduce_plan(const ucg_collective_params_t *coll_params,
                                            const ucg_params_t *params,
                                            const ucg_plan_t *plan)
{
    int noncontig_allreduce;

    if (coll_params->type.modifiers != ucg_predefined_modifiers[UCG_PRIMITIVE_ALLREDUCE]) {
        return 0;
    }

    noncontig_allreduce = ucg_is_noncontig_allreduce(params, coll_params);
    if (plan->is_noncontig_allreduce) {
        return !noncontig_allreduce;
    } else {
        return noncontig_allreduce;
    }
}

static void ucg_group_release_params(ucg_group_h group_p)
{
    ucg_group_params_t *params = &group_p->params;

    if (params->distance != NULL) {
        ucs_free(params->distance);
        params->distance = NULL;
    }

    if (params->node_index != NULL) {
        ucs_free(params->node_index);
        params->node_index = NULL;
    }

    return;
}

static void ucg_group_destroy_planner_context(ucg_group_h group)
{
    ucs_list_link_t *planner_head = &group->context->planners_head;
    ucg_planner_ctx_h *planner_ctx = group->planners_context;
    ucg_planner_h planner;
    ucs_list_for_each(planner, planner_head, list) {
        if (planner_ctx == NULL) {
            break;
        }
        planner->destroy(*planner_ctx++);
    }

    ucs_free(group->planners_context);
    group->planners_context = NULL;
    group->num_planners_context = 0;
    return;
}

static ucs_status_t ucg_group_create_planner_context(ucg_group_h group)
{
    ucg_context_h context = group->context;
    int malloc_size = ucs_list_length(&context->planners_head) * sizeof(ucg_planner_ctx_h);
    ucg_planner_ctx_h *planner_ctx = ucs_calloc(1, malloc_size, "planner context");
    if (planner_ctx == NULL) {
        return UCS_ERR_NO_MEMORY;
    }
    group->planners_context = planner_ctx;
    group->num_planners_context = 0;

    ucs_status_t status;
    int i = 0;
    ucg_planner_h planner;
    ucs_list_for_each(planner, &context->planners_head, list) {
        status = planner->create(group, group->context->planc_ctx[i], planner_ctx);
        if (status != UCS_OK) {
            goto err_destroy_planner_ctx;
        }
        ++planner_ctx;
        ++group->num_planners_context;
        ++i;
    }
    return UCS_OK;
err_destroy_planner_ctx:
    ucg_group_destroy_planner_context(group);
    return status;
}

static void ucg_init_group_root_used(ucg_group_h group)
{
    int i;
    for (i = 0; i < UCG_GROUP_MAX_ROOT_PARAM; ++i) {
        group->root_used[i] = (unsigned)-1;
    }
}

/* BEGIN: public interface for ucg user */
ucs_status_t ucg_group_create(ucg_context_h context,
                              const ucg_group_params_t *params,
                              ucg_group_h *group_p)
{
    ucg_group_h group = ucs_calloc(1, sizeof(ucg_group_t), "ucg group");
    if (group == NULL) {
        return UCS_ERR_NO_MEMORY;
    }

    group->context = context;
    ucs_status_t status = ucg_group_apply_params(group, params);
    if (status != UCS_OK) {
        goto err_free_group;
    }

    /* set shortcut before other operations */
    group->address.lookup_f  = group->context->params.address.lookup_f;
    group->address.release_f = group->context->params.address.release_f;
    group->worker            = group->params.ucp_worker;

    status = ucg_group_create_planner_context(group);
    if (status != UCS_OK) {
        goto err_release_group_params;
    }

    status = UCS_STATS_NODE_ALLOC(&group->stats,
                                  &ucg_group_stats_class,
                                  group->params.ucp_worker->stats,
                                  "-%p",
                                  group);
    if (status != UCS_OK) {
        goto err_release_group_params;
    }

    ucs_queue_head_init(&group->pending);
    ucs_list_add_head(&context->groups_head, &group->list);
    ucg_init_group_root_used(group);
    kh_init_inplace(ucg_groups_ep, &group->eps);
    *group_p = group;
    ucs_info("Create ucg group %hu members %u", group->params.group_id, group->params.member_count);

    return UCS_OK;
err_release_group_params:
    ucg_group_release_params(group);
err_free_group:
    ucs_free(group);
    return status;
}

void ucg_group_destroy(ucg_group_h group)
{
    ucs_info("Destroying ucg group %hu", group->params.group_id);

    /* First - make sure all the collective are completed */
    while (!ucs_queue_is_empty(&group->pending)) {
        ucg_group_progress(group);
    }

    UCP_WORKER_THREAD_CS_ENTER_CONDITIONAL(group->worker);
    ucs_list_del(&group->list);
    UCS_STATS_NODE_FREE(group->stats);
    ucg_group_destroy_planner_context(group);
    ucg_group_release_params(group);
    ucs_free(group);
    UCP_WORKER_THREAD_CS_EXIT_CONDITIONAL(group->worker);

    return;
}

unsigned ucg_group_progress(ucg_group_h group)
{
    unsigned ret = 0;

    ucs_list_link_t *planner_head = &group->context->planners_head;
    ucg_planner_ctx_h *planner_ctx = group->planners_context;
    ucg_planner_h planner;
    ucs_list_for_each(planner, planner_head, list) {
        if (planner_ctx == NULL) {
            break;
        }
        planner->progress(*planner_ctx++);
    }

    int i;
    for (i = 0; i < group->iface_cnt; ++i) {
        ret += uct_iface_progress(group->ifaces[i]);
    }

    return ret;
}
/* END: public interface for ucg user */

/* BEGIN: public interface in ucg module */
const ucg_group_params_t* ucg_group_get_params(ucg_group_h group)
{
    if (group == NULL) {
        return NULL;
    }
    return &group->params;
}

ucs_status_t ucg_group_connect(ucg_group_h group,
                               ucg_group_member_index_t dst,
                               ucg_group_ep_t *ep)
{
    /* fill-in UCP connection parameters */
    ucp_ep_h ucp_ep = NULL;
    ucp_address_t *remote_addr = NULL;
    ucs_status_t status = UCS_OK;
    ucg_group_member_index_t global_index = group->context->params.mpi_global_idx_f(group->params.cb_group_obj, dst);
    int ret = 0;
    khiter_t iter = kh_get(ucg_groups_ep, &group->eps, global_index);
    if (iter != kh_end(&group->eps)) {
        /* Use the cached connection */
        ucp_ep = kh_value(&group->eps, iter);
    } else {
        size_t remote_addr_len;
        status = group->address.lookup_f(group->params.cb_group_obj, dst, &remote_addr, &remote_addr_len);
        if (status != UCS_OK) {
            ucs_error("Failed to lookup the address of \"%"PRIu64"\"", dst);
            return status;
        }

        /* special case: connecting to a zero-length address means it's "debugging" */
        if (ucs_unlikely(remote_addr_len == 0)) {
            return UCS_OK;
        }
        /* create an endpoint for communication with the remote member */
        ucp_ep_params_t ep_params = {
            .field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS,
            .address = remote_addr
        };
        status = ucp_ep_create(group->worker, &ep_params, &ucp_ep);
        if (status != UCS_OK) {
            goto err_release_addr;
        }

        /* Store this endpoint, for future reference */
        iter = kh_put(ucg_groups_ep, &group->eps, global_index, &ret);
        kh_value(&group->eps, iter) = ucp_ep;
    }

    /* Connect for point-to-point communication */
    ucp_lane_index_t lane;
    uct_ep_h am_ep;
am_retry:
    lane = ucp_ep_get_am_lane(ucp_ep);
    am_ep = ucp_ep_get_am_uct_ep(ucp_ep);

    if (am_ep == NULL) {
        status = ucp_wireup_connect_remote(ucp_ep, lane);
        if (status != UCS_OK) {
           goto err_release_addr;
        }
        goto am_retry; /* Just to obtain the right lane */
    }
    if (ucp_proxy_ep_test(am_ep)) {
        ucp_proxy_ep_t *proxy_ep = ucs_derived_of(am_ep, ucp_proxy_ep_t);
        am_ep = proxy_ep->uct_ep;
        ucs_assert(am_ep != NULL);
    }
    ucs_assert(am_ep->iface != NULL);
    if (am_ep->iface->ops.ep_am_short ==
        (typeof(am_ep->iface->ops.ep_am_short))
        ucs_empty_function_return_no_resource) {
        ucp_worker_progress(group->worker);
        goto am_retry;
    }

    status = ucg_group_progress_add(am_ep->iface, group);
    if (status != UCS_OK) {
        goto err_close_ep;
    }

    lane = ucp_ep_get_am_lane(ucp_ep);
    ep->am_ep             = am_ep;
    ep->am_iface_attr     = ucp_ep_get_iface_attr(ucp_ep, lane);
    ep->ucp_ep            = ucp_ep;
    ep->md                = ucp_ep_md(ucp_ep, lane);
    ep->md_attr           = ucp_ep_md_attr(ucp_ep, lane);
    group->address.release_f(remote_addr);
    return UCS_OK;
err_close_ep:
    ucp_ep_close_nb(ucp_ep, UCP_EP_CLOSE_FLAG_FORCE);
err_release_addr:
    group->address.release_f(remote_addr);
    return status;
}

ucg_plan_t* ucg_group_get_cache_plan(ucg_group_h group,
                                     ucg_collective_params_t *params)
{
    unsigned coll_root;
    unsigned msg_size_level;
    ucg_group_calc_cache_index(params, &msg_size_level, &coll_root);

    ucg_group_member_index_t root = UCG_ROOT_RANK(params);
    if (root != group->root_used[coll_root]) {
        /* root is not used, means no cache plan */
        return NULL;
    }

    ucg_plan_t *plan = group->cache[msg_size_level][coll_root][params->plan_cache_index];
    if (plan == NULL || root != plan->type.root) {
        return NULL;
    }

    if (params->send.op_ext != NULL && !group->context->params.op_is_commute_f(params->send.op_ext) && params->send.count > 1
        && plan->is_ring_plan_topo_type) {
        return NULL;
    }

    if (ucg_chk_noncontig_allreduce_plan(params, &group->context->params, plan)) {
        return NULL;
    }

    if (plan->is_ring_plan_topo_type && ucg_is_segmented_allreduce(params)) {
        return NULL;
    }
    if (!plan->check(plan, params)) {
        return NULL;
    }

    ucs_debug("Select plan from cache: %p", plan);
    return plan;
}

void ucg_group_update_cache_plan(ucg_group_h group,
                                 ucg_collective_params_t *params,
                                 ucg_plan_t *plan)
{
    unsigned coll_root;
    unsigned msg_size_level;
    ucg_group_calc_cache_index(params, &msg_size_level, &coll_root);

    ucg_plan_t *old_plan = group->cache[msg_size_level][coll_root][params->plan_cache_index];
    if (old_plan != NULL) {
        old_plan->destroy(old_plan);
    }
    group->cache[msg_size_level][coll_root][params->plan_cache_index] = plan;
    group->root_used[coll_root] = UCG_ROOT_RANK(params);
    return;
}

ucs_status_t ucg_group_select_planner(ucg_group_h group,
                                      const char* planner_name,
                                      const ucg_collective_params_t *coll_params,
                                      ucg_planner_h *planner_p,
                                      ucg_planner_ctx_h *planner_ctx_p)
{
    ucs_list_link_t *planners_head = &group->context->planners_head;
    ucg_planner_h planner;
    int i = 0;
    if (planner_name != NULL && planner_name[0] != '\0') {
        ucs_list_for_each(planner, planners_head, list) {
            if (strcmp(planner->name, planner_name) == 0) {
                *planner_p = planner;
                *planner_ctx_p = group->planners_context[i];
                return UCS_OK;
            }
            ++i;
        }
        ucs_error("Unknown planner name:\"%s\"", planner_name);
        return UCS_ERR_INVALID_PARAM;
    }

    /* planner with high priority is in the front of the list */
    unsigned modifier = coll_params->type.modifiers;
    ucs_list_for_each(planner, planners_head, list) {
        if (modifier == (modifier & planner->modifiers_supported)) {
            *planner_p = planner;
            *planner_ctx_p = group->planners_context[i];
            return UCS_OK;
        }
        ++i;
    }

    ucs_error("None useable planner");

    return UCS_ERR_NO_RESOURCE;
}
/* END: public interface in ucg module */