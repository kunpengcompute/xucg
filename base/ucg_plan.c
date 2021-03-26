/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#include "ucg_plan.h"
#include "ucg_group.h"
#include "ucg_context.h"

#include <ucg/api/ucg.h>
#include <ucp/core/ucp_types.h>
#include <ucp/core/ucp_ep.inl>
#include <ucp/core/ucp_ep.inl>
#include <ucp/core/ucp_proxy_ep.h>
#include <uct/base/uct_component.h>
#include <ucs/config/parser.h>
#include <ucs/debug/log.h>
#include <ucs/debug/assert.h>
#include <ucs/debug/memtrack.h>
#include <ucs/type/class.h>
#include <ucs/sys/module.h>
#include <ucs/sys/string.h>
#include <ucs/arch/cpu.h>

UCS_LIST_HEAD(ucg_plan_components_list);

KHASH_IMPL(ucg_group_ep, ucg_group_member_index_t, ucp_ep_h,
           1, kh_int_hash_func, kh_int_hash_equal);

#define ucg_plan_foreach(_descs, _desc_cnt, _plan_ctx, _grp_ctx) \
    typeof(_desc_cnt) idx = 0; \
    ucg_plan_component_t* comp; \
    ucs_assert((_desc_cnt) > 0); \
    for (comp = (_descs)->component; \
         idx < (_desc_cnt); \
         idx++, (_descs)++, \
         (_plan_ctx) = UCS_PTR_BYTE_OFFSET((_plan_ctx), comp->global_ctx_size), \
         (_grp_ctx)  = UCS_PTR_BYTE_OFFSET((_grp_ctx), \
                           ucs_align_up(comp->per_group_ctx_size, \
                                        UCS_SYS_CACHE_LINE_SIZE)), \
         comp = (idx < (_desc_cnt)) ? (_descs)->component : NULL)

#define ucg_group_foreach(_group) \
    ucg_plan_desc_t *descs = (_group)->context->planners; \
    unsigned desc_cnt      = (_group)->context->num_planners; \
    ucg_plan_ctx_h pctx    = (_group)->context->planners_ctx; \
    ucg_group_ctx_h gctx   = (void*)ucs_align_up((uintptr_t)(_group + 1), \
                                                 UCS_SYS_CACHE_LINE_SIZE); \
    ucg_plan_foreach(descs, desc_cnt, pctx, gctx)

static ucs_status_t ucg_plan_config_read(ucg_plan_component_t *component,
                                         const char *env_prefix,
                                         const char *filename,
                                         ucg_plan_config_t **config_p)
{
    uct_config_bundle_t *bundle = NULL;

    ucs_status_t status = uct_config_read(&bundle, component->config.table,
                                          component->config.size, env_prefix,
                                          component->config.prefix);
    if (status != UCS_OK) {
        ucs_error("failed to read CM configuration");
        return status;
    }

    *config_p = (ucg_plan_config_t*) bundle->data;
    /* coverity[leaked_storage] */
    return UCS_OK;
}

ucs_status_t ucg_plan_query(ucg_plan_desc_t **desc_p, unsigned *num_desc_p,
                            size_t *total_plan_ctx_size)
{
    UCS_MODULE_FRAMEWORK_DECLARE(ucg);
    UCS_MODULE_FRAMEWORK_LOAD(ucg, 0);

    /* Calculate how many descriptors to allocate */
    ucg_plan_component_t *component;
    unsigned i, desc_cnt, desc_total = 0;
    ucs_list_for_each(component, &ucg_plan_components_list, list) {
        ucs_status_t status = component->query(NULL, &desc_cnt);
        if (status != UCS_OK) {
            ucs_warn("Failed to query planner %s (for size): %m",
                     component->name);
            continue;
        }

        desc_total += desc_cnt;
    }

    /* Allocate the descriptors */
    size_t size                = desc_total * sizeof(ucg_plan_desc_t);
    ucg_plan_desc_t *desc_iter = *desc_p = UCS_ALLOC_CHECK(size, "ucg descs");

    size = 0;
    ucs_list_for_each(component, &ucg_plan_components_list, list) {
        ucs_status_t status = component->query(desc_iter, &desc_cnt);
        if (status != UCS_OK) {
            ucs_warn("Failed to query planner %s (for content): %m",
                     component->name);
            continue;
        }

        for (i = 0; i < desc_cnt; ++i) {
            size += desc_iter[i].component->global_ctx_size;

            ucs_assertv_always(!strncmp(component->name, desc_iter[i].name,
                                        strlen(component->name)),
                               "Planner name must begin with topology component name."
                               "Planner name: %s Plan component name: %s ",
                               desc_iter[i].name, component->name);
        }

        desc_iter += desc_cnt;
    }

    *num_desc_p          = desc_iter - *desc_p;
    *total_plan_ctx_size = size;

    return UCS_OK;
}

ucs_status_t ucg_plan_init(ucg_plan_desc_t *descs, unsigned desc_cnt,
                           ucg_plan_ctx_h plan, size_t *per_group_ctx_size)
{
    ucg_plan_params_t plan_params;
    ucg_plan_config_t *plan_config;

    uint8_t am_id      = UCP_AM_ID_LAST;
    void *dummy_group  = (void*)(sizeof(ucg_group_t) + UCS_SYS_CACHE_LINE_SIZE);
    plan_params.am_id  = &am_id;

    ucg_plan_foreach(descs, desc_cnt, plan, dummy_group) {
        ucs_status_t status = ucg_plan_config_read(comp, NULL, NULL, &plan_config);
        if (status != UCS_OK) {
            continue;
        }

#ifndef HAVE_UCP_EXTENSIONS
        /*
         * Find an unused AM_ID between 0 and UCP_AM_ID_LAST, because UCP will
         * disregard any value above that (since UCP_AM_ID_MAX isn't there).
         */
        if (am_id == UCP_AM_ID_LAST) {
            am_id = 1; /* AM ID #0 would complicate debugging */
        }

        while (ucp_am_handlers[am_id].cb != NULL) {
            am_id++;
        }

        ucs_assert_always(am_id < UCP_AM_ID_LAST);
#endif

        status = comp->init(plan, &plan_params, plan_config);

        uct_config_release(plan_config);

        if (status != UCS_OK) {
            ucs_warn("failed to initialize planner %s: %m", descs->name);
            continue;
        }
    }

#if ENABLE_FAULT_TOLERANCE
    /* Initialize the fault-tolerance context for the entire UCG layer */
    status = ucg_ft_init(&worker->async, new_group, ucg_base_am_id + idx,
                         ucg_group_fault_cb, ctx, &ctx->ft_ctx);
    if (status != UCS_OK) {
        goto cleanup_pctx;
    }
#endif

    *per_group_ctx_size = (size_t)dummy_group;

    return UCS_OK;
}

void ucg_plan_finalize(ucg_plan_desc_t *descs, unsigned desc_cnt,
                       ucg_plan_ctx_h plan)
{

#if ENABLE_FAULT_TOLERANCE
    if (ucs_list_is_empty(&group->list)) {
        ucg_ft_cleanup(&gctx->ft_ctx);
    }
#endif

    void *dummy_group = NULL;
    ucg_plan_foreach(descs, desc_cnt, plan, dummy_group) {
        comp->finalize(plan);
    }
}

ucs_status_t ucg_plan_group_create(ucg_group_h group)
{
    kh_init_inplace(ucg_group_ep, &group->p2p_eps);
    kh_init_inplace(ucg_group_ep, &group->incast_eps);
    kh_init_inplace(ucg_group_ep, &group->bcast_eps);

    ucg_group_foreach(group) {
        /* Create the per-planner per-group context */
        ucs_status_t status = comp->create(pctx, gctx, group, &group->params);
        if (status != UCS_OK) {
            return status;
        }
    }

    return UCS_OK;
}

void ucg_plan_group_destroy(ucg_group_h group)
{
    ucg_group_foreach(group) {
        comp->destroy(gctx);
    }

    kh_destroy_inplace(ucg_group_ep, &group->bcast_eps);
    kh_destroy_inplace(ucg_group_ep, &group->incast_eps);
    kh_destroy_inplace(ucg_group_ep, &group->p2p_eps);
}

void ucg_plan_print_info(ucg_plan_desc_t *descs, unsigned desc_cnt, FILE *stream)
{
    void *dummy_plan  = NULL;
    void *dummy_group = NULL;

    ucg_plan_foreach(descs, desc_cnt, dummy_plan, dummy_group) {
        fprintf(stream, "#     planner %-2d :  %s\n", idx, comp->name);
    }
}

ucs_status_t ucg_plan_single(ucg_plan_component_t *component,
                             ucg_plan_desc_t *descs,
                             unsigned *desc_cnt_p)
{
    if (descs) {
        descs->component = component;
        ucs_snprintf_zero(&descs->name[0], UCG_PLAN_COMPONENT_NAME_MAX, "%s",
                          component->name);
    }

    *desc_cnt_p = 1;

    return UCS_OK;
}

ucs_status_t ucg_plan_choose(const ucg_collective_params_t *coll_params,
                             ucg_group_h group, ucg_plan_desc_t **desc_p,
                             ucg_group_ctx_h *gctx_p)
{
    ucs_assert(group->context->num_planners == 1); // TODO: support more...

    *desc_p = group->context->planners;
    *gctx_p = (void*)ucs_align_up((uintptr_t)(group + 1), UCS_SYS_CACHE_LINE_SIZE);

    return UCS_OK;
}

static ucs_status_t ucg_plan_connect_by_hash_key(ucg_group_h group,
                                                 ucg_group_member_index_t group_idx,
                                                 khash_t(ucg_group_ep) *khash,
                                                 uct_incast_cb_t incast_cb,
                                                 int is_p2p, ucp_ep_h *ep_p)
{
    int ret;
    ucp_ep_h ucp_ep;
    ucs_status_t status;
    size_t remote_addr_len;
    ucp_address_t *remote_addr;

    /* Look-up the UCP endpoint based on the index */
    khiter_t iter = kh_get(ucg_group_ep, khash, group_idx);
    if (iter != kh_end(khash)) {
        /* Use the cached connection */
        *ep_p = kh_value(khash, iter);
        return UCS_OK;
    }

    if (!is_p2p) {
        status = ucg_group_wireup_coll_ifaces(group, group_idx, incast_cb, &ucp_ep);
    } else {
        remote_addr = NULL;

        /* fill-in UCP connection parameters */
        status = ucg_global_params.address.lookup_f(group->params.cb_context,
                                                    group_idx,
                                                    &remote_addr,
                                                    &remote_addr_len);
        if (status != UCS_OK) {
            ucs_error("failed to obtain a UCP endpoint from the external callback");
            return status;
        }

        /* special case: connecting to a zero-length address means it's "debugging" */
        if (ucs_unlikely(remote_addr_len == 0)) {
            *ep_p = NULL;
            return UCS_OK;
        }

        /* create an endpoint for communication with the remote member */
        ucp_ep_params_t ep_params = {
                .field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS,
                .address = remote_addr
        };

        status = ucp_ep_create(group->worker, &ep_params, &ucp_ep);
        ucg_global_params.address.release_f(remote_addr);
    }

    if (status != UCS_OK) {
        return status;
    }

    /* Store this endpoint, for future reference */
    iter = kh_put(ucg_group_ep, khash, group_idx, &ret);
    *ep_p = kh_value(khash, iter) = ucp_ep;

    return UCS_OK;
}

static ucs_status_t ucg_plan_await_lane_connection(ucp_worker_h worker,
                                                   ucp_ep_h ucp_ep,
                                                   ucp_lane_index_t lane,
                                                   uct_ep_h uct_ep)
{
    if (uct_ep == NULL) {
        ucs_status_t status = ucp_wireup_connect_remote(ucp_ep, lane);
        return (status != UCS_OK) ? status : UCS_INPROGRESS;
    }

    ucs_assert(!ucp_proxy_ep_test(uct_ep));
//    if (ucp_proxy_ep_test(uct_ep)) {
//        ucp_proxy_ep_t *proxy_ep = ucs_derived_of(uct_ep, ucp_proxy_ep_t);
//        uct_ep = proxy_ep->uct_ep;
//        ucs_assert(uct_ep != NULL);
//    }

    ucs_assert(uct_ep->iface != NULL);
    if (uct_ep->iface->ops.ep_am_short ==
            (typeof(uct_ep->iface->ops.ep_am_short))
            ucs_empty_function_return_no_resource) {
        ucp_worker_progress(worker);
        return UCS_INPROGRESS;
    }

    return UCS_OK;
}

static ucs_status_t ucg_plan_connect_p2p(ucg_group_h group,
                                         ucg_group_member_index_t group_idx,
                                         ucp_lane_index_t *lane_p,
                                         ucp_ep_h *ucp_ep_p,
                                         uct_ep_h *uct_ep_p)
{
    ucp_ep_h ucp_ep;
    uct_ep_h uct_ep;
    ucp_lane_index_t lane;
    ucg_group_member_index_t dest_idx;

    if (ucg_global_params.field_mask & UCG_PARAM_FIELD_GLOBAL_INDEX) {
        int ret = ucg_global_params.get_global_index_f(group->params.cb_context,
                                                       group_idx, &dest_idx);
        if (ret) {
            ucs_error("Failed to get the global index for group #%u index #%u",
                      group->params.id, group_idx);
            return UCS_ERR_UNREACHABLE;
        }

        /* use global context for lookup, e.g. MPI_COMM_WORLD, not my context */
        group = ucs_list_head(&group->context->groups_head, ucg_group_t, list);
    } else {
        dest_idx = group_idx;
    }

    ucs_status_t status = ucg_plan_connect_by_hash_key(group, dest_idx,
                                                       &group->p2p_eps,
                                                       NULL, 1, &ucp_ep);
    if (status != UCS_OK) {
        return status;
    }

    do {
        lane   = ucp_ep_get_am_lane(ucp_ep);
        uct_ep = ucp_ep_get_am_uct_ep(ucp_ep);
        status = ucg_plan_await_lane_connection(group->worker, ucp_ep, lane, uct_ep);
    } while (status == UCS_INPROGRESS);

    if (status == UCS_OK) {
        *lane_p   = lane;
        *ucp_ep_p = ucp_ep;
        *uct_ep_p = uct_ep;
    }

    return status;
}

static ucs_status_t ucg_plan_connect_incast(ucg_group_h group,
                                            ucg_group_member_index_t group_idx,
                                            uct_incast_cb_t incast_cb,
                                            ucp_lane_index_t *lane_p,
                                            ucp_ep_h *ucp_ep_p,
                                            uct_ep_h *uct_ep_p)
{
    ucp_ep_h ucp_ep;
    uct_ep_h uct_ep;
    ucp_lane_index_t lane;

    ucs_status_t status = ucg_plan_connect_by_hash_key(group, group_idx,
                                                       &group->incast_eps,
                                                       incast_cb, 0, &ucp_ep);
    if (status != UCS_OK) {
        return status;
    }

    do {
        lane = ucp_ep_get_incast_lane(ucp_ep);
        if (ucs_unlikely(lane == UCP_NULL_LANE)) {
            ucs_warn("No transports with native incast support were found,"
                     " falling back to P2P transports (slower)");
            return UCS_ERR_UNREACHABLE;
        }

        uct_ep = ucp_ep_get_incast_uct_ep(ucp_ep);
        status = ucg_plan_await_lane_connection(group->worker, ucp_ep, lane, uct_ep);
    } while (status == UCS_INPROGRESS);

    if (status == UCS_OK) {
        *lane_p   = lane;
        *ucp_ep_p = ucp_ep;
        *uct_ep_p = uct_ep;
    }

    return status;
}

static ucs_status_t ucg_plan_connect_bcast(ucg_group_h group,
                                           ucg_group_member_index_t group_idx,
                                           ucp_lane_index_t *lane_p,
                                           ucp_ep_h *ucp_ep_p,
                                           uct_ep_h *uct_ep_p)
{
    ucp_ep_h ucp_ep;
    uct_ep_h uct_ep;
    ucp_lane_index_t lane;

    ucs_status_t status = ucg_plan_connect_by_hash_key(group, group_idx,
                                                       &group->bcast_eps,
                                                       NULL, 0, &ucp_ep);
    if (status != UCS_OK) {
        return status;
    }

    do {
        lane = ucp_ep_get_bcast_lane(ucp_ep);
        if (ucs_unlikely(lane == UCP_NULL_LANE)) {
            ucs_warn("No transports with native broadcast support were found,"
                     " falling back to P2P transports (slower)");
            return UCS_ERR_UNREACHABLE;
        }

        uct_ep = ucp_ep_get_bcast_uct_ep(ucp_ep);
        status = ucg_plan_await_lane_connection(group->worker, ucp_ep, lane, uct_ep);
    } while (status == UCS_INPROGRESS);

    if (status == UCS_OK) {
        *lane_p   = lane;
        *ucp_ep_p = ucp_ep;
        *uct_ep_p = uct_ep;
    }

    return status;
}

ucs_status_t ucg_plan_connect(ucg_group_h group,
                              ucg_group_member_index_t group_idx,
                              enum ucg_plan_connect_flags flags,
                              uct_incast_cb_t incast_cb,
                              uct_ep_h *ep_p, const uct_iface_attr_t **ep_attr_p,
                              uct_md_h *md_p, const uct_md_attr_t    **md_attr_p)
{
    ucp_ep_h ucp_ep;
    ucp_lane_index_t lane;
    ucs_status_t status;

    if (flags) {
#ifdef HAVE_UCT_COLLECTIVES
        switch (flags & (UCG_PLAN_CONNECT_FLAG_WANT_INCAST |
                         UCG_PLAN_CONNECT_FLAG_WANT_BCAST)) {
        case UCG_PLAN_CONNECT_FLAG_WANT_INCAST:
            status = ucg_plan_connect_incast(group, group_idx, incast_cb, &lane, &ucp_ep, ep_p);
            break;

        case UCG_PLAN_CONNECT_FLAG_WANT_BCAST:
            status = ucg_plan_connect_bcast(group, group_idx, &lane, &ucp_ep, ep_p);
            break;

        default:
            return UCS_ERR_INVALID_PARAM;
        }
#else
        return UCS_ERR_UNSUPPORTED;
#endif /* HAVE_UCT_COLLECTIVES */
    } else {
        status = ucg_plan_connect_p2p(group, group_idx, &lane, &ucp_ep, ep_p);
    }

    if (status != UCS_OK) {
        return status;
    }

    ucs_trace("connection successful to remote peer #%u", group_idx);

    *md_p      = ucp_ep_md(ucp_ep, lane);
    *md_attr_p = ucp_ep_md_attr(ucp_ep, lane);
    *ep_attr_p = ucp_ep_get_iface_attr(ucp_ep, lane);

    /* Sanity checks */
    ucs_assert((*md_p) != NULL);
    ucs_assert((*md_attr_p) != NULL);
    ucs_assert((*ep_attr_p) != NULL);
    if (flags) {
        ucs_assert(ucp_ep_get_incast_lane(ucp_ep) !=
                   ucp_ep_get_bcast_lane(ucp_ep));
        ucs_assert(ucp_ep_get_incast_uct_ep(ucp_ep) !=
                   ucp_ep_get_bcast_uct_ep(ucp_ep));

        if (flags & UCG_PLAN_CONNECT_FLAG_WANT_INCAST) {
            ucs_assert(((*ep_attr_p)->cap.flags & UCT_IFACE_FLAG_INCAST) != 0);
        }

        if (flags & UCG_PLAN_CONNECT_FLAG_WANT_BCAST) {
            ucs_assert(((*ep_attr_p)->cap.flags & UCT_IFACE_FLAG_BCAST) != 0);
        }
    }

    return UCS_OK;
}

ucp_worker_h ucg_plan_get_group_worker(ucg_group_h group)
{
    return group->worker;
}

ucs_status_t ucg_plan_query_resources(ucg_group_h group,
                                      ucg_plan_resources_t **resources)
{
    return UCS_OK;
}
