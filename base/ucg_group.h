/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019-2020.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#ifndef UCG_GROUP_H_
#define UCG_GROUP_H_

#include "ucg_component.h"

#include <ucg/api/ucg.h>
#include <ucs/stats/stats.h>

#define UCG_GROUP_MAX_IFACES 8

/* level number of categoried message size, 0 for short message size, 1 for mid-long message size */
#define UCG_GROUP_MSG_SIZE_LEVEL 2

/* threshold message size to switch algorithm */
#define UCG_GROUP_MED_MSG_SIZE 16384

/* max number of actual root rank used */
#define UCG_GROUP_MAX_ROOT_PARAM 96

/* max number of collective type in the plan cache. */
#define UCG_GROUP_MAX_COLL_TYPE_BUCKETS 16

#define UCG_FLAG_MASK(params) \
    ((params)->type.modifiers & UCG_GROUP_COLLECTIVE_MODIFIER_MASK)

#define UCG_ROOT_RANK(params) \
    ((params)->type.root)

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
#endif

typedef struct ucg_group {
    /*
     * Whether a current barrier is waited upon. If so, new collectives cannot
     * start until this barrier is cleared, so it is put in the pending queue.
     */
    int                is_barrier_outstanding;

    ucg_context_h      context;
    ucg_planner_ctx_h *planners_context;
    int                num_planners_context;

    ucp_worker_h       worker;       /* shortcut for params.ucp_worker */
    ucg_coll_id_t      next_id;      /* for the next collective operation */
    ucs_queue_head_t   pending;      /* requests currently pending execution */
    ucg_group_params_t params;       /* parameters, for future connections */
    ucs_list_link_t    list;         /* group list */

    UCS_STATS_NODE_DECLARE(stats);

    unsigned           iface_cnt;
    uct_iface_h        ifaces[UCG_GROUP_MAX_IFACES];

    /* per-group cache of previous plans/operations, arranged as follows:
     * for each collective type (e.g. Allreduce) there is a plan with a list of
     * operations. To re-use a past operation it must be available and match the
     * requested collective parameters.
     */
    ucg_plan_t        *cache[UCG_GROUP_MSG_SIZE_LEVEL][UCG_GROUP_MAX_ROOT_PARAM][UCG_GROUP_MAX_COLL_TYPE_BUCKETS];

    /* 
     * for root collective operations(e.g. Bcast), the parameter of root should be 
     * the criterion to decide whether plan has been found.
     */
    unsigned           root_used[UCG_GROUP_MAX_ROOT_PARAM];

    /* shortcut for address lookup and release */
    struct {
        ucg_addr_lookup_callback_t lookup_f;
        ucg_addr_release_callback_t release_f;
    } address;
} ucg_group_t;

typedef struct ucg_group_ep
{
    uct_ep_h am_ep;
    const uct_iface_attr_t *am_iface_attr;
    ucp_ep_h ucp_ep;
    uct_md_h md;
    const uct_md_attr_t *md_attr;
} ucg_group_ep_t;

const ucg_group_params_t* ucg_group_get_params(ucg_group_h group);

ucs_status_t ucg_group_connect(ucg_group_h group, 
                               ucg_group_member_index_t dst,
                               ucg_group_ep_t *ep);

ucg_plan_t* ucg_group_get_cache_plan(ucg_group_h group,
                                     ucg_collective_params_t *params);

void ucg_group_update_cache_plan(ucg_group_h group,
                                 ucg_collective_params_t *params,
                                 ucg_plan_t *plan);

ucs_status_t ucg_group_select_planner(ucg_group_h group,
                                      const char* planner_name,
                                      const ucg_collective_params_t *coll_params,
                                      ucg_planner_h *planner_p,
                                      ucg_planner_ctx_h *planner_ctx_p);

#endif /* UCG_GROUP_H_ */
