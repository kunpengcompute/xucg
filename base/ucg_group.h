/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019-2020.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#ifndef UCG_GROUP_H_
#define UCG_GROUP_H_

#include "ucg_plan.h"
#include "../api/ucg.h"
#include <ucs/datastruct/khash.h>

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

extern size_t ucg_ctx_worker_offset;
#define UCG_WORKER_TO_GROUPS_CTX(worker) \
    ((ucg_groups_t*)((char*)(worker) + ucg_ctx_worker_offset))

#define UCG_FLAG_MASK(params) \
    ((params)->type.modifiers & UCG_GROUP_COLLECTIVE_MODIFIER_MASK)

#define UCG_ROOT_RANK(params) \
    ((params)->type.root)

__KHASH_TYPE(ucg_groups_ep, ucg_group_member_index_t, ucp_ep_h)

/*
 * To enable the "Groups" feature in UCX - it's registered as part of the UCX
 * context - and allocated a context slot in each UCP Worker at a certain offset.
 */
typedef struct ucg_groups {
    ucs_list_link_t       groups_head;
    ucg_group_id_t        next_id;

    unsigned              iface_cnt;
    uct_iface_h           ifaces[UCG_GROUP_MAX_IFACES];

    khash_t(ucg_groups_ep) eps;                 /**< endpoints created */

    size_t                total_planner_sizes;
    unsigned              num_planners;
    ucg_plan_desc_t      *planners;
} ucg_groups_t;

struct ucg_group {
    /*
     * Whether a current barrier is waited upon. If so, new collectives cannot
     * start until this barrier is cleared, so it is put in the pending queue.
     */
    int                is_barrier_outstanding;

    ucg_worker_h       worker;       /* for conn. est. and progress calls */
    ucg_coll_id_t      next_id;      /* for the next collective operation */
    ucg_group_id_t     group_id;     /* group identifier (order of creation) */
    ucs_queue_head_t   pending;      /* requests currently pending execution */
    ucg_group_params_t params;       /* parameters, for future connections */
    ucs_list_link_t    list;         /* worker's group list */

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

    /* Below this point - the private per-planner data is allocated/stored */
};

int ucg_builtin_op_can_reuse(const ucg_plan_t *plan, const ucg_op_t *op,
                             const ucg_collective_params_t *params);

void ucg_builtin_update_op(const ucg_plan_t *plan, ucg_op_t *op,
                           const ucg_collective_params_t *params);

int ucg_is_segmented_allreduce(const ucg_collective_params_t *coll_params);

int ucg_is_noncontig_allreduce(const ucg_group_params_t *group_params,
                               const ucg_collective_params_t *coll_params);

#endif /* UCG_GROUP_H_ */