/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#ifndef UCG_GROUP_H_
#define UCG_GROUP_H_

#include <ucs/stats/stats.h>
#include <ucs/type/spinlock.h>
#include <ucs/datastruct/khash.h>
#include <ucp/core/ucp_types.h>

/* Note: <ucs/api/...> not used because this header is not installed */
#include "../api/ucg_plan_component.h"

#define UCG_GROUP_CACHE_MODIFIER_MASK UCS_MASK(7)

#define UCG_GROUP_MED_MSG_SIZE 16384 // TODO: (alex) share with plugins!

/* level number of categoried message size, 0 for short message size, 1 for mid-long message size */
#define UCG_GROUP_MSG_SIZE_LEVEL 2

/* max number of actual root rank used */
#define UCG_GROUP_MAX_ROOT_PARAM 96

#define UCG_ROOT_RANK(params) \
    ((params)->send.type.root)

KHASH_TYPE(ucg_group_ep, ucg_group_member_index_t, ucp_ep_h)

typedef struct ucg_group {
    /*
     * Whether a current barrier is waited upon. If so, new collectives cannot
     * start until this barrier is cleared, so it is put in the pending queue.
     */
    volatile uint32_t          is_barrier_outstanding;
    uint32_t                   is_cache_cleanup_due;

#if ENABLE_MT
    ucs_recursive_spinlock_t lock;
#endif

    ucg_context_h         context;      /**< Back-reference to UCP context */
    ucp_worker_h          worker;       /**< for conn. est. and progress calls */
    ucg_coll_id_t         next_coll_id; /**< for the next collective operation */
    ucs_queue_head_t      pending;      /**< requests currently pending execution */
    ucg_group_params_t    params;       /**< parameters, for future connections */
    ucs_list_link_t       list;         /**< worker's group list */
    ucg_plan_resources_t *resources;    /**< resources available to this group */
    khash_t(ucg_group_ep) p2p_eps;      /**< P2P endpoints, by member index */
    khash_t(ucg_group_ep) incast_eps;   /**< incast endpoints, by member index */
    khash_t(ucg_group_ep) bcast_eps;   /**< bcast endpoints, by member index */

    UCS_STATS_NODE_DECLARE(stats);

    /*
     * per-group cache of previous plans/operations, arranged as follows:
     * for each collective type (e.g. Allreduce) there is a plan with a list of
     * operations. To re-use a past operation it must be available and match the
     * requested collective parameters. The cache size is a total across all
     * collective types.
     */
    unsigned              cache_size;
    ucg_plan_t           *cache_by_modifiers[UCG_GROUP_CACHE_MODIFIER_MASK];
    ucg_plan_t           *cache_nonzero_root[UCG_GROUP_MSG_SIZE_LEVEL][UCG_GROUP_MAX_ROOT_PARAM];

    /*
     * for root collective operations(e.g. Bcast), the parameter of root should be
     * the criterion to decide whether plan has been found.
     */
    unsigned              root_used[UCG_GROUP_MAX_ROOT_PARAM];

    /* Below this point - the private per-planner data is allocated/stored */
} ucg_group_t;

const ucg_group_params_t* ucg_group_get_params(ucg_group_h group); /* for tests */

int ucg_builtin_op_can_reuse(const ucg_plan_t *plan, const ucg_op_t *op,
                             const ucg_collective_params_t *params);

void ucg_builtin_update_op(const ucg_plan_t *plan, ucg_op_t *op,
                           const ucg_collective_params_t *params);

int ucg_is_segmented_allreduce(const ucg_collective_params_t *coll_params);

int ucg_is_noncontig_allreduce(const ucg_group_params_t *group_params,
                               const ucg_collective_params_t *coll_params);

ucs_status_t ucg_group_wireup_coll_ifaces(ucg_group_h group,
                                          ucg_group_member_index_t root,
                                          uct_incast_cb_t incast_cb,
                                          ucp_ep_h *ep_p);

#endif /* UCG_GROUP_H_ */
