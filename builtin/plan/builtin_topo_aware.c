/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019-2021.  ALL RIGHTS RESERVED.
 * Description: Topo-aware algorithm
 */


#include <math.h>
#include  <ucs/debug/log.h>
#include  <ucs/debug/assert.h>
#include <ucs/debug/memtrack.h>
#include <uct/api/uct_def.h>
#include <ucs/arch/bitops.h>

#include "builtin_plan.h"

/**/

#define MAX_PEERS 100
#define MAX_PHASES 16

ucs_status_t ucg_builtin_topo_aware_add_intra(ucg_builtin_plan_t *topo_aware,
                                              const ucg_builtin_config_t *config,
                                              ucg_builtin_topo_aware_params_t *params,
                                              const ucg_group_member_index_t *member_list,
                                              const ucg_group_member_index_t member_cnt,
                                              enum ucg_builtin_plan_topology_type topo_type,
                                              enum ucg_group_hierarchy_level topo_level,
                                              enum ucg_builtin_plan_connect_pattern pattern)
{
    ucs_status_t status = UCS_OK;
    unsigned num_group = 1;
    unsigned leader_shift = 0;
    ucs_assert(member_cnt > 0);

    if (member_cnt == 1) {
        ucs_debug("member_cnt is 1, skip adding intra-phase");
        return status;
    }
    switch (topo_type) {
        case UCG_PLAN_RECURSIVE:
            status = ucg_builtin_recursive_build(topo_aware, params->super.ctx, config,
                                                member_list + leader_shift, member_cnt / num_group,
                                                UCG_PLAN_BUILD_PARTIAL, UCG_PLAN_RECURSIVE_TYPE_ALLREDUCE);
            break;
        case UCG_PLAN_BMTREE:
            status = ucg_builtin_bmtree_build(topo_aware, &params->super, config,
                                              member_list + leader_shift, member_cnt / num_group,
                                              member_list[leader_shift], UCG_PLAN_BUILD_PARTIAL, pattern);
            break;
        case UCG_PLAN_KMTREE: {
            unsigned degree;
            if (pattern == UCG_PLAN_PATTERN_MANY_TO_ONE) {
                degree = config->trees.intra_degree_fanin;
            } else if (pattern == UCG_PLAN_PATTERN_ONE_TO_MANY) {
                degree = config->trees.intra_degree_fanout;
            } else {
                ucs_error("Plan patten should be either ONE_TO_MANY or MANY_TO_ONE for tree!");
                return UCS_ERR_INVALID_PARAM;
            }
            status = ucg_builtin_kmtree_build(topo_aware, &params->super, config,
                                              member_list + leader_shift, member_cnt / num_group,
                                              member_list[leader_shift], degree, UCG_PLAN_BUILD_PARTIAL, pattern);
            break;
        }
        default:
            break;

    }
    return status;
}
