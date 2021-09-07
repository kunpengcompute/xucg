/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019-2021.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#include <ucs/debug/log.h>
#include <ucs/debug/assert.h>
#include <ucs/debug/memtrack.h>
#include <uct/api/uct_def.h>

#include "builtin_plan.h"
#define MAX_PEERS (100)
#define MAX_PHASES (16)

ucs_config_field_t ucg_builtin_trees_config_table[] = {
    {"INTER_TREE_TYPE", "1", "inter-node tree type. (0: binomial tree, 1: k-nomial tree)\n",
     ucs_offsetof(ucg_builtin_trees_config_t, inter_tree_type), UCS_CONFIG_TYPE_UINT},

    {"INTRA_TREE_TYPE", "0", "intra-node tree type. (0: binomial tree, 1: k-nomial tree)\n",
     ucs_offsetof(ucg_builtin_trees_config_t, intra_tree_type), UCS_CONFIG_TYPE_UINT},

    {"INTER_DEGREE_FANOUT", "8", "k-nomial tree degree for inter node with fanout process.\n",
     ucs_offsetof(ucg_builtin_trees_config_t, inter_degree_fanout), UCS_CONFIG_TYPE_UINT},

    {"INTER_DEGREE_FANIN", "8", "k-nomial tree degree for inter node with fanin process.\n",
     ucs_offsetof(ucg_builtin_trees_config_t, inter_degree_fanin), UCS_CONFIG_TYPE_UINT},

    {"INTRA_DEGREE_FANOUT", "2", "k-nomial tree degree for intra node with fanout process.\n",
     ucs_offsetof(ucg_builtin_trees_config_t, intra_degree_fanout), UCS_CONFIG_TYPE_UINT},

    {"INTRA_DEGREE_FANIN", "2", "k-nomial tree degree for intra node with fanin process.\n",
     ucs_offsetof(ucg_builtin_trees_config_t, intra_degree_fanin), UCS_CONFIG_TYPE_UINT},
    {NULL}
};
static inline void ucg_builtin_phase_init(ucg_builtin_plan_phase_t *phase,
                                           ucg_step_idx_t step_index,
                                           unsigned peer_cnt,
                                           enum ucg_builtin_plan_method_type method)
{
    phase->method = method;
    phase->ep_cnt = peer_cnt;
    phase->step_index = step_index;
}

ucs_status_t ucg_builtin_treenode_connect_to_phase(ucg_builtin_plan_phase_t *phase,
                                                   ucg_builtin_group_ctx_t *ctx,
                                                   ucg_step_idx_t step_index,
                                                   uct_ep_h **eps,
                                                   ucg_group_member_index_t *peers,
                                                   unsigned peer_cnt,
                                                   enum ucg_builtin_plan_method_type method)
{
    /* Initialization */
    ucs_assert(peer_cnt > 0);
    ucs_status_t status = UCS_OK;
    ucg_builtin_phase_init(phase, step_index, peer_cnt, method);
#if ENABLE_DEBUG_DATA || ENABLE_FAULT_TOLERANCE
    phase->indexes     = UCS_ALLOC_CHECK(peer_cnt * sizeof(*peers),
                                         "binomial tree topology indexes");
#endif
    if (peer_cnt == 1) {
        status = ucg_builtin_connect(ctx, peers[0], phase, UCG_BUILTIN_CONNECT_SINGLE_EP);
    } else {
        phase->multi_eps = *eps;
        *eps += peer_cnt;

        /* connect every endpoint, by group member index */
        unsigned idx;
        for (idx = 0; (idx < peer_cnt) && (status == UCS_OK); idx++, peers++) {
            status = ucg_builtin_connect(ctx, *peers, phase, idx);
        }
    }
    return status;
}

ucs_status_t ucg_builtin_treenode_connect(ucg_builtin_plan_t *tree,
                                          ucg_builtin_group_ctx_t *ctx,
                                          const ucg_builtin_config_t *config,
                                          enum ucg_collective_modifiers mod,
                                          uct_ep_h *next_ep,
                                          ucg_group_member_index_t *up,
                                          ucg_group_member_index_t up_cnt,
                                          ucg_group_member_index_t *down,
                                          ucg_group_member_index_t down_cnt,
                                          enum ucg_builtin_plan_topology_type tree_topo)
{
    ucs_status_t status = UCS_OK;
    ucg_builtin_plan_phase_t *phase = &tree->phss[tree->phs_cnt];

    /* find phase method */
    enum ucg_builtin_plan_method_type method;
    switch (tree_topo) {
        case UCG_PLAN_TREE_FANIN:
            if (down_cnt) {
                method = ((unsigned)mod & UCG_GROUP_COLLECTIVE_MODIFIER_AGGREGATE) ?
                    (up_cnt ? UCG_PLAN_METHOD_REDUCE_WAYPOINT : UCG_PLAN_METHOD_REDUCE_TERMINAL):
                    (up_cnt ? UCG_PLAN_METHOD_GATHER_WAYPOINT : UCG_PLAN_METHOD_RECV_TERMINAL);
            } else {
                method = UCG_PLAN_METHOD_SEND_TERMINAL;
            }
            break;
        case UCG_PLAN_TREE_FANOUT:
            if (down_cnt) {
                method = ((unsigned)mod & UCG_GROUP_COLLECTIVE_MODIFIER_BROADCAST) ?
                    (up_cnt ? UCG_PLAN_METHOD_BCAST_WAYPOINT : UCG_PLAN_METHOD_SEND_TERMINAL) :
                    (up_cnt ? UCG_PLAN_METHOD_SCATTER_WAYPOINT : UCG_PLAN_METHOD_SCATTER_TERMINAL);
            } else {
                method = UCG_PLAN_METHOD_RECV_TERMINAL;
            }
            break;
        default:
            ucs_error("Tree should be either FANIN or FANOUT!");
            return UCS_ERR_INVALID_PARAM;
    }

    /* connect to phase */
    /* Leaf */
    if (up_cnt == 1 && down_cnt == 0) {
        /* Connect this phase to its peers */
        status = ucg_builtin_treenode_connect_to_phase(phase, ctx, tree->step_cnt,
                                                    &next_ep, up, up_cnt, method);
    }
    /* root */
    if (up_cnt == 0 && down_cnt > 0) {
        /* Connect this phase to its peers */
        status = ucg_builtin_treenode_connect_to_phase(phase, ctx, tree->step_cnt,
                                                    &next_ep, down, down_cnt, method);
    }
    /* Waypoint */
    /**
     * layout of peers which need to be connected:
     * FANIN: [down][down][down][up]
     * FANOUT: [up][down][down][down]
     */
    if (up_cnt == 1 && down_cnt > 0) {
        /* Connect this phase to its peers */
        ucg_group_member_index_t member_idx;
        if (tree_topo == UCG_PLAN_TREE_FANIN) {
            for (member_idx = down_cnt; member_idx < down_cnt + up_cnt; member_idx++) {
                down[member_idx] = up[member_idx - down_cnt];
            }
            status = ucg_builtin_treenode_connect_to_phase(phase, ctx, tree->step_cnt,
                                                    &next_ep, down, down_cnt + up_cnt, method);
        } else if (tree_topo == UCG_PLAN_TREE_FANOUT) {
            for (member_idx = up_cnt; member_idx < down_cnt + up_cnt; member_idx++) {
                up[member_idx] = down[member_idx - up_cnt];
            }
            status = ucg_builtin_treenode_connect_to_phase(phase, ctx, tree->step_cnt,
                                                        &next_ep, up, up_cnt + down_cnt, method);
        }
    }
    return status;
}

static inline void ucg_builtin_tree_updata(ucg_builtin_plan_t *tree,
                                      ucg_group_member_index_t up_cnt,
                                      ucg_group_member_index_t down_cnt)
{
    tree->phs_cnt++;
    tree->step_cnt++;
    tree->ep_cnt += (up_cnt + down_cnt);
}

static inline void ucg_builtin_tree_step_cnt(ucg_builtin_plan_t *tree)
{
    tree->step_cnt += 1;
}

static ucs_status_t ucg_builtin_get_tree_topo(enum ucg_builtin_plan_connect_pattern pattern,
                                              enum ucg_builtin_plan_topology_type *tree_topo)
{
    switch (pattern){
        case UCG_PLAN_PATTERN_MANY_TO_ONE:
            *tree_topo = UCG_PLAN_TREE_FANIN;
            break;
        case UCG_PLAN_PATTERN_ONE_TO_MANY:
            *tree_topo = UCG_PLAN_TREE_FANOUT;
            break;
        default:
            ucs_error("For tree, do not support many-to-many pattern!!!");
            return UCS_ERR_INVALID_PARAM;
    }
    return UCS_OK;
}
ucs_status_t ucg_builtin_bmtree_build(ucg_builtin_plan_t *bmtree,
                                      ucg_builtin_base_params_t *params,
                                      const ucg_builtin_config_t *config,
                                      const ucg_group_member_index_t *member_list,
                                      const ucg_group_member_index_t member_cnt,
                                      const ucg_group_member_index_t member_root,
                                      enum ucg_builtin_plan_build_type build_type,
                                      enum ucg_builtin_plan_connect_pattern pattern)
{
    ucs_status_t status;
    ucg_builtin_group_ctx_t *ctx = params->ctx;
    enum ucg_collective_modifiers mod = params->coll_type->modifiers;
    enum ucg_builtin_plan_topology_type tree_topo = UCG_PLAN_LAST;
    /* next_ep shifts as ep_cnt grows */
    uct_ep_h *next_ep = (uct_ep_h *)(&bmtree->phss[MAX_PHASES]) + bmtree->ep_cnt;

    status = ucg_builtin_get_tree_topo(pattern, &tree_topo);
    if (status) {
        return status;
    }
    ucg_group_member_index_t member_idx, rank_shift, peer, value;
    /* my_index is always "local" */
    ucg_group_member_index_t my_index = 0;
    ucg_group_member_index_t root = 0;
    ucg_group_member_index_t up[MAX_PEERS] = {0};
    ucg_group_member_index_t down[MAX_PEERS] = {0};
    ucg_group_member_index_t up_cnt = 0;
    ucg_group_member_index_t down_cnt = 0;
    ucg_group_member_index_t num_child = 0;
    ucg_group_member_index_t tree_mask = 1;

    if (build_type == UCG_PLAN_BUILD_FULL) {
        my_index = bmtree->super.my_index;
        root = member_root;
    } else if (build_type == UCG_PLAN_BUILD_PARTIAL) {
        /* Find the local my own index */
        for (member_idx = 0; member_idx < member_cnt; member_idx++) {
            if (member_list[member_idx] == bmtree->super.my_index) {
                my_index = member_idx;
                break;
            }
        }

        if (member_idx == member_cnt) {
            /* step_cnt is updated by one for trees while phs_cnt is not*/
            ucg_builtin_tree_step_cnt(bmtree);
            return UCS_OK;
        }

        for (member_idx = 0; member_idx < member_cnt; member_idx++) {
            if (member_list[member_idx] == member_root) {
                root = member_idx;
                break;
            }
        }
        /* for trees, the root should be in member list */
        if (member_idx == member_cnt) {
            ucs_error("The root is not in the member list for binomial tree build!!!");
            return UCS_ERR_INVALID_PARAM;
        }
    }

/*
    left-most tree for FANOUT
    right-most tree for FANIN
*/
    if (tree_topo == UCG_PLAN_TREE_FANIN) {
    /* right-most tree */
        rank_shift = (my_index - root + member_cnt) % member_cnt;
        if (root == my_index) {
            up_cnt = 0;
        }
        while (tree_mask < member_cnt) {
            peer = rank_shift ^ tree_mask;
            if (peer < rank_shift) {
                up[0] = (peer + root) % member_cnt;
                up_cnt = 1;
                break;
            } else if (peer < member_cnt){
                down[num_child] = (peer + root) % member_cnt;
                num_child++;
            }
            tree_mask <<= 1;
        }
        down_cnt = num_child;
    } else if (tree_topo == UCG_PLAN_TREE_FANOUT) {
    /* left-most tree */
        rank_shift = (my_index - root + member_cnt) % member_cnt;
        value = rank_shift;
        for (tree_mask = 1; value > 0; value >>= 1, tree_mask <<= 1) {
        }
        if (root == my_index) {
            up_cnt = 0;
        } else {
            peer = rank_shift ^ (tree_mask >> 1);
            up[0] = (peer + root) % member_cnt;
            up_cnt = 1;
        }
        /* find children */
        while (tree_mask < member_cnt) {
            peer = rank_shift ^ tree_mask;
            if (peer >= member_cnt) {
                break;
            }
            down[num_child] = (peer + root) % member_cnt;
            num_child++;
            tree_mask <<= 1;
        }
        down_cnt = num_child;
    } else {
        ucs_error("Tree should be either FANIN or FANOUT!");
        return UCS_ERR_INVALID_PARAM;
    }

    if (build_type == UCG_PLAN_BUILD_PARTIAL) {
        /* convert index to real rank */
        for (member_idx = 0; member_idx < up_cnt; member_idx++) {
            up[member_idx] = member_list[up[member_idx]];
        }

        for (member_idx = 0; member_idx < down_cnt; member_idx++) {
            down[member_idx] = member_list[down[member_idx]];
        }

        if (ucg_builtin_need_calate_position(params->coll_type, up_cnt, params->ctx, tree_topo)) {
            bmtree->super.up_offset = ucg_get_tree_buffer_pos(bmtree->super.my_index, up[0], root, member_cnt,
                                                                config->bmtree.degree_intra_fanin, member_list);
            ucs_debug("up_offset:%u, degree_intra_fanin=%u, up[0]=%lu, myrank:%lu, root:%lu, size:%lu",
                    bmtree->super.up_offset, config->bmtree.degree_intra_fanin, up[0],
                    bmtree->super.my_index, root, member_cnt);
        }
    }
    status = ucg_builtin_treenode_connect(bmtree, ctx, config, mod, next_ep,
                                         up, up_cnt, down, down_cnt, tree_topo);
    ucg_builtin_tree_updata(bmtree, up_cnt, down_cnt);
    return status;
}

ucs_status_t ucg_builtin_kmtree_build(ucg_builtin_plan_t *kmtree,
                                      ucg_builtin_base_params_t *params,
                                      const ucg_builtin_config_t *config,
                                      const ucg_group_member_index_t *member_list,
                                      const ucg_group_member_index_t member_cnt,
                                      const ucg_group_member_index_t member_root,
                                      const unsigned degree,
                                      enum ucg_builtin_plan_build_type build_type,
                                      enum ucg_builtin_plan_connect_pattern pattern)
{
    if (degree == 0) {
        return UCS_ERR_INVALID_PARAM;
    }
    ucs_status_t status;
    ucg_builtin_group_ctx_t *ctx = params->ctx;
    enum ucg_collective_modifiers mod = params->coll_type->modifiers;
    enum ucg_builtin_plan_topology_type tree_topo = UCG_PLAN_LAST;
    /* next_ep shifts as ep_cnt grows */
    uct_ep_h *next_ep = (uct_ep_h *)(&kmtree->phss[MAX_PHASES]) + kmtree->ep_cnt;

    status = ucg_builtin_get_tree_topo(pattern, &tree_topo);
    if (status) {
        return status;
    }
    ucg_group_member_index_t member_idx, rank_shift, orig_mask, peer;
    /* my_index is always "local" */
    ucg_group_member_index_t my_index = 0;
    ucg_group_member_index_t root = 0;
    ucg_group_member_index_t up[MAX_PEERS] = {0};
    ucg_group_member_index_t down[MAX_PEERS] = {0};
    ucg_group_member_index_t up_cnt = 0;
    ucg_group_member_index_t down_cnt = 0;
    ucg_group_member_index_t num_child = 0;
    ucg_group_member_index_t tree_mask = 1;
    unsigned k;

    if (build_type == UCG_PLAN_BUILD_FULL) {
        my_index = kmtree->super.my_index;
        root = member_root;
    } else if (build_type == UCG_PLAN_BUILD_PARTIAL) {
        /* Find the local my own index */
        for (member_idx = 0; member_idx < member_cnt; member_idx++) {
            if (member_list[member_idx] == kmtree->super.my_index) {
                my_index = member_idx;
                break;
            }
        }

        if (member_idx == member_cnt) {
            /* step_cnt is updated by one for trees while phs_cnt is not*/
            ucg_builtin_tree_step_cnt(kmtree);
            return UCS_OK;
        }

        for (member_idx = 0; member_idx < member_cnt; member_idx++) {
            if (member_list[member_idx] == member_root) {
                root = member_idx;
                break;
            }
        }
        /* for trees, the root should be in member list */
        if (member_idx == member_cnt) {
            ucs_error("The root is not in the member list for binomial tree build!!!");
            return UCS_ERR_INVALID_PARAM;
        }
    }

/*
    left-most tree for FANOUT
    right-most tree for FANIN
*/
    if (tree_topo == UCG_PLAN_TREE_FANIN) {
    /* right-most tree */
        rank_shift = (my_index - root + member_cnt) % member_cnt;
        while (tree_mask < member_cnt) {
            if (rank_shift % (degree * tree_mask)) {
                peer = rank_shift / (degree * tree_mask) * (degree * tree_mask);
                up[0] = (peer + root) % member_cnt;
                up_cnt = 1;
                break;
            }
            tree_mask *= degree;

        }
        tree_mask /= degree;
        orig_mask = tree_mask;
        while (tree_mask > 0) {
            for (k = 1; k < degree; k++) {
                peer = rank_shift + tree_mask * k;
                if (peer < member_cnt) {
                    num_child++;
                }
            }
            tree_mask /= degree;
        }
        down_cnt = num_child;
        tree_mask = orig_mask;
        while (tree_mask > 0) {
            for (k = 1; k < degree; k++) {
                peer = rank_shift + tree_mask * k;
                if (peer < member_cnt) {
                    peer = (peer + root) % member_cnt;
                    down[--num_child] = peer;
                }
            }
            tree_mask /= degree;
        }
    } else if (tree_topo == UCG_PLAN_TREE_FANOUT) {
    /* left-most tree */
        rank_shift = (my_index - root + member_cnt) % member_cnt;
        while (tree_mask < member_cnt) {
            if (rank_shift % (degree * tree_mask)) {
                peer = rank_shift / (degree * tree_mask) * (degree * tree_mask);
                up[0] = (peer + root) % member_cnt;
                up_cnt = 1;
                break;
            }
            tree_mask *= degree;
        }
        /* find children */
        tree_mask /= degree;
        while (tree_mask > 0) {
            for (k = 1; k < degree; k++) {
                peer = rank_shift + tree_mask * k;
                if (peer < member_cnt) {
                    peer = (peer + root) % member_cnt;
                    down[num_child] = peer;
                    num_child++;
                }
            }
            tree_mask /= degree;
        }
        down_cnt = num_child;
    } else {
        ucs_error("Tree should be either FANIN or FANOUT!");
        return UCS_ERR_INVALID_PARAM;
    }

    if (build_type == UCG_PLAN_BUILD_PARTIAL) {
        /* convert index to real rank */
        for (member_idx = 0; member_idx < up_cnt; member_idx++) {
            up[member_idx] = member_list[up[member_idx]];
        }

        for (member_idx = 0; member_idx < down_cnt; member_idx++) {
            down[member_idx] = member_list[down[member_idx]];
        }

        if (ucg_builtin_need_calate_position(params->coll_type, up_cnt, params->ctx, tree_topo)) {
            kmtree->super.up_offset = ucg_get_tree_buffer_pos(kmtree->super.my_index, up[0], root, member_cnt,
                                                                config->bmtree.degree_intra_fanin, member_list);
            ucs_debug("up_offset:%u, degree_intra_fanin=%u, up[0]=%lu, myrank:%lu, root:%lu, size:%lu",
                    kmtree->super.up_offset, config->bmtree.degree_intra_fanin, up[0],
                    kmtree->super.my_index, root, member_cnt);
        }
    }
    status = ucg_builtin_treenode_connect(kmtree, ctx, config, mod, next_ep,
                                         up, up_cnt, down, down_cnt, tree_topo);
    ucg_builtin_tree_updata(kmtree, up_cnt, down_cnt);
    return status;
}

int ucg_builtin_kmtree_get_child_ranks(unsigned rank,
                                       unsigned root,
                                       unsigned size,
                                       unsigned degree,
                                       int *downBuff,
                                       int *pDownCnt)
{
    if (degree == 0 || size == 0) {
        return -1;
    }
    unsigned numChild = 0;
    unsigned mask = 1;
    unsigned localRank = (rank - root + size) % size;

    /* find max mask */
    while (mask < size) {
        if (localRank % (degree * mask)) {
            break;
        }
        mask *= degree;
    }

    /* find children */
    mask /= degree;
    unsigned k;
    unsigned i = 1;
    while (mask >= i) {
        for (k = 1; k < degree; k++) {
            unsigned childRank = localRank + i * k;
            if (childRank < size) {
                childRank = (childRank + root) % size;
                downBuff[numChild] = childRank;
                numChild++;
            }
        }
        i *= degree;
    }
    *pDownCnt = numChild;
    return 0;
}

short ucg_get_tree_buffer_pos(ucg_group_member_index_t myrank,
                              ucg_group_member_index_t uprank,
                              ucg_group_member_index_t root,
                              unsigned size, unsigned degree,
                              const ucg_group_member_index_t *member_list)
{
    int down[MAX_PEERS] = {0};
    int downCnt;
    short ret = -1;
    int idx;
    if (ucg_builtin_kmtree_get_child_ranks(uprank, root, size, degree, down, &downCnt) == -1) {
        return -1;
    }

    /* convert index to real rank */
    for (idx = 0; idx < downCnt; idx++) {
        down[idx] = member_list[down[idx]];
    }
    for (idx = 0; idx < downCnt; idx++) {
        if (down[idx] == myrank) {
            ret = idx;
            break;
        }
    }
    ucs_debug("myrank:%lu, up:%lu, root:%lu, size:%u, down_cnt:%d, pos:%d",
                   myrank, uprank, root, size, downCnt, ret);
    return ((ret == -1) ? downCnt : ret );
}

int ucg_builtin_need_calate_position(const ucg_collective_type_t *coll,
                                     unsigned up_cnt,
                                     const ucg_builtin_group_ctx_t *ctx,
                                     enum ucg_builtin_plan_topology_type tree_topo)
{
    if ((ucg_builtin_get_coll_type(coll) == COLL_TYPE_ALLREDUCE)
         && (up_cnt == 1)
         && (ucg_is_allreduce_consistency(ctx) == 1)
         && (tree_topo == UCG_PLAN_TREE_FANIN)) {
             return 1;
    }
    return 0;
}
