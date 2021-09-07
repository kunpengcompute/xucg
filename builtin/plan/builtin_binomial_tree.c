/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019-2021.  ALL RIGHTS RESERVED.
 * Description: Binomial-tree and K-tree algorithm
 */

#include "builtin_plan.h"
#include <math.h>
#include <ucs/debug/assert.h>
#include <ucs/debug/log.h>
#include <ucs/debug/memtrack.h>
#include <uct/api/uct_def.h>


/* number of sockets per node */
#define SPN 2

#define INC_CNT 2

#define MAX_PEERS 100
/*
 * max number of phases are determined by both tree & recursive
 * MAX_PHASES for tree plan      is 4
 * MAX_PHASES for recursive plan is 4 (namely it support 2^4 nodes !)
 */
#define MAX_PHASES 10 /* till now, binomial tree can only support 2^MAX_PHASES process at most */

ucs_config_field_t ucg_builtin_binomial_tree_config_table[] = {
    {"DEGREE_INTER_FANOUT", "8", "k-nomial tree degree for inter node with fanout process.\n",
     ucs_offsetof(ucg_builtin_binomial_tree_config_t, degree_inter_fanout), UCS_CONFIG_TYPE_UINT},

    {"DEGREE_INTER_FANIN", "8", "k-nomial tree degree for inter node with fanin process.\n",
     ucs_offsetof(ucg_builtin_binomial_tree_config_t, degree_inter_fanin), UCS_CONFIG_TYPE_UINT},

    {"DEGREE_INTRA_FANOUT", "2", "k-nomial tree degree for intra node with fanout process.\n",
     ucs_offsetof(ucg_builtin_binomial_tree_config_t, degree_intra_fanout), UCS_CONFIG_TYPE_UINT},

    {"DEGREE_INTRA_FANIN", "2", "k-nomial tree degree for intra node with fanin process.\n",
     ucs_offsetof(ucg_builtin_binomial_tree_config_t, degree_intra_fanin), UCS_CONFIG_TYPE_UINT},
    {NULL}
};

unsigned ucg_builtin_calculate_ppx(const ucg_group_params_t *group_params,
                                   enum ucg_group_member_distance domain_distance)
{
    if (domain_distance == UCG_GROUP_MEMBER_DISTANCE_SOCKET) {
        return group_params->topo_args.pps_local;
    } else {
        return group_params->topo_args.ppn_local;
    }
}

/*
    left-most tree for FANOUT
    right-most tree for FANIN
*/
enum ucg_builtin_tree_direction {
    UCG_PLAN_LEFT_MOST_TREE,
    UCG_PLAN_RIGHT_MOST_TREE
};

static void ucg_builtin_bmtree_algo_build_left(unsigned rank,
                                                       unsigned root,
                                                       unsigned size,
                                                       ucg_group_member_index_t *up, unsigned *up_cnt,
                                                       ucg_group_member_index_t *down, unsigned *down_cnt)
{
    ucs_assert(size > 0);

    unsigned num_child = 0;
    unsigned vrank, mask, remote;
    unsigned value;
    vrank = (rank - root + size) % size;

    value = vrank;

    for (mask = 1; value > 0; value >>= 1, mask <<= 1) {};  /* empty */

    /* find parent */
    if (root == rank) {
        *up_cnt = 0;
    } else {
        remote = vrank ^ (mask >> 1);
        up[0]  = (remote + root) % size;
        *up_cnt = 1;
    }

    /* find children */
    while (mask < size) {
        remote = vrank ^ mask;
        if (remote >= size) {
            break;
        }
        down[num_child] = (remote + root) % size;
        num_child++;
        mask <<= 1;
    }

    *down_cnt = num_child;
}

static void ucg_builtin_bmtree_algo_build_right(unsigned rank,
                                                        unsigned root,
                                                        unsigned size,
                                                        ucg_group_member_index_t *up, unsigned *up_cnt,
                                                        ucg_group_member_index_t *down, unsigned *down_cnt)
{
    ucs_assert(size > 0);

    unsigned num_child = 0;
    unsigned vrank;
    unsigned mask = 1;
    unsigned remote;

    vrank = (rank - root + size) % size;

    if (root == rank) {
        *up_cnt = 0;
    }

    while (mask < size) {
        remote = vrank ^ mask;
        if (remote < vrank) {
            up[0] = (remote + root) % size;
            *up_cnt = 1;
            break;
        } else if (remote < size) {
            down[num_child] = (remote + root) % size;
            num_child++;
        }
        mask <<= 1;
    }

    *down_cnt = num_child;
}


static ucs_status_t ucg_builtin_get_rank(const ucg_group_member_index_t *member_list,
                                         unsigned size,
                                         unsigned root,
                                         unsigned *rank)
{
    unsigned idx;
    unsigned root_num = 0;
    unsigned rank_num = 0;

    /* convert rank to index */
    for (idx = 0; idx < size; idx++) {
        if (*rank == member_list[idx]) {
            *rank = idx;
            rank_num++;
            break;
        }
    }

    if (root >= 0 || root < size) {
        root_num = 1;
    }

    /* if and only if one myrank or root */
    if (root_num != 1 || rank_num != 1) {
        ucs_error("Invalid member list: has %u myself and %u root/subroot", rank_num, root_num);
        return UCS_ERR_INVALID_PARAM;
    }

    return UCS_OK;
}

/* Function:
        input : member_list, size(bmtree size), rank(my own rank), root, tree_direction;
        output: left-most (FANOUT) / right-most (FANIN)
                Binomial tree { father info: up  (father), up_cnt  (father count)
                                child  info: down (child), down_cnt(child  count) }
*/
ucs_status_t ucg_builtin_bmtree_algo_build(const ucg_group_member_index_t *member_list, unsigned size,
                                           unsigned rank, unsigned root, enum ucg_builtin_tree_direction direction,
                                           ucg_group_member_index_t *up, unsigned *up_cnt,
                                           ucg_group_member_index_t *down, unsigned *down_cnt)
{
    ucs_status_t status = ucg_builtin_get_rank(member_list, size, root, &rank);
    if (status != UCS_OK) {
        return status;
    }

    /* Notes: rank & root both corresponds index in member_list */
    if (direction == UCG_PLAN_LEFT_MOST_TREE) {
        /* left-most Binomial Tree */
        ucg_builtin_bmtree_algo_build_left(rank, root, size, up, up_cnt, down, down_cnt);
    } else if (direction == UCG_PLAN_RIGHT_MOST_TREE) {
        /* right-most Binomial Tree */
        ucg_builtin_bmtree_algo_build_right(rank, root, size, up, up_cnt, down, down_cnt);
    } else {
        ucs_error("Invalid tree direction");
        return UCS_ERR_INVALID_PARAM;
    }

    unsigned idx;
    /* convert index to real rank */
    for (idx = 0; idx < *up_cnt; idx++) {
        up[idx] = member_list[up[idx]];
    }

    for (idx = 0; idx < *down_cnt; idx++) {
        down[idx] = member_list[down[idx]];
    }

    return UCS_OK;
}

static ucs_status_t ucg_builtin_kmtree_algo_build_left(unsigned rank,
                                                       unsigned root,
                                                       unsigned size,
                                                       unsigned degree,
                                                       ucg_group_member_index_t *up, unsigned *up_cnt,
                                                       ucg_group_member_index_t *down, unsigned *down_cnt)
{
    if (degree == 0 || size == 0) {
        return UCS_ERR_INVALID_PARAM;
    }

    unsigned mask = 1;
    unsigned num_child = 0;
    unsigned vrank = (rank - root + size) % size;

    if (root == rank) {
        *up_cnt = 0;
    }

    while (mask < size) {
        if (vrank % (degree * mask)) {
            up[0] = (vrank / (degree * mask) * (degree * mask) + root) % size;
            *up_cnt = 1;
            break;
        }
        mask *= degree;
    }

    /* find children */
    mask /= degree;
    unsigned k;
    while (mask > 0) {
        for (k = 1; k < degree; k++) {
            unsigned crank = vrank + mask * k;
            if (crank < size) {
                crank = (crank + root) % size;
                down[num_child] = crank;
                num_child++;
            }
        }
        mask /= degree;
    }

    *down_cnt = num_child;
    return UCS_OK;
}

static ucs_status_t ucg_builtin_kmtree_algo_build_right(unsigned rank,
                                                        unsigned root,
                                                        unsigned size,
                                                        unsigned degree,
                                                        ucg_group_member_index_t *up, unsigned *up_cnt,
                                                        ucg_group_member_index_t *down, unsigned *down_cnt)
{
    if (degree == 0 || size == 0) {
        return UCS_ERR_INVALID_PARAM;
    }

    unsigned num_child = 0;
    unsigned mask = 1;
    ucg_group_member_index_t down_temp[MAX_PEERS] = {0};
    unsigned vrank = (rank - root + size) % size;

    if (root == rank) {
        *up_cnt = 0;
    }

    /* find parent */
    while (mask < size) {
        if (vrank % (degree * mask)) {
            up[0] = (vrank / (degree * mask) * (degree * mask) + root) % size;
            *up_cnt = 1;
            break;
        }
        mask *= degree;
    }

    /* find children */
    mask /= degree;
    unsigned k;
    while (mask > 0) {
        for (k = 1; k < degree; k++) {
            unsigned crank = vrank + mask * k;
            if (crank < size) {
                crank = (crank + root) % size;
                down_temp[num_child] = crank;
                num_child++;
            }
        }
        mask /= degree;
    }

    *down_cnt = num_child;

    int i;
    /* change the order of children from leftmost to rightmost */
    for (i = *down_cnt - 1; i >= 0; i--) {
        down[*down_cnt - 1 - i] = down_temp[i];
    }

    return UCS_OK;
}

/* Function:
        input : member_list, size(bmtree size), rank(my own rank), root, tree_direction, K-value;
        output: left-most (FANOUT) / right-most (FANIN)
                K-nomial tree { father info: up  (father), up_cnt  (father count)
                                child  info: down (child), down_cnt(child  count) }
*/
ucs_status_t ucg_builtin_kmtree_algo_build(const ucg_group_member_index_t *member_list, unsigned size,
                                           unsigned rank, unsigned root, unsigned degree,
                                           enum ucg_builtin_tree_direction direction,
                                           ucg_group_member_index_t *up, unsigned *up_cnt,
                                           ucg_group_member_index_t *down, unsigned *down_cnt)
{
    ucs_status_t status = ucg_builtin_get_rank(member_list, size, root, &rank);
    if (status != UCS_OK) {
        return status;
    }

    if (direction == UCG_PLAN_LEFT_MOST_TREE) {
        /* leftmost k-nomial tree for fanout */
        status = ucg_builtin_kmtree_algo_build_left(rank, root, size, degree, up, up_cnt, down, down_cnt);
    } else if (direction == UCG_PLAN_RIGHT_MOST_TREE) {
        /* right-most k-nomial tree for fanin */
        status = ucg_builtin_kmtree_algo_build_right(rank, root, size, degree, up, up_cnt, down, down_cnt);
    } else {
        ucs_error("Invaild tree direction");
        return UCS_ERR_INVALID_PARAM;
    }
    if (status != UCS_OK) {
        return status;
    }
    unsigned idx;
    /* convert index to real rank */
    for (idx = 0; idx < *up_cnt; idx++) {
        up[idx] = member_list[up[idx]];
    }

    for (idx = 0; idx < *down_cnt; idx++) {
        down[idx] = member_list[down[idx]];
    }

    return UCS_OK;
}

ucs_status_t ucg_builtin_connect_leader(ucg_group_member_index_t my_index, unsigned ppx, unsigned ppx_last_level,
                                        ucg_group_member_index_t *up, unsigned *up_cnt,
                                        ucg_group_member_index_t *down, unsigned *down_cnt,
                                        ucg_group_member_index_t *up_fanin, unsigned *up_fanin_cnt,
                                        ucg_group_member_index_t *down_fanin, unsigned *down_fanin_cnt)
{
    ucs_status_t status = UCS_OK;
    ucs_assert(ppx > 0);
    if (ppx_last_level % ppx != 0) {
        ucs_error("check ppn and ppx in last topo level");
        return UCS_ERR_INVALID_PARAM;
    }

    int leader_cnt = ppx_last_level / ppx;

    if (my_index % ppx == 0 && my_index % ppx_last_level != 0) {
        /* recv_terminal --> waypoint */
        up_fanin[0]  = (my_index / ppx_last_level) * ppx_last_level; /* child  is the leader located at 1st socket */
        *up_fanin_cnt = 1;

        /* send_terminal --> waypoint */
        up[0]  = (my_index / ppx_last_level) * ppx_last_level;      /* parent is the leader located at 1st socket */
        *up_cnt = 1;
    }

    if (my_index % ppx == 0 && my_index % ppx_last_level == 0) {
        int i;
        /* add new parents to the tail */
        for (i = 0; i < leader_cnt - 1; i++) {
            /* parents is the leader located at 2nd socket */
            down_fanin[*down_fanin_cnt] = my_index + (i + 1) * ppx;
            (*down_fanin_cnt)++;
        }

        /* add new children to the head */
        for (i = *down_cnt - 1; i >= 0; i--) {
            down[i + leader_cnt - 1] = down[i];
        }

        for (i = 0; i < leader_cnt - 1; i++) {
            down[i] = my_index + (i + 1) * ppx;                 /* children is the leader located at 2nd socket */
            (*down_cnt)++;
        }
    }
    return status;
}

enum ucg_builtin_plan_method_type ucg_builtin_calculate_plan_method_type(enum ucg_collective_modifiers mod,
                                                                         enum ucg_collective_modifiers choose_mod,
                                                                         unsigned up_cnt, unsigned down_cnt)
{
    enum ucg_builtin_plan_method_type method = UCG_PLAN_METHOD_RECV_TERMINAL;
    switch (choose_mod) {
        case UCG_GROUP_COLLECTIVE_MODIFIER_AGGREGATE:
            if (down_cnt) {
                method = (mod & choose_mod) ?
                    (up_cnt ? UCG_PLAN_METHOD_REDUCE_WAYPOINT : UCG_PLAN_METHOD_REDUCE_TERMINAL):
                    (up_cnt ? UCG_PLAN_METHOD_GATHER_WAYPOINT : UCG_PLAN_METHOD_RECV_TERMINAL);
            } else {
                method = UCG_PLAN_METHOD_SEND_TERMINAL;
            }
            break;
        case UCG_GROUP_COLLECTIVE_MODIFIER_BROADCAST:
            if (down_cnt) {
                method = (mod & choose_mod) ?
                    (up_cnt ? UCG_PLAN_METHOD_BCAST_WAYPOINT : UCG_PLAN_METHOD_SEND_TERMINAL) :
                    (up_cnt ? UCG_PLAN_METHOD_SCATTER_WAYPOINT : UCG_PLAN_METHOD_SCATTER_TERMINAL);
            } else {
                method = UCG_PLAN_METHOD_RECV_TERMINAL;
            }
            break;
        default:
            ucs_error("Currently don't support such operation");
            break;
    }
    return method;
}

static ucs_status_t ucg_builtin_binomial_tree_connect_phase(ucg_builtin_plan_phase_t *phase,
                                                            const ucg_builtin_binomial_tree_params_t *params,
                                                            ucg_step_idx_t step_index,
                                                            uct_ep_h **eps,
                                                            ucg_group_member_index_t *peers,
                                                            unsigned peer_cnt,
                                                            enum ucg_builtin_plan_method_type method)
{
    /* Initialization */
    ucs_assert(peer_cnt > 0);
    ucs_status_t status = UCS_OK;
    phase->method = method;
    phase->ep_cnt = peer_cnt;
    phase->step_index = step_index;
#if ENABLE_DEBUG_DATA
    phase->indexes     = UCS_ALLOC_CHECK(peer_cnt * sizeof(*peers),
                                         "binomial tree topology indexes");
#endif
    if (peer_cnt == 1) {
        status = ucg_builtin_connect(params->ctx, peers[0], phase, UCG_BUILTIN_CONNECT_SINGLE_EP);
    } else {
        phase->multi_eps = *eps;
        *eps += peer_cnt;

        /* connect every endpoint, by group member index */
        unsigned idx;
        for (idx = 0; (idx < peer_cnt) && (status == UCS_OK); idx++, peers++) {
            status = ucg_builtin_connect(params->ctx, *peers, phase, idx);
        }
    }
    return status;
}

static void ucg_builtin_get_node_leaders_node_level(const uint16_t *node_index,
                                                    ucg_group_member_index_t member_count,
                                                    unsigned ppx,
                                                    ucg_group_member_index_t *leaders)
{
    ucg_group_member_index_t member_idx;
    int node_idx;
    leaders[0] = 0;
    for (member_idx = 1, node_idx = 0; member_idx < member_count; member_idx++) {
        if (node_index[member_idx] > node_idx) {
            node_idx++;
            leaders[node_idx] = member_idx;
        }
    }
}

static void ucg_builtin_get_node_leaders_normal_level(ucg_group_member_index_t member_count,
                                                      unsigned ppx,
                                                      ucg_group_member_index_t *leaders)
{
    ucg_group_member_index_t member_idx;
    int node_idx;
    for (member_idx = 0, node_idx = 0; member_idx < member_count; member_idx += ppx, node_idx++) {
        leaders[node_idx] = member_idx;
    }
}

static void ucg_builtin_get_node_leaders(const uint16_t *node_index, ucg_group_member_index_t member_count,
                                                 enum ucg_group_hierarchy_level level, unsigned ppx,
                                                 ucg_group_member_index_t *leaders)
{
    if (level == UCG_GROUP_HIERARCHY_LEVEL_NODE) {
        ucg_builtin_get_node_leaders_node_level(node_index, member_count, ppx, leaders);
    } else {
        ucg_builtin_get_node_leaders_normal_level(member_count, ppx, leaders);
    }
}

static ucs_status_t ucg_builtin_tree_inter_fanin_connect(const ucg_builtin_binomial_tree_params_t *params,
                                                         enum ucg_collective_modifiers mod,
                                                         ucg_group_member_index_t *up_fanin,
                                                         unsigned up_fanin_cnt,
                                                         ucg_group_member_index_t *down_fanin,
                                                         unsigned down_fanin_cnt,
                                                         ucg_builtin_plan_phase_t **phase,
                                                         uct_ep_h **eps,
                                                         ucg_builtin_plan_t *tree)
{
    ucs_status_t status = UCS_OK;
    /* Establish the connection between the parent and children */
    enum ucg_builtin_plan_method_type fanin_method = ucg_builtin_calculate_plan_method_type(mod,
        UCG_GROUP_COLLECTIVE_MODIFIER_AGGREGATE, up_fanin_cnt, down_fanin_cnt);

    /* ==== FANIN phase ==== */
    /* only recv (recv_terminal) */
    /* Receive from children */
    if (up_fanin_cnt == 0) {
        /* Connect this phase to its peers */
        status = ucg_builtin_binomial_tree_connect_phase((*phase)++, params, tree->phs_cnt, eps, down_fanin,
                                                         down_fanin_cnt, fanin_method);
    } else if (down_fanin_cnt == 0 && status == UCS_OK) {
        /* only send (send_terminal) */
        /* Send to parents */
        /* Connect this phase to its peers */
        status = ucg_builtin_binomial_tree_connect_phase((*phase)++, params, tree->phs_cnt, eps, up_fanin,
                                                         up_fanin_cnt, fanin_method);
    } else if (up_fanin_cnt == 1 && down_fanin_cnt > 0 && status == UCS_OK) {
        /* first recv then send (waypoint) */
        /* Connect this phase to its peers */
        ucg_group_member_index_t member_idx;
        for (member_idx = down_fanin_cnt; member_idx < down_fanin_cnt + up_fanin_cnt; member_idx++) {
            down_fanin[member_idx] = up_fanin[member_idx - down_fanin_cnt];
        }

        status = ucg_builtin_binomial_tree_connect_phase((*phase)++, params, tree->phs_cnt, eps, down_fanin,
                                                         down_fanin_cnt + up_fanin_cnt, fanin_method);
    }
    return status;
}

static ucs_status_t ucg_builtin_tree_inter_fanout_connect(const ucg_builtin_binomial_tree_params_t *params,
                                                          enum ucg_collective_modifiers mod,
                                                          ucg_group_member_index_t *up,
                                                          unsigned up_cnt,
                                                          ucg_group_member_index_t *down,
                                                          unsigned down_cnt,
                                                          ucg_builtin_plan_phase_t *phase,
                                                          uct_ep_h **eps,
                                                          ucg_builtin_plan_t *tree)
{
    ucs_status_t status = UCS_OK;
    enum ucg_builtin_plan_method_type fanout_method = ucg_builtin_calculate_plan_method_type(mod,
        UCG_GROUP_COLLECTIVE_MODIFIER_BROADCAST, up_cnt, down_cnt);

    /* ==== FANOUT phase ==== */
    /* only recv (recv_terminal) */
    /* Receive from parents */
    if (up_cnt == 1 && down_cnt == 0) {
        /* Connect this phase to its peers */
        ucs_assert(up_cnt == 1); /* sanity check: not multi-root */
        status = ucg_builtin_binomial_tree_connect_phase(phase, params, tree->phs_cnt + 1, eps, up, up_cnt,
                                                         fanout_method);
    }

    /* only send (send_terminal) */
    /* Send to children */
    if (up_cnt == 0 && down_cnt > 0 && status == UCS_OK) {
        /* Connect this phase to its peers */
        status = ucg_builtin_binomial_tree_connect_phase(phase, params, tree->phs_cnt + 1, eps, down, down_cnt,
                                                         fanout_method);
    }

    /* first recv then send (waypoint) */
    if (up_cnt == 1 && down_cnt > 0 && status == UCS_OK) {
        /* Connect this phase to its peers */
        ucg_group_member_index_t member_idx;
        for (member_idx = up_cnt; member_idx < up_cnt + down_cnt; member_idx++) {
            up[member_idx] = down[member_idx - up_cnt];
        }

        status = ucg_builtin_binomial_tree_connect_phase(phase, params, tree->phs_cnt + 1, eps, up, up_cnt + down_cnt,
                                                         fanout_method);
    }

    return status;
}

static ucs_status_t ucg_builtin_tree_inter_fanin_fanout_create(const ucg_builtin_binomial_tree_params_t *params,
                                                               unsigned ppx,
                                                               ucg_group_member_index_t my_index,
                                                               unsigned node_count,
                                                               unsigned local_root,
                                                               enum ucg_collective_modifiers mod,
                                                               ucg_builtin_plan_phase_t *phase,
                                                               uct_ep_h **eps,
                                                               ucg_builtin_topology_info_params_t *topo_params,
                                                               ucg_builtin_plan_t *tree)
{
    ucs_status_t status = UCS_OK;
    ucs_assert(ppx > 0);
    /* Calculate the number of binomial tree steps for inter-node only */
    if (my_index % ppx == local_root && node_count > 1) {
        ucg_group_member_index_t up[MAX_PEERS] = { 0 };
        ucg_group_member_index_t down[MAX_PEERS] = { 0 };
        unsigned up_cnt = 0;
        unsigned down_cnt = 0;

        ucg_group_member_index_t up_fanin[MAX_PEERS] = { 0 };
        ucg_group_member_index_t down_fanin[MAX_PEERS] = { 0 };
        unsigned up_fanin_cnt = 0;
        unsigned down_fanin_cnt = 0;

        unsigned size = node_count;
        unsigned idx;

        size_t alloc_size = sizeof(ucg_group_member_index_t) * size;
        ucg_group_member_index_t *member_list =
            (ucg_group_member_index_t *)(UCS_ALLOC_CHECK(alloc_size, "member list"));
        memset(member_list, 0, alloc_size);

        for (idx = 0; idx < size; idx++) {
            member_list[idx] = (params->root % ppx) + ppx * idx;
        }
        status = ucg_builtin_kmtree_algo_build(member_list, node_count, my_index, (params->root / ppx),
            params->tree_degree_inter_fanout, UCG_PLAN_LEFT_MOST_TREE, up, &up_cnt, down, &down_cnt);
        if (status != UCS_OK) {
            ucs_free(member_list);
            member_list = NULL;
            return status;
        }
        status = ucg_builtin_kmtree_algo_build(member_list, node_count, my_index, (params->root / ppx),
            params->tree_degree_inter_fanin, UCG_PLAN_RIGHT_MOST_TREE, up_fanin, &up_fanin_cnt, down_fanin,
            &down_fanin_cnt);
        ucs_free(member_list);
        member_list = NULL;
        if (status != UCS_OK) {
            return status;
        }

        status = ucg_builtin_tree_inter_fanin_connect(params, mod, up_fanin, up_fanin_cnt, down_fanin,
                                                      down_fanin_cnt, &phase, eps, tree);
        if (status != UCS_OK) {
            return status;
        }

        status = ucg_builtin_tree_inter_fanout_connect(params, mod, up, up_cnt, down,
                                                       down_cnt, phase, eps, tree);
    }

    return status;
}

static ucs_status_t ucg_builtin_binomial_tree_inter_fanout_connect(const ucg_builtin_binomial_tree_params_t *params,
                                                                   enum ucg_collective_modifiers mod,
                                                                   ucg_group_member_index_t *up,
                                                                   unsigned up_cnt,
                                                                   ucg_group_member_index_t *down,
                                                                   unsigned down_cnt,
                                                                   ucg_builtin_plan_phase_t *phase,
                                                                   uct_ep_h **eps)
{
    ucs_status_t status = UCS_OK;
    enum ucg_builtin_plan_method_type fanout_method =
            ucg_builtin_calculate_plan_method_type(mod, UCG_GROUP_COLLECTIVE_MODIFIER_BROADCAST, up_cnt, down_cnt);

    /* inter node connection and step_idx = 0 */
    /* only recv (recv_terminal) */
    /* Receive from parents */
    if (up_cnt == 1 && down_cnt == 0) {
        /* Connect this phase to its peers */
        status = ucg_builtin_binomial_tree_connect_phase(phase, params, 0, eps, up, up_cnt, fanout_method);
    }

    /* only send (send_terminal) */
    /* Send to children */
    if (up_cnt == 0 && down_cnt > 0 && status == UCS_OK) {
        /* Connect this phase to its peers */
        status = ucg_builtin_binomial_tree_connect_phase(phase, params, 0, eps, down, down_cnt, fanout_method);
    }

    /* first recv then send (waypoint) */
    if (up_cnt == 1 && down_cnt > 0 && status == UCS_OK) {
        /* Connect this phase to its peers */
        ucg_group_member_index_t member_idx;
        for (member_idx = up_cnt; member_idx < up_cnt + down_cnt; member_idx++) {
            up[member_idx] = down[member_idx - up_cnt];
        }

        status = ucg_builtin_binomial_tree_connect_phase(phase, params, 0, eps, up, up_cnt + down_cnt, fanout_method);
    }
    return status;
}

static ucs_status_t ucg_builtin_prepare_inter_fanout_member_idx(const ucg_builtin_binomial_tree_params_t *params,
                                                                unsigned size,
                                                                unsigned ppx,
                                                                unsigned is_use_topo_info,
                                                                unsigned node_idx,
                                                                unsigned *root,
                                                                ucg_builtin_topology_info_params_t *topo_params,
                                                                const ucg_group_member_index_t *subroot_array,
                                                                ucg_group_member_index_t **out_member_list)
{
    unsigned idx;
    /* create member_list */
    size_t alloc_size = sizeof(ucg_group_member_index_t) * size;
    ucg_group_member_index_t *member_list =
        (ucg_group_member_index_t *)(UCS_ALLOC_CHECK(alloc_size, "member list"));
    memset(member_list, 0, alloc_size);

    for (idx = 0; idx < size; idx++) {
        if (is_use_topo_info) {
            member_list[idx] = topo_params->subroot_array[idx];
            if (params->root == subroot_array[idx]) {
                *root = node_idx;
            }
        } else {
            ucs_assert(ppx > 0);
            member_list[idx] = (params->root % ppx) + ppx * idx;
        }
    }

    *out_member_list = member_list;
    return UCS_OK;
}

static void ucg_builtin_binomial_tree_log_topology(ucg_group_member_index_t my_index,
                                                   unsigned up_cnt,
                                                   unsigned down_cnt,
                                                   ucg_group_member_index_t *up,
                                                   ucg_group_member_index_t *down)
{
    long unsigned int member_idx;
    ucs_info("Inter Topology for member #%lu :", my_index);
    for (member_idx = 0; member_idx < up_cnt; member_idx++) {
        ucs_info("%lu's parent #%lu/%u: %lu ", my_index, member_idx, up_cnt, up[member_idx]);
    }
    for (member_idx = 0; member_idx < down_cnt; member_idx++) {
        ucs_info("%lu's child  #%lu/%u: %lu ", my_index, member_idx, down_cnt, down[member_idx]);
    }
}

static ucs_status_t ucg_builtin_binomial_tree_inter_fanout_create(const ucg_builtin_binomial_tree_params_t *params,
                                                                  unsigned ppx,
                                                                  ucg_group_member_index_t my_index,
                                                                  unsigned node_count,
                                                                  unsigned is_use_topo_info,
                                                                  unsigned is_subroot,
                                                                  enum ucg_collective_modifiers mod,
                                                                  ucg_builtin_plan_phase_t *phase,
                                                                  uct_ep_h **eps,
                                                                  ucg_builtin_topology_info_params_t *topo_params,
                                                                  ucg_group_member_index_t *subroot_array)
{
    ucs_assert(ppx > 0);
    ucs_status_t status = UCS_OK;
    unsigned local_root = (params->root % ppx);
    /* for FANOUT, root may change */
    if (!is_use_topo_info) {
        is_subroot = (my_index % ppx == local_root) ? 1 : 0;
    }
    /* Calculate the number of binomial tree steps for inter-node only */
    if (is_subroot && node_count > 1) {
        ucg_group_member_index_t up[MAX_PEERS] = { 0 };
        ucg_group_member_index_t down[MAX_PEERS] = { 0 };
        unsigned up_cnt = 0;
        unsigned down_cnt = 0;
        unsigned node_idx = 0;
        unsigned size, root;
        root = params->root / ppx;
        size = node_count;

        if (is_use_topo_info) {
            for (node_idx = 0; node_idx < topo_params->node_cnt; node_idx++) {
                if (params->root == subroot_array[node_idx]) {
                    root = node_idx;
                    break;
                }
            }
        }
        ucg_group_member_index_t *member_list = NULL;
        status = ucg_builtin_prepare_inter_fanout_member_idx(params, size, ppx, is_use_topo_info, node_idx, &root, topo_params, subroot_array, &member_list);
        if (status != UCS_OK) {
            return status;
        }

        if (ucg_algo.kmtree) {
            /* k-nomial tree */
            status = ucg_builtin_kmtree_algo_build(member_list, size, my_index, root, params->tree_degree_inter_fanout,
                UCG_PLAN_LEFT_MOST_TREE, up, &up_cnt, down, &down_cnt);
        } else {
            /* binomial tree */
            status = ucg_builtin_bmtree_algo_build(member_list, size, my_index, root, UCG_PLAN_LEFT_MOST_TREE, up,
                &up_cnt, down, &down_cnt);
        }
        ucs_free(member_list);
        member_list = NULL;
        if (status != UCS_OK) {
            return status;
        }

        ucg_builtin_binomial_tree_log_topology(my_index, up_cnt, down_cnt, up, down);
        status = ucg_builtin_binomial_tree_inter_fanout_connect(params, mod, up, up_cnt, down, down_cnt, phase, eps);
    }

    return status;
}

static ucs_status_t ucg_builtin_binomial_tree_inter_create(enum ucg_builtin_plan_topology_type topo_type,
                                                           const ucg_builtin_binomial_tree_params_t *params,
                                                           unsigned ppx,
                                                           ucg_group_member_index_t my_index,
                                                           ucg_group_member_index_t my_index_local,
                                                           unsigned node_count,
                                                           unsigned is_subroot,
                                                           unsigned is_use_topo_info,
                                                           enum ucg_collective_modifiers mod,
                                                           ucg_builtin_plan_phase_t *phase,
                                                           uct_ep_h **eps,
                                                           ucg_builtin_topology_info_params_t *topo_params,
                                                           ucg_group_member_index_t *subroot_array,
                                                           ucg_builtin_plan_t *tree,
                                                           unsigned *phs_inc_cnt,
                                                           unsigned *step_inc_cnt)
{
    ucs_status_t status = UCS_OK;
    const unsigned factor = 2;
    switch (topo_type) {
        case UCG_PLAN_RECURSIVE:
            if (is_subroot && node_count > 1) {
                unsigned phs_cnt = tree->phs_cnt;
                ucg_group_member_index_t *node_leaders = UCS_ALLOC_CHECK(node_count * sizeof(ucg_group_member_index_t),
                                                                         "recursive ranks");
                ucg_builtin_get_node_leaders(params->group_params->node_index,
                                                   params->group_params->member_count,
                                                   ucg_algo.topo_level, ppx, node_leaders);
                ucg_builtin_recursive_connect(params->ctx, my_index, node_leaders, node_count, factor, 0, tree);
                *phs_inc_cnt = tree->phs_cnt - phs_cnt;
                ucs_free(node_leaders);
                node_leaders = NULL;
            } else {
                *phs_inc_cnt = 0;
            }
            ucg_builtin_recursive_compute_steps(my_index_local, node_count, factor, step_inc_cnt);
            ucs_debug("phase inc: %d step inc: %d", *phs_inc_cnt, *step_inc_cnt);
            break;
        case UCG_PLAN_TREE_FANIN_FANOUT: /* for inter allreduce, another choice is reduce+bcast with k-nominal tree */
        {
            unsigned local_root = (params->root % ppx);
            status = ucg_builtin_tree_inter_fanin_fanout_create(params, ppx, my_index, node_count,
                                                                local_root, mod, phase, eps,
                                                                topo_params, tree);
            *phs_inc_cnt = (my_index % ppx == local_root) ? INC_CNT : 0;
            *step_inc_cnt = INC_CNT;

            break;
        }
        case UCG_PLAN_TREE_FANOUT:
        {
            unsigned is_real_subroot = (is_use_topo_info) ? is_subroot : (my_index % ppx == params->root % ppx);
            status = ucg_builtin_binomial_tree_inter_fanout_create(params, ppx, my_index, node_count,
                                                                   is_use_topo_info, is_subroot,
                                                                   mod, phase, eps, topo_params, subroot_array);
            *phs_inc_cnt = (is_real_subroot) ? 1 : 0;
            *step_inc_cnt = 1;
            break;
        }
        default:
            break;
    }

    return status;
}

/* For inter node level, use recursive algorithm */
static ucs_status_t ucg_builtin_binomial_tree_add_inter(
    ucg_builtin_plan_t *tree,
    ucg_builtin_plan_phase_t *phase,
    const ucg_builtin_binomial_tree_params_t *params,
    uct_ep_h **eps,
    enum ucg_builtin_plan_topology_type topo_type,
    unsigned *phs_inc_cnt,
    unsigned *step_inc_cnt,
    unsigned ppx,
    ucg_builtin_topology_info_params_t *topo_params)
{
    ucs_assert(ppx > 0);

    ucs_status_t status;

    /* Find my own index */
    ucg_group_member_index_t my_index = tree->super.my_index;
    unsigned node_count = params->group_params->member_count / ppx;
    ucg_group_member_index_t my_index_local = my_index / ppx;
    enum ucg_collective_modifiers mod = params->coll_type->modifiers;

    unsigned node_idx;
    unsigned member_idx;
    unsigned is_subroot =  0;

    /* node-aware: using subroot_array and node_cnt to support unbalance ppn case */
    size_t alloc_size = sizeof(ucg_group_member_index_t) * topo_params->node_cnt;
    ucg_group_member_index_t *subroot_array = (ucg_group_member_index_t *)UCS_ALLOC_CHECK(alloc_size, "subroot array");

    for (member_idx = 0; member_idx < topo_params->node_cnt; member_idx++) {
        subroot_array[member_idx] = topo_params->subroot_array[member_idx];
    }

    unsigned is_use_topo_info = (ucg_algo.topo_level == UCG_GROUP_HIERARCHY_LEVEL_NODE &&
            !ucg_algo.kmtree_intra && !ucg_algo.kmtree) ? 1 : 0;
    if (is_use_topo_info) {
        node_count = topo_params->node_cnt;
        for (node_idx = 0; node_idx < topo_params->node_cnt; node_idx++) {
            if (my_index == topo_params->subroot_array[node_idx]) {
                is_subroot = 1;
                my_index_local = node_idx;
                break;
            }
        }
    } else {
        if (my_index % ppx == 0) {
            is_subroot = 1;
        }
    }

    if (node_count == 1) {
        *phs_inc_cnt = 0;
        *step_inc_cnt = 0;
        ucs_free(subroot_array);
        subroot_array = NULL;
        return UCS_OK;
    }

    status = ucg_builtin_binomial_tree_inter_create(topo_type, params, ppx, my_index, my_index_local,
                                                    node_count, is_subroot, is_use_topo_info,
                                                    mod, phase, eps, topo_params, subroot_array, tree,
                                                    phs_inc_cnt, step_inc_cnt);
    ucs_free(subroot_array);
    subroot_array = NULL;
    return status;
}

static ucs_status_t ucg_builtin_binomial_tree_connect_fanin(ucg_builtin_plan_t *tree,
                                                            const ucg_builtin_binomial_tree_params_t *params,
                                                            enum ucg_builtin_plan_method_type fanin_method,
                                                            ucg_group_member_index_t *up_fanin,
                                                            unsigned up_fanin_cnt,
                                                            ucg_group_member_index_t *down_fanin,
                                                            unsigned down_fanin_cnt,
                                                            uct_ep_h **eps)
{
    ucs_status_t status = UCS_OK;
    /* only recv (recv_terminal) */
    /* Receive from children */
    if (up_fanin_cnt == 0) {
        /* Connect this phase to its peers */
        status = ucg_builtin_binomial_tree_connect_phase(&tree->phss[tree->phs_cnt++], params, 0, eps, down_fanin,
            down_fanin_cnt, fanin_method);
        if (status != UCS_OK) {
            return status;
        }
    }

    /* only send (send_terminal) */
    /* Send to parents */
    if (down_fanin_cnt == 0) {
        /* Connect this phase to its peers */
        status = ucg_builtin_binomial_tree_connect_phase(&tree->phss[tree->phs_cnt++], params, 0, eps, up_fanin,
            up_fanin_cnt, fanin_method);
        if (status != UCS_OK) {
            return status;
        }
    }

    /* first recv then send (waypoint) */
    if (up_fanin_cnt == 1 && down_fanin_cnt > 0) {
        /* Connect this phase to its peers */
        ucg_group_member_index_t member_idx;
        for (member_idx = down_fanin_cnt; member_idx < down_fanin_cnt + up_fanin_cnt; member_idx++) {
            down_fanin[member_idx] = up_fanin[member_idx - down_fanin_cnt];
        }
        down_fanin_cnt = down_fanin_cnt + up_fanin_cnt;
        status = ucg_builtin_binomial_tree_connect_phase(&tree->phss[tree->phs_cnt++], params, 0, eps, down_fanin,
            down_fanin_cnt, fanin_method);
    }

    return status;
}

static ucs_status_t ucg_builtin_tree_connect_fanout(unsigned step_inc_cnt,
                                                    ucg_builtin_plan_t *tree,
                                                    const ucg_builtin_binomial_tree_params_t *params,
                                                    ucg_group_member_index_t *up,
                                                    unsigned up_cnt,
                                                    ucg_group_member_index_t *down,
                                                    unsigned down_cnt,
                                                    enum ucg_builtin_plan_method_type fanout_method,
                                                    uct_ep_h **eps)
{
    ucs_status_t status = UCS_OK;
    /* only recv (recv_terminal) */
    /* Receive from parents */
    if (up_cnt == 1 && down_cnt == 0) {
        /* Connect this phase to its peers */
        ucs_assert(up_cnt == 1); /* sanity check: not multi-root */
        status = ucg_builtin_binomial_tree_connect_phase(&tree->phss[tree->phs_cnt++], params, step_inc_cnt, eps,
            up, up_cnt, fanout_method);
        if (status != UCS_OK) {
            return status;
        }
    }

    /* only send (send_terminal) */
    /* Send to children */
    if (up_cnt == 0 && down_cnt > 0) {
        /* Connect this phase to its peers */
        status = ucg_builtin_binomial_tree_connect_phase(&tree->phss[tree->phs_cnt++], params, step_inc_cnt, eps,
            down, down_cnt, fanout_method);
        if (status != UCS_OK) {
            return status;
        }
    }

    /* first recv then send (waypoint) */
    if (up_cnt == 1 && down_cnt > 0) {
        /* Connect this phase to its peers */
        ucg_group_member_index_t member_idx;
        for (member_idx = up_cnt; member_idx < up_cnt + down_cnt; member_idx++) {
            up[member_idx] = down[member_idx - up_cnt];
        }
        up_cnt = up_cnt + down_cnt;
        status = ucg_builtin_binomial_tree_connect_phase(&tree->phss[tree->phs_cnt++], params, step_inc_cnt, eps,
            up, up_cnt, fanout_method);
    }

    return status;
}
static ucs_status_t ucg_builtin_binomial_tree_connect_fanin_fanout(ucg_builtin_plan_t *tree,
                                                                   const ucg_builtin_binomial_tree_params_t *params,
                                                                   ucg_group_member_index_t *up,
                                                                   unsigned up_cnt,
                                                                   ucg_group_member_index_t *down,
                                                                   unsigned down_cnt,
                                                                   ucg_group_member_index_t *up_fanin,
                                                                   unsigned up_fanin_cnt,
                                                                   ucg_group_member_index_t *down_fanin,
                                                                   unsigned down_fanin_cnt,
                                                                   unsigned ppx,
                                                                   unsigned pps,
                                                                   enum ucg_builtin_plan_method_type fanin_method,
                                                                   enum ucg_builtin_plan_method_type fanout_method,
                                                                   uct_ep_h **eps,
                                                                   ucg_builtin_topology_info_params_t *topo_params)
{
    ucs_status_t status = UCS_OK;
    unsigned phs_inc_cnt = 0;
    unsigned step_inc_cnt = 0;
    enum ucg_builtin_plan_topology_type inter_node_topo_type;
    if (ppx > 1) {
        status = ucg_builtin_binomial_tree_connect_fanin(tree, params, fanin_method, up_fanin,
                                                         up_fanin_cnt, down_fanin, down_fanin_cnt, eps);
        if (status != UCS_OK) {
            return status;
        }
    }
    tree->step_cnt++;

    if (params->topo_type == UCG_PLAN_TREE_FANIN_FANOUT) {
        inter_node_topo_type = (ucg_algo.kmtree == 1) ? UCG_PLAN_TREE_FANIN_FANOUT : UCG_PLAN_RECURSIVE;
#if ENABLE_UCG_HICOLL
        if (ucg_algo.inc && inc_used(params->group_params)) {
            inter_node_topo_type = UCG_PLAN_INC;
        }
#endif
        /* For fanin-fanout (e.g. allreduce) - copy existing connections */
        /* recursive or k-nomial tree for inter-nodes */
        /* especially for k-nomial tree, socket-aware algorithm (topo_level) ppx should be replaced by real ppn */
        if (inter_node_topo_type == UCG_PLAN_RECURSIVE && ucg_algo.topo_level == UCG_GROUP_HIERARCHY_LEVEL_L3CACHE) {
            status = ucg_builtin_binomial_tree_add_inter(tree, &tree->phss[(ppx > 1) ? 1 : 0], params, eps,
                inter_node_topo_type, &phs_inc_cnt, &step_inc_cnt, (ppx != 1) ? pps : ppx, topo_params);
        } else if (inter_node_topo_type == UCG_PLAN_INC) {
#if ENABLE_UCG_HICOLL
            status = ucg_builtin_add_inc(tree, &tree->phss[ppx > 1 ? 1 : 0], params, eps, &phs_inc_cnt, &step_inc_cnt,
                (ucg_algo.kmtree == 1 && ucg_algo.topo_level && ppx != 1) ? ppx * SPN : ppx, ucg_algo.topo_level);
#endif
        } else {
            status = ucg_builtin_binomial_tree_add_inter(tree, &tree->phss[(ppx > 1) ? 1 : 0], params, eps,
                                                         inter_node_topo_type, &phs_inc_cnt, &step_inc_cnt,
                                                         (ucg_algo.kmtree == 1 && ucg_algo.topo_level && ppx != 1) ? ppx * SPN : ppx, topo_params);
        }
        if (status != UCS_OK) {
            return status;
        }

        /* fanout phase for intra-process can not be copied directly! */
        if (inter_node_topo_type != UCG_PLAN_RECURSIVE) {
            tree->phs_cnt += phs_inc_cnt;
        }
        status = ucg_builtin_tree_connect_fanout(step_inc_cnt, tree, params, up, up_cnt, down, down_cnt, fanout_method, eps);

        if (ppx > 1) {
            tree->phss[1 + phs_inc_cnt].step_index = 1 + step_inc_cnt;
        }
    }

    return status;
}

static ucs_status_t ucg_builtin_topo_tree_connect_fanout(ucg_builtin_plan_t *tree,
                                                         const ucg_builtin_binomial_tree_params_t *params,
                                                         ucg_group_member_index_t *up,
                                                         unsigned up_cnt,
                                                         ucg_group_member_index_t *down,
                                                         unsigned down_cnt,
                                                         unsigned ppx,
                                                         enum ucg_builtin_plan_method_type fanout_method,
                                                         uct_ep_h **eps,
                                                         ucg_builtin_topology_info_params_t *topo_params)
{
    ucs_status_t status;
    unsigned phs_inc_cnt = 0;
    unsigned step_inc_cnt = 0;
    enum ucg_builtin_plan_topology_type inter_node_topo_type;
    inter_node_topo_type = UCG_PLAN_TREE_FANOUT;

    int can_use_inc = 0;
#if ENABLE_UCG_HICOLL
    can_use_inc = ucg_algo.inc && inc_used(params->group_params);
#endif
    if (can_use_inc) {
        inter_node_topo_type = UCG_PLAN_INC;
#if ENABLE_UCG_HICOLL
        status = ucg_builtin_add_inc(tree, &tree->phss[0], params, eps, &phs_inc_cnt, &step_inc_cnt, ppx,
                                     ucg_algo.topo_level);
#endif
    } else {
        /* binomial tree for inter-nodes */
        status = ucg_builtin_binomial_tree_add_inter(tree, &tree->phss[0], params, eps, inter_node_topo_type,
                                                    &phs_inc_cnt, &step_inc_cnt, ppx, topo_params);
    }

    if (status != UCS_OK) {
        return status;
    }

    /*
        * for inter node, phase count is increased by 1
        * for intra node, phase count remain the same
        */
    tree->phs_cnt += phs_inc_cnt;

    status = ucg_builtin_tree_connect_fanout(step_inc_cnt, tree, params, up, up_cnt, down, down_cnt, fanout_method, eps);

    return status;
}

STATIC_GTEST ucs_status_t ucg_builtin_non_topo_tree_connect_fanout(ucg_builtin_plan_t *tree,
                                                             const ucg_builtin_binomial_tree_params_t *params,
                                                             ucg_group_member_index_t *up,
                                                             unsigned up_cnt,
                                                             ucg_group_member_index_t *down,
                                                             unsigned down_cnt,
                                                             unsigned ppx,
                                                             enum ucg_builtin_plan_method_type fanout_method,
                                                             uct_ep_h **eps,
                                                             ucg_builtin_topology_info_params_t *topo_params)
{
    unsigned step_inc_cnt = 0;
    ucs_status_t status = UCS_OK;
    /* only recv (recv_terminal) */
    /* Receive from parents */
    if (up_cnt == 1 && down_cnt == 0) {
        /* Connect this phase to its peers */
        ucs_assert(up_cnt == 1); /* sanity check: not multi-root */
        status = ucg_builtin_binomial_tree_connect_phase(&tree->phss[tree->phs_cnt++], params, step_inc_cnt, eps,
            up, up_cnt, fanout_method);
    }
    if (status != UCS_OK) {
        return status;
    }

    /* only send (send_terminal) */
    /* Send to children */
    if (up_cnt == 0 && down_cnt > 0) {
        /* Connect this phase to its peers */
        ucg_group_member_index_t member_idx;
        ucg_group_member_index_t *down_another = NULL;
        for (member_idx = 0; member_idx < down_cnt; member_idx++) {
            down_another = down + member_idx;
            status = ucg_builtin_binomial_tree_connect_phase(&tree->phss[tree->phs_cnt++], params, step_inc_cnt,
                eps, down_another, 1, fanout_method);
            if (status != UCS_OK) {
                return status;
            }
        }
    }

    /* first recv then send (waypoint) */
    if (up_cnt == 1 && down_cnt > 0) {
        /* Connect this phase to its peers */
        status = ucg_builtin_binomial_tree_connect_phase(&tree->phss[tree->phs_cnt++], params, step_inc_cnt, eps,
            up, up_cnt, UCG_PLAN_METHOD_RECV_TERMINAL);
        if (status != UCS_OK) {
            return status;
        }

        ucg_group_member_index_t member_idx;
        ucg_group_member_index_t *down_another = NULL;
        for (member_idx = 0; member_idx < down_cnt; member_idx++) {
            down_another = down + member_idx;
            status = ucg_builtin_binomial_tree_connect_phase(&tree->phss[tree->phs_cnt++], params, step_inc_cnt,
                eps, down_another, 1, UCG_PLAN_METHOD_SEND_TERMINAL);
            if (status != UCS_OK) {
                return status;
            }
        }
    }
    return status;
}

static inline ucs_status_t ucg_builtin_binomial_tree_connect_fanout(ucg_builtin_plan_t *tree,
                                                                    const ucg_builtin_binomial_tree_params_t *params,
                                                                    ucg_group_member_index_t *up,
                                                                    unsigned up_cnt,
                                                                    ucg_group_member_index_t *down,
                                                                    unsigned down_cnt,
                                                                    ucg_group_member_index_t *up_fanin,
                                                                    unsigned up_fanin_cnt,
                                                                    ucg_group_member_index_t *down_fanin,
                                                                    unsigned down_fanin_cnt,
                                                                    unsigned ppx,
                                                                    enum ucg_builtin_plan_method_type fanin_method,
                                                                    enum ucg_builtin_plan_method_type fanout_method,
                                                                    uct_ep_h **eps,
                                                                    ucg_builtin_topology_info_params_t *topo_params)
{
    ucs_status_t status;
    if (ucg_algo.topo) {
        status = ucg_builtin_topo_tree_connect_fanout(tree, params, up, up_cnt, down, down_cnt, ppx, fanout_method, eps, topo_params);
    } else {
        status = ucg_builtin_non_topo_tree_connect_fanout(tree, params, up, up_cnt, down, down_cnt,
                                                          ppx, fanout_method, eps, topo_params);
    }

    return status;
}

static ucs_status_t ucg_builtin_binomial_tree_connect(ucg_builtin_plan_t *tree,
                                                      const ucg_builtin_binomial_tree_params_t *params,
                                                      size_t *alloc_size,
                                                      ucg_group_member_index_t *up,
                                                      unsigned up_cnt,
                                                      ucg_group_member_index_t *down,
                                                      unsigned down_cnt,
                                                      ucg_group_member_index_t *up_fanin,
                                                      unsigned up_fanin_cnt,
                                                      ucg_group_member_index_t *down_fanin,
                                                      unsigned down_fanin_cnt,
                                                      unsigned ppx,
                                                      unsigned pps,
                                                      ucg_builtin_topology_info_params_t *topo_params)
{
    enum ucg_collective_modifiers mod = params->coll_type->modifiers;
    ucs_status_t status               = UCS_OK;
    uct_ep_h *first_ep                = (uct_ep_h*)(&tree->phss[MAX_PHASES]);
    uct_ep_h *eps                     = first_ep;
    tree->phs_cnt                     = 0;
    if (ppx > 1) {
        ucs_assert(up_cnt + down_cnt > 0);
    }

    enum ucg_builtin_plan_method_type fanin_method = ucg_builtin_calculate_plan_method_type(mod, UCG_GROUP_COLLECTIVE_MODIFIER_AGGREGATE, up_fanin_cnt, down_fanin_cnt);
    enum ucg_builtin_plan_method_type fanout_method = ucg_builtin_calculate_plan_method_type(mod, UCG_GROUP_COLLECTIVE_MODIFIER_BROADCAST, up_cnt, down_cnt);

    switch (params->topo_type) {
        case UCG_PLAN_TREE_FANIN:
        case UCG_PLAN_TREE_FANIN_FANOUT:
            status = ucg_builtin_binomial_tree_connect_fanin_fanout(tree, params, up, up_cnt, down, down_cnt,
                                                                    up_fanin, up_fanin_cnt, down_fanin,
                                                                    down_fanin_cnt, ppx, pps, fanin_method,
                                                                    fanout_method, &eps, topo_params);
            break;

        case UCG_PLAN_TREE_FANOUT:
            status = ucg_builtin_binomial_tree_connect_fanout(tree, params, up, up_cnt, down, down_cnt,
                                                              up_fanin, up_fanin_cnt, down_fanin,
                                                              down_fanin_cnt, ppx, fanin_method,
                                                              fanout_method, &eps, topo_params);
            break;

        default:
            status = UCS_ERR_INVALID_PARAM;
            break;
    }

    if (status != UCS_OK) {
        return status;
    }

    tree->ep_cnt = eps - first_ep;
    size_t ep_size = tree->ep_cnt * sizeof(*eps);
    memmove(&tree->phss[tree->phs_cnt], first_ep, ep_size);
    *alloc_size = (void*)first_ep + ep_size - (void*)tree;
    return UCS_OK;
}

static ucs_status_t ucg_builtin_kinomial_tree_build_intra(const ucg_builtin_binomial_tree_params_t *params,
                                                          ucg_group_member_index_t *member_list,
                                                          unsigned root,
                                                          ucg_group_member_index_t rank,
                                                          ucg_group_member_index_t *up,
                                                          unsigned *up_cnt,
                                                          ucg_group_member_index_t *down,
                                                          unsigned *down_cnt,
                                                          ucg_group_member_index_t *up_fanin,
                                                          unsigned *up_fanin_cnt,
                                                          ucg_group_member_index_t *down_fanin,
                                                          unsigned *down_fanin_cnt,
                                                          const unsigned *ppx,
                                                          const unsigned *ppn,
                                                          ucg_builtin_plan_t *tree)
{
    ucs_status_t status;
    /* left-most k-nomial tree for FANOUT */
    status = ucg_builtin_kmtree_algo_build(member_list, *ppx, rank, (root % *ppx),
        params->tree_degree_intra_fanout, UCG_PLAN_LEFT_MOST_TREE, up, up_cnt, down, down_cnt);
    if (status != UCS_OK) {
        return status;
    }
    /* right-most k-nomial tree for FANIN */
    status = ucg_builtin_kmtree_algo_build(member_list, *ppx, rank, (root % *ppx), params->tree_degree_intra_fanin,
                                           UCG_PLAN_RIGHT_MOST_TREE, up_fanin, up_fanin_cnt, down_fanin, down_fanin_cnt);
    if (status != UCS_OK) {
        return status;
    }

    /* for topo_level, the leader located at 2nd socket should be changed to waypoint type */
    if (ucg_algo.topo_level && ucg_algo.kmtree) {
        status = ucg_builtin_connect_leader(tree->super.my_index, *ppx, *ppn, up, up_cnt, down, down_cnt,
            up_fanin, up_fanin_cnt, down_fanin, down_fanin_cnt);
    }

    if (ucg_builtin_need_calate_position(params->coll_type, *up_fanin_cnt, params->ctx, UCG_PLAN_TREE_FANIN)) {
        int degree = ((params->group_params->node_index[rank] == params->group_params->node_index[up_fanin[0]])
            ? params->tree_degree_intra_fanin : params->tree_degree_inter_fanin);
        short upOffset = ucg_get_tree_buffer_pos(rank, up_fanin[0], root, *ppx, degree, member_list);
        tree->super.up_offset = ((upOffset == -1) ? *down_fanin_cnt : upOffset);
        ucs_debug("degree:%d, myrank:%lu, uprank:%lu, offset:%d, down_fanin_cnt:%u, root:%u, size:%u",
                   degree, rank, up_fanin[0], tree->super.up_offset, *down_fanin_cnt, root, *ppx);
    }
    return status;
}

ucs_status_t ucg_builtin_bmtree_algo_build_fanin_fanout(const ucg_group_member_index_t *member_list,
                                                        unsigned size,
                                                        ucg_group_member_index_t rank,
                                                        unsigned root_idx,
                                                        ucg_group_member_index_t *up,
                                                        unsigned *up_cnt,
                                                        ucg_group_member_index_t *down,
                                                        unsigned *down_cnt,
                                                        ucg_group_member_index_t *up_fanin,
                                                        unsigned *up_fanin_cnt,
                                                        ucg_group_member_index_t *down_fanin,
                                                        unsigned *down_fanin_cnt)
{
    ucs_status_t status;
    /* left-most Binomial tree for FANOUT */
    status = ucg_builtin_bmtree_algo_build(member_list, size, rank, root_idx, UCG_PLAN_LEFT_MOST_TREE, up,
                                           up_cnt, down, down_cnt);
    if (status != UCS_OK) {
        return status;
    }

    /* right-most Binomial tree for FANIN */
    status = ucg_builtin_bmtree_algo_build(member_list, size, rank, root_idx, UCG_PLAN_RIGHT_MOST_TREE, up_fanin,
                                           up_fanin_cnt, down_fanin, down_fanin_cnt);
    return status;
}

static ucs_status_t ucg_builtin_binomial_tree_build_intra(const ucg_builtin_binomial_tree_params_t *params,
                                                          ucg_group_member_index_t *member_list,
                                                          unsigned root,
                                                          ucg_group_member_index_t rank,
                                                          ucg_group_member_index_t *up,
                                                          unsigned *up_cnt,
                                                          ucg_group_member_index_t *down,
                                                          unsigned *down_cnt,
                                                          ucg_group_member_index_t *up_fanin,
                                                          unsigned *up_fanin_cnt,
                                                          ucg_group_member_index_t *down_fanin,
                                                          unsigned *down_fanin_cnt,
                                                          const unsigned *ppx,
                                                          const unsigned *pps,
                                                          unsigned *ppn,
                                                          ucg_builtin_plan_t *tree)
{
    unsigned cache3_per_socket = 0;
    /* calculate how much L3cache per socket */
    if (ucg_algo.topo_level == UCG_GROUP_HIERARCHY_LEVEL_L3CACHE) {
        cache3_per_socket = *pps / *ppx;
    }

    unsigned is_use_topo_params = (ucg_algo.topo_level == UCG_GROUP_HIERARCHY_LEVEL_NODE && !ucg_algo.kmtree) ? 1 : 0;
    /* index of root must be 0 in topo_params */
    unsigned root_idx = (is_use_topo_params) ? 0 : (root % *ppx);
    ucs_status_t status;
    status = ucg_builtin_bmtree_algo_build_fanin_fanout(member_list, *ppx, rank, root_idx, up, up_cnt, down, down_cnt, up_fanin,
                                                        up_fanin_cnt, down_fanin, down_fanin_cnt);
    if (status != UCS_OK) {
        return status;
    }

    if (ucg_algo.topo_level == UCG_GROUP_HIERARCHY_LEVEL_L3CACHE && cache3_per_socket != 1) {
        status = ucg_builtin_connect_leader(tree->super.my_index, *ppx, *pps, up, up_cnt, down, down_cnt,
            up_fanin, up_fanin_cnt, down_fanin, down_fanin_cnt);
    }

    if (ucg_builtin_need_calate_position(params->coll_type, *up_fanin_cnt, params->ctx, UCG_PLAN_TREE_FANIN)) {
        int degree = ((params->group_params->node_index[rank] == params->group_params->node_index[up_fanin[0]])
            ? params->tree_degree_intra_fanin : params->tree_degree_inter_fanin);
        tree->super.up_offset = ucg_get_tree_buffer_pos(rank, up_fanin[0], root, *ppx, degree, member_list);
        ucs_debug("degree:%d, myrank:%lu, uprank:%lu, offset:%d, root:%u, size:%u",
                   degree, rank, up_fanin[0], tree->super.up_offset, root, *ppx);
    }
    return status;
}

static void ucg_builtin_log_member_list(const unsigned *ppx,
                                        ucg_group_member_index_t rank,
                                        ucg_group_member_index_t *member_list)
{
    ucg_group_member_index_t member_idx;
    for (member_idx = 0; member_idx < *ppx; member_idx++) {
        ucs_info("rank #%u's member_list[%lu] %lu", (unsigned)rank, member_idx, member_list[member_idx]);
    }
}

static void ucg_builtin_prepare_member_idx(const ucg_builtin_binomial_tree_params_t *params,
                                           ucg_builtin_topology_info_params_t *topo_params,
                                           enum ucg_group_member_distance domain_distance,
                                           const unsigned *ppx,
                                           ucg_group_member_index_t rank,
                                           ucg_group_member_index_t *member_list)
{
    unsigned k;
    unsigned is_use_topo_params = (ucg_algo.topo_level == UCG_GROUP_HIERARCHY_LEVEL_NODE && !ucg_algo.kmtree) ? 1 : 0;
    if (is_use_topo_params) {
        for (k = 0; k < *ppx; k++) {
            member_list[k] = topo_params->rank_same_node[k];
        }
    } else {
        k = 0;
        ucg_group_member_index_t member_idx;
        for (member_idx = 0; member_idx < params->group_params->member_count; member_idx++) {
            if (ucg_builtin_get_distance(params->group_params, params->group_params->member_index,
                                         member_idx) <= domain_distance) {
                member_list[k++] = member_idx;
            }
        }
        ucg_builtin_log_member_list(ppx, rank, member_list);
    }
}

static ucs_status_t ucg_builtin_topo_tree_build(const ucg_builtin_binomial_tree_params_t *params,
                                                ucg_builtin_topology_info_params_t *topo_params,
                                                enum ucg_group_member_distance domain_distance,
                                                unsigned root,
                                                ucg_group_member_index_t rank,
                                                ucg_group_member_index_t *up,
                                                unsigned *up_cnt,
                                                ucg_group_member_index_t *down,
                                                unsigned *down_cnt,
                                                ucg_group_member_index_t *up_fanin,
                                                unsigned *up_fanin_cnt,
                                                ucg_group_member_index_t *down_fanin,
                                                unsigned *down_fanin_cnt,
                                                unsigned *ppx,
                                                unsigned *pps,
                                                unsigned *ppn,
                                                ucg_builtin_plan_t *tree)
{
    ucs_status_t status = UCS_OK;

    /* check ppn obtained from distance equal to topo->params */
    if (*ppn != topo_params->ppn_cnt) {
        ucs_error("ppn conflicit with each other");
        return UCS_ERR_INVALID_PARAM;
    }

    /*
        Socket-aware algorithm:
            special case 1: socket unbalance case in socket-aware algorithm;
               ( ppn = 2 * pps or ppn = pps is OK for socket-aware)
            special case 2: allreduce algo(8) ppx = 1 or pps = ppn;
            solution: change topo-aware level: socket -> node.
    */
    /* case 2 */
    if (ucg_algo.topo_level == UCG_GROUP_HIERARCHY_LEVEL_SOCKET && ucg_algo.kmtree && (*ppx == 1 || *pps == *ppn)) {
        ucg_algo.topo_level = UCG_GROUP_HIERARCHY_LEVEL_NODE;
        choose_distance_from_topo_aware_level(&domain_distance);
        *ppx = *ppn;
    }

    ucs_info("bmtree %u kmtree %u kmtree_intra %u recur %u bruck %u topo %u level %u ring %u pipe %u",
             ucg_algo.bmtree, ucg_algo.kmtree, ucg_algo.kmtree_intra, ucg_algo.recursive, ucg_algo.bruck,
             ucg_algo.topo, (unsigned)ucg_algo.topo_level, ucg_algo.ring, ucg_algo.pipeline);

    /* construct member list when topo_aware */
    size_t alloc_size = sizeof(ucg_group_member_index_t) * (*ppx);
    ucg_group_member_index_t *member_list = (ucg_group_member_index_t *)(UCS_ALLOC_CHECK(alloc_size, "member list"));
    memset(member_list, 0, alloc_size);

    ucg_builtin_prepare_member_idx(params, topo_params, domain_distance, ppx, rank, member_list);

    if (*ppx > 1) {
        if (ucg_algo.kmtree_intra) {
            status = ucg_builtin_kinomial_tree_build_intra(params, member_list, root, rank, up,
                                                           up_cnt, down, down_cnt, up_fanin,
                                                           up_fanin_cnt, down_fanin, down_fanin_cnt, ppx, ppn, tree);
        } else {
            status = ucg_builtin_binomial_tree_build_intra(params, member_list, root, rank, up,
                                                           up_cnt, down, down_cnt, up_fanin, up_fanin_cnt,
                                                           down_fanin, down_fanin_cnt, ppx, pps, ppn, tree);
        }
    } else if (*ppx == 1) {
        up_cnt = 0;
        down_cnt = 0;
    }
    ucs_free(member_list);
    member_list = NULL;

    return status;
}

static ucs_status_t ucg_builtin_binomial_tree_algo_build(ucg_group_member_index_t *member_list,
                                                         unsigned size,
                                                         unsigned rank,
                                                         unsigned root,
                                                         ucg_group_member_index_t *up,
                                                         unsigned *up_cnt,
                                                         ucg_group_member_index_t *down,
                                                         unsigned *down_cnt,
                                                         ucg_group_member_index_t *up_fanin,
                                                         unsigned *up_fanin_cnt,
                                                         ucg_group_member_index_t *down_fanin,
                                                         unsigned *down_fanin_cnt)
{
    ucs_status_t status;

    status = ucg_builtin_bmtree_algo_build(member_list, size, rank, root, UCG_PLAN_LEFT_MOST_TREE, up, up_cnt, down, down_cnt);
    if (status != UCS_OK) {
        ucs_free(member_list);
        member_list = NULL;
        return status;
    }

    status = ucg_builtin_bmtree_algo_build(member_list, size, rank, root, UCG_PLAN_RIGHT_MOST_TREE, up_fanin,
                                           up_fanin_cnt, down_fanin, down_fanin_cnt);
    ucs_free(member_list);
    member_list = NULL;
    return status;
}

static ucs_status_t ucg_builtin_tree_build(const ucg_builtin_binomial_tree_params_t *params,
                                           ucg_builtin_topology_info_params_t *topo_params,
                                           ucg_group_member_index_t rank,
                                           ucg_group_member_index_t *up,
                                           unsigned *up_cnt,
                                           ucg_group_member_index_t *down,
                                           unsigned *down_cnt,
                                           ucg_group_member_index_t *up_fanin,
                                           unsigned *up_fanin_cnt,
                                           ucg_group_member_index_t *down_fanin,
                                           unsigned *down_fanin_cnt,
                                           unsigned size,
                                           unsigned root,
                                           unsigned *ppx,
                                           unsigned *pps,
                                           unsigned *ppn,
                                           ucg_builtin_plan_t *tree)
{
    ucs_status_t status = UCS_OK;
    if (ucg_algo.topo) {
        /* calc processes per topo-aware unit (ppx)        */
        /* node-aware:    ppx = ppn (processes per node)   */
        /* socket-aware:  ppx = pps (processes per socket) */
        /* L3cache-aware: ppx = ppl (processes per L3cache) */
        enum ucg_group_member_distance domain_distance = UCG_GROUP_MEMBER_DISTANCE_HOST;
        choose_distance_from_topo_aware_level(&domain_distance);
        *ppx = ucg_builtin_calculate_ppx(params->group_params, domain_distance);
        *ppn = ucg_builtin_calculate_ppx(params->group_params, UCG_GROUP_MEMBER_DISTANCE_HOST);
        *pps = ucg_builtin_calculate_ppx(params->group_params, UCG_GROUP_MEMBER_DISTANCE_SOCKET);
        status = ucg_builtin_topo_tree_build(params, topo_params, domain_distance, root, rank, up, up_cnt, down,
                                             down_cnt, up_fanin, up_fanin_cnt,
                                             down_fanin, down_fanin_cnt,
                                             ppx, pps, ppn, tree);
    } else {
        /* create member_list for un-topo */
        size_t alloc_size = sizeof(ucg_group_member_index_t) * size;
        ucg_group_member_index_t *member_list = (ucg_group_member_index_t *)(UCS_ALLOC_CHECK(alloc_size, "member list"));
        memset(member_list, 0, alloc_size);

        ucg_group_member_index_t member_idx;
        for (member_idx = 0; member_idx < params->group_params->member_count; member_idx++) {
            member_list[member_idx] = member_idx;
        }
        /* binomial tree */
        status = ucg_builtin_binomial_tree_algo_build(member_list, size, rank, root, up, up_cnt, down, down_cnt,
                                                      up_fanin, up_fanin_cnt, down_fanin, down_fanin_cnt);
    }

    return status;
}

static void ucg_builtin_binomial_tree_free_topo_info(ucg_builtin_topology_info_params_t **topo_params)
{
    if (*topo_params != NULL) {
        if ((*topo_params)->subroot_array != NULL) {
            ucs_free((*topo_params)->subroot_array);
            (*topo_params)->subroot_array = NULL;
        }
        if ((*topo_params)->rank_same_node != NULL) {
            ucs_free((*topo_params)->rank_same_node);
            (*topo_params)->rank_same_node = NULL;
        }
    ucs_free(*topo_params);
    *topo_params = NULL;
    }
}


/* ***************************************************************************
*                                                                           *
*                               Binomial tree                               *
*                                                                           *
*************************************************************************** */
/*
*
*    p == 2                      p = 4                    p = 8
*      0                           0                        0
*     /                            | \                    / | \
*    1                             2  1                  4  2  1
*                                     |                     |  | \
*                                     3                     6  5  3
*                                                                 |
*                                                                 7
*/
static ucs_status_t ucg_builtin_binomial_tree_build(const ucg_builtin_binomial_tree_params_t *params,
                                                    ucg_builtin_plan_t *tree,
                                                    size_t *alloc_size)
{
    ucs_status_t status;
    ucg_group_member_index_t up[MAX_PEERS] = {0};
    ucg_group_member_index_t down[MAX_PEERS] = {0};
    unsigned up_cnt = 0;
    unsigned down_cnt = 0;

    ucg_group_member_index_t up_fanin[MAX_PEERS] = {0};
    ucg_group_member_index_t down_fanin[MAX_PEERS] = {0};
    unsigned up_fanin_cnt = 0;
    unsigned down_fanin_cnt = 0;

    ucg_group_member_index_t rank;
    unsigned size, root;
    unsigned ppx = 0;
    unsigned pps = 0;
    unsigned ppn = 0;

    root = params->root;
    size = params->group_params->member_count;

    /* find my own rank */
    status = ucg_builtin_find_myself(params->group_params, &rank);
    if (status != UCS_OK) {
        return status;
    }
    tree->super.my_index = rank;

    /* topology information obtain from ompi layer */
    ucg_builtin_topology_info_params_t *topo_params = (ucg_builtin_topology_info_params_t *)UCS_ALLOC_CHECK(
        sizeof(ucg_builtin_topology_info_params_t), "topo params");
    status = ucg_builtin_topology_info_create(topo_params, params->group_params, params->root);
    if (status != UCS_OK) {
        ucg_builtin_binomial_tree_free_topo_info(&topo_params);
        ucs_error("Invalid paramters in topological info create");
        return status;
    }

    status = ucg_builtin_tree_build(params, topo_params, rank, up, &up_cnt,
                                    down, &down_cnt, up_fanin, &up_fanin_cnt,
                                    down_fanin, &down_fanin_cnt, size, root,
                                    &ppx, &pps, &ppn, tree);
    if (status != UCS_OK) {
        ucg_builtin_binomial_tree_free_topo_info(&topo_params);
        ucs_error("Invalid paramters in tree build");
        return status;
    }

    /* Some output, for informational purposes */
    ucg_builtin_binomial_tree_log_topology(tree->super.my_index, up_cnt, down_cnt, up, down);

    /* fill in the tree phases while establishing the connections */
    status = ucg_builtin_binomial_tree_connect(tree, params,
                                               alloc_size, up, up_cnt, down, down_cnt, up_fanin, up_fanin_cnt,
                                               down_fanin, down_fanin_cnt, ppx, pps, topo_params);

    ucg_builtin_binomial_tree_free_topo_info(&topo_params);
    return status;
}

static void ucg_calcu_node_cnt(unsigned *node_cnt, const ucg_group_params_t *group_params)
{
    ucg_group_member_index_t member_idx;
    unsigned node_idx;

    for (member_idx = 0; member_idx < group_params->member_count; member_idx++) {
        node_idx = group_params->node_index[member_idx];
        if (*node_cnt < node_idx) {
            *node_cnt = node_idx;
        }
    }
    (*node_cnt)++;
}

ucs_status_t ucg_builtin_binomial_tree_create(ucg_builtin_group_ctx_t *ctx,
                                              enum ucg_builtin_plan_topology_type plan_topo_type,
                                              const ucg_builtin_config_t *config,
                                              const ucg_group_params_t *group_params,
                                              const ucg_collective_type_t *coll_type,
                                              ucg_builtin_plan_t **plan_p)
{
    /* Allocate worst-case memory footprint, resized down later */
    size_t alloc_size = sizeof(ucg_builtin_plan_t) +
            MAX_PHASES * sizeof(ucg_builtin_plan_phase_t) + MAX_PEERS * sizeof(uct_ep_h);
    ucg_builtin_plan_t *tree = (ucg_builtin_plan_t*)UCS_ALLOC_CHECK(alloc_size, "tree topology");
    memset(tree, 0, alloc_size);
    tree->phs_cnt = 0; /* will be incremented with usage */
    unsigned node_cnt = 0;

    /* node count */
    ucg_calcu_node_cnt(&node_cnt, group_params);

    /* tree discovery and construction, by phase */
    ucg_builtin_binomial_tree_params_t params = {
        .ctx = ctx,
        .coll_type = coll_type,
        .topo_type = plan_topo_type,
        .group_params = group_params,
        .root = coll_type->root,
        .tree_degree_inter_fanout = config->bmtree.degree_inter_fanout,
        .tree_degree_inter_fanin  = config->bmtree.degree_inter_fanin,
        .tree_degree_intra_fanout = config->bmtree.degree_intra_fanout,
        .tree_degree_intra_fanin  = config->bmtree.degree_intra_fanin
    };

    ucs_status_t ret = ucg_builtin_binomial_tree_build(&params, tree, &alloc_size);
    if (ret != UCS_OK) {
        ucs_free(tree);
        tree = NULL;
        ucs_error("Error in binomial tree create: %d", (int)ret);
        return ret;
    }

    /* Reduce the allocation size according to actual usage */
    *plan_p = tree;
    ucs_assert(*plan_p != NULL); /* only reduces size - should never fail */
    return UCS_OK;
}
