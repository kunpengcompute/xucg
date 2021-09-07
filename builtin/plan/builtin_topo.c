/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019-2021.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#include <ucs/debug/log.h>
#include <ucs/debug/assert.h>
#include <ucs/debug/memtrack.h>
#include <uct/api/uct_def.h>

#include "builtin_plan.h"
#include "builtin_topo.h"

static inline void ucg_builtin_topo_set_nodecnt(ucg_builtin_topo_params_t *topo_params,
                                                ucg_group_member_index_t max_node_idx)
{
    topo_params->node_cnt = ++max_node_idx;
}
static inline void ucg_builtin_topo_set_localprocs(ucg_builtin_topo_params_t *topo_params,
                                                ucg_group_member_index_t proc_cnt)
{
    topo_params->num_local_procs = proc_cnt;
}

static inline void ucg_builtin_topo_set_membercnt(ucg_builtin_topo_params_t *topo_params,
                                                ucg_group_member_index_t pps)
{
    topo_params->local.socket.member_cnt = pps;
}

static inline void ucg_builtin_topo_set_socketnum(ucg_builtin_topo_params_t *topo_params,
                                                ucg_group_member_index_t proc_cnt,
                                                ucg_group_member_index_t pps)
{
    topo_params->local.socket.num = (pps == 0) ? 0 : (proc_cnt / pps);
}

static inline void ucg_builtin_topo_set(ucg_builtin_topo_params_t *topo_params,
                                        ucg_group_member_index_t proc_cnt,
                                        ucg_group_member_index_t max_node_idx,
                                        ucg_group_member_index_t pps)
{
    ucg_builtin_topo_set_nodecnt(topo_params, max_node_idx);
    ucg_builtin_topo_set_localprocs(topo_params, proc_cnt);
    ucg_builtin_topo_set_membercnt(topo_params, pps);
    ucg_builtin_topo_set_socketnum(topo_params, proc_cnt, pps);
}

static inline void ucg_builtin_topo_init_local_leaders(ucg_builtin_topo_params_t *topo_params,
                                        ucg_group_member_index_t init_member_idx,
                                        ucg_group_member_index_t socket_idx)
{
    topo_params->local.socket.leaders[socket_idx] = init_member_idx;
}

static inline void ucg_builtin_topo_init_local_members(ucg_builtin_topo_params_t *topo_params,
                                        ucg_group_member_index_t init_member_idx,
                                        ucg_group_member_index_t proc_cnt)
{
    topo_params->local_members[proc_cnt] = init_member_idx;
}

static inline void ucg_builtin_topo_init_socket_members(ucg_builtin_topo_params_t *topo_params,
                                        ucg_group_member_index_t init_member_idx,
                                        ucg_group_member_index_t pps)
{
    topo_params->local.socket.members[pps] = init_member_idx;
}

static void ucg_builtin_topo_init_local(ucg_builtin_topo_params_t *topo_params,
                                        ucg_group_member_index_t init_member_idx,
                                        ucg_group_member_index_t proc_cnt,
                                        ucg_group_member_index_t pps,
                                        ucg_group_member_index_t socket_idx)
{
    for (socket_idx = 0; socket_idx < topo_params->local.socket.num; socket_idx++) {
        ucg_builtin_topo_init_local_leaders(topo_params, init_member_idx, socket_idx);
    }

    for (proc_cnt = 0; proc_cnt < topo_params->num_local_procs; proc_cnt++) {
        ucg_builtin_topo_init_local_members(topo_params, init_member_idx, proc_cnt);
    }

    for (pps = 0; pps < topo_params->local.socket.member_cnt; pps++) {
        ucg_builtin_topo_init_socket_members(topo_params, init_member_idx, pps);
    }
}

static inline void ucg_builtin_topo_init_node_leaders(ucg_builtin_topo_params_t *topo_params,
                                        ucg_group_member_index_t init_member_idx,
                                        ucg_group_member_index_t node_idx)
{
    topo_params->node_leaders[node_idx] = init_member_idx;
}

static void ucg_builtin_topo_init(const ucg_group_params_t *group_params,
                                        ucg_group_member_index_t member_idx,
                                        ucg_builtin_topo_params_t *topo_params,
                                        ucg_group_member_index_t init_member_idx,
                                        ucg_group_member_index_t node_idx,
                                        ucg_group_member_index_t proc_cnt,
                                        ucg_group_member_index_t my_rank,
                                        ucg_group_member_index_t pps,
                                        ucg_group_member_index_t socket_idx)
{
    for (node_idx = 0; node_idx < topo_params->node_cnt; node_idx++) {
        ucg_builtin_topo_init_node_leaders(topo_params, init_member_idx, node_idx);
    }
    ucg_builtin_topo_init_local(topo_params, init_member_idx, proc_cnt, pps, socket_idx);

    /* rank list on local node */
    for (member_idx = 0, proc_cnt = 0; member_idx < group_params->member_count; member_idx++) {
        if (group_params->node_index[member_idx] == group_params->node_index[my_rank]) {
            topo_params->local_members[proc_cnt++] = member_idx;
        }
    }
    ucs_assert(proc_cnt == topo_params->num_local_procs);
}

static void ucg_builtin_topo_leader(const ucg_group_params_t *group_params,
                                    ucg_group_member_index_t member_idx,
                                    ucg_builtin_topo_params_t *topo_params,
                                    ucg_group_member_index_t node_idx)
{
    topo_params->node_leaders[0] = 0;
    for (member_idx = 1, node_idx = 0; member_idx < group_params->member_count; member_idx++) {
        if (group_params->node_index[member_idx] > node_idx) {
            node_idx++;
            topo_params->node_leaders[node_idx] = member_idx;
        }
    }
    ucs_assert(node_idx + 1 == topo_params->node_cnt);
}

static  void  ucg_builtin_topo_list(const ucg_group_params_t *group_params,
                                        ucg_builtin_topo_params_t *topo_params)
{
    ucg_group_member_index_t member_idx;
    ucg_group_member_index_t pps;
    enum ucg_group_member_distance next_distance;
    for (member_idx = 0, pps = 0; member_idx < group_params->member_count; member_idx++) {
        next_distance = ucg_builtin_get_distance(group_params, group_params->member_index, member_idx);
        if (next_distance <= UCG_GROUP_MEMBER_DISTANCE_SOCKET) {
            topo_params->local.socket.members[pps++] = member_idx;
        }
    }
    ucs_assert(pps == topo_params->local.socket.member_cnt);
}

static inline void ucg_builtin_topo_get(const ucg_group_params_t *group_params,
                                        ucg_builtin_topo_params_t *topo_params)
{
    /* set my own index */
    topo_params->my_index = group_params->member_index;
    /* set total number of processes */
    topo_params->num_procs = group_params->member_count;
}

static void ucg_builtin_topo_subroot(ucg_group_member_index_t socket_idx,
                                        ucg_builtin_topo_params_t *topo_params,
                                        ucg_group_member_index_t pps)
{
    if (pps != 0) {
        for (socket_idx = 0; socket_idx < topo_params->local.socket.num; socket_idx++) {
            topo_params->local.socket.leaders[socket_idx] =
            topo_params->my_index % pps + socket_idx * pps + topo_params->local_members[0];
        }
        /* set my socket index */
        topo_params->local.socket.idx = (topo_params->my_index - topo_params->local_members[0]) / pps;
    }
}

static inline void free_local_members(ucg_builtin_topo_params_t *topo_params)
{
    ucs_free(topo_params->local_members);
    topo_params->local_members = NULL;
}

static inline void free_node_leaders(ucg_builtin_topo_params_t *topo_params)
{
    ucs_free(topo_params->node_leaders);
    topo_params->node_leaders = NULL;
}

static inline void free_socket_members(ucg_builtin_topo_params_t *topo_params)
{
    ucs_free(topo_params->local.socket.members);
    topo_params->local.socket.members = NULL;
}

ucs_status_t ucg_builtin_query_topo(const ucg_group_params_t *group_params,
                                        ucg_builtin_topo_params_t *topo_params)
{
    ucg_group_member_index_t member_idx;
    ucg_group_member_index_t node_idx = 0;
    ucg_group_member_index_t socket_idx = 0;
    ucg_group_member_index_t proc_cnt = 0;
    ucg_group_member_index_t pps = 0;
    ucg_group_member_index_t max_node_idx = 0;
    ucg_group_member_index_t my_rank = group_params->member_index;
    ucg_group_member_index_t init_member_idx = (ucg_group_member_index_t)-1;
    enum ucg_group_member_distance next_distance;

    ucg_builtin_topo_get(group_params, topo_params);
    for (member_idx = 0; member_idx < group_params->member_count; member_idx++) {
        if (max_node_idx < group_params->node_index[member_idx]) {
            max_node_idx = group_params->node_index[member_idx];
        }
        if (group_params->node_index[member_idx] == group_params->node_index[my_rank]) {
            proc_cnt++;
        }
        /* calculate process number per socket (pps)*/
        next_distance = ucg_builtin_get_distance(group_params, group_params->member_index, member_idx);
        if (next_distance <= UCG_GROUP_MEMBER_DISTANCE_SOCKET) {
            pps++;
        }
    }

    ucg_builtin_topo_set(topo_params, proc_cnt, max_node_idx, pps);

    /* allocate socket_members & local socket leaders */
    size_t alloc_size = sizeof(ucg_group_member_index_t) * topo_params->num_local_procs;
    topo_params->local_members = (ucg_group_member_index_t *)UCS_ALLOC_CHECK(alloc_size, "rank in same node");

    alloc_size = sizeof(ucg_group_member_index_t) * topo_params->node_cnt;
    topo_params->node_leaders = (ucg_group_member_index_t *)ucs_malloc(alloc_size, "node leaders list");
    if (topo_params->node_leaders == NULL) {
        free_local_members(topo_params);
        return UCS_ERR_INVALID_PARAM;
    }

    /* allocate local_members & node_leaders */
    alloc_size = sizeof(ucg_group_member_index_t) * topo_params->local.socket.member_cnt;
    topo_params->local.socket.members = (ucg_group_member_index_t *)ucs_malloc(alloc_size, "rank in same socket");
    if (topo_params->local.socket.members == NULL) {
        free_local_members(topo_params);
        free_node_leaders(topo_params);
        return UCS_ERR_INVALID_PARAM;
    }

    alloc_size = sizeof(ucg_group_member_index_t) * topo_params->local.socket.num;
    topo_params->local.socket.leaders = (ucg_group_member_index_t *)ucs_malloc(alloc_size, "socket leaders list");
    if (topo_params->local.socket.leaders == NULL) {
        free_local_members(topo_params);
        free_node_leaders(topo_params);
        free_socket_members(topo_params);
        return UCS_ERR_INVALID_PARAM;
    }

    /* Initialization */
    ucg_builtin_topo_init(group_params, member_idx, topo_params, init_member_idx,
                         node_idx, proc_cnt, my_rank, pps, socket_idx);

    /* node leaders: Pick first rank number as subroot in same node */
    ucg_builtin_topo_leader(group_params, member_idx, topo_params, node_idx);

    /* rank list on own socket */
    ucg_builtin_topo_list(group_params, topo_params);

    /**
     * socket leaders: Pick first rank number as subroot in same socket
     * with strong assumption that pps is uniform!
     * */
    ucg_builtin_topo_subroot(socket_idx, topo_params, pps);
    return UCS_OK;
}

void ucg_builtin_destroy_topo(ucg_builtin_topo_params_t *topo_params)
{
    if (topo_params->local_members) {
        ucs_free(topo_params->local_members);
        topo_params->local_members = NULL;
    }

    if (topo_params->node_leaders) {
        ucs_free(topo_params->node_leaders);
        topo_params->node_leaders = NULL;
    }

    if (topo_params->local.socket.members) {
        ucs_free(topo_params->local.socket.members);
        topo_params->local.socket.members = NULL;
    }

    if (topo_params->local.socket.leaders) {
        ucs_free(topo_params->local.socket.leaders);
        topo_params->local.socket.leaders = NULL;
    }
    ucg_builtin_free((void **)&topo_params);
}
