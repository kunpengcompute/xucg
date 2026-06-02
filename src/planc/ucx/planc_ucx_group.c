/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2024. All rights reserved.
 */

#include "planc_ucx_group.h"
#include "planc_ucx_global.h"

ucg_status_t ucg_planc_ucx_group_create(ucg_planc_context_h context,
                                        const ucg_planc_group_params_t *params,
                                        ucg_planc_group_h *planc_group)
{
    UCG_CHECK_NULL_INVALID(context, params, planc_group);

    ucg_status_t status;
    ucg_planc_ucx_group_t *ucx_group;

    ucx_group = ucg_calloc(1, sizeof(ucg_planc_ucx_group_t), "ucg planc ucx group");
    if (ucx_group == NULL) {
        return UCG_ERR_NO_MEMORY;
    }

    status = UCG_CLASS_CONSTRUCT(ucg_planc_group_t, &ucx_group->super, params->group);
    if (status != UCG_OK) {
        ucg_error("Failed to init planc_ucx_group->super");
        goto err_free_ucx_group;
    }

    ucx_group->context = (ucg_planc_ucx_context_t *)context;
    for (int i = 0; i < UCG_ALGO_GROUP_TYPE_LAST; ++i) {
        ucx_group->groups[i].super.myrank = UCG_INVALID_RANK;
        ucx_group->groups[i].super.group = params->group;
        ucx_group->groups[i].state = UCG_ALGO_GROUP_STATE_NOT_INIT;
    }

    *planc_group = (ucg_planc_group_h)ucx_group;
    return UCG_OK;

err_free_ucx_group:
    ucg_free(ucx_group);
    return status;
}

void ucg_planc_ucx_group_destroy(ucg_planc_group_h planc_group)
{
    ucg_planc_ucx_group_t *ucx_group = ucg_derived_of(planc_group, ucg_planc_ucx_group_t);
    UCG_CLASS_DESTRUCT(ucg_planc_group_t, &ucx_group->super);
    ucg_free(ucx_group);
    return;
}

ucg_status_t ucg_planc_ucx_create_node_leader_algo_group(ucg_planc_ucx_group_t *ucx_group,
                                                         ucg_vgroup_t *vgroup)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_algo_group_t *algo_group = &ucx_group->groups[UCG_ALGO_GROUP_TYPE_NODE_LEADER];
    if (algo_group->state != UCG_ALGO_GROUP_STATE_NOT_INIT) {
        return algo_group->state == UCG_ALGO_GROUP_STATE_ERROR ? UCG_ERR_NO_MEMORY : UCG_OK;
    }

    ucg_topo_group_t *socket_group = ucg_topo_get_group(vgroup->group->topo,
                                                        UCG_TOPO_GROUP_TYPE_SOCKET);
    ucg_topo_group_t *node_group = ucg_topo_get_group(vgroup->group->topo,
                                                      UCG_TOPO_GROUP_TYPE_NODE);
    if (node_group == NULL || node_group->state == UCG_TOPO_GROUP_STATE_ERROR ||
        socket_group == NULL || socket_group->state == UCG_TOPO_GROUP_STATE_ERROR ||
        node_group->state == UCG_TOPO_GROUP_STATE_DISABLE) {
        return UCG_ERR_UNSUPPORTED;
    }

    int32_t socket_id = ucg_topo_get_location_id(vgroup->group->topo, vgroup->myrank,
                                                 UCG_TOPO_LOC_SOCKET_ID);
    if (socket_id < 0) {
        return UCG_ERR_UNSUPPORTED;
    }
    uint32_t pps = socket_group->super.size;
    int32_t myoffset = socket_group->super.myrank + socket_id * pps;
    uint32_t size = vgroup->size;
    uint32_t ppn = node_group->super.size;
    int32_t num_node = size / ppn;
    ucg_rank_t *ranks = NULL;
    ranks = ucg_malloc(num_node * sizeof(ucg_rank_t), "ucg rabenseifner ranks");
    if (ranks == NULL) {
        goto err;
    }
    ucg_rank_t *offsets = NULL;
    offsets = ucg_calloc(size, sizeof(ucg_rank_t), "ucg rabenseifner offsets");
    if (offsets == NULL) {
        goto err_free_ranks;
    }
    ucg_rank_t *global_ranks = NULL;
    global_ranks = ucg_calloc(size, sizeof(ucg_rank_t), "ucg rabenseifner global_ranks");
    if (global_ranks == NULL) {
        goto err_free_offsets;
    }

    for (int i = 0; i < size; ++i) {
        int32_t node_id = ucg_topo_get_location_id(vgroup->group->topo, i,
                                                   UCG_TOPO_LOC_NODE_ID);
        int32_t socket_id = ucg_topo_get_location_id(vgroup->group->topo, i,
                                                     UCG_TOPO_LOC_SOCKET_ID);
        if (node_id < 0 || socket_id < 0) {
            status = UCG_ERR_UNSUPPORTED;
            goto err_free_global_ranks;
        }
        int inner_offset_idx = node_id * ppn + socket_id;
        ucg_assert(inner_offset_idx < size);
        int inner_offset = offsets[inner_offset_idx]++;
        int outter_offset = node_id * ppn + socket_id * pps;
        ucg_assert(outter_offset + inner_offset < size);
        global_ranks[outter_offset + inner_offset] = i;
    }
    memset(offsets, 0, size * sizeof(ucg_rank_t));

    ucg_rank_t myrank = vgroup->myrank;
    uint32_t vsize = 0;
    for (int j = 0; j < size; ++j) {
        int i = global_ranks[j];
        int32_t node_id = ucg_topo_get_location_id(vgroup->group->topo, i,
                                                   UCG_TOPO_LOC_NODE_ID);
        if (node_id < 0) {
            status = UCG_ERR_UNSUPPORTED;
            goto err_free_global_ranks;
        }
        if (offsets[node_id] == myoffset) {
            ranks[vsize++] = i;
        }
        ++offsets[node_id];
    }
    ucg_rank_t vrank = 0;
    for (int i = 0; i < num_node; ++i) {
        if (ranks[i] == myrank) {
            vrank = i;
            break;
        }
    }
    algo_group->super.myrank = vrank;
    algo_group->super.size = vsize;
    ucg_assert(algo_group->super.myrank < algo_group->super.size);

    if (vsize <= 1) { // Group is meaningless when it has one or less member
        algo_group->state = UCG_ALGO_GROUP_STATE_DISABLE;
    } else {
        algo_group->state = UCG_ALGO_GROUP_STATE_ENABLE;
        status = ucg_rank_map_init_by_array(&algo_group->super.rank_map,
                                            &ranks, vsize, 0);
        if (status != UCG_OK) {
            goto err_free_global_ranks;
        }
    }

err_free_global_ranks:
    ucg_free(global_ranks);
err_free_offsets:
    ucg_free(offsets);
err_free_ranks:
    ucg_free(ranks);
err:
    return status;
}

/* only be aware of nodes, but unaware of sockets */
ucg_status_t ucg_planc_ucx_create_only_node_leader_algo_group(ucg_planc_ucx_group_t *ucx_group,
                                                              ucg_vgroup_t *vgroup)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_algo_group_t *algo_group = &ucx_group->groups[UCG_ALGO_GROUP_TYPE_NODE_LEADER];
    if (algo_group->state != UCG_ALGO_GROUP_STATE_NOT_INIT) {
        return algo_group->state == UCG_ALGO_GROUP_STATE_ERROR ? UCG_ERR_NO_MEMORY : UCG_OK;
    }

    ucg_topo_group_t *node_group = ucg_topo_get_group(vgroup->group->topo,
                                                      UCG_TOPO_GROUP_TYPE_NODE);
    if (node_group == NULL || node_group->state == UCG_TOPO_GROUP_STATE_ERROR ||
        node_group->state == UCG_TOPO_GROUP_STATE_DISABLE) {
        return UCG_ERR_UNSUPPORTED;
    }
    uint32_t size = vgroup->size;
    uint32_t ppn = node_group->super.size;
    int32_t num_nodes = size / ppn;
    int myoffset = -1;
    ucg_rank_t *ranks = NULL;
    ranks = ucg_malloc(num_nodes * sizeof(ucg_rank_t), "ucg na rolling ranks");
    if (ranks == NULL) {
        goto err;
    }
    ucg_rank_t *offsets = NULL;
    offsets = ucg_calloc(size, sizeof(ucg_rank_t), "ucg na rolling offsets");
    if (offsets == NULL) {
        goto err_free_ranks;
    }
    ucg_rank_t *global_ranks = NULL;
    global_ranks = ucg_calloc(size, sizeof(ucg_rank_t), "ucg na rolling global_ranks");
    if (global_ranks == NULL) {
        goto err_free_offsets;
    }

    for (int i = 0; i < size; ++i) {
        int32_t node_id = ucg_topo_get_location_id(vgroup->group->topo, i,
                                                   UCG_TOPO_LOC_NODE_ID);
        if (node_id < 0) {
            status = UCG_ERR_UNSUPPORTED;
            goto err_free_global_ranks;
        }
        if (i == vgroup->myrank) {
            myoffset = offsets[node_id];
        }
        global_ranks[node_id * ppn + offsets[node_id]] = i;
        ++offsets[node_id];
    }
    memset(offsets, 0, size * sizeof(ucg_rank_t));
    ucg_rank_t myrank = vgroup->myrank;
    uint32_t vsize = 0;
    for (int j = 0; j < size; ++j) {
        int i = global_ranks[j];
        int32_t node_id = ucg_topo_get_location_id(vgroup->group->topo, i,
                                                   UCG_TOPO_LOC_NODE_ID);
        if (node_id < 0) {
            return UCG_ERR_UNSUPPORTED;
        }
        if (offsets[node_id] == myoffset) {
            ranks[vsize++] = i;
        }
        ++offsets[node_id];
    }
    ucg_rank_t vrank = 0;
    for (int i = 0; i < num_nodes; ++i) {
        if (ranks[i] == myrank) {
            vrank = i;
            break;
        }
    }

    algo_group->super.myrank = vrank;
    algo_group->super.size = vsize;
    ucg_assert(algo_group->super.myrank < algo_group->super.size);

    if (vsize <= 1) { // Group is meaningless when it has one or less member
        algo_group->state = UCG_ALGO_GROUP_STATE_DISABLE;
    } else {
        algo_group->state = UCG_ALGO_GROUP_STATE_ENABLE;
        status = ucg_rank_map_init_by_array(&algo_group->super.rank_map,
                                            &ranks, vsize, 0);
        if (status != UCG_OK) {
            goto err_free_global_ranks;
        }
    }

err_free_global_ranks:
    ucg_free(global_ranks);
err_free_offsets:
    ucg_free(offsets);
err_free_ranks:
    ucg_free(ranks);
err:
    return status;
}

ucg_status_t ucg_planc_ucx_create_socket_leader_algo_group(ucg_planc_ucx_group_t *ucx_group,
                                                           ucg_vgroup_t *vgroup)
{
    ucg_status_t status = UCG_OK;
    ucg_planc_ucx_algo_group_t *algo_group = &ucx_group->groups[UCG_ALGO_GROUP_TYPE_SOCKET_LEADER];
    if (algo_group->state != UCG_ALGO_GROUP_STATE_NOT_INIT) {
        return algo_group->state == UCG_ALGO_GROUP_STATE_ERROR ? UCG_ERR_NO_MEMORY : UCG_OK;
    }

    ucg_topo_group_t *socket_group = ucg_topo_get_group(vgroup->group->topo,
                                                        UCG_TOPO_GROUP_TYPE_SOCKET);
    ucg_topo_group_t *node_group = ucg_topo_get_group(vgroup->group->topo,
                                                      UCG_TOPO_GROUP_TYPE_NODE);
    if (node_group == NULL || node_group->state == UCG_TOPO_GROUP_STATE_ERROR ||
        socket_group == NULL || socket_group->state == UCG_TOPO_GROUP_STATE_ERROR ||
        socket_group->state == UCG_TOPO_GROUP_STATE_DISABLE) {
        return UCG_ERR_UNSUPPORTED;
    }
    int32_t myoffset = socket_group->super.myrank;
    uint32_t size = vgroup->size;
    uint32_t ppn = node_group->super.size;
    uint32_t pps = socket_group->super.size;
    int32_t num_socket = ppn / pps;
    ucg_rank_t *ranks = NULL;
    ranks = ucg_malloc(num_socket * sizeof(ucg_rank_t), "ucg rabenseifner ranks");
    if (ranks == NULL) {
        goto err;
    }
    ucg_rank_t *offsets = NULL;
    offsets = ucg_calloc(num_socket, sizeof(ucg_rank_t), "ucg rabenseifner offsets");
    if (offsets == NULL) {
        goto err_free_ranks;
    }

    ucg_location_t location;
    ucg_rank_t myrank = vgroup->myrank;
    status = ucg_group_get_location(vgroup->group, myrank, &location);
    if (status != UCG_OK) {
        ucg_error("Failed to get location of rank %d", myrank);
        goto err_free_offsets;
    }
    int32_t mynode_id = location.node_id;
    int32_t mysocket_id = location.socket_id;
    int32_t* map_v_socket_id = ucg_malloc(num_socket * sizeof(int32_t), "ucg rabenseifner v_socket_id");
    if (map_v_socket_id == NULL) {
        goto err_free_offsets;
    }
    for (int i = 0; i < num_socket; i++) {
        map_v_socket_id[i] = -1;
    }
    uint32_t vsize = 0;
    for (int i = 0; i < size; ++i) {
        status = ucg_group_get_location(vgroup->group, i, &location);
        if (status != UCG_OK) {
            ucg_error("Failed to get location of rank %d", i);
            goto err_free_map_v_socket_id;
        }
        if (location.node_id == mynode_id) {
            int v_socket_id = 0;
            for (; v_socket_id < num_socket; v_socket_id++) {
                if (map_v_socket_id[v_socket_id] == -1) {
                    map_v_socket_id[v_socket_id] = location.socket_id;
                }
                if (map_v_socket_id[v_socket_id] == location.socket_id) {
                    break;
                }
            }
            if (v_socket_id >= num_socket) {
                ucg_error("Failed to get v_socket_id of socket %d", location.socket_id);
                goto err_free_map_v_socket_id;
            }
            if (offsets[v_socket_id] == myoffset) {
                ranks[vsize++] = i;
            }
            ++offsets[v_socket_id];
        }
    }
    int v_socket_id = 0;
    for (; v_socket_id < vsize; v_socket_id++) {
        if (map_v_socket_id[v_socket_id] == mysocket_id) {
            break;
        }
    }
    algo_group->super.myrank = v_socket_id;
    algo_group->super.size = vsize;

    if (vsize <= 1) { // Group is meaningless when it has one or less member
        algo_group->state = UCG_ALGO_GROUP_STATE_DISABLE;
    } else {
        algo_group->state = UCG_ALGO_GROUP_STATE_ENABLE;
        status = ucg_rank_map_init_by_array(&algo_group->super.rank_map,
                                            &ranks, vsize, 1);
        if (status != UCG_OK) {
            goto err_free_offsets;
        }
    }

err_free_map_v_socket_id:
    ucg_free(map_v_socket_id);
err_free_offsets:
    ucg_free(offsets);
err_free_ranks:
    ucg_free(ranks);
err:
    return status;
}
