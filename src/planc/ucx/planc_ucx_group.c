/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "planc_ucx_group.h"
#include "planc_ucx_global.h"
#include "util/ucg_malloc.h"
#include "util/ucg_helper.h"
#include "util/ucg_log.h"
#include "core/ucg_def.h"
#include "core/ucg_group.h"

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

    ucg_topo_group_t *topo_group;
    topo_group = ucg_topo_get_group(vgroup->group->topo, UCG_TOPO_GROUP_TYPE_NODE);
    if (topo_group->state == UCG_TOPO_GROUP_STATE_ERROR) {
        return UCG_ERR_UNSUPPORTED;
    }
    int32_t myoffset = topo_group->super.myrank;
    uint32_t size = vgroup->size;
    int32_t node_num = size / topo_group->super.size;
    ucg_rank_t *ranks = NULL;
    ranks = ucg_malloc(node_num * sizeof(ucg_rank_t), "ucg rabenseifner ranks");
    if (ranks == NULL) {
        goto err;
    }
    ucg_rank_t *offsets = NULL;
    offsets = ucg_calloc(size, sizeof(ucg_rank_t), "ucg rabenseifner offsets");
    if (offsets == NULL) {
        goto err_free_ranks;
    }

    ucg_rank_t myrank = vgroup->myrank;
    uint32_t vsize = 0;
    for (int i = 0; i < size; ++i) {
        ucg_location_t location;
        status = ucg_group_get_location(ucx_group->super.super.group, i, &location);
        if (status != UCG_OK) {
            ucg_error("Failed to get location of rank %d", i);
            goto err_free_offsets;
        }
        int32_t node_id = location.node_id;
        if (offsets[node_id] == myoffset) {
            ranks[vsize++] = i;
        }
        ++offsets[node_id];
    }
    ucg_rank_t vrank = 0;
    for (int i = 0; i < node_num; ++i) {
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
            goto err_free_offsets;
        }
    }

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
    if (node_group->state == UCG_TOPO_GROUP_STATE_ERROR ||
        socket_group->state == UCG_TOPO_GROUP_STATE_ERROR) {
        return UCG_ERR_UNSUPPORTED;
    }
    int32_t myoffset = socket_group->super.myrank;
    uint32_t size = vgroup->size;
    uint32_t node_size = node_group->super.size;
    uint32_t socket_size = socket_group->super.size;
    int32_t socket_num = node_size / socket_size;
    ucg_rank_t *ranks = NULL;
    ranks = ucg_malloc(socket_num * sizeof(ucg_rank_t), "ucg rabenseifner ranks");
    if (ranks == NULL) {
        goto err;
    }
    ucg_rank_t *offsets = NULL;
    offsets = ucg_calloc(socket_num + 1, sizeof(ucg_rank_t), "ucg rabenseifner offsets");
    if (offsets == NULL) {
        goto err_free_ranks;
    }

    ucg_location_t location;
    ucg_group_t *group = ucx_group->super.super.group;
    ucg_rank_t myrank = vgroup->myrank;
    status = ucg_group_get_location(group, myrank, &location);
    if (status != UCG_OK) {
        ucg_error("Failed to get location of rank %d", myrank);
        goto err_free_offsets;
    }
    int32_t mynode_id = location.node_id;
    int32_t mysocket_id = location.socket_id;
    uint32_t vsize = 0;
    for (int i = 0; i < size; ++i) {
        status = ucg_group_get_location(group, i, &location);
        if (status != UCG_OK) {
            ucg_error("Failed to get location of rank %d", i);
            goto err_free_offsets;
        }
        if (location.node_id == mynode_id) {
            if (offsets[location.socket_id] == myoffset) {
                ranks[vsize++] = i;
            }
            ++offsets[location.socket_id];
        }
    }
    algo_group->super.myrank = mysocket_id;
    algo_group->super.size = vsize;
    ucg_assert(algo_group->super.myrank < algo_group->super.size);

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

err_free_offsets:
    ucg_free(offsets);
err_free_ranks:
    ucg_free(ranks);
err:
    return status;
}