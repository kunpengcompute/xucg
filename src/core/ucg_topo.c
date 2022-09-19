/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "ucg_topo.h"
#include "ucg_rank_map.h"
#include "ucg_group.h"

#include "util/ucg_malloc.h"
#include "util/ucg_helper.h"
#include "util/ucg_array.h"
#include "util/ucg_log.h"
#include "util/ucg_math.h"

/* Array of ucg rank. */
UCG_ARRAYX_DECLARE(ucg_rank_t);
/* Array for storing IDs (e.g. node id) to filter leader. */
UCG_ARRAY_DECLARE(int32_t);

/* The rank_map is a vgroup rank to group rank mapping table. */
static int32_t ucg_topo_is_self(const ucg_topo_t *topo,
                                const ucg_rank_map_t *rank_map,
                                ucg_rank_t rank)
{
    ucg_rank_t group_rank = ucg_rank_map_eval(rank_map, rank);
    ucg_assert(group_rank != UCG_INVALID_RANK);

    return group_rank == topo->myrank;
}

/* The rank_map is a vgroup rank to group rank mapping table. */
static ucg_status_t ucg_topo_get_location(const ucg_topo_t *topo,
                                          const ucg_rank_map_t *rank_map,
                                          ucg_rank_t rank,
                                          ucg_location_t *location)
{
    ucg_rank_t group_rank = ucg_rank_map_eval(rank_map, rank);
    ucg_assert(group_rank != UCG_INVALID_RANK);

    ucg_status_t status = topo->get_location(topo->group, group_rank, location);
    if(status != UCG_OK) {
        ucg_error("Failed to get location of group rank %d", group_rank);
    }

    return status;
}

static ucg_status_t ucg_topo_group_aux_init_empty(void **aux)
{
    UCG_UNUSED(aux);
    return UCG_OK;
}

static void ucg_topo_group_aux_cleanup_empty(void **aux)
{
    UCG_UNUSED(aux);
    return;
}

static ucg_status_t ucg_topo_group_aux_check_empty(void **aux,
                                                   const ucg_location_t *location)
{
    UCG_UNUSED(aux, location);
    return UCG_OK;
}

static int32_t ucg_topo_group_aux_is_member_empty(void **aux,
                                                  const ucg_topo_t *topo,
                                                  const ucg_rank_map_t *rank_map,
                                                  ucg_rank_t rank,
                                                  const ucg_location_t *location)
{
    UCG_UNUSED(aux, topo, rank_map, rank, location);
    return 1;
}

static ucg_status_t ucg_topo_group_aux_add_member_empty(void **aux,
                                                        const ucg_topo_t *topo,
                                                        const ucg_rank_map_t *rank_map,
                                                        ucg_rank_t rank,
                                                        const ucg_location_t *location)
{
    UCG_UNUSED(aux, topo, rank_map, rank, location);
    return UCG_OK;
}

static ucg_status_t ucg_topo_group_aux_init_leader_filter(void **aux)
{
    UCG_ARRAY_TYPE(int32_t) *filter;
    int capacity = 1022; /* usually enough. 1022 makes array space to be 4KB. */
    ucg_status_t status = UCG_ARRAY_INIT(int32_t, &filter, capacity);
    if (status != UCG_OK) {
        ucg_error("Failed to initialize leader filter");
        return status;
    }
    *aux = filter;
    return UCG_OK;
}

static void ucg_topo_group_aux_cleanup_leader_filter(void **aux)
{
    ucg_free(*aux);
    return;
}

static ucg_status_t ucg_topo_group_aux_check_subnet_id(void **aux, const ucg_location_t *location)
{
    UCG_UNUSED(aux);

    if (!(location->field_mask & UCG_LOCATION_FIELD_SUBNET_ID)) {
        ucg_warn("Rank has no subnet id");
        return UCG_ERR_NOT_FOUND;
    }
    return UCG_OK;
}

static int32_t ucg_topo_group_aux_is_subnet_member(void **aux, const ucg_topo_t *topo,
                                                   const ucg_rank_map_t *rank_map,
                                                   ucg_rank_t rank,
                                                   const ucg_location_t *location)
{
    UCG_UNUSED(aux, rank_map, rank);
    return location->subnet_id == topo->myloc.subnet_id;
}

static ucg_status_t ucg_topo_group_aux_check_node_id(void **aux, const ucg_location_t *location)
{
    UCG_UNUSED(aux);

    if (!(location->field_mask & UCG_LOCATION_FIELD_NODE_ID)) {
        ucg_warn("Rank has no node id");
        return UCG_ERR_NOT_FOUND;
    }
    return UCG_OK;
}

static int32_t ucg_topo_group_aux_is_node_member(void **aux, const ucg_topo_t *topo,
                                                 const ucg_rank_map_t *rank_map,
                                                 ucg_rank_t rank,
                                                 const ucg_location_t *location)
{
    UCG_UNUSED(aux, rank_map, rank);
    return location->node_id == topo->myloc.node_id;
}

static ucg_status_t ucg_topo_group_aux_check_socket_id(void **aux, const ucg_location_t *location)
{
    UCG_UNUSED(aux);

    if (!(location->field_mask & UCG_LOCATION_FIELD_NODE_ID) ||
        !(location->field_mask & UCG_LOCATION_FIELD_SOCKET_ID)) {
        ucg_warn("Rank has no socket id");
        return UCG_ERR_NOT_FOUND;
    }
    return UCG_OK;
}

static int32_t ucg_topo_group_aux_is_socket_member(void **aux, const ucg_topo_t *topo,
                                                   const ucg_rank_map_t *rank_map,
                                                   ucg_rank_t rank,
                                                   const ucg_location_t *location)
{
    UCG_UNUSED(aux, rank_map, rank);
    return location->node_id == topo->myloc.node_id &&
           location->socket_id == topo->myloc.socket_id;
}

static int32_t ucg_topo_group_aux_is_leader(void **aux, int32_t id)
{
    UCG_ARRAY_TYPE(int32_t) *filter = (UCG_ARRAY_TYPE(int32_t)*)(*aux);
    int32_t *criterion = NULL;
    UCG_ARRAY_FOR_EACH(criterion, filter) {
        if (id == *criterion) {
            // Leader exists, this id cannot be a leader.
            return 0;
        }
    }
    return 1;
}

static ucg_status_t ucg_topo_group_aux_add_leader(void **aux, int32_t id)
{
    UCG_ARRAY_TYPE(int32_t) *filter = (UCG_ARRAY_TYPE(int32_t)*)(*aux);
    if (UCG_ARRAY_IS_FULL(filter)) {
        ucg_status_t status = UCG_ARRAY_EXTEND(int32_t, &filter, 1024);
        if (status != UCG_OK) {
            ucg_error("Failed to extend leader filter");
            return status;
        }
        *aux = filter;
    }
    UCG_ARRAY_APPEND(filter, id);
    return UCG_OK;
}

static int32_t ucg_topo_group_aux_is_subnet_leader(void **aux, const ucg_topo_t *topo,
                                                   const ucg_rank_map_t *rank_map,
                                                   ucg_rank_t rank,
                                                   const ucg_location_t *location)
{
    UCG_UNUSED(topo, rank_map, rank);
    return ucg_topo_group_aux_is_leader(aux, location->subnet_id);
}

static ucg_status_t ucg_topo_group_aux_add_subnet_leader(void **aux, const ucg_topo_t *topo,
                                                         const ucg_rank_map_t *rank_map,
                                                         ucg_rank_t rank,
                                                         const ucg_location_t *location)
{
    UCG_UNUSED(topo, rank_map, rank);
    return ucg_topo_group_aux_add_leader(aux, location->subnet_id);
}

static int32_t ucg_topo_group_aux_is_node_leader(void **aux, const ucg_topo_t *topo,
                                                 const ucg_rank_map_t *rank_map,
                                                 ucg_rank_t rank,
                                                 const ucg_location_t *location)
{
    UCG_UNUSED(topo, rank_map, rank);
    return ucg_topo_group_aux_is_leader(aux, location->node_id);
}

static ucg_status_t ucg_topo_group_aux_add_node_leader(void **aux, const ucg_topo_t *topo,
                                                       const ucg_rank_map_t *rank_map,
                                                       ucg_rank_t rank,
                                                       const ucg_location_t *location)
{
    UCG_UNUSED(topo, rank_map, rank);
    return ucg_topo_group_aux_add_leader(aux, location->node_id);
}

static int32_t ucg_topo_group_aux_is_socket_leader(void **aux, const ucg_topo_t *topo,
                                                   const ucg_rank_map_t *rank_map,
                                                   ucg_rank_t rank,
                                                   const ucg_location_t *location)
{
    UCG_UNUSED(topo, rank_map, rank);
    return ucg_topo_group_aux_is_leader(aux, location->socket_id);
}

static ucg_status_t ucg_topo_group_aux_add_socket_leader(void **aux, const ucg_topo_t *topo,
                                                         const ucg_rank_map_t *rank_map,
                                                         ucg_rank_t rank,
                                                         const ucg_location_t *location)
{
    UCG_UNUSED(topo, rank_map, rank);
    return ucg_topo_group_aux_add_leader(aux, location->socket_id);
}

static ucg_topo_group_aux_t ucg_topo_group_aid[] = {
    [UCG_TOPO_GROUP_TYPE_NET] = {
        .init = ucg_topo_group_aux_init_empty,
        .cleanup = ucg_topo_group_aux_cleanup_empty,
        .check = ucg_topo_group_aux_check_empty,
        .is_member = ucg_topo_group_aux_is_member_empty,
        .add_member = ucg_topo_group_aux_add_member_empty,
    },
    [UCG_TOPO_GROUP_TYPE_SUBNET] = {
        .init = ucg_topo_group_aux_init_empty,
        .cleanup = ucg_topo_group_aux_cleanup_empty,
        .check = ucg_topo_group_aux_check_subnet_id,
        .is_member = ucg_topo_group_aux_is_subnet_member,
        .add_member = ucg_topo_group_aux_add_member_empty,
    },
    [UCG_TOPO_GROUP_TYPE_SUBNET_LEADER] = {
        .init = ucg_topo_group_aux_init_leader_filter,
        .cleanup = ucg_topo_group_aux_cleanup_empty,
        .check = ucg_topo_group_aux_check_subnet_id,
        .is_member = ucg_topo_group_aux_is_subnet_leader,
        .add_member = ucg_topo_group_aux_add_subnet_leader,
    },
    [UCG_TOPO_GROUP_TYPE_NODE] = {
        .init = ucg_topo_group_aux_init_empty,
        .cleanup = ucg_topo_group_aux_cleanup_empty,
        .check = ucg_topo_group_aux_check_node_id,
        .is_member = ucg_topo_group_aux_is_node_member,
        .add_member = ucg_topo_group_aux_add_member_empty,
    },
    [UCG_TOPO_GROUP_TYPE_NODE_LEADER] = {
        .init = ucg_topo_group_aux_init_leader_filter,
        .cleanup = ucg_topo_group_aux_cleanup_leader_filter,
        .check = ucg_topo_group_aux_check_node_id,
        .is_member = ucg_topo_group_aux_is_node_leader,
        .add_member = ucg_topo_group_aux_add_node_leader,
    },
    [UCG_TOPO_GROUP_TYPE_SOCKET] = {
        .init = ucg_topo_group_aux_init_empty,
        .cleanup = ucg_topo_group_aux_cleanup_empty,
        .check = ucg_topo_group_aux_check_socket_id,
        .is_member = ucg_topo_group_aux_is_socket_member,
        .add_member = ucg_topo_group_aux_add_member_empty,
    },
    [UCG_TOPO_GROUP_TYPE_SOCKET_LEADER] = {
        .init = ucg_topo_group_aux_init_leader_filter,
        .cleanup = ucg_topo_group_aux_cleanup_leader_filter,
        .check = ucg_topo_group_aux_check_socket_id,
        .is_member = ucg_topo_group_aux_is_socket_leader,
        .add_member = ucg_topo_group_aux_add_socket_leader,
    },
};

/* The rank_map is a vgroup rank to group rank mapping table. Memory allocation
 * failures are recoverable errors, so do not change state of the group. */
static ucg_status_t ucg_topo_create_group(ucg_topo_t *topo,
                                          const ucg_rank_map_t *rank_map,
                                          ucg_topo_group_type_t type)
{
    ucg_assert(topo != NULL);

    ucg_status_t status;
    int32_t size_ranks = 256; /** usually enough */
    ucg_rank_t *ranks = NULL;
    status = UCG_ARRAYX_INIT(ucg_rank_t, &ranks, size_ranks);
    if (status != UCG_OK) {
        ucg_error("Failed to allocate ranks");
        return status;
    }

    ucg_topo_group_aux_t *aid = &ucg_topo_group_aid[type];
    void *aux = NULL;
    status = aid->init(&aux);
    if (status != UCG_OK) {
        ucg_error("Failed to initialize group aid");
        goto err_free_ranks;
    }

    ucg_topo_group_t *group = &topo->groups[type];
    ucg_assert(group->state == UCG_TOPO_GROUP_STATE_NOT_INIT);

    ucg_rank_t vrank = 0;
    uint32_t group_size = rank_map->size;
    for (uint32_t i = 0; i < group_size; ++i) {
        ucg_location_t location;
        status = ucg_topo_get_location(topo, rank_map, i, &location);
        if (ucg_unlikely(status != UCG_OK)) {
            goto err_group;
        }
        // check prerequisites
        status = aid->check(&aux, &location);
        if (ucg_unlikely(status != UCG_OK)) {
            ucg_error("Prerequisites of group(type %d) not met", type);
            goto err_group;
        }

        if (!aid->is_member(&aux, topo, rank_map, i, &location)) {
            continue;
        }
        // Let auxiliary know the members
        status = aid->add_member(&aux, topo, rank_map, i, &location);
        if (status != UCG_OK) {
            ucg_error("Failed to add member to aid");
            goto err_cleanup_aux;
        }
        // generate vgroup rank to group rank mapping table
        if (ucg_unlikely(vrank == size_ranks)) {
            status = UCG_ARRAYX_EXTEND(ucg_rank_t, &ranks, &size_ranks, 128);
            if (status != UCG_OK) {
                ucg_error("Failed to extend ranks");
                goto err_cleanup_aux;
            }
        }
        ranks[vrank] = ucg_rank_map_eval(rank_map, i);
        if (ucg_topo_is_self(topo, rank_map, i)) {
            group->super.myrank = vrank;
        }
        ++vrank;
    }

    group->super.size = vrank;
    if (group->super.myrank == UCG_INVALID_RANK ||
        vrank <= 1) { // Group is meaningless when it has one or less member
        group->state = UCG_TOPO_GROUP_STATE_DISABLE;
        ucg_free(ranks);
    } else {
        group->state = UCG_TOPO_GROUP_STATE_ENABLE;
        status = ucg_rank_map_init_by_array(&group->super.rank_map, &ranks, vrank, 1);
        if (status != UCG_OK) {
            goto err_cleanup_aux;
        }
    }

    aid->cleanup(&aux);
    return UCG_OK;

err_group:
    group->state = UCG_TOPO_GROUP_STATE_ERROR;
err_cleanup_aux:
    aid->cleanup(&aux);
err_free_ranks:
    ucg_free(ranks);

    return status;
}

static void ucg_topo_group_cleanup(ucg_topo_group_t *group)
{
    if (group->state == UCG_TOPO_GROUP_STATE_ENABLE) {
        ucg_rank_map_cleanup(&group->super.rank_map);
    }
    return;
}

static ucg_status_t ucg_topo_create_net_group(ucg_topo_t *topo)
{
    ucg_rank_map_t rank_map;
    rank_map.type = UCG_RANK_MAP_TYPE_FULL;
    rank_map.size = topo->rank_map.size;
    return ucg_topo_create_group(topo, &rank_map, UCG_TOPO_GROUP_TYPE_NET);
}

static ucg_status_t ucg_topo_create_subnet_group(ucg_topo_t *topo)
{
    ucg_rank_map_t rank_map;
    rank_map.type = UCG_RANK_MAP_TYPE_FULL;
    rank_map.size = topo->rank_map.size;
    return ucg_topo_create_group(topo, &rank_map, UCG_TOPO_GROUP_TYPE_SUBNET);
}

static ucg_status_t ucg_topo_create_subnet_leader_group(ucg_topo_t *topo)
{
    ucg_rank_map_t rank_map;
    rank_map.type = UCG_RANK_MAP_TYPE_FULL;
    rank_map.size = topo->rank_map.size;
    return ucg_topo_create_group(topo, &rank_map, UCG_TOPO_GROUP_TYPE_SUBNET_LEADER);
}

static ucg_status_t ucg_topo_create_node_group(ucg_topo_t *topo)
{
    ucg_rank_map_t rank_map;
    rank_map.type = UCG_RANK_MAP_TYPE_FULL;
    rank_map.size = topo->rank_map.size;
    return ucg_topo_create_group(topo, &rank_map, UCG_TOPO_GROUP_TYPE_NODE);
}

static ucg_status_t ucg_topo_create_node_leader_group(ucg_topo_t *topo)
{
    ucg_rank_map_t rank_map;
    rank_map.type = UCG_RANK_MAP_TYPE_FULL;
    rank_map.size = topo->rank_map.size;
    return ucg_topo_create_group(topo, &rank_map, UCG_TOPO_GROUP_TYPE_NODE_LEADER);
}

static ucg_status_t ucg_topo_create_socket_group(ucg_topo_t *topo)
{
    ucg_topo_group_t *node_group = &topo->groups[UCG_TOPO_GROUP_TYPE_NODE];
    ucg_assert(node_group->state == UCG_TOPO_GROUP_STATE_ENABLE);
    return ucg_topo_create_group(topo, &node_group->super.rank_map, UCG_TOPO_GROUP_TYPE_SOCKET);
}

static ucg_status_t ucg_topo_create_socket_leader_group(ucg_topo_t *topo)
{
    ucg_topo_group_t *node_group = &topo->groups[UCG_TOPO_GROUP_TYPE_NODE];
    ucg_assert(node_group->state == UCG_TOPO_GROUP_STATE_ENABLE);
    return ucg_topo_create_group(topo, &node_group->super.rank_map, UCG_TOPO_GROUP_TYPE_SOCKET_LEADER);
}

static ucg_status_t ucg_topo_init_detail(ucg_topo_t *topo)
{
    ucg_status_t status = UCG_OK;
    ucg_group_t *group = topo->group;
    ucg_topo_detail_t *detail = &topo->detail;
    int32_t group_size = group->size;
    ucg_topo_location_t *locations;
    locations = ucg_malloc(group_size * sizeof(ucg_topo_location_t), "topo detail locations");
    if (locations == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    int32_t nnode = 0;
    int32_t nsocket = 0;
    ucg_location_t location;
    for (int i = 0; i < group_size; ++i) {
        status = ucg_group_get_location(group, i, &location);
        if (status != UCG_OK) {
            goto err_free_locations;
        }
        nnode = ucg_max(nnode, location.node_id + 1);
        nsocket = ucg_max(nsocket, location.socket_id + 1);
        locations[i].node_id = location.node_id;
        locations[i].socket_id = location.socket_id;
    }
    detail->nnode = nnode;
    detail->nsocket = nsocket;
    detail->locations = locations;
    return UCG_OK;

err_free_locations:
    ucg_free(locations);
    return status;
}

static void ucg_topo_cleanup_detail(ucg_topo_t *topo)
{
    ucg_topo_detail_t *detail = &topo->detail;
    if (detail->locations != NULL) {
        ucg_free(detail->locations);
        detail->locations = NULL;
    }
    return;
}

static ucg_status_t ucg_topo_calc_ppn(ucg_topo_t *topo)
{
    ucg_topo_detail_t *detail = &topo->detail;
    int32_t nnode = detail->nnode;
    ucg_topo_location_t *locations = detail->locations;
    int32_t *process_cnt = ucg_calloc(nnode, sizeof(int32_t), "topo process cnt");
    if (process_cnt == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    int32_t group_size = topo->group->size;
    for (int i = 0; i < group_size; ++i) {
        ++process_cnt[locations[i].node_id];
    }
    int32_t res = 0;
    for (int i = 0; i < nnode; ++i) {
        if (process_cnt[i] == 0) {
            continue;
        }
        if (res == 0) {
            res = process_cnt[i];
        } else if (process_cnt[i] != res) {
            topo->ppn = UCG_TOPO_PPX_UNBALANCED;
            goto out;
        }
    }
    topo->ppn = res;
out:
    ucg_free(process_cnt);
    return UCG_OK;
}

static ucg_status_t ucg_topo_calc_pps(ucg_topo_t *topo)
{
    ucg_topo_detail_t *detail = &topo->detail;
    int32_t nnode = detail->nnode;
    int32_t nsocket = detail->nsocket;
    ucg_topo_location_t *locations = detail->locations;
    int32_t size = nnode * nsocket;
    int32_t *process_cnt = ucg_calloc(size, sizeof(int32_t), "topo process cnt");
    if (process_cnt == NULL) {
        return UCG_ERR_NO_MEMORY;
    }
    int32_t group_size = topo->group->size;
    for (int i = 0; i < group_size; ++i) {
        ++process_cnt[locations[i].node_id * nsocket + locations[i].socket_id];
    }
    int32_t res = 0;
    for (int i = 0; i < size; ++i) {
        if (process_cnt[i] == 0) {
            continue;
        }
        if (res == 0) {
            res = process_cnt[i];
        } else if (process_cnt[i] != res) {
            topo->pps = UCG_TOPO_PPX_UNBALANCED;
            goto out;
        }
    }
    topo->pps = res;
out:
    ucg_free(process_cnt);
    return UCG_OK;
}

static ucg_status_t ucg_topo_calc_ppx(ucg_topo_t *topo)
{
    ucg_status_t status;
    status = ucg_topo_calc_ppn(topo);
    if (status != UCG_OK) {
        return status;
    }
    status = ucg_topo_calc_pps(topo);
    if (status != UCG_OK) {
        return status;
    }
    return UCG_OK;
}

ucg_status_t ucg_topo_init(const ucg_topo_params_t *params, ucg_topo_t **topo)
{
    UCG_CHECK_NULL_INVALID(params, topo);

    ucg_status_t status = UCG_OK;
    ucg_topo_t *new_topo = ucg_calloc(1, sizeof(ucg_topo_t), "ucg topo");
    if (new_topo == NULL) {
        return UCG_ERR_NO_MEMORY;
    }

    ucg_group_t *group = params->group;
    for (int i = 0; i < UCG_TOPO_GROUP_TYPE_LAST; ++i) {
        new_topo->groups[i].super.myrank = UCG_INVALID_RANK;
        new_topo->groups[i].super.group = group;
        new_topo->groups[i].state = UCG_TOPO_GROUP_STATE_NOT_INIT;
    }
    new_topo->group = group;
    new_topo->myrank = params->myrank;
    status = ucg_rank_map_copy(&new_topo->rank_map, params->rank_map);
    if (status != UCG_OK) {
        goto err_free_topo;
    }

    new_topo->get_location = params->get_location;
    status = new_topo->get_location(new_topo->group, new_topo->myrank, &new_topo->myloc);
    if (status != UCG_OK) {
        goto err_free_rank_map;
    }

    status = ucg_topo_init_detail(new_topo);
    if (status != UCG_OK) {
        goto err_free_rank_map;
    }

    status = ucg_topo_calc_ppx(new_topo);
    if (status != UCG_OK) {
        goto err_free_rank_map;
    }

    ucg_topo_cleanup_detail(new_topo);
    *topo = new_topo;
    return UCG_OK;

err_free_rank_map:
    ucg_rank_map_cleanup(&new_topo->rank_map);
err_free_topo:
    ucg_free(new_topo);
    return status;
}

void ucg_topo_cleanup(ucg_topo_t *topo)
{
    UCG_CHECK_NULL_VOID(topo);

    for (int i = 0; i < UCG_TOPO_GROUP_TYPE_LAST; ++i) {
        ucg_topo_group_cleanup(&topo->groups[i]);
    }
    ucg_free(topo);
    return;
}

ucg_topo_group_t* ucg_topo_get_group(ucg_topo_t *topo, ucg_topo_group_type_t type)
{
    UCG_CHECK_NULL(NULL, topo);
    UCG_CHECK_OUT_RANGE(NULL, type, 0, UCG_TOPO_GROUP_TYPE_LAST);

    ucg_topo_group_t *group = &topo->groups[type];
    if (group->state != UCG_TOPO_GROUP_STATE_NOT_INIT) {
        return group->state == UCG_TOPO_GROUP_STATE_ERROR ? NULL : group;
    }

    ucg_status_t status;
    switch (type) {
        case UCG_TOPO_GROUP_TYPE_NET:
            status = ucg_topo_create_net_group(topo);
            break;
        case UCG_TOPO_GROUP_TYPE_SUBNET:
            status = ucg_topo_create_subnet_group(topo);
            break;
        case UCG_TOPO_GROUP_TYPE_SUBNET_LEADER:
            status = ucg_topo_create_subnet_leader_group(topo);
            break;
        case UCG_TOPO_GROUP_TYPE_NODE:
            status = ucg_topo_create_node_group(topo);
            break;
        case UCG_TOPO_GROUP_TYPE_NODE_LEADER:
            status = ucg_topo_create_node_leader_group(topo);
            break;
        case UCG_TOPO_GROUP_TYPE_SOCKET:
        case UCG_TOPO_GROUP_TYPE_SOCKET_LEADER:
            if (topo->groups[UCG_TOPO_GROUP_TYPE_NODE].state == UCG_TOPO_GROUP_STATE_NOT_INIT) {
                status = ucg_topo_create_node_group(topo);
                if (status != UCG_OK) {
                    break;
                }
            }

            /* Directly return to avoid program interruption caused by assertions taking effect. */
            if (topo->groups[UCG_TOPO_GROUP_TYPE_NODE].state == UCG_TOPO_GROUP_STATE_DISABLE) {
                group->state = UCG_TOPO_GROUP_STATE_DISABLE;
                return group;
            }

            if (type == UCG_TOPO_GROUP_TYPE_SOCKET) {
                status = ucg_topo_create_socket_group(topo);
            } else {
                status = ucg_topo_create_socket_leader_group(topo);
            }

            break;
        default:
            status = UCG_ERR_NOT_FOUND;
            break;
    }

    return status == UCG_OK ? group : NULL;
}